use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::time::{sleep, Instant};

const IMAGE: &str = "redis:7.4";
const MASTER_NAME: &str = "mymaster";

pub struct Topology {
    id: String,
    pub base: u16,
}

impl Drop for Topology {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", "-v", &self.id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Topology {
    pub async fn sentinel() -> Topology {
        let topology = start(6, SENTINEL_SCRIPT).await;
        let sentinel = topology.base + 3;
        topology
            .wait_for("sentinel quorum", || async {
                let master = topology
                    .cli(sentinel, &["SENTINEL", "MASTER", MASTER_NAME])
                    .await;
                let replicas = field(&master, "num-slaves");
                let sentinels = field(&master, "num-other-sentinels");
                replicas.as_deref() == Some("2") && sentinels.as_deref() == Some("2")
            })
            .await;
        topology
    }

    pub async fn cluster() -> Topology {
        let topology = start(6, CLUSTER_SCRIPT).await;
        topology
            .wait_for("cluster state ok", || async {
                let mut ok = true;
                for port in topology.ports() {
                    let info = topology.cli(port, &["CLUSTER", "INFO"]).await;
                    ok &= info.contains("cluster_state:ok");
                    ok &= info.contains("cluster_known_nodes:6");
                }
                ok
            })
            .await;
        topology
    }

    pub fn ports(&self) -> impl Iterator<Item = u16> {
        self.base..self.base + 6
    }

    pub fn sentinel_url(&self) -> String {
        format!(
            "redis-sentinel://127.0.0.1:{}?sentinelServiceName={MASTER_NAME}&node=127.0.0.1:{}&node=127.0.0.1:{}",
            self.base + 3,
            self.base + 4,
            self.base + 5
        )
    }

    pub fn cluster_url(&self) -> String {
        let nodes: std::vec::Vec<String> = self
            .ports()
            .skip(1)
            .map(|port| format!("node=127.0.0.1:{port}"))
            .collect();
        format!(
            "redis-cluster://127.0.0.1:{}?{}",
            self.base,
            nodes.join("&")
        )
    }

    pub async fn cli(&self, port: u16, args: &[&str]) -> String {
        let output = tokio::process::Command::new("docker")
            .args(["exec", &self.id, "redis-cli", "-p", &port.to_string()])
            .args(args)
            .output()
            .await
            .expect("docker exec failed");
        String::from_utf8_lossy(&output.stdout).to_string()
    }

    pub async fn sentinel_master_port(&self) -> u16 {
        let reply = self
            .cli(
                self.base + 3,
                &["SENTINEL", "GET-MASTER-ADDR-BY-NAME", MASTER_NAME],
            )
            .await;
        reply
            .lines()
            .nth(1)
            .and_then(|line| line.trim().parse().ok())
            .expect("sentinel did not report a master")
    }

    pub async fn kill(&self, port: u16) {
        let _ = self.cli(port, &["SHUTDOWN", "NOSAVE"]).await;
    }

    pub async fn sentinel_failover(&self) -> (u16, u16) {
        let old = self.sentinel_master_port().await;
        self.kill(old).await;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let new = self.sentinel_master_port().await;
            if new != old {
                return (old, new);
            }
            assert!(
                Instant::now() < deadline,
                "sentinel never promoted a replica"
            );
            sleep(Duration::from_millis(200)).await;
        }
    }

    pub async fn cluster_master_of(&self, key: &str) -> u16 {
        match self.cluster_master_of_via(self.base, key).await {
            Some(port) => port,
            None => panic!(
                "no master owns the slot of {key}\n{}",
                self.cli(self.base, &["CLUSTER", "NODES"]).await
            ),
        }
    }

    async fn cluster_master_of_via(&self, via: u16, key: &str) -> Option<u16> {
        let slot = self.cli(via, &["CLUSTER", "KEYSLOT", key]).await;
        let slot: u16 = slot.trim().parse().expect("bad CLUSTER KEYSLOT reply");
        let nodes = self.cli(via, &["CLUSTER", "NODES"]).await;
        nodes
            .lines()
            .filter(|line| line.contains("master") && !line.contains("fail"))
            .find(|line| {
                line.split_whitespace().skip(8).any(|range| {
                    let mut bounds = range.split('-');
                    let start: u16 = bounds
                        .next()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(u16::MAX);
                    let end: u16 = bounds.next().and_then(|s| s.parse().ok()).unwrap_or(start);
                    (start..=end).contains(&slot)
                })
            })
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|address| address.split(['@', ':']).nth(1))
            .and_then(|port| port.parse().ok())
    }

    pub async fn cluster_failover_of(&self, key: &str) -> (u16, u16) {
        let old = self.cluster_master_of(key).await;
        self.kill(old).await;
        let alive = self.ports().find(|port| *port != old).unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let info = self.cli(alive, &["CLUSTER", "INFO"]).await;
            if info.contains("cluster_state:ok") {
                if let Some(new) = self.cluster_master_of_via(alive, key).await {
                    if new != old {
                        return (old, new);
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "cluster never promoted a replica"
            );
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_for<F, Fut>(&self, what: &str, mut check: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !check().await {
            if Instant::now() > deadline {
                let logs = Command::new("docker")
                    .args(["exec", &self.id, "sh", "-c", "tail -n 15 /tmp/*.log"])
                    .output()
                    .map(|output| String::from_utf8_lossy(&output.stdout).to_string())
                    .unwrap_or_default();
                panic!("timed out waiting for {what}\n{logs}");
            }
            sleep(Duration::from_millis(200)).await;
        }
    }
}

fn field(reply: &str, name: &str) -> Option<String> {
    let mut lines = reply.lines();
    while let Some(line) = lines.next() {
        if line.trim() == name {
            return lines.next().map(|value| value.trim().to_string());
        }
    }
    None
}

fn free_base(count: u16) -> u16 {
    loop {
        let base = 20_000 + (uuid::Uuid::new_v4().as_u128() % 30_000) as u16;
        let free = (base..base + count).all(|port| TcpListener::bind(("127.0.0.1", port)).is_ok());
        if free {
            return base;
        }
    }
}

async fn start(count: u16, script: &str) -> Topology {
    for _ in 0..5 {
        let base = free_base(count);
        let mut args = vec![
            "run".to_string(),
            "-d".to_string(),
            "--rm".to_string(),
            "-e".to_string(),
            format!("BASE={base}"),
        ];
        for port in base..base + count {
            args.push("-p".into());
            args.push(format!("127.0.0.1:{port}:{port}"));
        }
        args.extend([IMAGE.into(), "sh".into(), "-c".into(), script.into()]);
        let output = tokio::process::Command::new("docker")
            .args(&args)
            .output()
            .await
            .expect("docker is required for topology tests");
        if output.status.success() {
            let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
            return Topology { id, base };
        }
    }
    panic!("could not start a redis topology container");
}

const SENTINEL_SCRIPT: &str = r#"
set -e
M=$BASE
for p in $BASE $((BASE+1)) $((BASE+2)); do
  extra=""
  if [ "$p" != "$M" ]; then extra="--replicaof 127.0.0.1 $M"; fi
  redis-server --port $p --daemonize yes --save "" --appendonly no \
    --replica-announce-ip 127.0.0.1 --logfile /tmp/redis-$p.log $extra
done
for s in $((BASE+3)) $((BASE+4)) $((BASE+5)); do
  cat > /tmp/sentinel-$s.conf <<EOF
port $s
sentinel announce-ip 127.0.0.1
sentinel monitor mymaster 127.0.0.1 $M 2
sentinel down-after-milliseconds mymaster 1000
sentinel failover-timeout mymaster 5000
sentinel parallel-syncs mymaster 1
logfile /tmp/sentinel-$s.log
EOF
  redis-sentinel /tmp/sentinel-$s.conf --daemonize yes
done
exec tail -f /dev/null
"#;

const CLUSTER_SCRIPT: &str = r#"
set -e
nodes=""
for i in 0 1 2 3 4 5; do
  p=$((BASE+i))
  redis-server --port $p --daemonize yes --save "" --appendonly no \
    --cluster-enabled yes --cluster-config-file /tmp/nodes-$p.conf \
    --cluster-node-timeout 2000 --cluster-announce-ip 127.0.0.1 \
    --logfile /tmp/redis-$p.log
  nodes="$nodes 127.0.0.1:$p"
done
for i in 0 1 2 3 4 5; do
  until redis-cli -p $((BASE+i)) ping >/dev/null 2>&1; do sleep 0.1; done
done
redis-cli --cluster create $nodes --cluster-replicas 1 --cluster-yes
exec tail -f /dev/null
"#;
