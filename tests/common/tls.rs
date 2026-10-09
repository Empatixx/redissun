use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use std::sync::Mutex;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};

pub struct Authority {
    pub pem: String,
    params: CertificateParams,
    key: KeyPair,
}

pub struct Leaf {
    pub cert: String,
    pub key: String,
}

impl Authority {
    pub fn new(name: &str) -> Authority {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        let pem = params.self_signed(&key).unwrap().pem();
        Authority { pem, params, key }
    }

    pub fn server(&self, names: &[&str]) -> Leaf {
        self.leaf(
            names,
            vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
        )
    }

    pub fn client(&self) -> Leaf {
        self.leaf(
            &["redissun-client"],
            vec![ExtendedKeyUsagePurpose::ClientAuth],
        )
    }

    fn leaf(&self, names: &[&str], usages: Vec<ExtendedKeyUsagePurpose>) -> Leaf {
        let key = KeyPair::generate().unwrap();
        let names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
        let mut params = CertificateParams::new(names).unwrap();
        params.extended_key_usages = usages;
        params
            .distinguished_name
            .push(DnType::CommonName, "redissun");
        let issuer = Issuer::from_params(&self.params, &self.key);
        Leaf {
            cert: params.signed_by(&key, &issuer).unwrap().pem(),
            key: key.serialize_pem(),
        }
    }
}

static STARTED: Mutex<Vec<String>> = Mutex::new(Vec::new());

extern "C" fn remove_containers() {
    let ids = STARTED.lock().map(|ids| ids.clone()).unwrap_or_default();
    for id in ids {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", "-v", &id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

pub struct TlsRedis {
    _container: ContainerAsync<GenericImage>,
    pub port: u16,
}

impl TlsRedis {
    pub async fn start(authority: &Authority, server: &Leaf, auth_clients: bool) -> TlsRedis {
        let file = |path: &str| CopyTargetOptions::new(path).with_mode(0o644);
        let container = GenericImage::new("redis", "7.4")
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_copy_to(file("/tls/ca.crt"), authority.pem.clone().into_bytes())
            .with_copy_to(file("/tls/server.crt"), server.cert.clone().into_bytes())
            .with_copy_to(file("/tls/server.key"), server.key.clone().into_bytes())
            .with_cmd([
                "redis-server",
                "--port",
                "0",
                "--tls-port",
                "6379",
                "--tls-cert-file",
                "/tls/server.crt",
                "--tls-key-file",
                "/tls/server.key",
                "--tls-ca-cert-file",
                "/tls/ca.crt",
                "--tls-auth-clients",
                if auth_clients { "yes" } else { "no" },
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .start()
            .await
            .unwrap();
        let first = {
            let mut started = STARTED.lock().unwrap();
            started.push(container.id().to_string());
            started.len() == 1
        };
        if first {
            unsafe {
                libc::atexit(remove_containers);
            }
        }
        let port = container.get_host_port_ipv4(6379).await.unwrap();
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        TlsRedis {
            _container: container,
            port,
        }
    }

    pub fn url(&self) -> String {
        format!("rediss://127.0.0.1:{}", self.port)
    }
}
