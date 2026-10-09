#![cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]

mod common;

use common::tls::{Authority, Leaf, TlsRedis};
use common::topology::Topology;
use common::unique;
use redissun::{Client, ClientBuilder, Error, Object, TlsVerification};
use std::time::Duration;
use tokio::sync::OnceCell;

struct Servers {
    authority: Authority,
    client: Leaf,
    one_way: TlsRedis,
    mutual: TlsRedis,
}

static SERVERS: OnceCell<Servers> = OnceCell::const_new();

async fn servers() -> &'static Servers {
    SERVERS
        .get_or_init(|| async {
            let authority = Authority::new("redissun test CA");
            let by_ip = authority.server(&["127.0.0.1"]);
            let by_name = authority.server(&["redis.test"]);
            let client = authority.client();
            let (one_way, mutual) = tokio::join!(
                TlsRedis::start(&authority, &by_ip, false),
                TlsRedis::start(&authority, &by_name, true),
            );
            Servers {
                authority,
                client,
                one_way,
                mutual,
            }
        })
        .await
}

fn builder(url: String) -> ClientBuilder {
    Client::builder()
        .url(url)
        .connect_timeout(Duration::from_secs(5))
}

async fn connect(builder: ClientBuilder) -> redissun::Result<Client> {
    let mut last = None;
    for _ in 0..20 {
        match builder.clone().build().await {
            Ok(client) => return Ok(client),
            Err(error @ Error::Config(_)) => return Err(error),
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last.unwrap())
}

fn rejected(attempt: redissun::Result<Client>, reason: &str) {
    match attempt {
        Ok(_) => panic!("connected although the TLS check should fail with {reason}"),
        Err(error) => assert!(error.to_string().contains(reason), "{error}"),
    }
}

async fn works(client: &Client) {
    let bucket = client.bucket::<String>(unique("tls"));
    bucket.set("secret").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("secret"));

    let lock = client.lock(unique("tls-lock"));
    let guard = lock.lock().await.unwrap();
    let waiter = {
        let lock = lock.clone();
        tokio::spawn(async move { lock.lock().await.unwrap().unlock().await.unwrap() })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    guard.unlock().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_custom_ca_is_trusted() {
    let servers = servers().await;
    let client = connect(builder(servers.one_way.url()).tls_ca_pem(servers.authority.pem.clone()))
        .await
        .unwrap();
    works(&client).await;
}

#[tokio::test]
async fn the_system_roots_do_not_trust_a_private_ca() {
    let servers = servers().await;
    let attempt = builder(servers.one_way.url()).build().await;
    rejected(attempt, "UnknownIssuer");
}

#[tokio::test]
async fn a_certificate_from_another_ca_is_rejected() {
    let servers = servers().await;
    let other = Authority::new("another CA");
    let attempt = builder(servers.one_way.url())
        .tls_ca_pem(other.pem.clone())
        .build()
        .await;
    rejected(attempt, "UnknownIssuer");
}

#[tokio::test]
async fn verification_none_accepts_any_certificate() {
    let servers = servers().await;
    let client = connect(builder(servers.one_way.url()).tls_verification(TlsVerification::None))
        .await
        .unwrap();
    works(&client).await;
}

#[tokio::test]
async fn mutual_tls_needs_the_client_certificate() {
    let servers = servers().await;
    let without = builder(servers.mutual.url())
        .tls_ca_pem(servers.authority.pem.clone())
        .tls_verification(TlsVerification::CaOnly)
        .build()
        .await;
    rejected(without, "CertificateRequired");

    let client = connect(
        builder(servers.mutual.url())
            .tls_ca_pem(servers.authority.pem.clone())
            .tls_verification(TlsVerification::CaOnly)
            .tls_client_auth_pem(servers.client.cert.clone(), servers.client.key.clone()),
    )
    .await
    .unwrap();
    works(&client).await;
}

#[tokio::test]
async fn strict_verification_checks_the_host_name() {
    let servers = servers().await;
    let attempt = builder(servers.mutual.url())
        .tls_ca_pem(servers.authority.pem.clone())
        .tls_client_auth_pem(servers.client.cert.clone(), servers.client.key.clone())
        .build()
        .await;
    rejected(attempt, "NotValidForName");
}

#[tokio::test]
async fn certificates_can_be_read_from_files() {
    let servers = servers().await;
    let directory = std::env::temp_dir().join(unique("redissun-tls").replace(':', "-"));
    std::fs::create_dir_all(&directory).unwrap();
    let ca = directory.join("ca.pem");
    let cert = directory.join("client.pem");
    let key = directory.join("client.key");
    std::fs::write(&ca, &servers.authority.pem).unwrap();
    std::fs::write(&cert, &servers.client.cert).unwrap();
    std::fs::write(&key, &servers.client.key).unwrap();
    let client = connect(
        builder(servers.mutual.url())
            .tls_ca_file(&ca)
            .tls_verification(TlsVerification::CaOnly)
            .tls_client_auth_files(&cert, &key),
    )
    .await;
    std::fs::remove_dir_all(&directory).unwrap();
    works(&client.unwrap()).await;
}

#[tokio::test]
async fn a_missing_file_is_a_config_error() {
    let attempt = builder("rediss://127.0.0.1:1".into())
        .tls_ca_file("/does/not/exist.pem")
        .build()
        .await;
    assert!(matches!(attempt, Err(Error::Config(_))));
}

#[tokio::test]
async fn tls_settings_need_a_rediss_url() {
    let attempt = builder("redis://127.0.0.1:1".into())
        .tls_verification(TlsVerification::CaOnly)
        .build()
        .await;
    assert!(matches!(attempt, Err(Error::Config(_))));
}

#[tokio::test]
#[ignore = "starts a sentinel topology in docker; run with --ignored"]
async fn objects_work_through_sentinel_over_tls() {
    let authority = Authority::new("sentinel CA");
    let topology = Topology::sentinel_tls(&authority, &authority.server(&["127.0.0.1"])).await;
    let client = connect(
        builder(topology.sentinel_url().replacen("redis", "rediss", 1))
            .tls_ca_pem(authority.pem.clone()),
    )
    .await
    .unwrap();
    works(&client).await;
}

#[tokio::test]
#[ignore = "starts a cluster topology in docker; run with --ignored"]
async fn objects_work_through_cluster_over_tls() {
    let authority = Authority::new("cluster CA");
    let topology = Topology::cluster_tls(&authority, &authority.server(&["127.0.0.1"])).await;
    let client = connect(
        builder(topology.cluster_url().replacen("redis", "rediss", 1))
            .tls_ca_pem(authority.pem.clone()),
    )
    .await
    .unwrap();
    works(&client).await;
    let map = client.hash_map::<String, String>(unique("{tls}map"));
    map.insert("a", "b").await.unwrap();
    assert!(map.exists().await.unwrap());
}
