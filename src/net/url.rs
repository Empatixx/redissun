//! The connection URL: `redis://`, `rediss://`, `redis-sentinel://` and `redis-cluster://`, with or without TLS.

use crate::error::{Error, Result};
use crate::net::stream::Address;

const DEFAULT_PORT: u16 = 6379;

/// What kind of deployment a URL names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Single,
    /// Sentinels that know the master of `service`.
    Sentinel {
        service: String,
    },
    Cluster,
}

/// A parsed connection URL.
#[derive(Clone, Debug)]
pub(crate) struct Target {
    pub(crate) kind: Kind,
    /// The server, the sentinels, or the cluster's seed nodes: the URL's host first, then each `node=`.
    pub(crate) nodes: Vec<Address>,
    pub(crate) tls: bool,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) database: Option<u8>,
}

fn config(message: impl Into<String>) -> Error {
    Error::Config(message.into())
}

fn decode(text: &str) -> Result<String> {
    url::form_urlencoded::parse(format!("x={text}").as_bytes())
        .next()
        .map(|(_, value)| value.into_owned())
        .ok_or_else(|| config("bad percent encoding in the URL"))
}

fn node(text: &str) -> Result<Address> {
    let (host, port) = match text.rsplit_once(':') {
        Some((host, port)) => (
            host,
            port.parse()
                .map_err(|_| config(format!("bad port in node {text}")))?,
        ),
        None => (text, DEFAULT_PORT),
    };
    Ok(Address::new(host.trim_matches(['[', ']']), port))
}

/// Parses `redis://`, `rediss://`, `redis-sentinel://`, `rediss-sentinel://`, `redis-cluster://` and `rediss-cluster://` URLs.
pub(crate) fn parse(text: &str) -> Result<Target> {
    let url = url::Url::parse(text).map_err(|error| config(format!("invalid URL: {error}")))?;
    let (tls, kind) = match url.scheme() {
        "redis" => (false, Kind::Single),
        "rediss" => (true, Kind::Single),
        "redis-sentinel" => (
            false,
            Kind::Sentinel {
                service: String::new(),
            },
        ),
        "rediss-sentinel" => (
            true,
            Kind::Sentinel {
                service: String::new(),
            },
        ),
        "redis-cluster" => (false, Kind::Cluster),
        "rediss-cluster" => (true, Kind::Cluster),
        other => return Err(config(format!("unsupported URL scheme {other}"))),
    };
    let host = url
        .host_str()
        .ok_or_else(|| config("the URL has no host"))?
        .trim_matches(['[', ']'])
        .to_string();
    let mut nodes = vec![Address::new(host, url.port().unwrap_or(DEFAULT_PORT))];
    let mut service = None;
    for (name, value) in url.query_pairs() {
        match name.as_ref() {
            "node" => nodes.push(node(&value)?),
            "sentinelServiceName" => service = Some(value.into_owned()),
            _ => {}
        }
    }
    let kind = match kind {
        Kind::Sentinel { .. } => Kind::Sentinel {
            service: service.ok_or_else(|| config("a Sentinel URL needs sentinelServiceName"))?,
        },
        other => other,
    };
    let username = match url.username() {
        "" => None,
        user => Some(decode(user)?),
    };
    let password = url.password().map(decode).transpose()?;
    let database = match url.path().trim_start_matches('/') {
        "" => None,
        db => Some(
            db.parse()
                .map_err(|_| config(format!("bad database {db} in the URL")))?,
        ),
    };
    Ok(Target {
        kind,
        nodes,
        tls,
        username,
        password,
        database,
    })
}

#[cfg(test)]
mod tests {
    use super::{parse, Kind, Target};
    use crate::error::Error;
    use crate::net::stream::Address;

    fn parsed(url: &str) -> Target {
        parse(url).unwrap()
    }

    #[test]
    fn a_single_server_url_gives_its_host_port_credentials_and_database() {
        let target = parsed("redis://app:secret@redis.example.com:6380/3");
        assert_eq!(target.kind, Kind::Single);
        assert_eq!(target.nodes, vec![Address::new("redis.example.com", 6380)]);
        assert_eq!(target.username.as_deref(), Some("app"));
        assert_eq!(target.password.as_deref(), Some("secret"));
        assert_eq!(target.database, Some(3));
        assert!(!target.tls);
    }

    #[test]
    fn the_port_defaults_to_6379_and_a_password_alone_has_no_user() {
        let target = parsed("rediss://:secret@localhost");
        assert_eq!(target.nodes, vec![Address::new("localhost", 6379)]);
        assert_eq!(target.username, None);
        assert_eq!(target.password.as_deref(), Some("secret"));
        assert!(target.tls);
    }

    #[test]
    fn a_sentinel_url_names_the_service_and_every_sentinel() {
        let target = parsed("redis-sentinel://127.0.0.1:26379?sentinelServiceName=mymaster&node=127.0.0.1:26380&node=127.0.0.1:26381");
        assert_eq!(
            target.kind,
            Kind::Sentinel {
                service: "mymaster".into()
            }
        );
        assert_eq!(
            target.nodes,
            vec![
                Address::new("127.0.0.1", 26379),
                Address::new("127.0.0.1", 26380),
                Address::new("127.0.0.1", 26381)
            ]
        );
    }

    #[test]
    fn a_cluster_url_lists_its_seeds_and_may_use_tls() {
        let target = parsed("rediss-cluster://127.0.0.1:7000?node=127.0.0.1:7001");
        assert_eq!(target.kind, Kind::Cluster);
        assert_eq!(
            target.nodes,
            vec![
                Address::new("127.0.0.1", 7000),
                Address::new("127.0.0.1", 7001)
            ]
        );
        assert!(target.tls);
    }

    #[test]
    fn percent_encoded_credentials_are_decoded() {
        let target = parsed("redis://us%40er:p%3Ass@host:6379");
        assert_eq!(target.username.as_deref(), Some("us@er"));
        assert_eq!(target.password.as_deref(), Some("p:ss"));
    }

    #[test]
    fn bad_urls_are_config_errors() {
        for url in [
            "not a url",
            "http://host:6379",
            "redis-sentinel://host:26379",
            "redis://host:6379/x",
        ] {
            assert!(matches!(parse(url), Err(Error::Config(_))), "{url}");
        }
    }
}
