use crate::error::{Error, Result};
use fred::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use fred::rustls::client::verify_server_cert_signed_by_trust_anchor;
use fred::rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use fred::rustls::pki_types::pem::PemObject;
use fred::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use fred::rustls::server::ParsedCertificate;
use fred::rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use fred::types::config::{TlsConfig, TlsConnector, TlsHostMapping};
use std::path::PathBuf;
use std::sync::Arc;

/// How the server's TLS certificate is checked, like Redisson's `SslVerificationMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum TlsVerification {
    /// The certificate must be signed by a trusted CA and name the host the client connects to. The default, like Redisson's `STRICT`.
    #[default]
    Strict,
    /// The certificate must be signed by a trusted CA; its host name is not checked, like Redisson's `CA_ONLY`. Useful when Sentinel or Cluster report IP addresses that the certificate does not name.
    CaOnly,
    /// Any certificate is accepted, like Redisson's `NONE`. The connection is encrypted but not protected against a man in the middle. Only for tests.
    None,
}

#[derive(Clone)]
pub(crate) enum Pem {
    Bytes(Vec<u8>),
    File(PathBuf),
}

impl Pem {
    fn read(&self) -> Result<Vec<u8>> {
        match self {
            Pem::Bytes(bytes) => Ok(bytes.clone()),
            Pem::File(path) => std::fs::read(path)
                .map_err(|e| Error::Config(format!("cannot read {}: {e}", path.display()))),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct TlsSettings {
    pub(crate) verification: TlsVerification,
    pub(crate) ca: Option<Pem>,
    pub(crate) client_auth: Option<(Pem, Pem)>,
}

impl TlsSettings {
    pub(crate) fn is_default(&self) -> bool {
        self.verification == TlsVerification::Strict
            && self.ca.is_none()
            && self.client_auth.is_none()
    }

    pub(crate) fn config(&self) -> Result<TlsConfig> {
        let provider = provider();
        let builder = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(tls_error)?;
        let builder = match self.verification {
            TlsVerification::Strict => builder.with_root_certificates(self.roots()?),
            TlsVerification::CaOnly => {
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(Verifier {
                        roots: Some(self.roots()?),
                        provider,
                    }))
            }
            TlsVerification::None => {
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(Verifier {
                        roots: None,
                        provider,
                    }))
            }
        };
        let config = match &self.client_auth {
            Some((chain, key)) => {
                let chain = certificates(&chain.read()?)?;
                let key = PrivateKeyDer::from_pem_slice(&key.read()?)
                    .map_err(|e| Error::Config(format!("invalid TLS client key: {e}")))?;
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(tls_error)?
            }
            None => builder.with_no_client_auth(),
        };
        Ok(TlsConfig {
            connector: TlsConnector::from(config),
            hostnames: TlsHostMapping::None,
        })
    }

    fn roots(&self) -> Result<RootCertStore> {
        let mut roots = RootCertStore::empty();
        match &self.ca {
            Some(ca) => {
                for certificate in certificates(&ca.read()?)? {
                    roots.add(certificate).map_err(tls_error)?;
                }
            }
            None => {
                let native = fred::rustls_native_certs::load_native_certs();
                roots.add_parsable_certificates(native.certs);
            }
        }
        if roots.is_empty() {
            return Err(Error::Config("no trusted TLS certificate was found".into()));
        }
        Ok(roots)
    }
}

fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Config(format!("invalid PEM certificate: {e}")))?;
    if certificates.is_empty() {
        return Err(Error::Config("the PEM data holds no certificate".into()));
    }
    Ok(certificates)
}

fn tls_error(error: impl std::fmt::Display) -> Error {
    Error::Config(format!("TLS: {error}"))
}

fn provider() -> Arc<CryptoProvider> {
    if let Some(installed) = CryptoProvider::get_default() {
        return installed.clone();
    }
    #[cfg(feature = "tls-rustls-aws-lc")]
    return Arc::new(fred::rustls::crypto::aws_lc_rs::default_provider());
    #[cfg(not(feature = "tls-rustls-aws-lc"))]
    Arc::new(fred::rustls::crypto::ring::default_provider())
}

#[derive(Debug)]
struct Verifier {
    roots: Option<RootCertStore>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, fred::rustls::Error> {
        if let Some(roots) = &self.roots {
            let certificate = ParsedCertificate::try_from(end_entity)?;
            verify_server_cert_signed_by_trust_anchor(
                &certificate,
                roots,
                intermediates,
                now,
                self.provider.signature_verification_algorithms.all,
            )?;
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, fred::rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, fred::rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
