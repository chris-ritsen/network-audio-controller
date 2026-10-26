use anyhow::{Context, Result, bail, ensure};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{ServerConfig, crypto};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufReader;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::parse_x509_certificate;

#[derive(Clone, Debug)]
pub struct CertificatePaths {
    pub certificate: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug)]
struct Certificate {
    key: Arc<CertifiedKey>,
    names: BTreeSet<String>,
    addresses: BTreeSet<IpAddr>,
    valid_after: i64,
    valid_until: i64,
    fingerprint: String,
}

impl Certificate {
    fn current(&self) -> bool {
        let now = chrono::Utc::now().timestamp();
        self.valid_after <= now && now < self.valid_until
    }

    fn covers(&self, name: &str) -> bool {
        let name = name.trim_end_matches('.').to_ascii_lowercase();

        if self.names.contains(&name) {
            return true;
        }

        name.split_once('.').is_some_and(|(label, parent)| {
            !label.is_empty() && parent.contains('.') && self.names.contains(&format!("*.{parent}"))
        })
    }
}

#[derive(Debug)]
struct Certificates {
    primary: Certificate,
    named: Option<Certificate>,
    primary_error: Option<String>,
    named_error: Option<String>,
}

#[derive(Debug)]
struct Resolver {
    primary: CertificatePaths,
    named: Option<CertificatePaths>,
    certificates: RwLock<Certificates>,
    fingerprints: watch::Sender<String>,
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let certificates = match self.certificates.read() {
            Ok(certificates) => certificates,
            Err(error) => {
                tracing::error!(%error, "TLS certificate lock poisoned");
                return None;
            }
        };

        if let Some(name) = hello.server_name()
            && let Some(named) = &certificates.named
            && named.covers(name)
        {
            return named.current().then(|| Arc::clone(&named.key));
        }

        certificates
            .primary
            .current()
            .then(|| Arc::clone(&certificates.primary.key))
    }
}

impl Resolver {
    fn reload(&self) {
        let primary = load_certificate(&self.primary, false);
        let named = self
            .named
            .as_ref()
            .map(|paths| load_certificate(paths, true));
        let mut certificates = match self.certificates.write() {
            Ok(certificates) => certificates,
            Err(error) => {
                tracing::error!(%error, "TLS certificate lock poisoned during reload");
                return;
            }
        };

        match primary {
            Ok(primary) => {
                self.fingerprints.send_if_modified(|fingerprint| {
                    if *fingerprint == primary.fingerprint {
                        return false;
                    }

                    fingerprint.clone_from(&primary.fingerprint);
                    true
                });
                certificates.primary = primary;
                certificates.primary_error = None;
            }
            Err(error) => {
                tracing::error!(%error, "TLS identity reload failed; retaining previous identity until its expiry");
                certificates.primary_error = Some(error.to_string());
            }
        }

        match named {
            Some(Ok(named)) => {
                certificates.named = Some(named);
                certificates.named_error = None;
            }
            Some(Err(error)) => {
                tracing::error!(%error, "named TLS identity reload failed; retaining previous identity until its expiry");
                certificates.named_error = Some(error.to_string());
            }
            None => {}
        }
    }
}

pub struct TlsIdentity {
    resolver: Arc<Resolver>,
    configuration: Arc<ServerConfig>,
    _watcher: RecommendedWatcher,
}

impl TlsIdentity {
    pub fn open(primary: CertificatePaths, named: Option<CertificatePaths>) -> Result<Self> {
        let primary_certificate = load_certificate(&primary, false)?;
        let (named_certificate, named_error) =
            match named.as_ref().map(|paths| load_certificate(paths, true)) {
                Some(Ok(certificate)) => (Some(certificate), None),
                Some(Err(error)) => {
                    tracing::error!(%error, "named TLS identity is unavailable");
                    (None, Some(error.to_string()))
                }
                None => (None, None),
            };
        let mut watched_files = BTreeSet::new();

        for paths in std::iter::once(&primary).chain(named.iter()) {
            for path in [&paths.certificate, &paths.key] {
                watched_files.insert(path.clone());

                if let Ok(canonical) = path.canonicalize() {
                    watched_files.insert(canonical);
                }
            }
        }

        let (fingerprints, _) = watch::channel(primary_certificate.fingerprint.clone());
        let resolver = Arc::new(Resolver {
            primary,
            named,
            certificates: RwLock::new(Certificates {
                primary: primary_certificate,
                named: named_certificate,
                primary_error: None,
                named_error,
            }),
            fingerprints,
        });
        let mut configuration =
            ServerConfig::builder_with_provider(Arc::new(crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()?
                .with_no_client_auth()
                .with_cert_resolver(resolver.clone());
        configuration.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let weak = Arc::downgrade(&resolver);
        let directories: BTreeSet<PathBuf> = watched_files
            .iter()
            .filter_map(|path| path.parent().map(Path::to_owned))
            .collect();
        let event_directories = directories.clone();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                let Some(resolver) = weak.upgrade() else {
                    return;
                };

                match event {
                    Ok(event)
                        if !matches!(event.kind, EventKind::Access(_))
                            && (event.need_rescan()
                                || event.paths.iter().any(|path| {
                                    watched_files.contains(path) || event_directories.contains(path)
                                })) =>
                    {
                        resolver.reload()
                    }
                    Err(error) => tracing::error!(%error, "TLS certificate watch failed"),
                    _ => {}
                }
            })?;

        for directory in directories {
            watcher
                .watch(&directory, RecursiveMode::NonRecursive)
                .with_context(|| format!("watch TLS identity directory {}", directory.display()))?;
        }

        resolver.reload();
        Ok(Self {
            resolver,
            configuration: Arc::new(configuration),
            _watcher: watcher,
        })
    }

    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(Arc::clone(&self.configuration))
    }

    pub fn fingerprint(&self) -> Result<String> {
        let certificates = self
            .resolver
            .certificates
            .read()
            .map_err(|_| anyhow::anyhow!("TLS certificate lock poisoned"))?;
        Ok(certificates.primary.fingerprint.clone())
    }

    pub fn subscribe_fingerprint(&self) -> watch::Receiver<String> {
        self.resolver.fingerprints.subscribe()
    }

    pub fn status(&self) -> Result<Value> {
        let certificates = self
            .resolver
            .certificates
            .read()
            .map_err(|_| anyhow::anyhow!("TLS certificate lock poisoned"))?;
        let named = self.resolver.named.as_ref().map(|paths| {
            let mut status = json!({"certificate": paths.certificate, "key": paths.key});

            if let Some(certificate) = &certificates.named {
                status["names"] = json!(certificate.names);
                status["valid_until"] = json!(
                    chrono::DateTime::from_timestamp(certificate.valid_until, 0)
                        .map(|time| time.to_rfc3339())
                );
            }

            if let Some(error) = &certificates.named_error {
                status["error"] = json!(error);
            }

            status
        });

        Ok(json!({
            "tls_sha256": certificates.primary.fingerprint,
            "current": certificates.primary.current(),
            "error": certificates.primary_error,
            "named": named,
        }))
    }
}

fn load_certificate(paths: &CertificatePaths, require_names: bool) -> Result<Certificate> {
    let mut certificate_reader = BufReader::new(
        File::open(&paths.certificate)
            .with_context(|| format!("open TLS certificate {}", paths.certificate.display()))?,
    );
    let chain: Vec<_> =
        rustls_pemfile::certs(&mut certificate_reader).collect::<std::io::Result<_>>()?;
    let leaf = chain
        .first()
        .context("TLS certificate file contains no certificates")?;
    let (_, parsed) = parse_x509_certificate(leaf.as_ref())
        .map_err(|error| anyhow::anyhow!("parse TLS certificate: {error}"))?;
    let names: BTreeSet<String> = parsed
        .subject_alternative_name()?
        .map(|extension| {
            extension
                .value
                .general_names
                .iter()
                .filter_map(|name| match name {
                    GeneralName::DNSName(name) => {
                        Some(name.trim_end_matches('.').to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    let addresses = parsed
        .subject_alternative_name()?
        .map(|extension| {
            extension
                .value
                .general_names
                .iter()
                .filter_map(|name| match name {
                    GeneralName::IPAddress(bytes) => match bytes.len() {
                        4 => <[u8; 4]>::try_from(*bytes)
                            .ok()
                            .map(|bytes| IpAddr::V4(bytes.into())),
                        16 => <[u8; 16]>::try_from(*bytes)
                            .ok()
                            .map(|bytes| IpAddr::V6(bytes.into())),
                        _ => None,
                    },
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    ensure!(
        !require_names || !names.is_empty(),
        "named TLS certificate has no DNS names"
    );
    let valid_after = parsed.validity().not_before.timestamp();
    let valid_until = parsed.validity().not_after.timestamp();
    let fingerprint = Sha256::digest(leaf.as_ref())
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":");
    let mut key_reader = BufReader::new(
        File::open(&paths.key)
            .with_context(|| format!("open TLS private key {}", paths.key.display()))?,
    );
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .context("TLS key file contains no supported private key")?;
    let key = CertifiedKey::from_der(chain, key, &crypto::ring::default_provider())?;
    let certificate = Certificate {
        key: Arc::new(key),
        names,
        addresses,
        valid_after,
        valid_until,
        fingerprint,
    };

    if !certificate.current() {
        bail!(
            "TLS certificate is not currently valid: {}",
            paths.certificate.display()
        );
    }

    Ok(certificate)
}

pub(super) fn renewal_deadline(
    path: &Path,
    names: &BTreeSet<String>,
    addresses: &BTreeSet<IpAddr>,
    renewal_window: i64,
) -> Result<Option<i64>> {
    let certificate = load_certificate(
        &CertificatePaths {
            certificate: path.to_owned(),
            key: path.to_owned(),
        },
        false,
    )?;
    let deadline = certificate.valid_until - renewal_window;
    Ok((names.is_subset(&certificate.names)
        && addresses
            .iter()
            .filter(|address| address.is_ipv4())
            .all(|address| certificate.addresses.contains(address))
        && chrono::Utc::now().timestamp() < deadline)
        .then_some(deadline))
}
