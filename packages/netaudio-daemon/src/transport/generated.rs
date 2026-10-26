use super::tls;
use crate::network::NetworkSnapshot;
use anyhow::{Context, Result};
use fs4::FileExt;
use rcgen::{CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const CERTIFICATE_LIFETIME_DAYS: i64 = 365;
const RENEWAL_WINDOW_SECONDS: i64 = 30 * 24 * 60 * 60;
const CLOCK_ALLOWANCE_MINUTES: i64 = 5;

fn required_names() -> Result<BTreeSet<String>> {
    let hostname = hostname::get()?
        .into_string()
        .map_err(|_| anyhow::anyhow!("hostname is not UTF-8"))?;
    let hostname = hostname.trim_end_matches('.');
    [
        "localhost".to_owned(),
        hostname.to_owned(),
        format!("{}.local", hostname.trim_end_matches(".local")),
    ]
    .into_iter()
    .map(|name| match url::Host::parse(&name)? {
        url::Host::Domain(name) => Ok(name.to_ascii_lowercase()),
        _ => anyhow::bail!("hostname is not a DNS name"),
    })
    .collect()
}

pub fn ensure(path: &Path, network: &NetworkSnapshot) -> Result<i64> {
    let parent = path
        .parent()
        .context("TLS identity has no parent directory")?;
    let mut directory = fs::DirBuilder::new();
    directory.recursive(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        directory.mode(0o700);
    }

    directory.create(parent)?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let lock = options.open(path.with_extension("lock"))?;
    FileExt::try_lock(&lock).context("generated TLS identity is being updated")?;
    let names = required_names()?;
    let mut addresses = network.addresses();
    addresses.extend([
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(Ipv6Addr::LOCALHOST),
    ]);

    match tls::renewal_deadline(path, &names, &addresses, RENEWAL_WINDOW_SECONDS) {
        Ok(Some(deadline)) => return Ok(deadline),
        Ok(None) => tracing::info!("renewing generated TLS identity coverage"),
        Err(error) if path.exists() => {
            tracing::warn!(%error, "replacing invalid generated TLS identity");
        }
        Err(_) => {}
    }

    let alternatives = names
        .into_iter()
        .chain(addresses.into_iter().map(|address| address.to_string()))
        .collect::<Vec<_>>();
    let now = OffsetDateTime::now_utc();
    let mut parameters = CertificateParams::new(alternatives)?;
    parameters.distinguished_name = DistinguishedName::new();
    parameters
        .distinguished_name
        .push(DnType::CommonName, "NetAudio");
    parameters.not_before = now - time::Duration::minutes(CLOCK_ALLOWANCE_MINUTES);
    parameters.not_after = now + time::Duration::days(CERTIFICATE_LIFETIME_DAYS);
    parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let deadline = parameters.not_after.unix_timestamp() - RENEWAL_WINDOW_SECONDS;
    let key = KeyPair::generate()?;
    let certificate = parameters.self_signed(&key)?;
    let mut output = tempfile::NamedTempFile::new_in(parent)?;
    output.write_all(certificate.pem().as_bytes())?;
    output.write_all(key.serialize_pem().as_bytes())?;
    output.as_file().sync_all()?;
    output.persist(path)?;

    #[cfg(unix)]
    File::open(parent)?.sync_all()?;

    Ok(deadline)
}

pub async fn maintain(
    path: PathBuf,
    mut network: watch::Receiver<NetworkSnapshot>,
    cancellation: CancellationToken,
) -> Result<()> {
    loop {
        let path = path.clone();
        let snapshot = network.borrow_and_update().clone();
        let renewal = tokio::task::spawn_blocking(move || ensure(&path, &snapshot)).await??;
        let remaining = renewal.saturating_sub(chrono::Utc::now().timestamp());
        let delay = Duration::from_secs(u64::try_from(remaining).unwrap_or_default());

        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
            changed = network.changed() => changed.context("network monitor stopped")?,
        }
    }
}
