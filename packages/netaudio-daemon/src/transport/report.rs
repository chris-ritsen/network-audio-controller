use std::net::IpAddr;

use anyhow::Result;
use serde_json::{json, Value};

use super::{generated, TlsIdentity};
use crate::configuration::Configuration;
use crate::network::NetworkSnapshot;

pub fn describe(configuration: &Configuration) -> Result<Value> {
    let network = NetworkSnapshot::read()?;
    let mut addresses: Vec<String> = network
        .interfaces
        .iter()
        .filter(|interface| interface.running)
        .filter_map(|interface| match interface.network.addr() {
            IpAddr::V4(address) if !address.is_loopback() => Some(address.to_string()),
            _ => None,
        })
        .collect();
    addresses.sort();
    addresses.dedup();
    let Some(settings) = configuration.tls()? else {
        return Ok(json!({"addresses": addresses, "tls": null}));
    };

    if settings.generated {
        generated::ensure(&settings.primary.certificate, &network)?;
    }

    let identity = TlsIdentity::open(settings.primary.clone(), settings.named.clone())?;
    let status = identity.status()?;

    Ok(json!({
        "addresses": addresses,
        "tls": {
            "certificate": settings.primary.certificate,
            "certificate_sha256": status["tls_sha256"],
            "key": settings.primary.key,
            "named_certificate": status["named"],
            "port": settings.port,
        },
    }))
}
