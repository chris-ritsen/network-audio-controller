use serde::{Deserialize, Serialize};

use crate::spec::SpecError;

#[derive(Clone, Copy, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
enum Authority {
    Direct,
    Managed,
    Observed,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
struct Evidence {
    direct: Option<bool>,
    managed: Option<bool>,
    managed_fresh: bool,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ReceiverCapabilityRequest {
    authority: Authority,
    channels: Vec<Evidence>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Capability {
    supported: Option<bool>,
    conflict: bool,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReceiverCapabilities {
    channels: Vec<Capability>,
    support: &'static str,
}

/// Resolve advertised evidence without turning stale observations or absence into
/// permission. Observed authority is for inventory display, not write preflight.
pub fn self_connection(input: &str) -> Result<ReceiverCapabilities, SpecError> {
    let request: ReceiverCapabilityRequest =
        serde_json::from_str(input).map_err(|error| SpecError::InvalidJson(error.to_string()))?;
    let mut channels = Vec::with_capacity(request.channels.len());

    for evidence in request.channels {
        let managed = evidence.managed.filter(|_| evidence.managed_fresh);
        let conflict = match (evidence.direct, managed) {
            (Some(direct), Some(managed)) => direct != managed,
            _ => false,
        };
        let supported = if conflict {
            None
        } else {
            match request.authority {
                Authority::Direct => evidence.direct,
                Authority::Managed => managed,
                Authority::Observed => evidence.direct.or(managed),
            }
        };

        channels.push(Capability {
            supported,
            conflict,
        });
    }

    let supported = channels
        .iter()
        .any(|channel| channel.supported == Some(true));
    let unsupported = channels
        .iter()
        .any(|channel| channel.supported == Some(false));
    let unknown = channels.iter().any(|channel| channel.supported.is_none());
    let support = match (supported, unsupported, unknown) {
        (false, false, _) => "unknown",
        (_, _, true) => "partial",
        (true, true, false) => "mixed",
        (true, false, false) => "supported",
        (false, true, false) => "unsupported",
    };

    Ok(ReceiverCapabilities { channels, support })
}
