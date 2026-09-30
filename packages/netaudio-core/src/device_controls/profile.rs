use super::PanelRequest;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PanelFamily {
    Bluetooth,
    DanteAv,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PanelQuery {
    pub category: &'static str,
    pub request: PanelRequest,
    pub prerequisites: Vec<&'static str>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PanelProfile {
    pub family: Option<PanelFamily>,
    pub queries: Vec<PanelQuery>,
    pub read_unavailable_reason: Option<&'static str>,
    pub write_unavailable_reason: Option<&'static str>,
}

#[derive(Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PanelProfileRequest {
    pub plugins: Vec<String>,
    pub managed: bool,
    pub address_available: bool,
    pub online: Option<bool>,
    pub virtual_panel_supported: bool,
    pub read_allowed: bool,
    pub write_allowed: bool,
    pub locked: Option<bool>,
    pub video_transmission_supported: bool,
}

/// Resolve only explicitly advertised plugin identities. Models do not establish support.
pub fn profile(facts: &PanelProfileRequest) -> PanelProfile {
    let plugins = &facts.plugins;
    let family = plugins.first().and_then(|first| {
        if !plugins.iter().all(|plugin| plugin == first) {
            return None;
        }

        match first.as_str() {
            "DIOBT" | "417564696E617465-0003" => Some(PanelFamily::Bluetooth),
            "DanteAV" => Some(PanelFamily::DanteAv),
            _ => None,
        }
    });
    let categories: &[&str] = match family {
        Some(PanelFamily::Bluetooth) => &[
            "bluetooth_connection",
            "bluetooth_identification",
            "bluetooth_discovery",
            "bluetooth_pairing",
        ],
        Some(PanelFamily::DanteAv) => &[
            "video_format",
            "codec_format",
            "video_channel",
            "serial",
            "bandwidth",
            "hdcp",
            "visca",
        ],
        None => &[],
    };
    let queries = categories
        .iter()
        .zip(1u32..)
        .map(|(category, selector)| PanelQuery {
            category,
            prerequisites: match *category {
                "video_format" => vec!["video_format", "visca"],
                "bandwidth" => vec!["bandwidth", "video_format"],
                _ => vec![category],
            },
            request: match family {
                Some(PanelFamily::Bluetooth) => PanelRequest::BluetoothQuery { selector },
                _ => PanelRequest::VideoQuery { selector },
            },
        })
        .collect();

    let read_unavailable_reason = if facts.managed {
        Some("Managed panel transport is not established.")
    } else if !facts.address_available {
        Some("Device address is unavailable.")
    } else if facts.online == Some(false) {
        Some("Device is offline.")
    } else if family.is_none() && facts.plugins.is_empty() {
        Some("Device advertises no control panel.")
    } else if family.is_none() {
        Some("Device panel identity is unknown or ambiguous.")
    } else if !facts.virtual_panel_supported {
        Some("Device has not advertised panel support.")
    } else if !facts.read_allowed {
        Some("Panel read permission is unavailable.")
    } else {
        None
    };
    let write_unavailable_reason = read_unavailable_reason.or_else(|| {
        if !facts.write_allowed {
            Some("Panel write permission is unavailable.")
        } else if facts.locked != Some(false) {
            Some("Device is locked or its lock state is unknown.")
        } else {
            None
        }
    });

    PanelProfile {
        family,
        queries,
        read_unavailable_reason,
        write_unavailable_reason,
    }
}
