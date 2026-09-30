use super::PanelRequest;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PanelFamily {
    Bluetooth,
    DanteAv,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PanelSelection {
    Advertised,
    PlatformDefault,
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
    pub selection: Option<PanelSelection>,
    pub unrecognized_panels: Vec<Option<String>>,
    pub queries: Vec<PanelQuery>,
    pub read_unavailable_reason: Option<&'static str>,
    pub write_unavailable_reason: Option<&'static str>,
}

#[derive(Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PanelProfileRequest {
    pub plugins: Vec<Option<String>>,
    pub platform_model_identifier_hexadecimal: Option<String>,
    pub managed: bool,
    pub address_available: bool,
    pub online: Option<bool>,
    pub virtual_panel_supported: bool,
    pub read_allowed: bool,
    pub write_allowed: bool,
    pub locked: Option<bool>,
    pub video_transmission_supported: bool,
}

const BLUETOOTH_PANEL_IDENTIFIER: &str = "417564696E617465-0003";
const BLUETOOTH_PLATFORM_MODEL_IDENTIFIER: &str = "44494f4254000000";

fn advertised_family(identifier: &str) -> Option<PanelFamily> {
    match identifier {
        BLUETOOTH_PANEL_IDENTIFIER => Some(PanelFamily::Bluetooth),
        "DanteAV" => Some(PanelFamily::DanteAv),
        _ => None,
    }
}

fn is_bluetooth_platform(facts: &PanelProfileRequest) -> bool {
    facts
        .platform_model_identifier_hexadecimal
        .as_deref()
        .is_some_and(|model| model.eq_ignore_ascii_case(BLUETOOTH_PLATFORM_MODEL_IDENTIFIER))
}

pub fn profile(facts: &PanelProfileRequest) -> PanelProfile {
    let mut advertised_families = Vec::new();
    let mut unrecognized_panels = Vec::new();

    for entry in &facts.plugins {
        match entry.as_deref().and_then(advertised_family) {
            Some(family) if !advertised_families.contains(&family) => {
                advertised_families.push(family)
            }
            Some(_) => {}
            None => unrecognized_panels.push(entry.clone()),
        }
    }

    let (family, selection) = if !facts.plugins.is_empty() {
        match advertised_families.as_slice() {
            [family] => (Some(*family), Some(PanelSelection::Advertised)),
            _ => (None, None),
        }
    } else if is_bluetooth_platform(facts) && facts.virtual_panel_supported {
        (
            Some(PanelFamily::Bluetooth),
            Some(PanelSelection::PlatformDefault),
        )
    } else {
        (None, None)
    };
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
    } else if family.is_none() && advertised_families.len() > 1 {
        Some("Device advertises control panels from more than one panel family.")
    } else if family.is_none() && !facts.plugins.is_empty() {
        Some("Device advertises only unrecognized control panels.")
    } else if family.is_none() && is_bluetooth_platform(facts) {
        Some("Device has not advertised panel support.")
    } else if family.is_none() && facts.platform_model_identifier_hexadecimal.is_none() {
        Some("Device platform identity has not been read.")
    } else if family.is_none() {
        Some("Device advertises no control panel.")
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
        selection,
        unrecognized_panels,
        queries,
        read_unavailable_reason,
        write_unavailable_reason,
    }
}
