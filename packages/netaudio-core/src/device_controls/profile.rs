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
pub enum Panel {
    Video,
    Serial,
    Bluetooth,
}

impl Panel {
    fn family(self) -> PanelFamily {
        match self {
            Self::Bluetooth => PanelFamily::Bluetooth,
            Self::Video | Self::Serial => PanelFamily::DanteAv,
        }
    }

    fn categories(self) -> &'static [&'static str] {
        match self {
            Self::Bluetooth => &[
                "bluetooth_connection",
                "bluetooth_identification",
                "bluetooth_discovery",
                "bluetooth_pairing",
            ],
            Self::Video => &[
                "video_format",
                "codec_format",
                "video_channel",
                "bandwidth",
                "hdcp",
                "visca",
            ],
            Self::Serial => &["serial"],
        }
    }
}

pub fn category_panel(category: &str) -> Option<Panel> {
    [Panel::Bluetooth, Panel::Video, Panel::Serial]
        .into_iter()
        .find(|panel| panel.categories().contains(&category))
}

fn query_selector(category: &str) -> Option<u32> {
    Some(match category {
        "bluetooth_connection" | "video_format" => 1,
        "bluetooth_identification" | "codec_format" => 2,
        "bluetooth_discovery" | "video_channel" => 3,
        "bluetooth_pairing" | "serial" => 4,
        "bandwidth" => 5,
        "hdcp" => 6,
        "visca" => 7,
        _ => return None,
    })
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
    pub panels: Vec<Panel>,
    pub categories: Vec<&'static str>,
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

const VIDEO_PANEL_IDENTIFIER: &str = "417564696E617465-0001";
const SERIAL_PANEL_IDENTIFIER: &str = "417564696E617465-0002";
const BLUETOOTH_PANEL_IDENTIFIER: &str = "417564696E617465-0003";
const BLUETOOTH_PLATFORM_MODEL_IDENTIFIER: &str = "44494f4254000000";
const DANTE_AV_PLATFORM_MODEL_IDENTIFIER: &str = "44616e7465415600";

fn advertised_panel(identifier: &str) -> Option<Panel> {
    match identifier {
        VIDEO_PANEL_IDENTIFIER => Some(Panel::Video),
        SERIAL_PANEL_IDENTIFIER => Some(Panel::Serial),
        BLUETOOTH_PANEL_IDENTIFIER => Some(Panel::Bluetooth),
        _ => None,
    }
}

fn platform_default_panels(facts: &PanelProfileRequest) -> &'static [Panel] {
    let model = facts.platform_model_identifier_hexadecimal.as_deref();
    if model.is_some_and(|model| model.eq_ignore_ascii_case(BLUETOOTH_PLATFORM_MODEL_IDENTIFIER)) {
        &[Panel::Bluetooth]
    } else if model
        .is_some_and(|model| model.eq_ignore_ascii_case(DANTE_AV_PLATFORM_MODEL_IDENTIFIER))
    {
        &[Panel::Video, Panel::Serial]
    } else {
        &[]
    }
}

pub fn profile(facts: &PanelProfileRequest) -> PanelProfile {
    let mut advertised_panels = Vec::new();
    let mut unrecognized_panels = Vec::new();

    for entry in &facts.plugins {
        match entry.as_deref().and_then(advertised_panel) {
            Some(panel) if !advertised_panels.contains(&panel) => advertised_panels.push(panel),
            Some(_) => {}
            None => unrecognized_panels.push(entry.clone()),
        }
    }

    let mut families = Vec::new();
    for panel in &advertised_panels {
        if !families.contains(&panel.family()) {
            families.push(panel.family());
        }
    }

    let (panels, selection) = if !facts.plugins.is_empty() {
        match families.as_slice() {
            [_] => (advertised_panels, Some(PanelSelection::Advertised)),
            _ => (Vec::new(), None),
        }
    } else if facts.virtual_panel_supported && !platform_default_panels(facts).is_empty() {
        (
            platform_default_panels(facts).to_vec(),
            Some(PanelSelection::PlatformDefault),
        )
    } else {
        (Vec::new(), None)
    };
    let family = panels.first().map(|panel| panel.family());
    let categories: Vec<&'static str> = [Panel::Bluetooth, Panel::Video, Panel::Serial]
        .into_iter()
        .filter(|panel| panels.contains(panel))
        .flat_map(|panel| panel.categories().iter().copied())
        .collect();
    let mut queries: Vec<PanelQuery> = categories
        .iter()
        .filter_map(|category| {
            let selector = query_selector(category)?;
            Some(PanelQuery {
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
        })
        .collect();
    queries.sort_by_key(|query| query_selector(query.category));

    let read_unavailable_reason = if facts.managed {
        Some("Managed panel transport is not established.")
    } else if !facts.address_available {
        Some("Device address is unavailable.")
    } else if facts.online == Some(false) {
        Some("Device is offline.")
    } else if family.is_none() && families.len() > 1 {
        Some("Device advertises control panels from more than one panel family.")
    } else if family.is_none() && !facts.plugins.is_empty() {
        Some("Device advertises only unrecognized control panels.")
    } else if family.is_none() && !platform_default_panels(facts).is_empty() {
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
        panels,
        categories,
        selection,
        unrecognized_panels,
        queries,
        read_unavailable_reason,
        write_unavailable_reason,
    }
}
