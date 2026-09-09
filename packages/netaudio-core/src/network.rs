use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkInterface {
    #[default]
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy)]
pub struct StaticInterfaceConfiguration {
    pub ip_address: [u8; 4],
    pub netmask: [u8; 4],
    pub dns_server: [u8; 4],
    pub gateway: [u8; 4],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DanteRedundancyMode {
    Switched,
    Redundant,
    SplitRedundant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DanteRedundancyStatus {
    pub current: Option<DanteRedundancyMode>,
    pub configured: Option<DanteRedundancyMode>,
    pub supported: Vec<DanteRedundancyMode>,
    pub reboot_required: bool,
}
