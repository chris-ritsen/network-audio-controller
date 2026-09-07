use super::*;
use crate::network::{DanteRedundancyMode, DanteRedundancyStatus, NetworkInterface};

const INTERFACE_CONFIGURATION_SIZE: usize = 24;
const INTERFACE_RECORD_STRIDE: usize = 28;
const CONMON_RECORD_BASE: usize = 24;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceStatus {
    pub record_protocol_identifier: u16,
    pub link_speed_mbps: u32,
    pub interfaces: Vec<InterfaceStatusEntry>,
    pub reboot_required: bool,
    pub redundancy: Option<DanteRedundancyStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceStatusEntry {
    pub interface: Option<NetworkInterface>,
    pub mode: String,
    pub mac_address: String,
    pub ip_address: String,
    pub netmask: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_server: Option<String>,
    pub configured: Option<InterfaceConfiguration>,
    pub reboot_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceConfiguration {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub netmask: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_server: Option<String>,
}

fn current_configuration(interface: &InterfaceStatusEntry) -> Option<InterfaceConfiguration> {
    match interface.mode.as_str() {
        "dynamic" => Some(InterfaceConfiguration {
            mode: interface.mode.clone(),
            ip_address: None,
            netmask: None,
            gateway: None,
            dns_server: None,
        }),
        "static" => Some(InterfaceConfiguration {
            mode: interface.mode.clone(),
            ip_address: Some(interface.ip_address.clone()),
            netmask: Some(interface.netmask.clone()),
            gateway: interface.gateway.clone(),
            dns_server: interface.dns_server.clone(),
        }),
        _ => None,
    }
}

pub(super) fn interface_configuration_matches_running(
    configured: &InterfaceConfiguration,
    running: &InterfaceStatusEntry,
) -> bool {
    if configured.mode != running.mode {
        return false;
    }
    match configured.mode.as_str() {
        "dynamic" => true,
        "static" => {
            configured.ip_address.as_deref() == Some(running.ip_address.as_str())
                && configured.netmask.as_deref() == Some(running.netmask.as_str())
                // Secondary records omit DNS and gateway even when the stored
                // configuration retains them after a successful reboot.
                && running.gateway.as_ref().is_none_or(|v| configured.gateway.as_ref() == Some(v))
                && running.dns_server.as_ref().is_none_or(|v| configured.dns_server.as_ref() == Some(v))
        }
        _ => false,
    }
}

fn configured_interface(data: &[u8], offset: usize) -> Option<Option<InterfaceConfiguration>> {
    data.get(offset..offset.checked_add(20)?)?;
    let mode = read_u16(data, offset)?;
    match mode {
        0 => Some(None),
        INTERFACE_REBOOT_PENDING_DYNAMIC => Some(Some(InterfaceConfiguration {
            mode: "dynamic".to_owned(),
            ip_address: None,
            netmask: None,
            gateway: None,
            dns_server: None,
        })),
        INTERFACE_REBOOT_PENDING_STATIC => Some(Some(InterfaceConfiguration {
            mode: "static".to_owned(),
            ip_address: Some(ipv4_at(data, offset + 4)?),
            netmask: Some(ipv4_at(data, offset + 8)?),
            dns_server: Some(ipv4_at(data, offset + 12)?),
            gateway: Some(ipv4_at(data, offset + 16)?),
        })),
        _ => None,
    }
}

pub fn parse_interface_status(data: &[u8]) -> Option<InterfaceStatus> {
    validate_conmon_envelope(data, CONMON_OPCODE_INTERFACE_STATUS)?;
    let record_protocol_identifier = read_u16(data, CONMON_RECORD_BASE)?;
    let interface_count = usize::from(read_u16(data, CONMON_INTERFACE_COUNT_OFFSET)?);
    if !(1..=8).contains(&interface_count) {
        return None;
    }
    let link_speed_mbps = read_u32(data, CONMON_INTERFACE_LINK_SPEED_OFFSET)?;
    let mut interfaces = Vec::with_capacity(interface_count);
    let mut mac_addresses = HashSet::with_capacity(interface_count);
    let mut offset = CONMON_INTERFACE_RECORDS_OFFSET;
    let mut fixed_records = true;

    for index in 0..interface_count {
        let mode_value = read_u16(data, offset)?;
        let known = mode_value <= 3;
        let stride = if known {
            INTERFACE_RECORD_STRIDE
        } else {
            CONMON_INTERFACE_RECORD_SIZE
        };
        let required = if known {
            CONMON_INTERFACE_CONFIGURED_RECORD_SIZE
        } else {
            CONMON_INTERFACE_RECORD_SIZE
        };
        data.get(offset..offset.checked_add(required)?)?;
        let mode = match (index, mode_value) {
            (_, 1) | (1, 0) => "dynamic",
            (_, 3) | (1, 2) => "static",
            _ => "unknown",
        };
        let mac = data.get(offset + 2..offset + 8)?;
        let mac_address = format!(
            "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
            mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
        );
        if !mac_addresses.insert(mac_address.clone()) {
            return None;
        }
        let (gateway, dns_server) = match mode_value {
            1 => (
                Some(ipv4_at(data, offset + 16)?),
                Some(ipv4_at(data, offset + 20)?),
            ),
            3 => (
                Some(ipv4_at(data, offset + 20)?),
                Some(ipv4_at(data, offset + 16)?),
            ),
            _ => (None, None),
        };
        let mut entry = InterfaceStatusEntry {
            interface: match index {
                0 => Some(NetworkInterface::Primary),
                1 => Some(NetworkInterface::Secondary),
                _ => None,
            },
            mode: mode.to_owned(),
            mac_address,
            ip_address: ipv4_at(data, offset + 8)?,
            netmask: ipv4_at(data, offset + 12)?,
            gateway,
            dns_server,
            configured: None,
            reboot_required: false,
        };
        entry.configured = current_configuration(&entry);
        interfaces.push(entry);
        offset = offset.checked_add(stride)?;
        fixed_records &= known;
    }

    let mut redundancy = None;
    // These revisions carry a configuration descriptor after the last active
    // interface. Its pointer is relative to the notification record, not UDP.
    if fixed_records && matches!(record_protocol_identifier, 0x0724 | 0x0727 | 0x072e) {
        let descriptor = offset.checked_sub(4)?;
        let size = usize::from(read_u16(data, descriptor)?);
        let pointer = usize::from(read_u16(data, descriptor + 2)?);
        if size != INTERFACE_CONFIGURATION_SIZE || pointer + CONMON_RECORD_BASE != offset + 4 {
            return None;
        }
        let flags = read_u16(data, offset)?;
        if record_protocol_identifier == 0x0724 && flags & !3 == 0 {
            let mode = |mask| {
                if flags & mask == 0 {
                    DanteRedundancyMode::Switched
                } else {
                    DanteRedundancyMode::Redundant
                }
            };
            redundancy = Some(DanteRedundancyStatus {
                current: Some(mode(1)),
                configured: Some(mode(2)),
                supported: vec![
                    DanteRedundancyMode::Switched,
                    DanteRedundancyMode::Redundant,
                ],
                reboot_required: mode(1) != mode(2),
            });
        }
        let start = CONMON_RECORD_BASE.checked_add(pointer)?;
        for (index, entry) in interfaces.iter_mut().enumerate() {
            let position = start.checked_add(index.checked_mul(size)?)?;
            if let Some(configured) = configured_interface(data, position)? {
                entry.reboot_required =
                    !interface_configuration_matches_running(&configured, entry);
                entry.configured = Some(configured);
            }
        }
    }
    Some(InterfaceStatus {
        record_protocol_identifier,
        link_speed_mbps,
        reboot_required: interfaces.iter().any(|v| v.reboot_required)
            || redundancy.as_ref().is_some_and(|v| v.reboot_required),
        redundancy,
        interfaces,
    })
}
