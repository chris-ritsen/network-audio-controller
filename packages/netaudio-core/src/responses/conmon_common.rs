use super::*;

#[repr(u16)]
pub enum NetaudioNotification {
    TopologyChange = 16,
    InterfaceStatus = 17,
    ClockingStatus = 32,
    VersionsStatus = 96,
    ClearConfigStatus = 120,
    SampleRateStatus = 128,
    EncodingStatus = 130,
    SampleRatePullupStatus = 132,
    DeviceReboot = 146,
    ManfVersionsStatus = 192,
    RoutingReady = 256,
    TxChannelChange = 257,
    RxChannelChange = 258,
    TxLabelChange = 259,
    TxFlowChange = 260,
    RxFlowChange = 261,
    PropertyChange = 262,
    RoutingDeviceChange = 288,
    Aes67Status = 4103,
    CodecStatus = 4107,
    SettingsChange = 4110,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NotificationEnvelope {
    pub notification_id: u16,
    pub notification_name: Option<&'static str>,
    pub is_conmon: bool,
    pub response_kind: Option<&'static str>,
}

pub fn parse_notification_envelope(data: &[u8]) -> Option<NotificationEnvelope> {
    if let Some(opcode) = conmon_opcode(data) {
        return Some(NotificationEnvelope {
            notification_id: opcode,
            notification_name: None,
            is_conmon: true,
            response_kind: conmon_response_kind(opcode),
        });
    }

    if read_u16(data, 0)? != crate::protocol::PROTOCOL_ID
        || usize::from(read_u16(data, 2)?) != data.len()
    {
        return None;
    }

    let notification_id = read_u16(data, 26)?;
    use NetaudioNotification as Notification;

    let notification_name = match notification_id {
        id if id == Notification::TopologyChange as u16 => Some("Topology Change"),
        id if id == Notification::InterfaceStatus as u16 => Some("Interface Status"),
        id if id == Notification::ClockingStatus as u16 => Some("Clocking Status"),
        id if id == Notification::VersionsStatus as u16 => Some("Versions Status"),
        id if id == Notification::ClearConfigStatus as u16 => Some("Clear Config Status"),
        id if id == Notification::SampleRateStatus as u16 => Some("Sample Rate Status"),
        id if id == Notification::EncodingStatus as u16 => Some("Encoding Status"),
        id if id == Notification::SampleRatePullupStatus as u16 => {
            Some("Sample Rate Pull-Up Status")
        }
        id if id == Notification::DeviceReboot as u16 => Some("Device Reboot"),
        id if id == Notification::ManfVersionsStatus as u16 => Some("Manufacturer Versions Status"),
        id if id == Notification::RoutingReady as u16 => Some("Routing Ready"),
        id if id == Notification::TxChannelChange as u16 => Some("TX Channel Change"),
        id if id == Notification::RxChannelChange as u16 => Some("RX Channel Change"),
        id if id == Notification::TxLabelChange as u16 => Some("TX Label Change"),
        id if id == Notification::TxFlowChange as u16 => Some("TX Flow Change"),
        id if id == Notification::RxFlowChange as u16 => Some("RX Flow Change"),
        id if id == Notification::PropertyChange as u16 => Some("Property Change"),
        id if id == Notification::RoutingDeviceChange as u16 => Some("Routing Device Change"),
        id if id == Notification::Aes67Status as u16 => Some("AES67 Status"),
        id if id == Notification::CodecStatus as u16 => Some("Codec Status"),
        id if id == Notification::SettingsChange as u16 => Some("Settings Change"),
        _ => None,
    };

    Some(NotificationEnvelope {
        notification_id,
        notification_name,
        is_conmon: false,
        response_kind: None,
    })
}

pub(crate) fn conmon_response_kind(opcode: u16) -> Option<&'static str> {
    Some(match opcode {
        CONMON_OPCODE_CLOCK_MASTER_STATUS => "clock_master_status",
        CONMON_OPCODE_CLOCK_UNICAST_STATUS => "clock_unicast_status",
        CONMON_OPCODE_CLOCK_IDENTIFIER_STATUS => "clock_identifier_status",
        CONMON_OPCODE_AES67_CURRENT_NEW => "aes67_status",
        CONMON_OPCODE_PANEL_STATUS => "panel_status",
        CONMON_OPCODE_CLEAR_CONFIGURATION_STATUS => "clear_configuration_status",
        CONMON_OPCODE_DANTE_MODEL_RESPONSE => "dante_model",
        CONMON_OPCODE_ENCODING_STATUS => "encoding_status",
        CONMON_OPCODE_CODEC_STATUS => "codec_status",
        CONMON_OPCODE_INTERFACE_STATUS => "interface_status",
        CONMON_OPCODE_INTERFACE_STATISTICS_STATUS => "interface_statistics_status",
        CONMON_OPCODE_UNMAPPED_0086_STATUS => "unmapped_0086_status",
        CONMON_OPCODE_UNMAPPED_00E0_STATUS => "unmapped_00e0_status",
        CONMON_OPCODE_UNMAPPED_0102_STATUS => "unmapped_0102_status",
        CONMON_OPCODE_UNMAPPED_0106_STATUS => "unmapped_0106_status",
        CONMON_OPCODE_LOCK_RESET_STATUS => "lock_reset_status",
        CONMON_OPCODE_MAKE_MODEL_RESPONSE => "make_model",
        CONMON_OPCODE_PTP_CLOCK_STATUS => "ptp_clock_status",
        CONMON_OPCODE_ROUTING_CAPACITY_STATUS => "routing_capacity_status",
        CONMON_OPCODE_SAMPLE_RATE_PULLUP_STATUS => "sample_rate_pullup_status",
        CONMON_OPCODE_SAMPLE_RATE_STATUS => "sample_rate_status",
        CONMON_OPCODE_SWITCH_CONFIGURATION_STATUS => "switch_configuration_status",
        CONMON_OPCODE_EXPORT_FRAGMENT => "conmon_export_fragment",
        _ => return None,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConmonOpcode {
    pub opcode: Option<u16>,
}

pub fn parse_conmon_opcode(data: &[u8]) -> Option<ConmonOpcode> {
    Some(ConmonOpcode {
        opcode: Some(conmon_opcode(data)?),
    })
}

pub const CONMON_OPCODE_INTERFACE_STATUS: u16 = 0x0011;
pub const CONMON_OPCODE_SWITCH_CONFIGURATION_STATUS: u16 = 0x0014;
pub const CONMON_OPCODE_CLEAR_CONFIGURATION_STATUS: u16 = 0x0078;
pub const CONMON_OPCODE_MAKE_MODEL_RESPONSE: u16 = 0x00C0;
pub const CONMON_OPCODE_DANTE_MODEL_RESPONSE: u16 = 0x0060;
pub const CONMON_OPCODE_SAMPLE_RATE_STATUS: u16 = 0x0080;
pub const CONMON_OPCODE_CLOCK_MASTER_STATUS: u16 = 0x0022;
pub const CONMON_OPCODE_CLOCK_UNICAST_STATUS: u16 = 0x0024;
pub const CONMON_OPCODE_CLOCK_IDENTIFIER_STATUS: u16 = 0x0026;
pub const CONMON_OPCODE_INTERFACE_STATISTICS_STATUS: u16 = 0x0040;
pub const CONMON_OPCODE_UNMAPPED_0086_STATUS: u16 = 0x0086;
pub const CONMON_OPCODE_UNMAPPED_00E0_STATUS: u16 = 0x00E0;
pub const CONMON_OPCODE_UNMAPPED_0102_STATUS: u16 = 0x0102;
pub const CONMON_OPCODE_UNMAPPED_0106_STATUS: u16 = 0x0106;
pub const CONMON_OPCODE_ENCODING_STATUS: u16 = 0x0082;
pub const CONMON_OPCODE_SAMPLE_RATE_PULLUP_STATUS: u16 = 0x0084;
pub const CONMON_OPCODE_CODEC_STATUS: u16 = 0x100B;
pub const CONMON_OPCODE_AES67_CURRENT_NEW: u16 = 0x1007;
pub const CONMON_OPCODE_LOCK_RESET_STATUS: u16 = 0x1009;
pub const CONMON_OPCODE_PTP_CLOCK_STATUS: u16 = 0x0020;
pub const CONMON_OPCODE_ROUTING_CAPACITY_STATUS: u16 = 0x0100;
pub const CONMON_OPCODE_EXPORT_FRAGMENT: u16 = 0xFF05;

pub(super) const CONMON_CLEAR_CONFIGURATION_RECORD_IDENTIFIER_OFFSET: usize = 0x18;
pub(super) const CONMON_CLEAR_CONFIGURATION_FIRST_WORD_OFFSET: usize = 0x1C;
pub(super) const CONMON_CLEAR_CONFIGURATION_AVAILABLE_ACTIONS_MASK_OFFSET: usize = 0x20;
pub(super) const CONMON_CLEAR_CONFIGURATION_ACTION_RESULT_CODE_OFFSET: usize = 0x24;
pub(super) const CONMON_CLEAR_CONFIGURATION_PACKET_SIZE: usize = 0x28;
pub(super) const CONMON_ROUTING_CAPACITY_RECORD_OFFSET: usize = 0x18;
pub(super) const CONMON_ROUTING_CAPACITY_UNMAPPED_PREFIX_WORD_OFFSET: usize = 0x1C;
pub(super) const CONMON_ROUTING_CAPACITY_READY_OFFSET: usize = 0x20;
pub(super) const CONMON_ROUTING_CAPACITY_LINK_STATUS_OFFSET: usize = 0x21;
pub(super) const CONMON_ROUTING_CAPACITY_UNMAPPED_WORD_OFFSET: usize = 0x22;
pub(super) const CONMON_ROUTING_CAPACITY_TRANSMIT_CHANNEL_COUNT_OFFSET: usize = 0x24;
pub(super) const CONMON_ROUTING_CAPACITY_RECEIVE_CHANNEL_COUNT_OFFSET: usize = 0x26;
pub(super) const CONMON_AES67_CURRENT_NEW_OFFSET: usize = 0x21;
pub(super) const CONMON_LOCK_RESET_RECORD_OFFSET: usize = 24;
pub(super) const CONMON_LOCK_RESET_FIXED_RECORD_SIZE: usize = 24;
pub(super) const CONMON_LOCK_RESET_IDENTIFIER_WIDTH: usize = 8;
pub(super) const CONMON_EXPORT_HEADER_SIZE: usize = 28;
pub(super) const CONMON_EXPORT_DATA_OFFSET: usize = 52;
pub(super) const CONMON_INTERFACE_COUNT_OFFSET: usize = 0x20;
pub(super) const CONMON_INTERFACE_LINK_SPEED_OFFSET: usize = 0x24;
pub(super) const CONMON_INTERFACE_RECORDS_OFFSET: usize = 0x28;
pub(super) const CONMON_INTERFACE_RECORD_SIZE: usize = 20;
pub(super) const CONMON_INTERFACE_CONFIGURED_RECORD_SIZE: usize = 24;
pub(super) const INTERFACE_REBOOT_PENDING_DYNAMIC: u16 = 0x0004;
pub(super) const INTERFACE_REBOOT_PENDING_STATIC: u16 = 0x0006;
