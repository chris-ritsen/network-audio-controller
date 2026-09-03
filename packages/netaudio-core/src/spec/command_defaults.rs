use crate::commands;

pub(super) fn default_arc_protocol() -> u16 {
    crate::protocol::PROTOCOL_ID
}

pub(super) fn default_channel_name_protocol() -> u16 {
    commands::PROTOCOL_DANTE_FLOW
}

pub(super) fn default_flow_start() -> u16 {
    1
}

pub(super) fn default_flow_protocol() -> u16 {
    commands::PROTOCOL_DANTE_FLOW
}

pub(super) fn default_lock_reset_request_value() -> u32 {
    100
}

pub(super) fn default_modern_arc_protocol() -> u16 {
    crate::protocol::PROTOCOL_ARC_2809
}

pub(super) fn default_true() -> bool {
    true
}
