use crate::commands;

pub(super) fn default_true() -> bool {
    true
}

pub(super) fn default_aes67_sequence() -> u16 {
    0x1007
}

pub(super) fn default_lock_reset_sequence() -> u16 {
    0x1008
}

pub(super) fn default_lock_reset_request_value() -> u32 {
    100
}

pub(super) fn default_conmon_export_sequence() -> u16 {
    1
}

pub(super) fn default_clear_configuration_sequence() -> u16 {
    0x0077
}

pub(super) fn default_sample_rate_sequence() -> u16 {
    0x0081
}

pub(super) fn default_set_sample_rate_sequence() -> u16 {
    0x03D4
}

pub(super) fn default_set_encoding_sequence() -> u16 {
    0x03D7
}

pub(super) fn default_enable_aes67_sequence() -> u16 {
    0x22DC
}

pub(super) fn default_encoding_sequence() -> u16 {
    0x0083
}

pub(super) fn default_sample_rate_pullup_sequence() -> u16 {
    0x0085
}

pub(super) fn default_gain_sequence() -> u16 {
    0x100A
}

pub(super) fn default_leader_sequence() -> u16 {
    0x0021
}

pub(super) fn default_system_reset_sequence() -> u16 {
    1
}

pub(super) fn default_interface_sequence() -> u16 {
    1
}

pub(super) fn default_link_status_sequence() -> u16 {
    0x0041
}

pub(super) fn default_switch_configuration_sequence() -> u16 {
    0x0015
}

pub(super) fn default_flow_start() -> u16 {
    1
}

pub(super) fn default_channel_name_protocol() -> u16 {
    commands::PROTOCOL_DANTE_FLOW
}
