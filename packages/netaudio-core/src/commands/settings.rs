use super::*;

pub fn build_set_latency(
    latency_milliseconds: f64,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if !latency_milliseconds.is_finite()
        || !(0.0..=MAX_LATENCY_MILLISECONDS).contains(&latency_milliseconds)
    {
        return Err(NetaudioError::InvalidLatency);
    }
    let rounded_nanoseconds = (latency_milliseconds * 1_000_000.0).round();
    if rounded_nanoseconds > f64::from(u32::MAX) {
        return Err(NetaudioError::InvalidLatency);
    }
    let latency_ns = rounded_nanoseconds as u32;
    let latency_bytes = latency_ns.to_be_bytes();

    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&LATENCY_SET_PREAMBLE);
    payload.extend_from_slice(&latency_bytes);
    payload.extend_from_slice(&latency_bytes);

    build_control_packet(OPCODE_DEVICE_SETTINGS_SET, &payload, transaction_id)
}

fn build_system_reset(
    mac: [u8; 6],
    sequence: u16,
    reset_mode: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = Vec::with_capacity(10);
    tail.extend_from_slice(&SYSTEM_RESET_MESSAGE_TYPE.to_be_bytes());
    tail.extend_from_slice(&SYSTEM_RESET_REQUEST_VALUE.to_be_bytes());
    tail.extend_from_slice(&SYSTEM_RESET_PRESENT.to_be_bytes());
    tail.extend_from_slice(&reset_mode.to_be_bytes());
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_reboot(mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    build_system_reset(mac, sequence, SYSTEM_RESET_MODE_REBOOT)
}

pub fn build_factory_reset(mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    build_system_reset(mac, sequence, SYSTEM_RESET_MODE_FACTORY)
}

fn build_clear_configuration_request(
    mac: [u8; 6],
    sequence: u16,
    action_mode: u32,
) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = Vec::with_capacity(10);
    tail.extend_from_slice(&CLEAR_CONFIGURATION_MESSAGE_TYPE.to_be_bytes());
    tail.extend_from_slice(&CLEAR_CONFIGURATION_REQUEST_VALUE.to_be_bytes());
    tail.extend_from_slice(&action_mode.to_be_bytes());
    settings_packet(sequence, mac, SETTINGS_SUFFIX_CLEAR_CONFIGURATION, &tail)
}

pub fn build_probe_clear_configuration_status(
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_clear_configuration_request(mac, sequence, 0)
}

pub fn build_clear_all_configuration(
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_clear_configuration_request(mac, sequence, CLEAR_CONFIGURATION_ACTION_ALL)
}

pub fn build_clear_all_configuration_preserving_internet_protocol_settings(
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_clear_configuration_request(
        mac,
        sequence,
        CLEAR_CONFIGURATION_ACTION_PRESERVE_INTERNET_PROTOCOL,
    )
}

pub fn build_identify(sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    settings_packet(
        sequence,
        [0u8; 6],
        SETTINGS_SUFFIX_IDENTITY,
        &[0x00, 0x63, 0x00, 0x00, 0x00, 0x64],
    )
}

pub fn build_set_encoding(encoding: u32) -> Result<Vec<u8>, NetaudioError> {
    build_set_encoding_with_sequence(encoding, 0x03D7)
}

pub fn build_set_encoding_with_sequence(
    encoding: u32,
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if encoding == 0 {
        return Err(NetaudioError::InvalidEncoding);
    }
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = vec![0x00, 0x83, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x01];
    tail.extend_from_slice(&encoding.to_be_bytes());
    settings_packet(
        sequence,
        AUDIO_CONFIG_PSEUDO_MAC,
        SETTINGS_SUFFIX_AUDIO_CONFIG,
        &tail,
    )
}

pub fn build_set_sample_rate(sample_rate: u32) -> Result<Vec<u8>, NetaudioError> {
    build_set_sample_rate_with_sequence(sample_rate, 0x03D4)
}

pub fn build_set_sample_rate_with_sequence(
    sample_rate: u32,
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if sample_rate == 0 {
        return Err(NetaudioError::InvalidSampleRate);
    }
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = vec![0x00, 0x81, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x01];
    tail.extend_from_slice(&sample_rate.to_be_bytes());
    settings_packet(
        sequence,
        AUDIO_CONFIG_PSEUDO_MAC,
        SETTINGS_SUFFIX_AUDIO_CONFIG,
        &tail,
    )
}

fn build_audio_config_probe(
    host_mac: [u8; 6],
    sequence: u16,
    message_type: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut body = Vec::with_capacity(14);
    body.extend_from_slice(&message_type.to_be_bytes());
    body.extend_from_slice(&100u32.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    settings_packet(sequence, host_mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &body)
}

pub fn build_probe_sample_rate(host_mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    build_audio_config_probe(host_mac, sequence, 0x0081)
}

pub fn build_probe_encoding(host_mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    build_audio_config_probe(host_mac, sequence, 0x0083)
}

fn build_sample_rate_pullup_control(
    host_mac: [u8; 6],
    sequence: u16,
    flags: u32,
    raw_value: u32,
) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut body = Vec::with_capacity(30);
    body.extend_from_slice(&0x0085u16.to_be_bytes());
    for value in [0, flags, raw_value, 0, 0, 0, 0] {
        body.extend_from_slice(&value.to_be_bytes());
    }
    settings_packet(sequence, host_mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &body)
}

pub fn build_probe_sample_rate_pullup(
    host_mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_sample_rate_pullup_control(host_mac, sequence, 0, 0)
}

pub fn build_set_sample_rate_pullup(
    host_mac: [u8; 6],
    sequence: u16,
    raw_value: u32,
) -> Result<Vec<u8>, NetaudioError> {
    build_sample_rate_pullup_control(host_mac, sequence, 1, raw_value)
}

pub fn build_probe_gain_level(host_mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    let mut body = Vec::with_capacity(14);
    body.extend_from_slice(&GAIN_MESSAGE_TYPE.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    settings_packet(sequence, host_mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &body)
}

pub fn build_set_gain_level(
    host_mac: [u8; 6],
    sequence: u16,
    channel_number: u16,
    gain_level: u8,
    is_input: bool,
) -> Result<Vec<u8>, NetaudioError> {
    if channel_number == 0 {
        return Err(NetaudioError::InvalidChannel);
    }
    if !(MIN_GAIN_LEVEL..=MAX_GAIN_LEVEL).contains(&gain_level) {
        return Err(NetaudioError::InvalidGainLevel);
    }
    let direction = if is_input {
        GAIN_INPUT_DIRECTION
    } else {
        GAIN_OUTPUT_DIRECTION
    };
    let mut body = Vec::with_capacity(26);
    body.extend_from_slice(&GAIN_MESSAGE_TYPE.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&12u16.to_be_bytes());
    body.extend_from_slice(&16u16.to_be_bytes());
    body.extend_from_slice(&direction.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&channel_number.to_be_bytes());
    body.extend_from_slice(&u32::from(gain_level).to_be_bytes());
    settings_packet(sequence, host_mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &body)
}

pub fn build_enable_aes67(enabled: bool, mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    build_enable_aes67_with_sequence(enabled, mac, 0x22DC)
}

pub fn build_enable_aes67_with_sequence(
    enabled: bool,
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = vec![0x10, 0x06, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0001u16.to_be_bytes());
    tail.extend_from_slice(&(if enabled { 0x0001u16 } else { 0x0000u16 }).to_be_bytes());
    settings_packet(sequence, mac, SETTINGS_SUFFIX_AES67_WRITE, &tail)
}

pub fn build_probe_interface_status(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = Vec::new();
    tail.extend_from_slice(&0x0013u16.to_be_bytes());
    tail.extend_from_slice(&0x64u32.to_be_bytes());
    tail.extend(std::iter::repeat_n(0, 8));
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_probe_link_status(mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = Vec::with_capacity(30);
    tail.extend_from_slice(&0x0041u16.to_be_bytes());
    tail.extend(std::iter::repeat_n(0, 28));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_probe_switch_configuration(
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = Vec::with_capacity(10);
    tail.extend_from_slice(&0x0015u16.to_be_bytes());
    tail.extend_from_slice(&100u32.to_be_bytes());
    tail.extend_from_slice(&0u32.to_be_bytes());
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_set_interface_dhcp(mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = Vec::new();
    tail.extend_from_slice(&0x0013u16.to_be_bytes());
    tail.extend_from_slice(&0x64u32.to_be_bytes());
    tail.extend_from_slice(&[0x01, 0x1c, 0x00, 0x10]);
    tail.extend(std::iter::repeat_n(0, 24));
    tail.extend_from_slice(&[0x00, 0x02, 0x00, 0x00]);
    tail.extend(std::iter::repeat_n(0, 4));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_set_interface_static(
    ip_address: [u8; 4],
    netmask: [u8; 4],
    dns_server: [u8; 4],
    gateway: [u8; 4],
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = Vec::new();
    tail.extend_from_slice(&0x0013u16.to_be_bytes());
    tail.extend_from_slice(&0x64u32.to_be_bytes());
    tail.extend_from_slice(&[0x01, 0x1c, 0x0f, 0x10]);
    tail.extend(std::iter::repeat_n(0, 4));
    tail.extend_from_slice(&0x02u32.to_be_bytes());
    tail.extend_from_slice(&ip_address);
    tail.extend_from_slice(&netmask);
    tail.extend_from_slice(&dns_server);
    tail.extend_from_slice(&gateway);
    tail.extend_from_slice(&[0x00, 0x02, 0x00, 0x00]);
    tail.extend(std::iter::repeat_n(0, 4));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_probe_aes67(mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = vec![0x10, 0x06, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0000u16.to_be_bytes());
    tail.extend_from_slice(&0x0000u16.to_be_bytes());
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_probe_lock_reset_status(
    mac: [u8; 6],
    sequence: u16,
    request_value: u32,
) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = Vec::with_capacity(6);
    tail.extend_from_slice(&0x1008u16.to_be_bytes());
    tail.extend_from_slice(&request_value.to_be_bytes());
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

fn build_conmon_export_request(
    mac: [u8; 6],
    sequence: u16,
    echoed_tag: [u8; 4],
    selector_value: u16,
) -> Result<Vec<u8>, NetaudioError> {
    if sequence == 0 {
        return Err(NetaudioError::InvalidSequence);
    }
    let mut tail = Vec::with_capacity(16);
    tail.extend_from_slice(&CONMON_EXPORT_MESSAGE_TYPE.to_be_bytes());
    tail.extend_from_slice(&0u32.to_be_bytes());
    tail.extend_from_slice(&echoed_tag);
    tail.extend_from_slice(&selector_value.to_be_bytes());
    tail.extend_from_slice(&0u16.to_be_bytes());
    settings_packet(sequence, mac, SETTINGS_SUFFIX_DIAGNOSTIC_EXPORT, &tail)
}

pub fn build_device_log_export(mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    build_conmon_export_request(
        mac,
        sequence,
        DIAGNOSTIC_LOG_EXPORT_TAG,
        DIAGNOSTIC_LOG_EXPORT_SELECTOR,
    )
}

pub fn build_capability_partition_export(
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_conmon_export_request(
        mac,
        sequence,
        CAPABILITY_PARTITION_EXPORT_TAG,
        CAPABILITY_PARTITION_EXPORT_SELECTOR,
    )
}

pub fn build_set_preferred_leader(
    is_preferred: bool,
    clock_source: u16,
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = vec![0x00, 0x21, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0002u16.to_be_bytes());
    tail.extend_from_slice(&clock_source.to_be_bytes());
    tail.push(if is_preferred { 0x01 } else { 0x00 });
    tail.extend(std::iter::repeat_n(0, 55));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_set_clock_source(
    clock_source: u16,
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = vec![0x00, 0x21, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0001u16.to_be_bytes());
    tail.extend_from_slice(&clock_source.to_be_bytes());
    tail.push(0x00);
    tail.extend(std::iter::repeat_n(0, 55));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_set_clock_subdomain(
    subdomain: [u8; 16],
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = vec![0x00, 0x21, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0008u16.to_be_bytes());
    tail.extend_from_slice(&0x0000u16.to_be_bytes());
    tail.push(0x00);
    tail.extend(std::iter::repeat_n(0, 3));
    tail.extend_from_slice(&subdomain);
    tail.extend(std::iter::repeat_n(0, 36));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_probe_preferred_leader(
    clock_source: u16,
    mac: [u8; 6],
    sequence: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut tail = vec![0x00, 0x21, 0x00, 0x00, 0x00, 0x64];
    tail.extend_from_slice(&0x0000u16.to_be_bytes());
    tail.extend_from_slice(&clock_source.to_be_bytes());
    tail.push(0x00);
    tail.extend(std::iter::repeat_n(0, 55));
    settings_packet(sequence, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_refresh_clock_status(mac: [u8; 6], sequence: u16) -> Result<Vec<u8>, NetaudioError> {
    build_probe_preferred_leader(0, mac, sequence)
}

pub fn build_bluetooth_status(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    let tail = [
        0x10, 0x0d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x0c, 0x0a, 0x0a, 0x10, 0x09, 0x1a,
        0x06, 0x0a, 0x04, 0x0a, 0x02, 0x08, 0x01,
    ];
    settings_packet(0x0000, mac, SETTINGS_SUFFIX_SYSTEM_CONFIG, &tail)
}

pub fn build_make_model(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    settings_packet(
        0x0FDB,
        mac,
        SETTINGS_SUFFIX_IDENTITY,
        &[0x00, 0xC1, 0x00, 0x00, 0x00, 0x00],
    )
}

pub fn build_dante_model(mac: [u8; 6]) -> Result<Vec<u8>, NetaudioError> {
    settings_packet(
        0x0FDB,
        mac,
        SETTINGS_SUFFIX_IDENTITY,
        &[0x00, 0x61, 0x00, 0x00, 0x00, 0x00],
    )
}

pub fn build_query_latency_config(transaction_id: u16) -> Result<Vec<u8>, NetaudioError> {
    protocol_packet(
        PROTOCOL_ARC_2809,
        OPCODE_DEVICE_SETTINGS,
        &LATENCY_CONFIG_QUERY_INFO_CODES,
        transaction_id,
    )
}

pub fn build_set_aes67_multicast_prefix(
    prefix: std::net::Ipv4Addr,
    transaction_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut body = Vec::with_capacity(10);
    body.extend_from_slice(&0x0101u16.to_be_bytes());
    body.extend_from_slice(&0x8060u16.to_be_bytes());
    body.extend_from_slice(&0x0010u16.to_be_bytes());
    body.extend_from_slice(&prefix.octets());
    protocol_packet(
        PROTOCOL_ARC_2809,
        OPCODE_DEVICE_SETTINGS_SET,
        &body,
        transaction_id,
    )
}
