use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClockBasePort {
    pub state_code: u16,
    pub state: Option<String>,
    pub unknown: u16,
    pub role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PtpClockPortRecord {
    pub record_flags: u16,
    pub user_disabled: Option<bool>,
    pub link_down: Option<bool>,
    pub unicast_delay_requests: Option<bool>,
    pub record_number: u16,
    pub ptp_version: u8,
    pub record_format_code: u8,
    pub transport_path_code: u8,
    pub transport_path: Option<String>,
    pub reserved_byte: u8,
    pub unknown_word: u32,
    pub network_interface_index: Option<u16>,
    pub interface_flags: Option<u16>,
    pub interface_record: Option<Vec<u8>>,
    pub state_code: u16,
    pub state: Option<String>,
    pub role: Option<String>,
    pub status_flags: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClockVector {
    pub offset: u16,
    pub count: u16,
    pub unknown: u16,
    pub first_record: u16,
    pub stride: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClockInterfaceVector {
    pub offset: u16,
    pub unknown_header_words: [u16; 2],
    pub first_record: u16,
    pub stride: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PtpClockStatus {
    pub record_revision: u16,
    pub raw_record: Vec<u8>,
    pub status_supported: bool,
    pub congestion_delay_microseconds: u32,
    pub clock_state_code: u16,
    pub clock_state: Option<String>,
    pub servo_state_code: u16,
    pub servo_state: Option<String>,
    pub synchronization: String,
    pub preferred_leader: Option<bool>,
    pub clock_source_code: u16,
    pub clock_source: Option<String>,
    pub stratum: u8,
    pub ptpv1_device_uuid: Option<[u8; 6]>,
    pub ptpv1_master_uuid: Option<[u8; 6]>,
    pub ptpv1_grandmaster_uuid: Option<[u8; 6]>,
    pub uuid_reserved: Vec<u8>,
    pub base_ports: Vec<ClockBasePort>,
    pub extension_offset: Option<u16>,
    pub clock_capabilities: Option<u16>,
    pub extension_unknown_word: Option<u16>,
    pub maximum_drift_parts_per_billion: Option<i32>,
    pub extension_unknown_byte: Option<u8>,
    pub domain_raw: Option<u8>,
    pub ptpv2_domain: Option<u8>,
    pub extension_flags: Option<u16>,
    pub preferred_leader_locked: Option<bool>,
    pub global_unicast_delay_requests: Option<bool>,
    pub aggregate_ptpv1_unicast_delay_requests: Option<bool>,
    pub clock_subdomain: Option<[u8; 16]>,
    pub mute_flags: Option<u16>,
    pub mute_reasons: Vec<String>,
    pub word_clock_state_code: Option<u16>,
    pub word_clock_state: Option<String>,
    pub descriptor_bytes: Vec<u8>,
    pub port_vector: Option<ClockVector>,
    pub interface_vector: Option<ClockInterfaceVector>,
    pub clock_frequency_offset_parts_per_billion: i32,
    pub clock_port_state_code: Option<u16>,
    pub clock_role: Option<String>,
    pub clock_port_records: Option<Vec<PtpClockPortRecord>>,
}

fn label(value: u16, labels: &[&str]) -> Option<String> {
    labels.get(usize::from(value)).map(|v| (*v).to_owned())
}
fn role(value: u16) -> Option<String> {
    match value {
        6 => Some("Leader".into()),
        9 => Some("Follower".into()),
        _ => None,
    }
}
fn port_label(value: u16) -> Option<String> {
    label(
        value,
        &[
            "startup",
            "initializing",
            "faulty",
            "disabled",
            "listening",
            "pre-master",
            "master",
            "passive",
            "uncalibrated",
            "follower",
            "passive-l",
        ],
    )
}

// Every interpreted region owns its bytes. Pointers cannot alias headers or other vectors.
fn claim(
    data: &[u8],
    used: &mut Vec<std::ops::Range<usize>>,
    start: usize,
    size: usize,
) -> Option<()> {
    let end = start.checked_add(size)?;
    data.get(start..end)?;
    if size != 0 && used.iter().any(|r| start < r.end && r.start < end) {
        return None;
    }
    used.push(start..end);
    Some(())
}
fn vector(data: &[u8], used: &mut Vec<std::ops::Range<usize>>, offset: u16) -> Option<ClockVector> {
    let p = usize::from(offset);
    if p == 0 {
        return None;
    }
    claim(data, used, p, 8)?;
    Some(ClockVector {
        offset,
        count: read_u16(data, p)?,
        unknown: read_u16(data, p + 2)?,
        first_record: read_u16(data, p + 4)?,
        stride: read_u16(data, p + 6)?,
    })
}

pub fn parse_ptp_clock_status(data: &[u8]) -> Option<PtpClockStatus> {
    validate_conmon_envelope(data, 0x0020)?;
    let r = data.get(24..)?;
    let rev = read_u16(r, 0)?;
    let clock_state_code = read_u16(r, 8)?;
    let servo_state_code = read_u16(r, 10)?;
    let source = read_u16(r, 12)?;
    let preferred = match *r.get(14)? {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    };
    let stratum = *r.get(15)?;
    let supported = rev == 0x0100 || rev >= 0x0200;
    let mut s = PtpClockStatus {
        record_revision: rev,
        raw_record: r.to_vec(),
        status_supported: supported,
        congestion_delay_microseconds: read_u32(r, 4)?,
        clock_state_code,
        clock_state: label(
            clock_state_code,
            &["none", "passive", "undisciplined", "disciplined"],
        ),
        servo_state_code,
        servo_state: label(
            servo_state_code,
            &[
                "faulty",
                "reset",
                "synchronizing",
                "synchronized",
                "unknown",
                "delay reset",
                "none",
            ],
        ),
        synchronization: "unknown".into(),
        preferred_leader: preferred,
        clock_source_code: source,
        clock_source: label(source, &["internal", "external/BNC", "AES"]),
        stratum,
        ptpv1_device_uuid: None,
        ptpv1_master_uuid: None,
        ptpv1_grandmaster_uuid: None,
        uuid_reserved: vec![],
        base_ports: vec![],
        extension_offset: None,
        clock_capabilities: None,
        extension_unknown_word: None,
        maximum_drift_parts_per_billion: None,
        extension_unknown_byte: None,
        domain_raw: None,
        ptpv2_domain: None,
        extension_flags: None,
        preferred_leader_locked: None,
        global_unicast_delay_requests: None,
        aggregate_ptpv1_unicast_delay_requests: None,
        clock_subdomain: None,
        mute_flags: None,
        mute_reasons: vec![],
        word_clock_state_code: None,
        word_clock_state: None,
        descriptor_bytes: vec![],
        port_vector: None,
        interface_vector: None,
        clock_frequency_offset_parts_per_billion: read_u32(r, 16)? as i32,
        clock_port_state_code: None,
        clock_role: None,
        clock_port_records: None,
    };
    if !supported {
        return Some(s);
    }
    let (count_at, ports_at) = if rev == 0x0100 {
        (0x14, 0x18)
    } else {
        s.ptpv1_device_uuid = Some(r.get(0x14..0x1a)?.try_into().ok()?);
        s.ptpv1_master_uuid = Some(r.get(0x1c..0x22)?.try_into().ok()?);
        s.ptpv1_grandmaster_uuid = Some(r.get(0x24..0x2a)?.try_into().ok()?);
        s.uuid_reserved = [r.get(0x1a..0x1c)?, r.get(0x22..0x24)?, r.get(0x2a..0x2c)?].concat();
        s.extension_offset = Some(read_u16(r, 0x2e)?);
        (0x2c, 0x30)
    };
    let count = usize::from(read_u16(r, count_at)?);
    let mut used = vec![];
    claim(r, &mut used, 0, ports_at + count.checked_mul(4)?)?;
    for i in 0..count {
        let p = ports_at + i * 4;
        let state = read_u16(r, p)?;
        s.base_ports.push(ClockBasePort {
            state_code: state,
            state: port_label(state),
            unknown: read_u16(r, p + 2)?,
            role: role(state),
        });
    }
    s.clock_port_state_code = s.base_ports.first().map(|p| p.state_code);
    s.clock_role = s.clock_port_state_code.and_then(role);
    s.synchronization = if servo_state_code == 3 {
        "synchronized"
    } else if clock_state_code == 3
        || (stratum == 255 && s.base_ports.iter().any(|p| p.state_code == 4))
    {
        "lost"
    } else if servo_state_code == 2 {
        "synchronizing"
    } else {
        "unknown"
    }
    .into();
    let e = usize::from(s.extension_offset.unwrap_or(0));
    if rev < 0x0400 || e == 0 || e >= r.len() {
        return Some(s);
    }
    let size = if rev >= 0x0726 {
        if read_u16(r, e + 38)? > 11 {
            44
        } else {
            40
        }
    } else if rev >= 0x071c {
        36
    } else if rev >= 0x0705 {
        32
    } else if rev >= 0x0704 {
        30
    } else {
        28
    };
    claim(r, &mut used, e, size)?;
    s.clock_capabilities = Some(read_u16(r, e)?);
    s.extension_unknown_word = Some(read_u16(r, e + 2)?);
    s.maximum_drift_parts_per_billion = Some(read_u32(r, e + 4)? as i32);
    s.extension_unknown_byte = Some(r[e + 8]);
    s.domain_raw = Some(r[e + 9]);
    let flags = read_u16(r, e + 10)?;
    s.extension_flags = Some(flags);
    s.preferred_leader_locked = (rev >= 0x072e).then_some(flags & 0x1000 != 0);
    s.ptpv2_domain = (rev >= 0x0728 && flags & 0x0800 != 0).then_some(r[e + 9]);
    s.global_unicast_delay_requests = (0x0603..0x071f).contains(&rev).then_some(flags & 1 != 0);
    let subdomain = r.get(e + 12..e + 28)?;
    if !subdomain.contains(&0) {
        return None;
    }
    s.clock_subdomain = Some(subdomain.try_into().ok()?);
    if rev >= 0x0704 {
        let mute = read_u16(r, e + 28)?;
        s.mute_flags = Some(mute);
        for (bit, name) in [
            (1, "synchronization loss"),
            (2, "external-clock problem"),
            (4, "internal-clock problem"),
            (8, "PTP state unknown"),
            (16, "user mute"),
        ] {
            if mute & bit != 0 {
                s.mute_reasons.push(name.into());
            }
        }
    }
    if rev >= 0x0705 {
        let word = read_u16(r, e + 30)?;
        s.word_clock_state_code = Some(word);
        s.word_clock_state = label(word, &["unknown", "none", "invalid", "valid", "missing"]);
    }
    if rev < 0x071c {
        return Some(s);
    }
    s.descriptor_bytes = r[e + 32..e + size].to_vec();
    if read_u16(r, e + 34)? == 0 {
        return Some(s);
    }
    let v = vector(r, &mut used, read_u16(r, e + 32)?)?;
    // The retained 0x0738 fixture encodes 16-byte geometry as 0x1000 metadata.
    if v.stride != 16 && !(rev >= 0x0726 && v.stride == 0x1000) {
        return None;
    }
    let start = usize::from(v.first_record);
    let count = usize::from(v.count);
    claim(r, &mut used, start, count.checked_mul(16)?)?;
    let mut ports = Vec::with_capacity(count);
    for i in 0..count {
        let p = start + i * 16;
        let values = read_u16(r, p)?;
        let valid = read_u16(r, p + 14)?;
        let available = |bit| (rev < 0x071f || valid & bit != 0).then_some(values & bit != 0);
        let state = read_u16(r, p + 12)?;
        let path = r[p + 6];
        ports.push(PtpClockPortRecord {
            record_flags: values,
            user_disabled: available(1),
            link_down: available(2),
            unicast_delay_requests: (rev >= 0x071f && valid & 4 != 0).then_some(values & 4 != 0),
            record_number: read_u16(r, p + 2)?,
            ptp_version: r[p + 4],
            record_format_code: r[p + 5],
            transport_path_code: path,
            transport_path: label(u16::from(path), &["unknown", "multicast", "unicast"]),
            reserved_byte: r[p + 7],
            unknown_word: read_u32(r, p + 8)?,
            network_interface_index: None,
            interface_flags: None,
            interface_record: None,
            state_code: state,
            state: port_label(state),
            role: role(state),
            status_flags: valid,
        });
    }
    s.port_vector = Some(v);
    if rev >= 0x0726 && read_u16(r, e + 38)? > 11 && read_u16(r, e + 42)? != 0 {
        let v3 = vector(r, &mut used, read_u16(r, e + 40)?)?;
        let stride = usize::from(v3.stride);
        if stride < 4 {
            return None;
        }
        let start = usize::from(v3.first_record);
        claim(r, &mut used, start, count.checked_mul(stride)?)?;
        for (i, port) in ports.iter_mut().enumerate() {
            let p = start + i * stride;
            let flags = read_u16(r, p)?;
            port.interface_flags = Some(flags);
            port.interface_record = Some(r[p..p + stride].to_vec());
            port.network_interface_index = (flags & 1 != 0).then(|| read_u16(r, p + 2)).flatten();
        }
        s.interface_vector = Some(ClockInterfaceVector {
            offset: v3.offset,
            unknown_header_words: [v3.count, v3.unknown],
            first_record: v3.first_record,
            stride: v3.stride,
        });
    }
    let ptpv1: Vec<_> = ports.iter().filter(|p| p.ptp_version == 1).collect();
    if !ptpv1.is_empty() && ptpv1.iter().all(|p| p.unicast_delay_requests.is_some()) {
        let first = ptpv1[0].unicast_delay_requests;
        if ptpv1.iter().all(|p| p.unicast_delay_requests == first) {
            s.aggregate_ptpv1_unicast_delay_requests = first;
        }
    }
    s.clock_port_records = Some(ports);
    Some(s)
}
