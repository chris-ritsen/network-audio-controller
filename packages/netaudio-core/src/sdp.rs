use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SdpConnection {
    pub network_type: String,
    pub address_type: String,
    pub address: String,
    pub time_to_live: Option<u64>,
    pub address_count: Option<u64>,
    pub raw_value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SdpRtpMap {
    pub payload_type: u64,
    pub encoding: String,
    pub sample_rate: u64,
    pub channels: u64,
    pub raw_value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SdpMediaDescription {
    pub media_type: String,
    pub port: Option<u64>,
    pub port_count: Option<u64>,
    pub protocol: String,
    pub payload_types: Vec<u64>,
    pub information: Option<String>,
    pub connections: Vec<SdpConnection>,
    pub attributes: Vec<String>,
    pub raw_value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SdpRoutableAudio {
    pub media_title: Option<String>,
    pub primary_destination_address: String,
    pub secondary_destination_address: Option<String>,
    pub destination_port: u64,
    pub payload_type: u64,
    pub encoding: String,
    pub sample_rate: u64,
    pub channel_count: u64,
    pub packet_time_microseconds: Option<u64>,
    pub direction: Option<String>,
    pub media_clock: Option<String>,
    pub clock_offset: Option<u64>,
    pub ptp_reference: Option<String>,
    pub ptp_domain_token: Option<String>,
    pub dante_origin: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SdpDocument {
    pub version: u64,
    pub origin_username: String,
    pub session_id: u64,
    pub session_version: u64,
    pub origin_network_type: String,
    pub origin_address_type: String,
    pub origin_address: String,
    pub session_name: String,
    pub session_information: Option<String>,
    pub start_time: u64,
    pub stop_time: u64,
    pub session_connections: Vec<SdpConnection>,
    pub session_attributes: Vec<String>,
    pub media_descriptions: Vec<SdpMediaDescription>,
    pub rtp_maps: Vec<SdpRtpMap>,
    pub routable_audio: Option<SdpRoutableAudio>,
    pub routability_errors: Vec<String>,
    pub unknown_lines: Vec<String>,
    pub raw_sdp: String,
    pub routable: bool,
}

fn unsigned(value: &str, description: &str, maximum: u64) -> Result<u64, String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("{description} must be an unsigned decimal integer"));
    }

    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value <= maximum)
        .ok_or_else(|| format!("{description} exceeds its supported range"))
}

fn connection(value: &str) -> Result<SdpConnection, String> {
    let fields: Vec<_> = value.split_whitespace().collect();

    if fields.len() != 3 {
        return Err("c= must contain network type, address type, and address".into());
    }

    let parts: Vec<_> = fields[2].split('/').collect();

    if parts.len() > 3 || parts[0].is_empty() {
        return Err("c= has an invalid connection address".into());
    }

    let ttl = parts
        .get(1)
        .map(|value| unsigned(value, "c= time to live", 255))
        .transpose()?;
    let count = parts
        .get(2)
        .map(|value| unsigned(value, "c= address count", 65535))
        .transpose()?;

    if count == Some(0) {
        return Err("c= address count must be positive".into());
    }

    let address = if fields[1].eq_ignore_ascii_case("IP4") {
        parts[0]
            .parse::<Ipv4Addr>()
            .map_err(|_| "c= contains an invalid IPv4 address")?
            .to_string()
    } else {
        parts[0].to_owned()
    };

    Ok(SdpConnection {
        network_type: fields[0].into(),
        address_type: fields[1].into(),
        address,
        time_to_live: ttl,
        address_count: count,
        raw_value: value.into(),
    })
}

fn media(value: &str) -> Result<SdpMediaDescription, String> {
    let fields: Vec<_> = value.split_whitespace().collect();

    if fields.len() < 4 {
        return Err("m= must contain media, port, protocol, and a payload type".into());
    }

    let ports: Vec<_> = fields[1].split('/').collect();

    if ports.len() > 2 {
        return Err("m= has an invalid port field".into());
    }

    let numbers = (|| {
        let port = unsigned(ports[0], "m= port", 65535)?;
        let count = ports
            .get(1)
            .map(|value| unsigned(value, "m= port count", 65535))
            .transpose()?;
        let payloads = fields[3..]
            .iter()
            .map(|value| unsigned(value, "m= payload type", 127))
            .collect::<Result<Vec<_>, String>>()?;
        Ok::<_, String>((Some(port), count, payloads))
    })();
    let (port, port_count, payload_types) = numbers.unwrap_or((None, None, Vec::new()));

    Ok(SdpMediaDescription {
        media_type: fields[0].into(),
        port,
        port_count,
        protocol: fields[2].into(),
        payload_types,
        information: None,
        connections: Vec::new(),
        attributes: Vec::new(),
        raw_value: value.into(),
    })
}

fn attribute<'a>(attributes: &'a [String], name: &str) -> Option<&'a str> {
    attributes.iter().find_map(|value| {
        if value == name {
            return Some("");
        }
        value
            .split_once(':')
            .filter(|(key, _)| *key == name)
            .map(|(_, value)| value)
    })
}

fn inherited<'a>(media: &'a [String], session: &'a [String], name: &str) -> Option<&'a str> {
    attribute(media, name).or_else(|| attribute(session, name))
}

fn rtp_maps(attributes: &[String]) -> Result<Vec<SdpRtpMap>, String> {
    let mut maps = Vec::new();

    for attribute in attributes {
        let Some((name, value)) = attribute.split_once(':') else {
            continue;
        };

        if !name.eq_ignore_ascii_case("rtpmap") {
            continue;
        }

        let Some((payload, encoding)) = value.trim_start().split_once(char::is_whitespace) else {
            return Err("a=rtpmap must contain a payload type and encoding".into());
        };
        let parts: Vec<_> = encoding.trim_start().split('/').collect();

        if !(2..=3).contains(&parts.len()) || parts[0].is_empty() {
            return Err("a=rtpmap has an invalid encoding".into());
        }

        maps.push(SdpRtpMap {
            payload_type: unsigned(payload, "a=rtpmap payload type", 127)?,
            encoding: parts[0].to_uppercase(),
            sample_rate: unsigned(parts[1], "a=rtpmap sample rate", u64::from(u32::MAX))?,
            channels: parts
                .get(2)
                .map(|value| unsigned(value, "a=rtpmap channel count", 65535))
                .transpose()?
                .unwrap_or(1),
            raw_value: value.into(),
        });
    }

    Ok(maps)
}

// Decimal scaling stays integral: sub-microsecond values must not round into a valid packet time.
fn packet_time(value: &str) -> Result<u64, String> {
    let invalid = || "a=ptime must resolve to a positive whole number of microseconds".to_owned();
    let value = value.trim().strip_prefix('+').unwrap_or(value.trim());
    let (mantissa, exponent) = match value.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().map_err(|_| invalid())?),
        None => (value, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");

    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }

    let digits = digits.trim_start_matches('0');

    if digits.is_empty() {
        return Err(invalid());
    }

    let scale = i64::from(exponent) + 3 - i64::try_from(fraction.len()).map_err(|_| invalid())?;
    let scaled = if scale >= 0 {
        if digits.len() as i64 + scale > 10 {
            return Err("a=ptime exceeds its supported range".into());
        }
        format!("{digits}{}", "0".repeat(scale as usize))
    } else {
        let remove = usize::try_from(-scale).map_err(|_| invalid())?;

        if remove >= digits.len()
            || !digits[digits.len() - remove..]
                .bytes()
                .all(|byte| byte == b'0')
        {
            return Err(invalid());
        }

        digits[..digits.len() - remove].to_owned()
    };

    unsigned(&scaled, "a=ptime", u64::from(u32::MAX))
}

fn direction(attributes: &[String]) -> Result<Option<String>, String> {
    let mut directions = attributes.iter().filter(|value| {
        matches!(
            value.as_str(),
            "sendrecv" | "sendonly" | "recvonly" | "inactive"
        )
    });
    let direction = directions.next().cloned();

    if directions.next().is_some() {
        return Err("SDP scope contains more than one direction attribute".into());
    }

    Ok(direction)
}

fn routable_audio(
    media: &SdpMediaDescription,
    session_connections: &[SdpConnection],
    session_attributes: &[String],
    maps: &[SdpRtpMap],
) -> Result<(Option<SdpRoutableAudio>, Vec<String>), String> {
    let mut errors = Vec::new();

    if media.port.is_none_or(|port| port == 0 || port > 65535) {
        errors.push("audio media port is not a valid UDP port".into());
    }

    if !["RTP/AVP", "RTP/AVPF"]
        .iter()
        .any(|protocol| media.protocol.eq_ignore_ascii_case(protocol))
    {
        errors.push("audio media protocol is not supported RTP over UDP".into());
    }

    let connections = if media.connections.is_empty() {
        session_connections
    } else {
        &media.connections
    };
    let mut addresses = Vec::new();

    for connection in connections {
        if !connection.network_type.eq_ignore_ascii_case("IN")
            || !connection.address_type.eq_ignore_ascii_case("IP4")
        {
            continue;
        }

        let address = connection
            .address
            .parse::<Ipv4Addr>()
            .map_err(|_| "invalid IPv4 connection")?;

        if !address.is_unspecified()
            && !address.is_loopback()
            && !address.is_broadcast()
            && !addresses.contains(&connection.address)
        {
            addresses.push(connection.address.clone());
        }
    }

    if addresses.is_empty() {
        errors.push("audio media has no usable IPv4 connection address".into());
    }

    let supported: BTreeMap<_, _> = maps
        .iter()
        .filter(|map| {
            matches!(map.encoding.as_str(), "L16" | "L24" | "L32")
                && map.sample_rate > 0
                && map.channels > 0
        })
        .map(|map| (map.payload_type, map))
        .collect();
    let selected = media
        .payload_types
        .iter()
        .find_map(|payload| supported.get(payload));

    if selected.is_none() {
        errors.push("audio media has no matching L16, L24, or L32 rtpmap".into());
    }

    let packet_time_microseconds = inherited(&media.attributes, session_attributes, "ptime")
        .map(packet_time)
        .transpose()?;
    let direction = match direction(&media.attributes)? {
        Some(value) => Some(value),
        None => direction(session_attributes)?,
    };
    let media_clock =
        inherited(&media.attributes, session_attributes, "mediaclk").map(str::to_owned);
    let clock_offset = media_clock
        .as_deref()
        .and_then(|value| {
            value
                .split_whitespace()
                .find_map(|token| token.strip_prefix("direct="))
        })
        .map(|value| unsigned(value, "a=mediaclk direct offset", u64::from(u32::MAX)))
        .transpose()?;
    let ptp_reference =
        inherited(&media.attributes, session_attributes, "ts-refclk").map(str::to_owned);
    let ptp_domain_token = ptp_reference
        .as_deref()
        .filter(|value| value.to_ascii_lowercase().starts_with("ptp="))
        .and_then(|value| {
            let parts: Vec<_> = value.split(':').collect();
            (parts.len() >= 3)
                .then(|| parts.last().copied())
                .flatten()
                .filter(|value| !value.is_empty())
        })
        .map(str::to_owned);
    let dante_origin = session_attributes
        .iter()
        .chain(&media.attributes)
        .any(|value| {
            let name = value.split(':').next().unwrap_or_default();
            name.eq_ignore_ascii_case("dante") || name.eq_ignore_ascii_case("x-dante")
        });

    if !errors.is_empty() {
        return Ok((None, errors));
    }

    let selected = selected.expect("missing map produces a routability error");
    Ok((
        Some(SdpRoutableAudio {
            media_title: media.information.clone(),
            primary_destination_address: addresses[0].clone(),
            secondary_destination_address: addresses.get(1).cloned(),
            destination_port: media
                .port
                .expect("missing port produces a routability error"),
            payload_type: selected.payload_type,
            encoding: selected.encoding.clone(),
            sample_rate: selected.sample_rate,
            channel_count: selected.channels,
            packet_time_microseconds,
            direction,
            media_clock,
            clock_offset,
            ptp_reference,
            ptp_domain_token,
            dante_origin,
        }),
        errors,
    ))
}

pub fn parse(raw_sdp: &str) -> Result<SdpDocument, String> {
    if raw_sdp.contains('\0') {
        return Err("SDP must not contain NUL bytes".into());
    }

    let normalized = raw_sdp.replace("\r\n", "\n").replace('\r', "\n");
    let lines: Vec<_> = normalized.trim_end_matches('\n').split('\n').collect();
    let mut singletons = BTreeMap::new();
    let mut session_information = None;
    let mut session_connections = Vec::new();
    let mut session_attributes = Vec::new();
    let mut media_descriptions: Vec<SdpMediaDescription> = Vec::new();
    let mut unknown_lines = Vec::new();

    for line in lines {
        if line.len() < 2 || line.as_bytes()[1] != b'=' || !line.as_bytes()[0].is_ascii_alphabetic()
        {
            return Err("SDP contains a malformed line".into());
        }

        let value = &line[2..];

        match line.as_bytes()[0] {
            kind @ (b'v' | b'o' | b's' | b't') => {
                if singletons.insert(kind, value).is_some() {
                    return Err(format!(
                        "SDP contains more than one {}= line",
                        char::from(kind)
                    ));
                }
            }
            b'm' => media_descriptions.push(media(value)?),
            b'c' => match media_descriptions.last_mut() {
                Some(media) => media.connections.push(connection(value)?),
                None => session_connections.push(connection(value)?),
            },
            b'i' => {
                let target = match media_descriptions.last_mut() {
                    Some(media) => &mut media.information,
                    None => &mut session_information,
                };

                if target.replace(value.to_owned()).is_some() {
                    return Err("SDP contains more than one i= line in a scope".into());
                }
            }
            b'a' => match media_descriptions.last_mut() {
                Some(media) => media.attributes.push(value.into()),
                None => session_attributes.push(value.into()),
            },
            _ => unknown_lines.push(line.to_owned()),
        }
    }

    let missing: Vec<_> = (*b"vost")
        .into_iter()
        .filter(|kind| !singletons.contains_key(kind))
        .map(|kind| format!("{}=", char::from(kind)))
        .collect();

    if !missing.is_empty() {
        return Err(format!(
            "SDP is missing required lines: {}",
            missing.join(", ")
        ));
    }

    let version = unsigned(singletons[&b'v'], "v= value", 255)?;

    if version != 0 {
        return Err("only SDP version 0 is supported".into());
    }

    let origin: Vec<_> = singletons[&b'o'].split_whitespace().collect();

    if origin.len() != 6 {
        return Err("o= must contain six fields".into());
    }

    let session_id = unsigned(origin[1], "o= session ID", u64::MAX)?;
    let session_version = unsigned(origin[2], "o= session version", u64::MAX)?;
    let timing: Vec<_> = singletons[&b't'].split_whitespace().collect();

    if timing.len() != 2 {
        return Err("t= must contain start and stop times".into());
    }

    let start_time = unsigned(timing[0], "t= start time", u64::MAX)?;
    let stop_time = unsigned(timing[1], "t= stop time", u64::MAX)?;
    let mut rtp_maps_all = Vec::new();
    let mut routable_audio_result = None;
    let mut routability_errors = Vec::new();
    let mut audio_found = false;

    for media in &media_descriptions {
        let attributes: Vec<_> = session_attributes
            .iter()
            .chain(&media.attributes)
            .cloned()
            .collect();
        let maps = rtp_maps(&attributes)?;

        for map in &maps {
            if !rtp_maps_all.contains(map) {
                rtp_maps_all.push(map.clone());
            }
        }

        if media.media_type.eq_ignore_ascii_case("audio") {
            audio_found = true;

            if routable_audio_result.is_none() {
                let (candidate, errors) =
                    routable_audio(media, &session_connections, &session_attributes, &maps)?;

                if candidate.is_some() {
                    routable_audio_result = candidate;
                    routability_errors.clear();
                } else {
                    for error in errors {
                        if !routability_errors.contains(&error) {
                            routability_errors.push(error);
                        }
                    }
                }
            }
        }
    }

    if !audio_found {
        routability_errors.push("SDP has no audio media description".into());
    }

    if session_id == 0 {
        routable_audio_result = None;
        routability_errors.push("SDP session ID is zero".into());
    }

    if singletons[&b's'].is_empty() {
        routable_audio_result = None;
        routability_errors.push("SDP session name is empty".into());
    }

    let inactive = routable_audio_result
        .as_ref()
        .is_some_and(|audio| audio.direction.as_deref() == Some("inactive"));

    if inactive {
        routability_errors.push("audio media direction is inactive".into());
    }

    Ok(SdpDocument {
        version,
        origin_username: origin[0].into(),
        session_id,
        session_version,
        origin_network_type: origin[3].into(),
        origin_address_type: origin[4].into(),
        origin_address: origin[5].into(),
        session_name: singletons[&b's'].into(),
        session_information,
        start_time,
        stop_time,
        session_connections,
        session_attributes,
        media_descriptions,
        rtp_maps: rtp_maps_all,
        routable: routable_audio_result.is_some() && !inactive,
        routable_audio: routable_audio_result,
        routability_errors,
        unknown_lines,
        raw_sdp: raw_sdp.into(),
    })
}
