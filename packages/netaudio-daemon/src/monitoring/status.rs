use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use netaudio_core::responses;
use serde::Serialize;
use serde_json::{Map, Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusKind {
    Topology,
    TransmitFlows,
    Latency,
    MulticastPrefix,
    SampleRate,
    Encoding,
    Pullup,
    Aes67,
    ClearConfiguration,
    Lock,
    RoutingCapacity,
    Manufacturer,
    Platform,
    Clock,
    Interface,
    InterfaceStatistics,
    SwitchConfiguration,
    Codec,
    ClockDiagnostics,
}

impl StatusKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Topology => "topology",
            Self::TransmitFlows => "transmitter_flows",
            Self::Latency => "latency",
            Self::MulticastPrefix => "aes67_multicast_prefix",
            Self::SampleRate => "sample_rate",
            Self::Encoding => "encoding",
            Self::Pullup => "sample_rate_pullup",
            Self::Aes67 => "aes67",
            Self::ClearConfiguration => "clear_configuration",
            Self::Lock => "lock_status",
            Self::RoutingCapacity => "routing_capacity",
            Self::Manufacturer => "make_model",
            Self::Platform => "dante_model",
            Self::Clock => "clock_status",
            Self::Interface => "interface",
            Self::InterfaceStatistics => "interface_statistics",
            Self::SwitchConfiguration => "switch_configuration",
            Self::Codec => "codec",
            Self::ClockDiagnostics => "clock_diagnostics",
        }
    }
}

pub struct StatusObservation {
    pub kind: StatusKind,
    pub fields: Map<String, Value>,
}

pub fn decode(
    kind: &str,
    payload: &[u8],
    previous: &Map<String, Value>,
    received_at: DateTime<Utc>,
) -> Result<Option<StatusObservation>> {
    let (kind, fields) = match kind {
        "sample_rate_status" => (
            StatusKind::SampleRate,
            audio_fields(
                parsed(responses::parse_sample_rate_status(payload), kind)?,
                "sample_rate",
            ),
        ),
        "encoding_status" => (
            StatusKind::Encoding,
            audio_fields(
                parsed(responses::parse_encoding_status(payload), kind)?,
                "encoding",
            ),
        ),
        "sample_rate_pullup_status" => (
            StatusKind::Pullup,
            audio_fields(
                parsed(responses::parse_sample_rate_pullup_status(payload), kind)?,
                "sample_rate_pullup",
            ),
        ),
        "aes67_status" => (
            StatusKind::Aes67,
            parsed(responses::parse_aes67_status(payload), kind)?
                .into_iter()
                .filter(|(_, value)| !value.is_null())
                .collect(),
        ),
        "clear_configuration_status" => (
            StatusKind::ClearConfiguration,
            Map::from_iter([(
                "clear_configuration_status".to_owned(),
                Value::Object(parsed(
                    responses::parse_clear_configuration_status(payload),
                    kind,
                )?),
            )]),
        ),
        "lock_reset_status" => {
            let parsed = parsed(responses::parse_lock_reset_status(payload), kind)?;
            (
                StatusKind::Lock,
                Map::from_iter([
                    (
                        "is_locked".to_owned(),
                        parsed.get("is_locked").cloned().unwrap_or(Value::Null),
                    ),
                    ("lock_reset_status".to_owned(), Value::Object(parsed)),
                    (
                        "lock_status_observed_at".to_owned(),
                        json!(received_at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
                    ),
                ]),
            )
        }
        "routing_capacity_status" => {
            let parsed = parsed(responses::parse_routing_capacity_status(payload), kind)?;
            (
                StatusKind::RoutingCapacity,
                renamed(
                    &parsed,
                    &[
                        (
                            "receive_channel_count",
                            "routing_capacity_receive_channel_count",
                        ),
                        (
                            "transmit_channel_count",
                            "routing_capacity_transmit_channel_count",
                        ),
                        ("routing_ready", "routing_ready"),
                        ("routing_ready_code", "routing_ready_code"),
                        ("link_status", "routing_link_status"),
                    ],
                ),
            )
        }
        "make_model" => {
            let parsed = parsed(responses::parse_make_model(payload), kind)?;
            let mut fields = selected(
                &parsed,
                &[
                    "product_name",
                    "product_version",
                    "friendly_product_version",
                    "manufacturer_software_version",
                    "manufacturer_firmware_version",
                    "manufacturer",
                ],
            );
            fields.retain(|_, value| !value.is_null() && value.as_str() != Some(""));
            fields.insert(
                "manufacturer_versions_record".to_owned(),
                Value::Object(parsed),
            );
            (StatusKind::Manufacturer, fields)
        }
        "dante_model" => {
            let parsed = parsed(responses::parse_dante_model(payload), kind)?;
            let mut fields = selected(
                &parsed,
                &[
                    "platform_model_identifier",
                    "platform_model_name",
                    "platform_software_version",
                    "platform_hardware_version",
                    "platform_api_version",
                    "rom_boot_version",
                    "identify_supported",
                    "sample_rate_configuration_supported",
                    "encoding_configuration_supported",
                    "sample_rate_pullup_configuration_supported",
                    "switch_redundancy_supported",
                    "static_ipv4_configuration_supported",
                    "device_locking_supported",
                    "external_word_clock_read_only",
                    "switch_redundancy_read_only",
                    "static_ipv4_configuration_read_only",
                    "generic_codec_control_supported",
                    "detailed_metering_supported",
                    "interface_statistics_supported",
                    "clock_monitoring_supported",
                    "per_channel_signal_presence_supported",
                    "rx_flow_maximum_latency_monitoring_supported",
                    "rx_flow_late_packet_monitoring_supported",
                    "aes67_configuration_supported",
                    "virtual_panel_supported",
                    "video_transmission_supported",
                    "video_reception_supported",
                ],
            );

            for property in [
                "record_protocol_version",
                "primary_capabilities",
                "read_only_capabilities",
                "monitoring_capabilities",
                "secondary_capabilities",
                "domain_capability_values",
                "domain_capability_validity",
            ] {
                fields.insert(
                    format!("dante_model_{property}"),
                    parsed.get(property).cloned().unwrap_or(Value::Null),
                );
            }

            let opcode = responses::parse_notification_envelope(payload)
                .context("platform status has no notification envelope")?
                .notification_id;
            let source = json!({
                "kind": "conmon_dante_model",
                "opcode": opcode,
                "record_protocol_version": parsed.get("record_protocol_version"),
                "observed_at_unix": received_at.timestamp_micros() as f64 / 1_000_000.0,
                "fresh": true,
            });
            let mut support = source.clone();
            support["field_reported"] = json!(
                parsed
                    .get("switch_redundancy_supported")
                    .is_some_and(|value| !value.is_null())
            );
            fields.insert("redundancy_advertised_support_source".to_owned(), support);
            let mut read_only = source;
            let reported = parsed
                .get("switch_redundancy_read_only")
                .is_some_and(|value| !value.is_null());
            read_only["field_reported"] = json!(reported);
            read_only["field_applicable"] = json!(reported);

            if !reported {
                read_only["unavailable_reason"] = json!("record_protocol_version");
            }

            fields.insert("redundancy_read_only_source".to_owned(), read_only);
            fields.insert("platform_versions_record".to_owned(), Value::Object(parsed));
            (StatusKind::Platform, fields)
        }
        "ptp_clock_status" => {
            let parsed = parsed(responses::parse_ptp_clock_status(payload), kind)?;
            let mut fields = selected(
                &parsed,
                &[
                    "clock_frequency_offset_parts_per_billion",
                    "clock_port_records",
                    "clock_port_state_code",
                    "clock_role",
                    "clock_source_code",
                    "preferred_leader",
                    "ptpv1_device_uuid",
                    "ptpv1_master_uuid",
                    "ptpv1_grandmaster_uuid",
                    "clock_subdomain",
                ],
            );
            for field in [
                "ptpv1_device_uuid",
                "ptpv1_master_uuid",
                "ptpv1_grandmaster_uuid",
            ] {
                let identity =
                    netaudio_core::device_identity::normalize(serde_json::from_value(json!({
                        "kind": "ptpv1",
                        "value": parsed.get(field),
                    }))?);
                fields.insert(field.to_owned(), json!(identity));
            }

            fields.insert("clock_status".to_owned(), Value::Object(parsed));
            fields.insert(
                "clock_observed_at".to_owned(),
                json!(received_at.to_rfc3339()),
            );
            (StatusKind::Clock, fields)
        }
        "interface_status" => {
            let parsed = parsed(responses::parse_interface_status(payload), kind)?;
            let mut fields = renamed(
                &parsed,
                &[
                    ("record_protocol_identifier", "interface_status_protocol"),
                    ("reboot_required", "interface_reboot_required"),
                    ("interfaces", "interfaces"),
                    ("link_speed_mbps", "link_speed_mbps"),
                ],
            );

            let observation = serde_json::from_value(json!({
                "flags": parsed.get("redundancy_flags"),
                "previous": previous.get("dante_redundancy"),
                "record_protocol_identifier": parsed.get("record_protocol_identifier"),
                "raw_record_hexadecimal": parsed.get("raw_record_hexadecimal"),
                "observed_at_unix": received_at.timestamp_micros() as f64 / 1_000_000.0,
            }))?;

            if let Some(redundancy) =
                netaudio_core::network::interface_redundancy_status(observation)
            {
                fields.insert(
                    "dante_redundancy".to_owned(),
                    serde_json::to_value(redundancy)?,
                );
            } else if let Some(mut redundancy) = previous.get("dante_redundancy").cloned() {
                redundancy["state_fresh"] = json!(false);
                fields.insert("dante_redundancy".to_owned(), redundancy);
            }

            (StatusKind::Interface, fields)
        }
        "switch_configuration_status" => {
            let parsed = parsed(responses::parse_switch_configuration_status(payload), kind)?;
            let mut state = parsed
                .get("state")
                .cloned()
                .context("switch status has no state")?;
            state["state_source"]["observed_at_unix"] =
                json!(received_at.timestamp_micros() as f64 / 1_000_000.0);
            (
                StatusKind::SwitchConfiguration,
                Map::from_iter([("dante_redundancy".to_owned(), state)]),
            )
        }
        "codec_status" => {
            let parsed = parsed(responses::parse_codec_status(payload), kind)?;
            let adapter = parsed.get("gain_adapter").cloned().unwrap_or(Value::Null);
            let fields = Map::from_iter([
                (
                    "codec_observed_at".to_owned(),
                    json!(received_at.timestamp_micros() as f64 / 1_000_000.0),
                ),
                (
                    "codec_parameters".to_owned(),
                    parsed
                        .get("parameters")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                ),
                (
                    "gain_device_type".to_owned(),
                    adapter.get("device_type").cloned().unwrap_or(Value::Null),
                ),
                (
                    "gain_levels".to_owned(),
                    adapter
                        .get("channel_levels")
                        .cloned()
                        .unwrap_or(Value::Null),
                ),
                (
                    "supported_gain_levels".to_owned(),
                    adapter
                        .get("supported_levels")
                        .cloned()
                        .unwrap_or(Value::Null),
                ),
                ("gain_adapter".to_owned(), adapter),
                ("codec_status".to_owned(), Value::Object(parsed)),
            ]);
            (StatusKind::Codec, fields)
        }
        "clock_master_status" | "clock_unicast_status" | "clock_identifier_status" => {
            let observation = match kind {
                "clock_master_status" => {
                    parsed(responses::parse_clock_master_status(payload), kind)?
                }
                "clock_unicast_status" => {
                    parsed(responses::parse_clock_unicast_status(payload), kind)?
                }
                _ => parsed(responses::parse_clock_identifier_status(payload), kind)?,
            };
            let mut diagnostics = previous
                .get("clock_diagnostics")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            diagnostics.insert(kind.to_owned(), Value::Object(observation));
            (
                StatusKind::ClockDiagnostics,
                Map::from_iter([("clock_diagnostics".to_owned(), Value::Object(diagnostics))]),
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(StatusObservation { kind, fields }))
}

fn parsed<T: Serialize>(value: Option<T>, kind: &str) -> Result<Map<String, Value>> {
    let value = value.with_context(|| format!("malformed {kind} status"))?;
    serde_json::to_value(value)?
        .as_object()
        .cloned()
        .context("status decoder returned a non-object")
}

fn audio_fields(status: Map<String, Value>, kind: &str) -> Map<String, Value> {
    let (current, requested, supported) = match kind {
        "sample_rate" => (
            "sample_rate_hz",
            "requested_sample_rate",
            "supported_sample_rates_hz",
        ),
        "encoding" => ("encoding", "requested_encoding", "supported_encodings"),
        _ => (
            "sample_rate_pullup_raw_value",
            "requested_sample_rate_pullup_raw_value",
            "supported_sample_rate_pullup_raw_values",
        ),
    };
    let mut fields = renamed(
        &status,
        &[
            ("current_value", current),
            ("requested_value", requested),
            ("available_values", supported),
        ],
    );
    fields.insert(
        format!("{kind}_update_mode"),
        status.get("update_mode").cloned().unwrap_or(Value::Null),
    );

    if kind == "sample_rate_pullup" {
        fields.extend(renamed(
            &status,
            &[
                ("flags", "sample_rate_pullup_flags"),
                ("host_disabled", "sample_rate_pullup_host_disabled"),
            ],
        ));
    }

    fields
}

fn selected(source: &Map<String, Value>, fields: &[&str]) -> Map<String, Value> {
    fields
        .iter()
        .filter_map(|field| {
            source
                .get(*field)
                .map(|value| ((*field).to_owned(), value.clone()))
        })
        .collect()
}

fn renamed(source: &Map<String, Value>, fields: &[(&str, &str)]) -> Map<String, Value> {
    fields
        .iter()
        .map(|(source_field, destination)| {
            (
                (*destination).to_owned(),
                source.get(*source_field).cloned().unwrap_or(Value::Null),
            )
        })
        .collect()
}
