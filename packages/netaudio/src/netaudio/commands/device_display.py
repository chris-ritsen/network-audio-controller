from __future__ import annotations

from fnmatch import fnmatch

from netaudio.dante.clock_config import format_clock_subdomain
from netaudio.dante.const import BLUETOOTH_MODEL_IDS
from netaudio.dante.latency import standard_latency_choices_for_range
from netaudio.dante.sample_rate_pullup import (
    format_supported_sample_rate_pullup_values,
    sample_rate_pullup_label,
)


def _format_mac(mac: str) -> str:
    if not mac:
        return ""
    raw = mac.replace(":", "").replace("-", "").upper()
    if len(raw) == 16 and raw[6:10] == "FFFE":
        raw = raw[:6] + raw[10:]
    elif len(raw) == 16 and raw.endswith("0000"):
        raw = raw[:12]
    return ":".join(raw[i : i + 2] for i in range(0, len(raw), 2))


def _format_latency_milliseconds(latency_milliseconds: float) -> str:
    if latency_milliseconds == int(latency_milliseconds):
        return str(int(latency_milliseconds))
    return f"{latency_milliseconds:g}"


def _format_standard_latency_choices(
    minimum_latency_milliseconds: float | None,
    maximum_latency_milliseconds: float | None,
) -> str:
    choices = standard_latency_choices_for_range(
        minimum_latency_milliseconds,
        maximum_latency_milliseconds,
    )
    if choices is None:
        return ""
    if not choices:
        return ""
    return ", ".join(_format_latency_milliseconds(choice) for choice in choices) + "ms"


def _format_latency_range(
    minimum_latency_milliseconds: float | None,
    maximum_latency_milliseconds: float | None,
) -> str:
    if minimum_latency_milliseconds is None or maximum_latency_milliseconds is None:
        return ""
    minimum_label = _format_latency_milliseconds(minimum_latency_milliseconds)
    maximum_label = _format_latency_milliseconds(maximum_latency_milliseconds)
    return f"{minimum_label}-{maximum_label}ms"


def _format_last_seen(last_seen: float | None) -> str:
    if last_seen is None:
        return ""
    from datetime import datetime, timezone

    return datetime.fromtimestamp(last_seen, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _format_sample_rate(sample_rate_hertz: int | None) -> str:
    if sample_rate_hertz is None:
        return "unknown"
    return f"{sample_rate_hertz / 1000:g} kHz"


def _format_link_speed(link_speed_mbps: int | None) -> str:
    if link_speed_mbps is None:
        return "unknown"
    if link_speed_mbps >= 1_000:
        return f"{link_speed_mbps / 1_000:g} Gbps"
    return f"{link_speed_mbps} Mbps"


def _format_bluetooth(device) -> str:
    if device.bluetooth_connected is True:
        return device.bluetooth_device or "connected"
    if device.bluetooth_connected is False:
        return "disconnected"
    return "unknown"


def _format_clock_frequency_offset(clock_frequency_offset_parts_per_billion: int) -> str:
    return f"{clock_frequency_offset_parts_per_billion / 1000:.3f} ppm"


def _format_clock_port_record(record: dict) -> str:
    role = record.get("role") or "unknown"
    transport_path = record.get("transport_path") or "unknown"
    ptp_version = record["ptp_version"]
    ptp_version_text = f"PTP v{ptp_version}" if ptp_version in {1, 2} else "PTP version unknown"
    return (
        f"{ptp_version_text} (0x{ptp_version:02X}), "
        f"transport path {transport_path} (0x{record['transport_path_code']:02X}), "
        f"state 0x{record['state_code']:04X} ({role}), "
        f"link down {'yes' if record['link_down'] else 'no'}, "
        f"record flags 0x{record['record_flags']:04X}, "
        f"status flags 0x{record['status_flags']:04X}, "
        f"format 0x{record['record_format_code']:02X}, "
        f"reserved 0x{record['reserved_byte']:02X}, "
        f"network interface index {record['network_interface_index']} "
        f"(0x{record['network_interface_index']:08X})"
    )


def _format_supported_sample_rates(supported_sample_rates: list[int] | None) -> str:
    if supported_sample_rates is None:
        return "unknown"
    if not supported_sample_rates:
        return "none advertised"
    return ", ".join(f"{sample_rate_hertz / 1000:g}" for sample_rate_hertz in supported_sample_rates) + " kHz"


def _format_encoding(encoding: int | None) -> str:
    return f"PCM{encoding}" if encoding is not None else "unknown"


def _format_supported_encodings(supported_encodings: list[int] | None) -> str:
    if supported_encodings is None:
        return "unknown"
    if not supported_encodings:
        return "none advertised"
    return ", ".join(f"PCM{encoding}" for encoding in supported_encodings)


def _format_reference_levels(device) -> str:
    if device.gain_device_type is None or device.gain_levels is None:
        return "unknown"
    return ", ".join(
        f"{channel_number}: {device.gain_level_label_for_channel(channel_number, 'tx' if device.gain_device_type == 'input' else 'rx')}"
        for channel_number in range(1, len(device.gain_levels) + 1)
    )


def _format_reference_options(device) -> str:
    choices = device.gain_level_choices
    if choices is None:
        return "unknown"
    if not choices:
        return "none advertised"
    return ", ".join(choice["label"] for choice in choices)


def _format_latency(latency_milliseconds: float | None) -> str:
    if latency_milliseconds is None:
        return "unknown"
    return f"{_format_latency_milliseconds(latency_milliseconds)} ms"


def _format_show_latency_range(
    minimum_latency_milliseconds: float | None,
    maximum_latency_milliseconds: float | None,
) -> str:
    if minimum_latency_milliseconds is None or maximum_latency_milliseconds is None:
        return "unknown"
    minimum_label = _format_latency_milliseconds(minimum_latency_milliseconds)
    maximum_label = _format_latency_milliseconds(maximum_latency_milliseconds)
    return f"{minimum_label}-{maximum_label} ms"


def _format_show_standard_latencies(device) -> str:
    choices = device.standard_latency_choices
    if choices is None:
        return "unknown"
    if not choices:
        return "none advertised"
    return ", ".join(_format_latency_milliseconds(choice) for choice in choices) + " ms"


def _format_aes67(device) -> str:
    if device.aes67_supported is False:
        return "unsupported"
    current = device.aes67_current
    configured = device.aes67_configured
    prefix = device.aes67_multicast_prefix
    prefix_suffix = f"; multicast prefix {prefix}" if prefix else ""
    if current is None and configured is None:
        return f"unknown{prefix_suffix}" if prefix_suffix else "unknown"
    if current is not None and configured is not None and current != configured:
        current_label = "enabled" if current else "disabled"
        configured_label = "enabled" if configured else "disabled"
        return f"{current_label}; configured {configured_label} (reboot required){prefix_suffix}"
    effective_state = configured if configured is not None else current
    return ("enabled" if effective_state else "disabled") + prefix_suffix


def _format_transmitter_flow(flow: dict) -> str:
    flow_type = flow.get("flow_type")
    if flow_type is None:
        flow_type_code = flow.get("flow_type_code")
        flow_type = f"0x{flow_type_code:04X}" if isinstance(flow_type_code, int) else "unknown"
    channel_numbers = flow.get("channels")
    if isinstance(channel_numbers, list) and channel_numbers:
        channel_text = ",".join(str(channel_number) for channel_number in channel_numbers)
    elif flow.get("channel_count") is not None:
        channel_text = f"{flow['channel_count']}ch"
    else:
        channel_text = "unknown channels"
    sample_rate = flow.get("sample_rate")
    encoding = flow.get("encoding")
    audio_parts = []
    if sample_rate is not None:
        audio_parts.append(str(sample_rate))
    if encoding is not None:
        audio_parts.append(f"PCM{encoding}")
    destination_address = flow.get("destination_internet_protocol_version_four_address")
    destination_port = flow.get("destination_user_datagram_port")
    if destination_address and destination_port:
        destination = f"{destination_address}:{destination_port}"
    else:
        destination = destination_address or ""
    subscriber_device = flow.get("subscriber_device_name") or ""
    subscriber_flow = flow.get("subscriber_flow_name") or ""
    if subscriber_device and subscriber_flow:
        subscriber = f"{subscriber_device}/{subscriber_flow}"
    else:
        subscriber = subscriber_device
    parts = [str(flow_type), channel_text]
    if audio_parts:
        parts.append("/".join(audio_parts))
    if destination:
        parts.append(f"-> {destination}")
    if subscriber:
        parts.append(subscriber)
    return " ".join(parts)


def _format_connection_latency_nanoseconds(latency_nanoseconds: int | None) -> str:
    if type(latency_nanoseconds) is not int or latency_nanoseconds < 0:
        return "unknown"
    if latency_nanoseconds < 1_000:
        return f"{latency_nanoseconds} ns"
    if latency_nanoseconds < 1_000_000:
        return f"{latency_nanoseconds / 1_000:g} us"
    return f"{latency_nanoseconds / 1_000_000:g} ms"


def _connection_health_rows(connection_health: dict | None) -> list[list[str]]:
    if not isinstance(connection_health, dict):
        return []
    fresh = connection_health.get("fresh")
    if fresh is True:
        freshness = "fresh"
    elif fresh is False:
        freshness = "stale"
    else:
        freshness = "unknown"
    observed_at = connection_health.get("observed_at")
    if isinstance(observed_at, str) and observed_at:
        freshness = f"{freshness}; received {observed_at}"
    rows = [["Receiver Flow Connection Health", freshness]]
    flows = connection_health.get("flows")
    if not isinstance(flows, list):
        return rows
    for flow in flows:
        if not isinstance(flow, dict):
            continue
        receiver_flow_slot = flow.get("receiver_flow_slot")
        if type(receiver_flow_slot) is not int:
            continue
        current = _format_connection_latency_nanoseconds(flow.get("current_latency_nanoseconds"))
        average = _format_connection_latency_nanoseconds(flow.get("average_latency_nanoseconds"))
        peak = _format_connection_latency_nanoseconds(flow.get("peak_latency_nanoseconds"))
        rows.append(
            [
                f"Receiver Flow Slot {receiver_flow_slot} Latency",
                f"current {current}; average {average}; peak {peak}",
            ]
        )
        raw_impairment_value = flow.get("raw_impairment_value")
        raw_impairment_delta = flow.get("raw_impairment_delta")
        value_label = str(raw_impairment_value) if type(raw_impairment_value) is int else "unknown"
        delta_label = f"{raw_impairment_delta:+d}" if type(raw_impairment_delta) is int else "unknown"
        rows.append(
            [
                f"Receiver Flow Slot {receiver_flow_slot} Raw Impairment",
                f"value {value_label}; delta {delta_label}",
            ]
        )
    return rows


def _receiver_flow_setting_rows(receiver_flows: list[dict] | None) -> list[list[str]]:
    if not isinstance(receiver_flows, list):
        return []
    rows = []
    for flow in receiver_flows:
        if not isinstance(flow, dict):
            continue
        flow_number = flow.get("flow_number")
        if type(flow_number) is not int:
            continue
        parts = [f"latency {_format_connection_latency_nanoseconds(flow.get('latency_nanoseconds'))}"]
        frames_per_packet = flow.get("frames_per_packet")
        if type(frames_per_packet) is int:
            parts.append(f"frames per packet {frames_per_packet}")
        flow_type = flow.get("flow_type")
        if isinstance(flow_type, str) and flow_type:
            parts.append(flow_type)
        rows.append([f"Receiver Flow {flow_number} Setting", "; ".join(parts)])
    return rows


def _format_channel_count(channels: dict, reported_count: int | None) -> str:
    if channels:
        return str(len(channels))
    if reported_count is not None:
        return str(reported_count)
    return "unknown"


def _diagnostic_audio_capabilities_data(capabilities) -> dict:
    return {
        "diagnostic_log_export_supported": True,
        "license_signature_length_bytes": capabilities.license_signature_length_bytes,
        "licensed_receive_channel_count": capabilities.licensed_receive_channel_count,
        "licensed_transmit_channel_count": capabilities.licensed_transmit_channel_count,
        "licensed_redundancy_enabled": capabilities.licensed_redundancy_enabled,
        "default_sample_rate_hertz": capabilities.default_sample_rate_hertz,
        "current_sample_rate_hertz": capabilities.current_sample_rate_hertz,
        "sample_rate_channel_capacities": [
            {
                "sample_rate_hertz": capacity.sample_rate_hertz,
                "receive_channel_count": capacity.receive_channel_count,
                "transmit_channel_count": capacity.transmit_channel_count,
            }
            for capacity in capabilities.channel_capacities
        ],
    }


def _diagnostic_audio_capability_rows(capabilities) -> list[list[str]]:
    rows = [["Diagnostic Log Export", "supported"]]
    if capabilities.license_signature_length_bytes is not None:
        rows.append(["License Signature", f"{capabilities.license_signature_length_bytes} bytes"])
    if (
        capabilities.licensed_transmit_channel_count is not None
        and capabilities.licensed_receive_channel_count is not None
    ):
        rows.append(
            [
                "Licensed Channels",
                f"{capabilities.licensed_transmit_channel_count} TX / {capabilities.licensed_receive_channel_count} RX",
            ]
        )
    if capabilities.licensed_redundancy_enabled is not None:
        rows.append(
            [
                "Licensed Redundancy",
                "enabled" if capabilities.licensed_redundancy_enabled else "disabled",
            ]
        )
    if capabilities.default_sample_rate_hertz is not None:
        rows.append(["Default Sample Rate", _format_sample_rate(capabilities.default_sample_rate_hertz)])
    if capabilities.current_sample_rate_hertz is not None:
        rows.append(["Current Sample Rate", _format_sample_rate(capabilities.current_sample_rate_hertz)])
    for capacity in capabilities.channel_capacities:
        rows.append(
            [
                f"Channels at {_format_sample_rate(capacity.sample_rate_hertz)}",
                f"{capacity.transmit_channel_count} TX / {capacity.receive_channel_count} RX",
            ]
        )
    return rows


def _device_channel_count_labels(device) -> tuple[str, str]:
    tx_count = device.tx_count
    if tx_count is None and device.tx_channels:
        tx_count = len(device.tx_channels)
    rx_count = device.rx_count
    if rx_count is None and device.rx_channels:
        rx_count = len(device.rx_channels)
    return (
        str(tx_count) if tx_count is not None else "unknown",
        str(rx_count) if rx_count is not None else "unknown",
    )


def _device_identity_rows(device) -> list[list[str]]:
    model = device.dante_model or device.model_id or device.model
    manufacturer = device.manufacturer or device.manufacturer_mdns
    rows = [
        ["Name", device.name or "unknown"],
        ["Status", "online" if device.online else "offline"],
        ["Model", model or "unknown"],
        ["Manufacturer", manufacturer or "unknown"],
        ["IP Address", str(device.ipv4) if device.ipv4 else "unknown"],
        ["MAC Address", _format_mac(device.mac_address) if device.mac_address else "unknown"],
        ["Link Speed", _format_link_speed(device.link_speed_mbps)],
    ]
    if device.firmware_version:
        rows.append(["Firmware", device.firmware_version])
    if device.software_version:
        rows.append(["Software", device.software_version])
    if device.model_id in BLUETOOTH_MODEL_IDS or device.bluetooth_connected is not None:
        rows.append(["Bluetooth", _format_bluetooth(device)])
    return rows


def _device_license_rows(device) -> list[list[str]]:
    rows = []
    if device.is_licensed is False:
        rows.append(["License State", "unlicensed"])
    if device.diagnostic_log_export_supported is not None:
        rows.append(
            [
                "Diagnostic Log Export",
                "supported" if device.diagnostic_log_export_supported else "unsupported",
            ]
        )
    if device.license_signature_length_bytes is not None:
        rows.append(["License Signature", f"{device.license_signature_length_bytes} bytes"])
    if device.licensed_transmit_channel_count is not None and device.licensed_receive_channel_count is not None:
        rows.append(
            [
                "Licensed Channels",
                f"{device.licensed_transmit_channel_count} TX / {device.licensed_receive_channel_count} RX",
            ]
        )
    if device.licensed_redundancy_enabled is not None:
        rows.append(
            [
                "Licensed Redundancy",
                "enabled" if device.licensed_redundancy_enabled else "disabled",
            ]
        )
    for capacity in device.sample_rate_channel_capacities or []:
        rows.append(
            [
                f"Channels at {_format_sample_rate(capacity['sample_rate_hertz'])}",
                f"{capacity['transmit_channel_count']} TX / {capacity['receive_channel_count']} RX",
            ]
        )
    return rows


def _device_audio_rows(device) -> list[list[str]]:
    tx_count_label, rx_count_label = _device_channel_count_labels(device)
    active_latency = device.active_latency if device.active_latency is not None else device.latency
    return [
        ["Channels", f"{tx_count_label} TX / {rx_count_label} RX"],
        ["Sample Rate", _format_sample_rate(device.sample_rate)],
        ["Supported Sample Rates", _format_supported_sample_rates(device.supported_sample_rates)],
        ["Encoding", _format_encoding(device.encoding)],
        ["Supported Encodings", _format_supported_encodings(device.supported_encodings)],
        ["Active Latency", _format_latency(active_latency)],
        ["Configured Latency", _format_latency(device.configured_latency)],
        ["Default Latency", _format_latency(device.default_latency)],
        ["Latency Range", _format_show_latency_range(device.min_latency, device.max_latency)],
        ["Latency Options", _format_show_standard_latencies(device)],
        ["AES67", _format_aes67(device)],
    ]


def _device_sample_rate_pullup_rows(device) -> list[list[str]]:
    if (
        device.sample_rate_pullup_raw_value is not None
        or device.requested_sample_rate_pullup_raw_value is not None
        or device.supported_sample_rate_pullup_raw_values is not None
    ):
        return [
            ["Sample Rate Pull-Up", sample_rate_pullup_label(device.sample_rate_pullup_raw_value)],
            [
                "Pull-Up Requested",
                sample_rate_pullup_label(device.requested_sample_rate_pullup_raw_value),
            ],
            [
                "Pull-Up Supported",
                format_supported_sample_rate_pullup_values(device.supported_sample_rate_pullup_raw_values),
            ],
        ]
    return []


def _device_transmitter_flow_rows(device) -> list[list[str]]:
    if device.transmitter_flows is None:
        return []
    if not device.transmitter_flows:
        return [["Transmitter Flows", "none"]]
    rows = []
    for flow in device.transmitter_flows:
        flow_number = flow.get("flow_number")
        label = f"Transmitter Flow {flow_number}" if flow_number is not None else "Transmitter Flow"
        rows.append([label, _format_transmitter_flow(flow)])
    return rows


def _device_control_rows(device) -> list[list[str]]:
    rows = [
        [
            "Lock",
            "unknown" if device.is_locked is None else ("locked" if device.is_locked else "unlocked"),
        ],
        [
            "Preferred Leader",
            "unknown" if device.preferred_leader is None else ("enabled" if device.preferred_leader else "disabled"),
        ],
    ]
    if device.clock_subdomain is not None:
        rows.append(["Clock Subdomain", format_clock_subdomain(device.clock_subdomain)])
    if device.gain_device_type is not None:
        rows.extend(
            [
                ["Reference Controls", device.gain_device_type],
                ["Reference Levels", _format_reference_levels(device)],
                ["Reference Options", _format_reference_options(device)],
            ]
        )
    return rows


def _device_clock_rows(device) -> list[list[str]]:
    rows = []
    if device.clock_role:
        rows.append(["Clock Role", device.clock_role])
    if device.clock_identity:
        rows.append(["Clock Identity", device.clock_identity])
    if device.leader_clock_identity:
        rows.append(["Leader Clock Identity", device.leader_clock_identity])
    if device.clock_frequency_offset_parts_per_billion is not None:
        rows.append(
            [
                "Clock Frequency Offset",
                _format_clock_frequency_offset(device.clock_frequency_offset_parts_per_billion),
            ]
        )
    if device.clock_port_state_code is not None:
        rows.append(["Clock Port State", f"0x{device.clock_port_state_code:04X}"])
    for record in device.clock_port_records or []:
        rows.append(
            [
                f"Clock Port Record {record['record_number']}",
                _format_clock_port_record(record),
            ]
        )
    return rows


def _device_show_rows(device) -> list[list[str]]:
    rows = _device_identity_rows(device)
    rows.extend(_device_license_rows(device))
    rows.extend(_device_audio_rows(device))
    rows.extend(_device_sample_rate_pullup_rows(device))
    rows.extend(_device_transmitter_flow_rows(device))
    rows.extend(_receiver_flow_setting_rows(device.receiver_flows))
    rows.extend(_connection_health_rows(device.receiver_flow_connection_health))
    rows.extend(_device_control_rows(device))
    rows.extend(_device_clock_rows(device))
    rows.append(["Server Name", device.server_name or "unknown"])
    if device.interface_reboot_required:
        rows.append(["Network Reboot Required", "yes"])
    return rows


def _channel_matches(channel_key: int, channel_name: str, patterns: list[str]) -> bool:
    for pat in patterns:
        try:
            if int(pat) == channel_key:
                return True
        except ValueError:
            pass
        if fnmatch(channel_name.lower(), pat.lower()):
            return True
    return False
