import math

from netaudio.dante.clock_config import format_clock_subdomain


DANTE_PROPERTY_NAMES = {
    "name": "Name",
    "mac_address": "MacAddress",
    "ipv4": "Ipv4",
    "model": "Model",
    "model_id": "ModelId",
    "manufacturer": "Manufacturer",
    "dante_model": "DanteModel",
    "board_name": "BoardName",
    "link_speed_mbps": "LinkSpeedMbps",
    "sample_rate": "SampleRate",
    "supported_sample_rates": "SupportedSampleRates",
    "encoding": "Encoding",
    "supported_encodings": "SupportedEncodings",
    "bit_depth": "BitDepth",
    "latency": "Latency",
    "active_latency": "ActiveLatency",
    "configured_latency": "ConfiguredLatency",
    "default_latency": "DefaultLatency",
    "min_latency": "MinLatency",
    "max_latency": "MaxLatency",
    "tx_count": "TxCount",
    "rx_count": "RxCount",
    "online": "Online",
    "is_locked": "IsLocked",
    "lock_state_known": "LockStateKnown",
    "aes67_current": "Aes67Enabled",
    "aes67_supported": "Aes67Supported",
    "aes67_support_known": "Aes67SupportKnown",
    "aes67_multicast_prefix": "Aes67MulticastPrefix",
    "sample_rate_pullup_raw_value": "SampleRatePullupRawValue",
    "requested_sample_rate_pullup_raw_value": "RequestedSampleRatePullupRawValue",
    "supported_sample_rate_pullup_raw_values": "SupportedSampleRatePullupRawValues",
    "sample_rate_pullup_known": "SampleRatePullupKnown",
    "transmitter_flows": "TransmitterFlows",
    "last_seen": "LastSeen",
    "clock_frequency_offset_parts_per_billion": "ClockFrequencyOffsetPartsPerBillion",
    "clock_source_code": "ClockSourceCode",
    "clock_subdomain": "ClockSubdomain",
    "clock_role": "ClockRole",
    "clock_port_records": "ClockPortRecords",
    "clock_identity": "ClockIdentity",
    "leader_clock_identity": "LeaderClockIdentity",
    "software_version": "SoftwareVersion",
    "firmware_version": "FirmwareVersion",
    "subscriptions": "Subscriptions",
}

SHURE_PROPERTY_NAMES = {
    "ip": "Ip",
    "mac": "Mac",
    "device_type": "DeviceType",
    "name": "Name",
    "model": "Model",
    "firmware_version": "FirmwareVersion",
    "rf_band": "RfBand",
    "transmission_mode": "TransmissionMode",
    "quadversity_mode": "QuadversityMode",
    "encryption_mode": "EncryptionMode",
    "online": "Online",
    "last_seen": "LastSeen",
}


def dbus_string(value) -> str:
    return "" if value is None else str(value)


def dbus_uint(value, bits: int = 32) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    maximum = (1 << bits) - 1
    return result if 0 <= result <= maximum else 0


def dbus_int32(value) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return result if -(1 << 31) <= result < (1 << 31) else 0


def dbus_uint_list(values) -> list[int]:
    return [dbus_uint(value) for value in values or []]


def dbus_double(value) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def latency_milliseconds(value) -> float:
    return dbus_double(value)


def aes67_enabled(device) -> bool:
    return bool(device.aes67_current)


def subscription_rows(device) -> list[tuple[str, str, str, str, int]]:
    rows = []
    for subscription in device.subscriptions or []:
        rx_device = subscription.rx_device
        rx_device_name = rx_device.server_name if rx_device is not None else subscription.rx_device_name
        rows.append(
            (
                dbus_string(rx_device_name),
                dbus_string(subscription.rx_channel_name),
                dbus_string(subscription.tx_device_name),
                dbus_string(subscription.tx_channel_name),
                dbus_uint(subscription.status_code, bits=16),
            )
        )
    return rows


def transmitter_flow_rows(device) -> list[tuple[int, int, str, int, int, str, int, str, str]]:
    rows = []
    for flow in device.transmitter_flows or []:
        flow_type = flow.get("flow_type")
        if not flow_type:
            flow_type_code = flow.get("flow_type_code")
            flow_type = f"0x{flow_type_code:04X}" if isinstance(flow_type_code, int) else ""
        channel_count = flow.get("channel_count")
        if channel_count is None:
            channel_numbers = flow.get("channels")
            channel_count = len(channel_numbers) if isinstance(channel_numbers, list) else 0
        rows.append(
            (
                dbus_uint(flow.get("flow_number")),
                dbus_uint(channel_count),
                dbus_string(flow_type),
                dbus_uint(flow.get("sample_rate")),
                dbus_uint(flow.get("encoding")),
                dbus_string(flow.get("destination_internet_protocol_version_four_address")),
                dbus_uint(flow.get("destination_user_datagram_port"), bits=16),
                dbus_string(flow.get("subscriber_device_name")),
                dbus_string(flow.get("subscriber_flow_name")),
            )
        )
    return rows


def clock_port_rows(device) -> list[tuple[int, bool, int, int, int, int, str, int, int, int, int, str]]:
    return [
        (
            dbus_uint(record.get("record_flags"), bits=16),
            bool(record.get("link_down")),
            dbus_uint(record.get("record_number"), bits=16),
            dbus_uint(record.get("ptp_version"), bits=8),
            dbus_uint(record.get("record_format_code"), bits=8),
            dbus_uint(record.get("transport_path_code"), bits=8),
            dbus_string(record.get("transport_path")),
            dbus_uint(record.get("reserved_byte"), bits=8),
            dbus_uint(record.get("network_interface_index")),
            dbus_uint(record.get("state_code"), bits=16),
            dbus_uint(record.get("status_flags"), bits=16),
            dbus_string(record.get("role")),
        )
        for record in device.clock_port_records or []
    ]


def snapshot_dante_device(device):
    return {
        "name": dbus_string(device.name),
        "mac_address": dbus_string(device.mac_address),
        "ipv4": dbus_string(device.ipv4),
        "model": dbus_string(device.model),
        "model_id": dbus_string(device.model_id),
        "manufacturer": dbus_string(device.manufacturer),
        "dante_model": dbus_string(device.dante_model),
        "board_name": dbus_string(device.board_name),
        "link_speed_mbps": dbus_uint(device.link_speed_mbps),
        "sample_rate": dbus_uint(device.sample_rate),
        "supported_sample_rates": dbus_uint_list(device.supported_sample_rates),
        "encoding": dbus_uint(device.encoding),
        "supported_encodings": dbus_uint_list(device.supported_encodings),
        "bit_depth": dbus_uint(device.bit_depth),
        "latency": latency_milliseconds(device.latency),
        "active_latency": latency_milliseconds(device.active_latency),
        "configured_latency": latency_milliseconds(device.configured_latency),
        "default_latency": latency_milliseconds(device.default_latency),
        "min_latency": latency_milliseconds(device.min_latency),
        "max_latency": latency_milliseconds(device.max_latency),
        "tx_count": dbus_uint(device.tx_count),
        "rx_count": dbus_uint(device.rx_count),
        "online": bool(device.online),
        "is_locked": bool(device.is_locked),
        "lock_state_known": device.is_locked is not None,
        "aes67_current": aes67_enabled(device),
        "aes67_supported": device.aes67_supported is True,
        "aes67_support_known": device.aes67_supported is not None,
        "aes67_multicast_prefix": dbus_string(device.aes67_multicast_prefix),
        "sample_rate_pullup_raw_value": dbus_uint(device.sample_rate_pullup_raw_value),
        "requested_sample_rate_pullup_raw_value": dbus_uint(device.requested_sample_rate_pullup_raw_value),
        "supported_sample_rate_pullup_raw_values": dbus_uint_list(device.supported_sample_rate_pullup_raw_values),
        "sample_rate_pullup_known": device.supported_sample_rate_pullup_raw_values is not None,
        "transmitter_flows": transmitter_flow_rows(device),
        "last_seen": dbus_double(device.last_seen),
        "clock_frequency_offset_parts_per_billion": dbus_int32(device.clock_frequency_offset_parts_per_billion),
        "clock_source_code": dbus_uint(device.clock_source_code, bits=16),
        "clock_subdomain": dbus_string(
            None if device.clock_subdomain is None else format_clock_subdomain(device.clock_subdomain)
        ),
        "clock_role": dbus_string(device.clock_role),
        "clock_port_records": clock_port_rows(device),
        "clock_identity": dbus_string(device.clock_identity),
        "leader_clock_identity": dbus_string(device.leader_clock_identity),
        "software_version": dbus_string(device.software_version),
        "firmware_version": dbus_string(device.firmware_version),
        "subscriptions": subscription_rows(device),
    }


def snapshot_shure_device(device):
    return {
        "ip": dbus_string(device.ip),
        "mac": dbus_string(device.mac),
        "device_type": dbus_string(device.device_type.value),
        "name": dbus_string(device.name),
        "model": dbus_string(device.model),
        "firmware_version": dbus_string(device.firmware_version),
        "rf_band": dbus_string(device.rf_band),
        "transmission_mode": dbus_string(device.transmission_mode),
        "quadversity_mode": dbus_string(device.quadversity_mode),
        "encryption_mode": dbus_string(device.encryption_mode),
        "online": bool(device.online),
        "last_seen": dbus_double(device.last_seen),
    }
