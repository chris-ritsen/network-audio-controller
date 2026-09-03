DEVICE_SCALAR_FIELDS = (
    "active_latency",
    "aes67_configured",
    "aes67_current",
    "aes67_multicast_prefix",
    "aes67_supported",
    "availability_state",
    "bit_depth",
    "bluetooth_connected",
    "bluetooth_device",
    "board_name",
    "clear_configuration_status",
    "clock_frequency_offset_parts_per_billion",
    "clock_identity",
    "clock_port_records",
    "clock_port_state_code",
    "clock_role",
    "clock_source_code",
    "clock_subdomain",
    "configured_latency",
    "control_transports",
    "dante_model",
    "dante_model_id",
    "ddm_capabilities",
    "ddm_clock_preferences",
    "ddm_clocking_state",
    "ddm_connection_last_changed",
    "ddm_connection_state",
    "ddm_context",
    "ddm_device_id",
    "ddm_domain_id",
    "ddm_domain_name",
    "ddm_enrolment_state",
    "ddm_identity",
    "ddm_inputs",
    "ddm_last_sync",
    "ddm_outputs",
    "ddm_parameters",
    "ddm_server_profile",
    "ddm_status",
    "default_latency",
    "diagnostic_log_export_supported",
    "direct_control_available",
    "encoding",
    "field_sources",
    "firmware_version",
    "gain_device_type",
    "gain_levels",
    "interface_pending_config",
    "interfaces",
    "inventory_id",
    "inventory_sources",
    "is_locked",
    "last_seen",
    "latency",
    "leader_clock_identity",
    "license_signature_length_bytes",
    "licensed_receive_channel_count",
    "licensed_redundancy_enabled",
    "licensed_transmit_channel_count",
    "link_speed_mbps",
    "lock_reset_status",
    "mac_address",
    "management_state",
    "manufacturer",
    "max_latency",
    "min_latency",
    "model",
    "model_id",
    "network_interface_traffic",
    "num_networks",
    "preferred_leader",
    "product_version",
    "receiver_flow_connection_health",
    "receiver_flow_latency_nanoseconds",
    "receiver_flows",
    "requested_sample_rate_pullup_raw_value",
    "routing_capacity_receive_channel_count",
    "routing_capacity_transmit_channel_count",
    "routing_ready",
    "routing_ready_state_code",
    "rx_count",
    "rx_count_raw",
    "rx_flow_count",
    "sample_rate",
    "sample_rate_channel_capacities",
    "sample_rate_pullup_raw_value",
    "settings_properties",
    "software_version",
    "supported_encodings",
    "supported_gain_levels",
    "supported_sample_rate_pullup_raw_values",
    "supported_sample_rates",
    "transmitter_flows",
    "tx_count",
    "tx_count_raw",
    "tx_flow_count",
)

CHANNEL_OPTIONAL_FIELDS = (
    "bit_depth",
    "ddm_can_subscribe_self",
    "ddm_channel_id",
    "ddm_enabled",
    "ddm_encryption_policy",
    "ddm_encryption_scheme",
    "ddm_media_type",
    "ddm_signal_presence",
    "ddm_status",
    "ddm_status_message",
    "ddm_summary",
    "factory_name",
    "friendly_name",
    "muted",
    "samples_per_frame",
    "status_text",
    "volume",
)

SUBSCRIPTION_MANAGED_FIELDS = ("ddm_status", "ddm_status_message", "ddm_summary")

DEVICE_JSON_FIELD_NAMES = {
    "active_latency": "active_latency_ms",
    "configured_latency": "configured_latency_ms",
    "default_latency": "default_latency_ms",
    "latency": "latency_ms",
    "max_latency": "max_latency_ms",
    "min_latency": "min_latency_ms",
    "receiver_flow_latency_nanoseconds": "receiver_flow_latency_ns",
    "sample_rate": "sample_rate_hz",
    "supported_sample_rates": "supported_sample_rates_hz",
}


def device_json_field_name(field_name: str) -> str:
    return DEVICE_JSON_FIELD_NAMES.get(field_name, field_name)


def with_legacy_field_names(record: dict) -> dict:
    aliased = dict(record)
    for legacy_name, json_field_name in DEVICE_JSON_FIELD_NAMES.items():
        if json_field_name in record and legacy_name not in record:
            aliased[legacy_name] = record[json_field_name]
    return {key: aliased[key] for key in sorted(aliased)}


class DanteDeviceSerializer:
    @staticmethod
    def to_json(device):
        rx_channels = {
            k: DanteDeviceSerializer.channel_to_json(v)
            for k, v in sorted(device.rx_channels.items(), key=lambda x: x[1].number)
        }
        tx_channels = {
            k: DanteDeviceSerializer.channel_to_json(v)
            for k, v in sorted(device.tx_channels.items(), key=lambda x: x[1].number)
        }

        as_json = {
            "channels": {"receivers": rx_channels, "transmitters": tx_channels},
            "ipv4": str(device.ipv4),
            "kind": device.kind,
            "name": device.name,
            "online": device.online,
            "server_name": device.server_name,
            "services": device.services,
            "subscriptions": [DanteDeviceSerializer.subscription_to_json(s) for s in device.subscriptions],
        }

        for field_name in DEVICE_SCALAR_FIELDS:
            field_value = getattr(device, field_name)
            if field_value is None and field_name != "is_locked":
                continue
            if isinstance(field_value, (bytes, bytearray)):
                field_value = list(field_value)
            as_json[device_json_field_name(field_name)] = field_value

        if device.is_licensed is not None:
            as_json["is_licensed"] = device.is_licensed

        standard_latency_choices = device.standard_latency_choices
        if standard_latency_choices is not None:
            as_json["standard_latency_choices_ms"] = standard_latency_choices

        encoding_configurable = device.encoding_configurable
        if encoding_configurable is not None:
            as_json["encoding_configurable"] = encoding_configurable

        gain_configurable = device.gain_configurable
        if gain_configurable is not None:
            as_json["gain_configurable"] = gain_configurable

        gain_level_choices = device.gain_level_choices
        if gain_level_choices is not None:
            as_json["gain_level_choices"] = gain_level_choices

        if device.interface_reboot_required:
            as_json["interface_reboot_required"] = device.interface_reboot_required

        return {key: as_json[key] for key in sorted(as_json.keys())}

    @staticmethod
    def device_from_json(data):
        from netaudio.dante.device import DanteDevice

        device = DanteDevice(server_name=data.get("server_name", ""))

        if data.get("ipv4") and data["ipv4"] != "None":
            device.ipv4 = data["ipv4"]
        device.name = data.get("name", "")
        device.online = data.get("online", True)
        device.services = data.get("services") or {}

        for field_name in DEVICE_SCALAR_FIELDS:
            json_field_name = device_json_field_name(field_name)
            if json_field_name in data:
                setattr(device, field_name, data[json_field_name])
            elif field_name in data:
                setattr(device, field_name, data[field_name])

        if data.get("interface_reboot_required"):
            device.interface_reboot_required = True

        channels = data.get("channels") or {}
        device.rx_channels = DanteDeviceSerializer._channels_from_json(channels.get("receivers") or {}, "rx", device)
        device.tx_channels = DanteDeviceSerializer._channels_from_json(channels.get("transmitters") or {}, "tx", device)

        device.subscriptions = [
            DanteDeviceSerializer._subscription_from_json(entry) for entry in data.get("subscriptions") or []
        ]

        return device

    @staticmethod
    def _channels_from_json(channels_json, channel_type, device):
        from netaudio.dante.channel import DanteChannel

        channels = {}
        for number_key, channel_json in channels_json.items():
            channel = DanteChannel()
            channel.channel_type = channel_type
            channel.device = device
            channel.number = int(number_key)
            channel.name = channel_json.get("name")
            for field_name in CHANNEL_OPTIONAL_FIELDS:
                setattr(channel, field_name, channel_json.get(field_name))
            channels[channel.number] = channel
        return channels

    @staticmethod
    def _subscription_from_json(entry):
        from netaudio.dante.subscription import DanteSubscription

        subscription = DanteSubscription()
        subscription.rx_channel_name = entry.get("rx_channel")
        subscription.rx_device_name = entry.get("rx_device")
        subscription.tx_channel_name = entry.get("tx_channel")
        subscription.tx_device_name = entry.get("tx_device")
        for field_name in SUBSCRIPTION_MANAGED_FIELDS:
            setattr(subscription, field_name, entry.get(field_name))
        status = entry.get("status")
        if status:
            subscription.status_code = status.get("code")
        rx_channel_status = entry.get("rx_channel_status")
        if rx_channel_status:
            subscription.rx_channel_status_code = rx_channel_status.get("code")
        elif status:
            subscription.rx_channel_status_code = status.get("code")
        return subscription

    @staticmethod
    def channel_to_json(channel):
        as_json = {"name": channel.name}

        gain_level = None
        gain_level_label = None
        if channel.device is not None:
            gain_level = channel.device.gain_level_for_channel(channel.number, channel.channel_type)
            gain_level_label = channel.device.gain_level_label_for_channel(channel.number, channel.channel_type)

        optional_fields = [
            *((field_name, getattr(channel, field_name)) for field_name in CHANNEL_OPTIONAL_FIELDS),
            ("gain_level", gain_level),
            ("gain_level_label", gain_level_label),
        ]

        for field_name, field_value in optional_fields:
            if field_value is not None:
                as_json[field_name] = field_value

        return {key: as_json[key] for key in sorted(as_json.keys())}

    @staticmethod
    def _status_to_json(code):
        from netaudio.dante.const import (
            subscription_status_detail,
            subscription_status_entry,
            subscription_status_label,
        )
        from netaudio.icons import severity_icon

        if code is None:
            return None

        entry = subscription_status_entry(code)
        severity = entry["severity"]
        return {
            "code": code,
            "detail": subscription_status_detail(code),
            "icon": severity_icon(severity),
            "label": subscription_status_label(code),
            "severity": severity,
            "state": entry["state"],
            "status": entry.get("status"),
        }

    @staticmethod
    def _receiver_status_to_json(code):
        if code is None:
            return None
        return {
            "code": code,
            "detail": None,
            "label": f"Receiver status 0x{code:04X}",
            "state": "uncharacterized",
        }

    @staticmethod
    def subscription_to_json(subscription):
        as_json = {
            "rx_channel": subscription.rx_channel_name,
            "rx_device": subscription.rx_device_name,
            "status": DanteDeviceSerializer._status_to_json(subscription.status_code),
            "tx_channel": subscription.tx_channel_name,
            "tx_device": subscription.tx_device_name,
        }
        for field_name in SUBSCRIPTION_MANAGED_FIELDS:
            field_value = getattr(subscription, field_name)
            if field_value is not None:
                as_json[field_name] = field_value

        if (
            subscription.rx_channel_status_code is not None
            and subscription.rx_channel_status_code != subscription.status_code
        ):
            as_json["rx_channel_status"] = DanteDeviceSerializer._receiver_status_to_json(
                subscription.rx_channel_status_code
            )

        return as_json
