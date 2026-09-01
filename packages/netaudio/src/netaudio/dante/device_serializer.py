DEVICE_SCALAR_FIELDS = (
    "bluetooth_device",
    "bluetooth_connected",
    "is_locked",
    "dante_model",
    "dante_model_id",
    "latency",
    "active_latency",
    "configured_latency",
    "default_latency",
    "mac_address",
    "manufacturer",
    "model",
    "model_id",
    "sample_rate",
    "supported_sample_rates",
    "sample_rate_pullup_raw_value",
    "requested_sample_rate_pullup_raw_value",
    "supported_sample_rate_pullup_raw_values",
    "aes67_configured",
    "aes67_current",
    "aes67_supported",
    "aes67_multicast_prefix",
    "settings_properties",
    "preferred_leader",
    "clock_source_code",
    "clock_subdomain",
    "tx_flow_count",
    "transmitter_flows",
    "rx_flow_count",
    "receiver_flow_latency_nanoseconds",
    "receiver_flows",
    "num_networks",
    "encoding",
    "supported_encodings",
    "gain_device_type",
    "gain_levels",
    "supported_gain_levels",
    "bit_depth",
    "software_version",
    "firmware_version",
    "clock_frequency_offset_parts_per_billion",
    "clock_port_state_code",
    "clock_role",
    "clock_port_records",
    "clock_identity",
    "leader_clock_identity",
    "min_latency",
    "max_latency",
    "product_version",
    "board_name",
    "interfaces",
    "network_interface_traffic",
    "receiver_flow_connection_health",
    "link_speed_mbps",
    "interface_pending_config",
    "lock_reset_status",
    "clear_configuration_status",
    "diagnostic_log_export_supported",
    "license_signature_length_bytes",
    "licensed_receive_channel_count",
    "licensed_transmit_channel_count",
    "licensed_redundancy_enabled",
    "sample_rate_channel_capacities",
    "last_seen",
    "tx_count",
    "rx_count",
    "tx_count_raw",
    "rx_count_raw",
    "routing_capacity_receive_channel_count",
    "routing_capacity_transmit_channel_count",
    "routing_ready",
    "routing_ready_state_code",
)


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
                as_json[field_name] = list(field_value)
            else:
                as_json[field_name] = field_value

        if device.is_licensed is not None:
            as_json["is_licensed"] = device.is_licensed

        standard_latency_choices = device.standard_latency_choices
        if standard_latency_choices is not None:
            as_json["standard_latency_choices"] = standard_latency_choices

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
            if field_name in data:
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
            channel.friendly_name = channel_json.get("friendly_name")
            channel.factory_name = channel_json.get("factory_name")
            channel.status_text = channel_json.get("status_text")
            channel.volume = channel_json.get("volume")
            channel.muted = channel_json.get("muted")
            channel.bit_depth = channel_json.get("bit_depth")
            channel.samples_per_frame = channel_json.get("samples_per_frame")
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
            ("friendly_name", channel.friendly_name),
            ("factory_name", channel.factory_name),
            ("status_text", channel.status_text),
            ("volume", channel.volume),
            ("muted", channel.muted),
            ("bit_depth", channel.bit_depth),
            ("samples_per_frame", channel.samples_per_frame),
            ("gain_level", gain_level),
            ("gain_level_label", gain_level_label),
        ]

        for field_name, field_value in optional_fields:
            if field_value:
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
            "status": entry.get("status"),
            "state": entry["state"],
            "severity": severity,
            "label": subscription_status_label(code),
            "detail": subscription_status_detail(code),
            "icon": severity_icon(severity),
        }

    @staticmethod
    def _receiver_status_to_json(code):
        if code is None:
            return None
        return {
            "code": code,
            "state": "uncharacterized",
            "label": f"Receiver status 0x{code:04X}",
            "detail": None,
        }

    @staticmethod
    def subscription_to_json(subscription):
        as_json = {
            "rx_channel": subscription.rx_channel_name,
            "rx_device": subscription.rx_device_name,
            "tx_channel": subscription.tx_channel_name,
            "tx_device": subscription.tx_device_name,
            "status": DanteDeviceSerializer._status_to_json(subscription.status_code),
        }

        if (
            subscription.rx_channel_status_code is not None
            and subscription.rx_channel_status_code != subscription.status_code
        ):
            as_json["rx_channel_status"] = DanteDeviceSerializer._receiver_status_to_json(
                subscription.rx_channel_status_code
            )

        return as_json
