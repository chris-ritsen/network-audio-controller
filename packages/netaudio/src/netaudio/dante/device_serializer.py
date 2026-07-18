DEVICE_SCALAR_FIELDS = (
    "bluetooth_device",
    "is_locked",
    "dante_model",
    "dante_model_id",
    "latency",
    "mac_address",
    "manufacturer",
    "model",
    "model_id",
    "sample_rate",
    "aes67_configured",
    "aes67_current",
    "preferred_leader",
    "ptp_v1_role",
    "tx_flow_count",
    "rx_flow_count",
    "num_networks",
    "encoding",
    "bit_depth",
    "software_version",
    "firmware_version",
    "clock_role",
    "clock_mac",
    "min_latency",
    "max_latency",
    "product_version",
    "board_name",
    "interfaces",
    "interface_pending_config",
    "last_seen",
    "tx_count",
    "rx_count",
    "tx_count_raw",
    "rx_count_raw",
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
            if field_value is not None:
                as_json[field_name] = field_value

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
    def channels_to_json(channels):
        return dict(sorted(channels.items(), key=lambda x: x[1].number))

    @staticmethod
    def channel_to_json(channel):
        as_json = {"name": channel.name}

        optional_fields = [
            ("friendly_name", channel.friendly_name),
            ("status_text", channel.status_text),
            ("volume", channel.volume),
            ("muted", channel.muted),
            ("bit_depth", channel.bit_depth),
            ("samples_per_frame", channel.samples_per_frame),
        ]

        for field_name, field_value in optional_fields:
            if field_value:
                as_json[field_name] = field_value

        return {key: as_json[key] for key in sorted(as_json.keys())}

    @staticmethod
    def _status_to_json(code):
        from netaudio.dante.const import SUBSCRIPTION_STATUS_INFO

        if code is None:
            return None

        info = SUBSCRIPTION_STATUS_INFO.get(code)
        if info is None:
            return {"code": code, "state": "unknown", "label": f"Unknown ({code})", "detail": None}

        state, label, detail = info
        return {"code": code, "state": state, "label": label, "detail": detail}

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
            as_json["rx_channel_status"] = DanteDeviceSerializer._status_to_json(subscription.rx_channel_status_code)

        return as_json

    @staticmethod
    def device_summary_to_json(device):
        return {
            "name": device.name,
            "ipv4": str(device.ipv4),
            "server_name": device.server_name,
            "model_id": device.model_id,
            "tx_count": device.tx_count,
            "rx_count": device.rx_count,
        }
