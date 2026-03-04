class DanteDeviceSerializer:
    @staticmethod
    def to_json(device):
        rx_channels = dict(
            sorted(device.rx_channels.items(), key=lambda x: x[1].number)
        )
        tx_channels = dict(
            sorted(device.tx_channels.items(), key=lambda x: x[1].number)
        )

        as_json = {
            "channels": {"receivers": rx_channels, "transmitters": tx_channels},
            "ipv4": str(device.ipv4),
            "name": device.name,
            "server_name": device.server_name,
            "services": device.services,
            "subscriptions": [DanteDeviceSerializer.subscription_to_json(s) for s in device.subscriptions],
        }

        optional_fields = [
            ("bluetooth_device", device.bluetooth_device),
            ("dante_model", device.dante_model),
            ("dante_model_id", device.dante_model_id),
            ("latency", device.latency),
            ("mac_address", device.mac_address),
            ("manufacturer", device.manufacturer),
            ("model", device.model),
            ("model_id", device.model_id),
            ("sample_rate", device.sample_rate),
            ("aes67_enabled", device.aes67_enabled),
            ("tx_flow_count", device.tx_flow_count),
            ("rx_flow_count", device.rx_flow_count),
            ("num_networks", device.num_networks),
            ("encoding", device.encoding),
            ("bit_depth", device.bit_depth),
            ("software_version", device.software_version),
            ("firmware_version", device.firmware_version),
            ("clock_role", device.clock_role),
            ("clock_mac", device.clock_mac),
            ("min_latency", device.min_latency),
            ("max_latency", device.max_latency),
        ]

        for field_name, field_value in optional_fields:
            if field_value is not None:
                as_json[field_name] = field_value

        return {key: as_json[key] for key in sorted(as_json.keys())}

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

        if subscription.rx_channel_status_code is not None and subscription.rx_channel_status_code != subscription.status_code:
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
