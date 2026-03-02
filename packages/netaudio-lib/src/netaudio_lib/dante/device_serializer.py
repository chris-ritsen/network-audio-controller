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
            "subscriptions": device.subscriptions,
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
        ]

        for field_name, field_value in optional_fields:
            if field_value:
                as_json[field_name] = field_value

        return {key: as_json[key] for key in sorted(as_json.keys())}

    @staticmethod
    def subscription_to_json(subscription):
                as_json = {
            "rx_channel": subscription.rx_channel_name,
            "rx_channel_status_code": subscription.rx_channel_status_code,
            "rx_device": subscription.rx_device_name,
            "status_code": subscription.status_code,
            "status_text": list(subscription.status_text()),
            "tx_channel": subscription.tx_channel_name,
            "tx_device": subscription.tx_device_name,
        }

        if subscription.rx_channel_status_code != 0:
            as_json["rx_channel_status_text"] = list(
                subscription.rx_channel_status_text()
            )

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
