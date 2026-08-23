def dante_device_correlation_view(device) -> dict:
    result = {
        "server_name": device.server_name or "",
        "name": device.name or "",
        "ip": str(device.ipv4) if device.ipv4 else "",
        "mac": device.mac_address or "",
        "sample_rate": device.sample_rate,
        "supported_sample_rates": device.supported_sample_rates,
        "encoding": device.encoding,
        "supported_encodings": device.supported_encodings,
        "encoding_configurable": device.encoding_configurable,
        "latency": device.latency,
        "active_latency": device.active_latency,
        "configured_latency": device.configured_latency,
        "default_latency": device.default_latency,
        "min_latency": device.min_latency,
        "max_latency": device.max_latency,
        "standard_latency_choices": device.standard_latency_choices,
    }

    if device.tx_channels:
        result["tx_channels"] = {
            str(number): {"name": channel.name or "", "friendly_name": channel.friendly_name or ""}
            for number, channel in sorted(device.tx_channels.items())
            if channel.name
        }

    if device.rx_channels:
        result["rx_channels"] = {
            str(number): {"name": channel.name or "", "friendly_name": channel.friendly_name or ""}
            for number, channel in sorted(device.rx_channels.items())
            if channel.name
        }

    if device.subscriptions:
        result["subscriptions"] = [
            {
                "rx_channel_name": subscription.rx_channel_name or "",
                "tx_device_name": subscription.tx_device_name or "",
                "tx_channel_name": subscription.tx_channel_name or "",
                "status": (", ".join(subscription.status_text()) if subscription.status_code is not None else ""),
            }
            for subscription in device.subscriptions
        ]

    return result
