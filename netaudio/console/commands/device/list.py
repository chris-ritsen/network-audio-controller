import json

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from netaudio.console.json_encoder import dump_json_formatted

from netaudio.dante.browser import DanteBrowser
from netaudio.dante.channel import DanteChannel
from netaudio.dante.const import SERVICE_CMC
from netaudio.dante.device import DanteDevice
from netaudio.dante.subscription import DanteSubscription

from typing import Any, Dict, Optional

def _get_devices_from_redis() -> Optional[Dict[str, Any]]:
    redis_client = None
    redis_host = "localhost"
    redis_port = 6379
    redis_db = 0

    try:
        redis_client = Redis(
            db=redis_db,
            decode_responses=True,
            host=redis_host,
            port=redis_port,
            socket_timeout=0.1,
        )

        redis_client.ping()
    except RedisConnectionError:
        return None

    if not redis_client:
        return None

    host_keys = redis_client.smembers("netaudio:dante:hosts")
    devices = {}

    for host_key in host_keys:
        host_data = redis_client.hgetall(f"netaudio:dante:host:{host_key}")

        if not host_data or "server_name" not in host_data:
            continue

        server_name = host_data["server_name"]

        device = DanteDevice(server_name=server_name)
        device.ipv4 = host_data.get("ipv4")

        device_data = redis_client.hgetall(f"netaudio:dante:device:{server_name}")

        if device_data:
            rx_channels = json.loads(device_data.get("rx_channels", "{}"))

            for channel_number, rx_channel_data in rx_channels.items():
                rx_channel = DanteChannel()
                rx_channel.channel_type = "rx"
                rx_channel.device = device
                rx_channel.name = rx_channel_data.get("name")
                rx_channel.number = channel_number
                rx_channel.status_code = rx_channel_data.get("status_code")
                device.rx_channels[channel_number] = rx_channel

            tx_channels = json.loads(device_data.get("tx_channels", "{}"))

            for channel_number, tx_channel_data in tx_channels.items():
                tx_channel = DanteChannel()
                tx_channel.channel_type = "tx"
                tx_channel.device = device
                tx_channel.name = tx_channel_data.get("name")
                tx_channel.number = channel_number
                tx_channel.status_code = tx_channel_data.get("status_code")
                device.tx_channels[channel_number] = tx_channel

            device.rx_count = int(device_data.get("rx_channel_count"), 0)
            device.tx_count = int(device_data.get("tx_channel_count"), 0)

            subscriptions = json.loads(device_data.get("subscriptions", "{}"))

            for (
                subscription_number,
                subscription_data,
            ) in subscriptions.items():
                subscription = DanteSubscription()
                subscription.rx_channel_name = subscription_data.get(
                    "rx_channel_name"
                )

                subscription.rx_device_name = subscription_data.get(
                    "rx_device_name"
                )

                subscription.tx_channel_name = subscription_data.get(
                    "tx_channel_name"
                )

                subscription.tx_device_name = subscription_data.get(
                    "tx_device_name"
                )

                subscription.status_code = subscription_data.get("status_code")

                subscription.rx_channel_status_code = subscription_data.get(
                    "rx_channel_status_code"
                )

                subscription.status_message = subscription_data.get(
                    "status_message", []
                )

                device.subscriptions.append(subscription)

            device.name = device_data.get("device_name")
            device.sample_rate = device_data.get("sample_rate_status")
            device.model_id = device_data.get("model")
            device.software = device_data.get("software")
            device.latency = device_data.get("latency")

        service_keys = redis_client.keys(f"netaudio:dante:service:{server_name}:*")

        for service_key in service_keys:
            service_data = redis_client.hgetall(service_key)

            service_properties_key = service_key.replace(
                "service", "service:properties"
            )

            service_properties = redis_client.hgetall(service_properties_key)

            if service_data:
                service_name = service_data.get("name")
                device.services[service_name] = {
                    "ipv4": service_data.get("ipv4"),
                    "name": service_data.get("name"),
                    "port": int(service_data.get("port", 0)),
                    "properties": {
                        k: v
                        for k, v in service_properties.items()
                        if k not in ["ipv4", "name", "port"]
                    },
                    "server_name": server_name,
                    "type": service_data.get("type"),
                }

                if (
                    "id" in service_properties
                    and service_data.get("type") == SERVICE_CMC
                ):
                    device.mac_address = service_properties["id"]

        device.services = dict(sorted(device.services.items()))
        devices[server_name] = device

    return devices if devices else None


async def device_list(
    name: str | None = None,
    host: str | None = None,
    json: bool = False
) -> None:
    cached_devices = _get_devices_from_redis()

    dante_browser = DanteBrowser(mdns_timeout=1.5)

    devices = cached_devices if cached_devices is not None else await dante_browser.get_devices(
            filter_name=name,
            filter_host=host
        )

    for _, device in devices.items():
        await device.get_controls()

    devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

    if json:
        print(dump_json_formatted(devices))
    else:
        for _, device in devices.items():
            print(f"{device}")

