import asyncio
import ipaddress
import json
import pprint
import socket

from json import JSONEncoder

from cleo.commands.command import Command
from cleo.helpers import option

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from netaudio.dante.browser import DanteBrowser
from netaudio.dante.channel import DanteChannel
from netaudio.dante.const import SERVICE_CMC, SERVICES
from netaudio.dante.device import DanteDevice
from netaudio.dante.subscription import DanteSubscription
from netaudio.utils.timeout import Timeout


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


def get_host_by_name(host):
    ipv4 = None

    try:
        ipv4 = ipaddress.ip_address(Timeout(socket.gethostbyname, 0.1)(host))
    except socket.gaierror:
        pass
    except TimeoutError:
        pass

    return ipv4


class DeviceListCommand(Command):
    name = "device list"
    description = "List devices"

    options = [
        option("json", None, "Output as JSON", flag=True),
        option("host", None, "Specify device by host", flag=False),
        option("name", None, "Specify device by name", flag=False),
    ]

    def filter_devices(self, devices):
        if self.option("name"):
            devices = dict(
                filter(lambda d: d[1].name == self.option("name"), devices.items())
            )
        elif self.option("host"):
            host = self.option("host")
            ipv4 = None

            try:
                ipv4 = ipaddress.ip_address(host)
            except ValueError:
                pass

            possible_names = set([host, host + ".local.", host + "."])

            if possible_names.intersection(set(devices.keys())):
                devices = dict(
                    filter(
                        lambda d: d[1].server_name in possible_names, devices.items()
                    )
                )
            else:
                try:
                    ipv4 = get_host_by_name(host)
                except TimeoutError:
                    pass

                devices = dict(filter(lambda d: d[1].ipv4 == ipv4, devices.items()))

        return devices

    def get_devices_from_redis(self):
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
                    rx_channel.device = self
                    rx_channel.name = rx_channel_data.get("name")
                    rx_channel.number = channel_number
                    rx_channel.status_code = rx_channel_data.get("status_code")
                    device.rx_channels[channel_number] = rx_channel

                tx_channels = json.loads(device_data.get("tx_channels", "{}"))

                for channel_number, tx_channel_data in tx_channels.items():
                    tx_channel = DanteChannel()
                    tx_channel.channel_type = "tx"
                    tx_channel.device = self
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

    async def device_list(self):
        cached_devices = self.get_devices_from_redis()

        if cached_devices is not None:
            devices = cached_devices
        else:
            dante_browser = DanteBrowser(mdns_timeout=1.5)
            devices = await dante_browser.get_devices()

        if self.option("name"):
            for _, device in devices.items():
                await device.get_controls()

        devices = self.filter_devices(devices)

        if not self.option("name"):
            for _, device in devices.items():
                await device.get_controls()

        devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

        if self.option("json"):
            json_object = json.dumps(devices, indent=2)
            self.line(f"{json_object}")
        else:
            for _, device in devices.items():
                self.line(f"{device}")

    def handle(self):
        asyncio.run(self.device_list())
