import ipaddress
import json
import socket
from json import JSONEncoder

from netaudio.dante.channel import DanteChannel
from netaudio.dante.device import DanteDevice
from netaudio.dante.subscription import DanteSubscription


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "to_json"):
            return obj.to_json()

        if isinstance(obj, (DanteDevice, DanteChannel, DanteSubscription)):
            return {
                key: value
                for key, value in obj.__dict__.items()
                if not key.startswith("_")
            }
        try:
            return JSONEncoder.default(self, obj)
        except TypeError:
            return str(obj)


def get_host_by_name_util(host: str) -> ipaddress.IPv4Address | None:
    ipv4 = None
    try:
        address_info = socket.gethostbyname(host)
        ipv4 = ipaddress.ip_address(address_info)
    except (
        socket.gaierror,
        ValueError,
        OSError,
    ):
        pass
    return ipv4 if isinstance(ipv4, ipaddress.IPv4Address) else None


def filter_devices_util(
    devices: dict[str, DanteDevice], name_filter: str | None, host_filter: str | None
) -> dict[str, DanteDevice]:
    if name_filter:
        name_filter_lower = name_filter.lower()
        return {
            k: v
            for k, v in devices.items()
            if (hasattr(v, "name") and v.name and v.name.lower() == name_filter_lower)
            or (
                hasattr(v, "server_name")
                and v.server_name
                and (
                    v.server_name.lower() == name_filter_lower
                    or v.server_name.lower().startswith(name_filter_lower + ".")
                )
            )
        }
    elif host_filter:
        try:
            ipv4_filter = ipaddress.ip_address(host_filter)
            return {
                k: v
                for k, v in devices.items()
                if hasattr(v, "ipv4")
                and v.ipv4
                and ipaddress.ip_address(v.ipv4) == ipv4_filter
            }
        except ValueError:
            possible_names_lower = {
                host_filter.lower(),
                host_filter.lower() + ".local.",
                host_filter.lower() + ".",
            }
            name_filtered_devices = {
                k: v
                for k, v in devices.items()
                if hasattr(v, "server_name")
                and v.server_name
                and v.server_name.lower() in possible_names_lower
            }
            if name_filtered_devices:
                return name_filtered_devices

            resolved_ip = get_host_by_name_util(host_filter)
            if resolved_ip:
                return {
                    k: v
                    for k, v in devices.items()
                    if hasattr(v, "ipv4")
                    and v.ipv4
                    and ipaddress.ip_address(v.ipv4) == resolved_ip
                }
            return {}
    return devices
