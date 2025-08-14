from typing import Any, Dict, Optional
import ipaddress
from netaudio.utils import get_host_by_name
from netaudio.dante.device import DanteDevice

def filter_devices(devices: Dict[str, Any],
                name: Optional[str] = None,
                host: Optional[str] = None) -> Dict[str, DanteDevice]:
    if name:
        devices = dict(
            filter(
                lambda d: d[1].name == name, devices.items()
            )
        )
    elif host:
        ipv4: ipaddress.IPv4Address | None = None

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

    if not devices:
        raise ValueError("No devices found.")

    return devices