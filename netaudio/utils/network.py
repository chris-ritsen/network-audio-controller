import socket
from netaudio.utils.timeout import Timeout
from typing import Optional
import ipaddress


def get_host_by_name(host: str) -> Optional[ipaddress.IPv4Address]:
    ipv4: Optional[ipaddress.IPv4Address] = None

    try:
        ipv4 = ipaddress.ip_address(Timeout(socket.gethostbyname, 0.1)(host))
    except socket.gaierror:
        pass
    except TimeoutError:
        pass

    return ipv4