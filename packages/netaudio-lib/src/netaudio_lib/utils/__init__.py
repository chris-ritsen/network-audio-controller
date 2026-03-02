import ipaddress
import socket

from .timeout import Timeout


def get_host_by_name(host):
    ipv4 = None

    try:
        ipv4 = ipaddress.ip_address(Timeout(socket.gethostbyname, 0.1)(host))
    except socket.gaierror:
        pass
    except TimeoutError:
        pass

    return ipv4
