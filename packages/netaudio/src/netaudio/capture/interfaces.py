from __future__ import annotations

import sys

from netaudio.common.app_config import get_available_interfaces
from netaudio.common.app_config import settings as app_settings


def _default_interface() -> tuple[str, str]:
    if app_settings.interface:
        return app_settings.interface, "config"

    if sys.platform == "darwin":
        interface, service_name = _default_interface_macos()
        if interface:
            return interface, service_name

    interfaces = get_available_interfaces()
    for name, ip, _ in interfaces:
        if ip != "127.0.0.1":
            return name, "first available"

    if interfaces:
        return interfaces[0][0], "first available"

    return "any", "fallback"


def _default_interface_macos() -> tuple[str | None, str | None]:
    import subprocess

    try:
        result = subprocess.run(
            ["networksetup", "-listnetworkserviceorder"],
            capture_output=True, text=True, timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None, None

    if result.returncode != 0:
        return None, None

    available_ips = {
        name: ip
        for name, ip, _ in get_available_interfaces()
        if ip != "127.0.0.1"
    }

    import re
    for match in re.finditer(
        r"^\(\d+\)\s+(.+)\n\(Hardware Port: .+, Device: (\S+)\)",
        result.stdout,
        re.MULTILINE,
    ):
        service_name = match.group(1)
        device = match.group(2)
        if device in available_ips:
            return device, service_name

    return None, None
