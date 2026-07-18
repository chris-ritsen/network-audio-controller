from __future__ import annotations

import ipaddress
import logging
import re
import subprocess
import sys

SHURE_OUI = "00:0e:dd"

logger = logging.getLogger("netaudio")

_IPV4_ADDRESS_PATTERN = re.compile(r"(?<![\d.])((?:\d{1,3}\.){3}\d{1,3})(?![\d.])")
_MAC_ADDRESS_PATTERN = re.compile(
    r"(?<![0-9a-f])([0-9a-f]{1,2}(?:[:-][0-9a-f]{1,2}){5})(?![0-9a-f])",
    re.IGNORECASE,
)


def _neighbor_commands():
    if sys.platform.startswith("linux"):
        return (("ip", "neigh", "show"), ("arp", "-an"))
    if sys.platform == "darwin" or "bsd" in sys.platform:
        return (("arp", "-an"),)
    if sys.platform == "win32":
        return (("arp", "-a"),)
    return (("ip", "neigh", "show"), ("arp", "-an"), ("arp", "-a"))


def _parse_neighbor_table(output):
    entries = []
    seen = set()

    for line in output.splitlines():
        mac_address_match = _MAC_ADDRESS_PATTERN.search(line)
        if not mac_address_match:
            continue

        mac_address = ":".join(
            address_part.zfill(2) for address_part in re.split(r"[:-]", mac_address_match.group(1).lower())
        )
        if not mac_address.startswith(f"{SHURE_OUI}:"):
            continue

        ip_address = None
        for candidate in _IPV4_ADDRESS_PATTERN.findall(line):
            try:
                ip_address = str(ipaddress.IPv4Address(candidate))
                break
            except ipaddress.AddressValueError:
                continue

        neighbor_entry = (ip_address, mac_address)
        if ip_address is not None and neighbor_entry not in seen:
            entries.append(neighbor_entry)
            seen.add(neighbor_entry)

    return entries


def get_shure_neighbor_entries():
    for command in _neighbor_commands():
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=2,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exception:
            logger.debug(f"Shure neighbor command failed to start: {' '.join(command)}: {exception}")
            continue

        if result.returncode != 0:
            logger.debug(f"Shure neighbor command failed with status {result.returncode}: {' '.join(command)}")
            continue

        entries = _parse_neighbor_table(result.stdout)
        if entries:
            return entries

    return []
