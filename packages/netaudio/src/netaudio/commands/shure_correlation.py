from __future__ import annotations

import re
import socket


def _normalize_mac(mac):
    normalized_address = re.sub(r"[:\-.]", "", mac).lower()[:12]
    return ":".join(normalized_address[index : index + 2] for index in range(0, len(normalized_address), 2))


def _mac_match(first_address, second_address):
    return _normalize_mac(first_address) == _normalize_mac(second_address)


def _config_path():
    from netaudio.common.config_loader import default_config_path

    return default_config_path()


def _load_correlation(shure_mac):
    config_path = _config_path()
    if not config_path.exists():
        return None
    for line in config_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("["):
            continue
        if "=" in line and '"' in line:
            line_parts = line.split("=", 1)
            stored_shure_mac = line_parts[0].strip().strip('"')
            if _mac_match(stored_shure_mac, shure_mac):
                return line_parts[1].strip().strip('"')
    return None


def _save_correlation(shure_mac, dante_mac):
    config_path = _config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    config_text = config_path.read_text() if config_path.exists() else ""
    section_header = "[shure.correlations]"

    output = []
    skip_section = False
    in_section = False
    replaced = False

    for line in config_text.splitlines(keepends=True):
        stripped = line.strip()

        if stripped.startswith("[shure.correlations."):
            header_mac_address = stripped.split('"')[1] if '"' in stripped else ""
            if _mac_match(header_mac_address, shure_mac):
                skip_section = True
                continue

        if skip_section:
            if stripped.startswith("["):
                skip_section = False
            else:
                continue

        if stripped == section_header:
            in_section = True
            output.append(line)
            continue

        if in_section:
            if stripped.startswith("["):
                in_section = False
            elif '"' in stripped:
                stored_mac_address = stripped.split('"')[1]
                if _mac_match(stored_mac_address, shure_mac):
                    output.append(f'"{shure_mac}" = "{dante_mac}"\n')
                    replaced = True
                    continue

        output.append(line)

    if not replaced:
        if not any(line.strip() == section_header for line in output):
            if output and not output[-1].endswith("\n"):
                output.append("\n")
            output.append(f"\n{section_header}\n")
        output.append(f'"{shure_mac}" = "{dante_mac}"\n')

    config_path.write_text("".join(output))
    return config_path


def _shure_query(host, port, commands):
    with socket.create_connection((host, port), timeout=2) as connection_socket:
        connection_socket.settimeout(0.3)
        connection_socket.sendall("".join(f"< {requested_command} >\r\n" for requested_command in commands).encode())

        response = b""
        expected_response_count = len(commands)
        while response.count(b"REP") < expected_response_count:
            try:
                chunk = connection_socket.recv(4096)
                if not chunk:
                    break
                response += chunk
            except socket.timeout:
                break

    parsed_responses = {}
    for match in re.finditer(
        r"REP\s+(\d)\s+(\S+)\s+\{?([^}>]*?)\}?\s*>",
        response.decode("utf-8", errors="replace"),
    ):
        channel = int(match.group(1))
        parsed_responses.setdefault(channel, {})[match.group(2)] = match.group(3).strip()
    return parsed_responses


def _sample_shure_levels(host, port, channels):
    responses = _shure_query(
        host,
        port,
        [f"GET {channel} AUDIO_LEVEL_RMS" for channel in channels],
    )
    return {
        channel: int(values["AUDIO_LEVEL_RMS"]) for channel, values in responses.items() if "AUDIO_LEVEL_RMS" in values
    }


def _get_active_shure_channels(host, port, channels):
    responses = _shure_query(
        host,
        port,
        [command for channel in channels for command in [f"GET {channel} TX_MODEL", f"GET {channel} ANTENNA_STATUS"]],
    )

    return [
        int(channel)
        for channel in channels
        if responses.get(int(channel), {}).get("TX_MODEL", "UNKNOWN") != "UNKNOWN"
        and responses.get(int(channel), {}).get("ANTENNA_STATUS", "XX") != "XX"
    ]
