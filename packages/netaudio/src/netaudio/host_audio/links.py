from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

from netaudio.host_audio.alsa import card_for_reference

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

logger = logging.getLogger("netaudio")

SECTION = "host_audio.cards"
HARDWARE_ALIAS = re.compile(r"^alsa_pcm:hw:([^,:]+),(\d+):(out|in)(\d+)$")
BRIDGE_PROGRAMS = {
    "alsa_in": "capture",
    "zita-a2j": "capture",
    "alsa_out": "playback",
    "zita-j2a": "playback",
}
BRIDGE_PORT = re.compile(r"^(?:capture|playback)_(\d+)$")


def _config_path() -> Path:
    from netaudio.common.config_loader import default_config_path

    return default_config_path()


def card_links() -> dict[str, str]:
    path = _config_path()
    try:
        document = tomllib.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exception:
        logger.warning("Unable to read audio card links from %s: %s", path, exception)
        return {}
    section = (document.get("host_audio") or {}).get("cards") or {}
    if not isinstance(section, dict):
        logger.warning("[%s] in %s must be a table of Dante device names to ALSA card IDs", SECTION, path)
        return {}
    return {str(device): str(card) for device, card in section.items() if isinstance(card, str)}


def save_card_link(card: str, device: str | None) -> Path:
    path = _config_path()
    text = path.read_text() if path.exists() else ""
    links = {name: linked for name, linked in card_links().items() if linked != card}
    if device:
        links[device] = card
    lines = text.splitlines(keepends=True)
    output: list[str] = []
    inside = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("["):
            inside = stripped == f"[{SECTION}]"
            if inside:
                continue
        if inside:
            continue
        output.append(line)
    while output and not output[-1].strip():
        output.pop()
    if links:
        if output and not output[-1].endswith("\n"):
            output.append("\n")
        if output:
            output.append("\n")
        output.append(f"[{SECTION}]\n")
        for name, linked in sorted(links.items()):
            output.append(f"{json.dumps(name)} = {json.dumps(linked)}\n")
    elif output and not output[-1].endswith("\n"):
        output.append("\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(output))
    return path


def hardware_channel(port: dict, cards: list[dict]) -> dict | None:
    for alias in port.get("aliases") or ():
        match = HARDWARE_ALIAS.match(alias)
        if match is None:
            continue
        card = card_for_reference(match.group(1), cards)
        if card is None:
            continue
        return {
            "card": card["id"],
            "pcm": int(match.group(2)),
            "direction": "capture" if match.group(3) == "out" else "playback",
            "channel": int(match.group(4)),
        }
    return None


def _arguments(path: Path) -> list[str]:
    try:
        return [part.decode(errors="replace") for part in path.read_bytes().split(b"\0") if part]
    except OSError:
        return []


def bridge_clients(cards: list[dict]) -> dict[str, dict]:
    bridges: dict[str, dict] = {}
    try:
        processes = list(Path("/proc").iterdir())
    except OSError:
        return bridges
    for process in processes:
        if not process.name.isdigit():
            continue
        arguments = _arguments(process / "cmdline")
        if not arguments:
            continue
        program = Path(arguments[0]).name
        direction = BRIDGE_PROGRAMS.get(program)
        if direction is None:
            continue
        options = dict(zip(arguments[1:], arguments[2:]))
        client = options.get("-j")
        device = options.get("-d")
        if not client or not device:
            continue
        card = card_for_reference(device, cards)
        if card is None:
            continue
        bridges[client] = {"card": card["id"], "direction": direction, "program": program}
    return bridges


def bridge_channel(port_name: str, bridges: dict[str, dict]) -> dict | None:
    client, _, short_name = port_name.partition(":")
    bridge = bridges.get(client)
    if bridge is None:
        return None
    match = BRIDGE_PORT.match(short_name)
    if match is None:
        return None
    return {"card": bridge["card"], "pcm": 0, "direction": bridge["direction"], "channel": int(match.group(1))}
