from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger("netaudio")

PROC_ASOUND = Path("/proc/asound")
SYS_SOUND = Path("/sys/class/sound")
CARD_LINE = re.compile(r"^\s*(\d+)\s+\[(\S+)\s*\]:\s+(\S+)\s+-\s+(.*)$")
PCM_DIRECTORY = re.compile(r"^pcm(\d+)([pc])$")
AUDINATE_USB_VENDOR = "3018"


def _read(path: Path) -> str | None:
    try:
        return path.read_text().strip() or None
    except OSError:
        return None


def _usb_details(index: int) -> dict:
    try:
        device = (SYS_SOUND / f"card{index}" / "device").resolve()
    except OSError:
        return {}
    for directory in (device, *device.parents):
        if (directory / "idVendor").exists():
            return {
                "usb_vendor": _read(directory / "manufacturer"),
                "usb_product": _read(directory / "product"),
                "usb_serial": _read(directory / "serial"),
                "usb_path": directory.name,
            }
        if directory == directory.parent or directory.name == "devices":
            break
    return {}


def cards() -> list[dict]:
    text = _read(PROC_ASOUND / "cards")
    if text is None:
        return []
    lines = text.splitlines()
    found = []
    for position, line in enumerate(lines):
        match = CARD_LINE.match(line)
        if match is None:
            continue
        index = int(match.group(1))
        card_directory = PROC_ASOUND / f"card{index}"
        playback, capture = [], []
        try:
            entries = sorted(card_directory.iterdir())
        except OSError:
            entries = []
        for entry in entries:
            pcm = PCM_DIRECTORY.match(entry.name)
            if pcm is not None:
                (playback if pcm.group(2) == "p" else capture).append(int(pcm.group(1)))
        card = {
            "index": index,
            "id": match.group(2),
            "driver": match.group(3),
            "name": match.group(4).strip(),
            "long_name": lines[position + 1].strip() if position + 1 < len(lines) else None,
            "usb_id": _read(card_directory / "usbid"),
            "playback_devices": playback,
            "capture_devices": capture,
        }
        if card["usb_id"]:
            card.update(_usb_details(index))
        found.append(card)
    return found


def card_for_reference(reference: str, known: list[dict]) -> dict | None:
    value = reference.strip()
    if "CARD=" in value:
        value = value.split("CARD=", 1)[1]
    elif ":" in value:
        value = value.split(":", 1)[1]
    value = value.split(",", 1)[0]
    for card in known:
        if value == card["id"] or (value.isdigit() and int(value) == card["index"]):
            return card
    return None


def is_dante_interface(card: dict) -> bool:
    text = " ".join(str(card.get(key) or "") for key in ("id", "driver", "name", "long_name", "usb_vendor"))
    return "dante" in text.casefold() or str(card.get("usb_id") or "").startswith(f"{AUDINATE_USB_VENDOR}:")
