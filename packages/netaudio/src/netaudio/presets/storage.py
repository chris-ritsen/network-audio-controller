from __future__ import annotations

import os
import re
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from netaudio.presets.parsing import parse_preset


def preset_directory() -> Path:
    from netaudio.common.config_loader import default_config_path, get_config_value

    configured, _ = get_config_value("preset_directory")
    if configured:
        return Path(str(configured)).expanduser()
    return default_config_path().parent / "presets"


def write_preset_atomic(path: Path, content: str, *, force: bool) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())

        if force:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
            temporary.unlink()
    finally:
        temporary.unlink(missing_ok=True)


def preset_reference(name: str) -> str:
    reference = re.sub(r"[^\w.-]+", "-", name.strip()).strip("-.")
    if not reference:
        raise ValueError("the preset name needs at least one letter or digit")
    return reference


def stored_preset_path(reference: str) -> Path:
    if reference != preset_reference(reference):
        raise ValueError(f"{reference!r} is not a saved preset name; list_presets shows them")
    return preset_directory() / f"{reference}.xml"


def list_presets() -> tuple[list[dict], list[str]]:
    directory = preset_directory()
    presets, problems = [], []
    for path in sorted(directory.glob("*.xml")) if directory.is_dir() else []:
        try:
            name, devices = parse_preset(path)
        except (ET.ParseError, OSError, ValueError) as exception:
            problems.append(f"{path.name}: {exception}")
            continue
        presets.append(
            {
                "preset": path.stem,
                "name": name,
                "devices": sorted(devices),
                "saved": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "path": str(path),
            }
        )
    return presets, problems
