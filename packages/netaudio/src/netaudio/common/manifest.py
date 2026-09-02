from __future__ import annotations

import json
from pathlib import Path

MANIFEST_FILE_NAME = "manifest.json"


def manifest_bytes(manifest: dict) -> bytes:
    return (json.dumps(manifest, indent=2) + "\n").encode("utf-8")


def write_manifest(directory: Path, manifest: dict) -> Path:
    manifest_path = directory / MANIFEST_FILE_NAME
    manifest_path.write_bytes(manifest_bytes(manifest))
    return manifest_path
