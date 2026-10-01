from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
BASE_DIGEST_LABEL = "org.opencontainers.image.base.digest"


def _inspect(reference: str, template: str) -> object:
    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", reference, "--format", template],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise LookupError(result.stderr.strip() or f"cannot inspect {reference}")
    return json.loads(result.stdout)


def base_reference(dockerfile: Path) -> str:
    text = dockerfile.read_text()
    values = {}
    for name in ("PYTHON_VERSION", "DEBIAN_RELEASE"):
        match = re.search(rf"^ARG {name}=(\S+)$", text, re.MULTILINE)
        if match is None:
            raise ValueError(f"{dockerfile} has no default for {name}")
        values[name] = match.group(1)
    return f"docker.io/library/python:{values['PYTHON_VERSION']}-slim-{values['DEBIAN_RELEASE']}"


def current_digest(dockerfile: Path) -> str:
    manifest = _inspect(base_reference(dockerfile), "{{json .Manifest}}")
    if not isinstance(manifest, dict) or not isinstance(manifest.get("digest"), str):
        raise ValueError("the base image manifest has no digest")
    return manifest["digest"]


def published_digest(image: str) -> str:
    try:
        images = _inspect(image, "{{json .Image}}")
    except LookupError:
        return ""
    candidates = [images] if isinstance(images, dict) and "config" in images else []
    if isinstance(images, dict) and not candidates:
        candidates = [value for key, value in sorted(images.items()) if key.startswith("linux/")]
    for candidate in candidates:
        labels = (candidate.get("config") or {}).get("Labels") or {}
        if labels.get(BASE_DIGEST_LABEL):
            return labels[BASE_DIGEST_LABEL]
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare the image's base with the base a published image used.")
    parser.add_argument(
        "--dockerfile",
        type=Path,
        default=ROOT / "containers" / "Dockerfile",
        help="Dockerfile whose PYTHON_VERSION and DEBIAN_RELEASE defaults name the base image",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("current", help="print the base image's current index digest")
    published = subcommands.add_parser("published", help="print the base digest a published image was built from")
    published.add_argument("image")
    arguments = parser.parse_args()

    if arguments.command == "current":
        print(current_digest(arguments.dockerfile))
    else:
        print(published_digest(arguments.image))
    return 0


if __name__ == "__main__":
    sys.exit(main())
