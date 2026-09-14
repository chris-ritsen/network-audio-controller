from __future__ import annotations

import json
import logging
import re
import socket
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, distribution
from importlib.resources import files

logger = logging.getLogger("netaudio")


REVISION_PATTERN = re.compile(r"[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?")


def server_info() -> dict[str, str]:
    information = {
        "hostname": socket.gethostname().removesuffix(".local"),
        "product": "netaudio",
        "started_at": _started_at(),
    }
    if revision := _built_revision():
        information["git_revision"] = revision
    try:
        installed = distribution("netaudio")
    except PackageNotFoundError:
        return information

    version = installed.version
    if isinstance(version, str) and re.fullmatch(r"[A-Za-z0-9.+_-]{1,128}", version):
        information["version"] = version
    try:
        source_metadata = installed.read_text("direct_url.json")
    except (OSError, UnicodeError):
        logger.warning("Installed package source metadata is unavailable")
        return information
    if not source_metadata:
        return information
    try:
        source = json.loads(source_metadata)
    except (TypeError, ValueError):
        logger.warning("Installed package source metadata could not be read")
        return information
    source_control = source.get("vcs_info") if isinstance(source, dict) else None
    if not isinstance(source_control, dict) or source_control.get("vcs") != "git":
        return information
    revision = source_control.get("commit_id")
    if isinstance(revision, str) and REVISION_PATTERN.fullmatch(revision):
        information["git_revision"] = revision.lower()
    return information


def _started_at() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _built_revision() -> str | None:
    resource = files("netaudio").joinpath("_build_info.json")
    if not resource.is_file():
        return None
    try:
        built = json.loads(resource.read_text())
    except (OSError, UnicodeError, ValueError):
        logger.warning("Build information could not be read")
        return None
    revision = built.get("git_revision") if isinstance(built, dict) else None
    if isinstance(revision, str) and REVISION_PATTERN.fullmatch(revision):
        return revision.lower()
    return None
