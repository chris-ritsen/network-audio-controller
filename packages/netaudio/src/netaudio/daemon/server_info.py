from __future__ import annotations

import json
import logging
import re
from importlib.metadata import PackageNotFoundError, distribution

logger = logging.getLogger("netaudio")


def server_info() -> dict[str, str]:
    information = {"product": "netaudio"}
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
    if isinstance(revision, str) and re.fullmatch(r"[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?", revision):
        information["git_revision"] = revision.lower()
    return information
