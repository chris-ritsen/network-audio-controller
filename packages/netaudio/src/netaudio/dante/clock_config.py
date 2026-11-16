from __future__ import annotations

from netaudio import core


def format_clock_source_code(clock_source_code) -> str:
    return core.clock_sources({"current": clock_source_code, "supported": []})["current"] or "unknown"


def format_clock_subdomain(value) -> str:
    return core.clock_subdomain_presentation(value)["label"]
