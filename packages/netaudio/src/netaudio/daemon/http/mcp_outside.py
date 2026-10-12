from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass

from netaudio.common.config_loader import load_config_document

logger = logging.getLogger("netaudio.mcp")

READ_TIMEOUT_SECONDS = 0.5
GENERIC_DEVICE_WORDS = frozenset(
    {"avio", "usb", "usbc", "dante", "rx", "tx", "lx", "aes", "aes3", "bt", "output", "input"}
)


@dataclass(frozen=True)
class OutletSource:
    url: str
    key: str
    field: str


def outlet_source() -> OutletSource | None:
    try:
        section = load_config_document().get("power_outlets") or {}
    except ValueError as exception:
        logger.warning("Unable to read [power_outlets]: %s", exception)
        return None
    if not isinstance(section, dict) or not section:
        return None
    url, key, field = section.get("redis_url"), section.get("hash"), section.get("field")
    if not (isinstance(url, str) and url and isinstance(key, str) and key and isinstance(field, str) and field):
        logger.warning("[power_outlets] needs redis_url, hash and field")
        return None
    return OutletSource(url, key, field)


def elsewhere() -> dict[str, str]:
    try:
        section = (load_config_document().get("mcp") or {}).get("elsewhere") or {}
    except ValueError as exception:
        logger.warning("Unable to read [mcp.elsewhere]: %s", exception)
        return {}
    if not isinstance(section, dict):
        return {}
    return {str(task): str(where) for task, where in section.items() if isinstance(where, str) and where}


def elsewhere_instructions() -> str:
    pointers = elsewhere()
    if not pointers:
        return ""
    listed = "; ".join(f"{task}: {where}" for task, where in pointers.items())
    return f" netaudio cannot do these; say so plainly and name where they are done: {listed}."


def outlet_for(device: str, outlets: list[dict]) -> dict | None:
    words = {
        word
        for word in re.findall(r"[a-z0-9]+", device.casefold())
        if len(word) > 1 and not word.isdigit() and word not in GENERIC_DEVICE_WORDS
    }
    if not words:
        return None
    for outlet in outlets:
        if words <= set(re.findall(r"[a-z0-9]+", str(outlet.get("name") or "").casefold())):
            return outlet
    return None


def outlet_text(outlet: dict) -> str:
    return f"{outlet.get('name')}, outlet {outlet.get('outlet')}, is switched {outlet.get('state')}"


class OutletReader:
    def __init__(self, source: OutletSource) -> None:
        self.source = source
        self._client = None

    async def read(self) -> list[dict] | None:
        try:
            import redis.asyncio as aioredis
            from redis.exceptions import RedisError
        except ImportError:
            return None
        if self._client is None:
            self._client = aioredis.from_url(self.source.url, decode_responses=True)
        try:
            raw = await asyncio.wait_for(self._client.hget(self.source.key, self.source.field), READ_TIMEOUT_SECONDS)
        except (RedisError, OSError, asyncio.TimeoutError) as exception:
            logger.debug("Could not read power outlets: %s", exception)
            return None
        try:
            value = json.loads(raw) if isinstance(raw, str) else None
        except ValueError:
            return None
        if not isinstance(value, list):
            return None
        return [entry for entry in value if isinstance(entry, dict) and entry.get("name")]
