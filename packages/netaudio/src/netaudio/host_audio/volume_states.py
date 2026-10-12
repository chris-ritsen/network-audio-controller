from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

from netaudio.common.config_loader import load_config_document

logger = logging.getLogger("netaudio")

READ_TIMEOUT_SECONDS = 0.5


@dataclass(frozen=True)
class VolumeStateSource:
    url: str
    key: str
    field: str


def volume_state_source() -> VolumeStateSource | None:
    try:
        section = ((load_config_document().get("host_audio") or {}).get("volume_states")) or {}
    except ValueError as exception:
        logger.warning("Unable to read [host_audio.volume_states]: %s", exception)
        return None
    if not isinstance(section, dict) or not section:
        return None
    url, key, field = section.get("redis_url"), section.get("hash"), section.get("field")
    if not (isinstance(url, str) and url and isinstance(key, str) and key and isinstance(field, str) and field):
        logger.warning("[host_audio.volume_states] needs redis_url, hash and field, such as {host}:volume:{client}")
        return None
    return VolumeStateSource(url, key, field)


def _state(raw: Any) -> dict | None:
    if not isinstance(raw, str):
        return None
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(value, dict):
        return None
    level = value.get("level")
    return {
        "muted": value.get("muted") is True or value.get("solo_muted") is True,
        "level": level if isinstance(level, (int, float)) and not isinstance(level, bool) else None,
        "name": value.get("name") if isinstance(value.get("name"), str) else None,
    }


class VolumeStateReader:
    def __init__(self, source: VolumeStateSource) -> None:
        self.source = source
        self._client = None

    async def read(self, clients: list[tuple[str, str]]) -> dict[tuple[str, str], dict]:
        if not clients:
            return {}
        try:
            import redis.asyncio as aioredis
            from redis.exceptions import RedisError
        except ImportError:
            return {}
        if self._client is None:
            self._client = aioredis.from_url(self.source.url, decode_responses=True)
        fields = [self.source.field.format(host=host, client=client) for host, client in clients]
        try:
            values = await asyncio.wait_for(self._client.hmget(self.source.key, fields), READ_TIMEOUT_SECONDS)
        except (RedisError, OSError, asyncio.TimeoutError) as exception:
            logger.debug("Could not read volume stage states: %s", exception)
            return {}
        states = {}
        for client, raw in zip(clients, values):
            state = _state(raw)
            if state is not None:
                states[client] = state
        return states
