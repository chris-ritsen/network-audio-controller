from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from netaudio.common.config_loader import load_config_document

logger = logging.getLogger("netaudio")

READ_TIMEOUT_SECONDS = 0.5
PICTURE_STATES = {"normal": "on", "pictureoff": "off"}


@dataclass(frozen=True)
class DisplaySource:
    url: str
    key: str
    presentation: str
    lease: str
    outputs: dict[str, str]
    words: frozenset[str]

    def matches(self, point: str) -> bool:
        return bool(set(re.findall(r"[a-z0-9]+", point.casefold())) & self.words)


@dataclass
class Presentation:
    host: str
    display: str
    endpoint: str
    output: str
    picture: str | None = None


@dataclass
class DisplayReading:
    presentations: list[Presentation] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)


def display_source() -> DisplaySource | None:
    try:
        section = ((load_config_document().get("host_audio") or {}).get("displays")) or {}
    except ValueError as exception:
        logger.warning("Unable to read [host_audio.displays]: %s", exception)
        return None
    if not isinstance(section, dict) or not section:
        return None
    url, key = section.get("redis_url"), section.get("hash")
    presentation, lease = section.get("presentation"), section.get("lease")
    outputs, words = section.get("outputs"), section.get("words")
    if not (
        all(isinstance(value, str) and value for value in (url, key, presentation, lease))
        and isinstance(outputs, dict)
        and outputs
        and isinstance(words, list)
    ):
        logger.warning(
            "[host_audio.displays] needs redis_url, hash, presentation and lease (with {host}), "
            "outputs (computer = output point) and words"
        )
        return None
    return DisplaySource(
        str(url),
        str(key),
        str(presentation),
        str(lease),
        {str(host): str(point) for host, point in outputs.items()},
        frozenset(str(word).casefold() for word in words),
    )


def _decode(raw: Any) -> dict | None:
    if not isinstance(raw, str):
        return None
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _clock(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).strftime("%I:%M %p").lstrip("0")


def evaluate(host: str, record: dict | None, lease: dict | None, now: float) -> tuple[bool, str | None]:
    if record is None or record.get("presented") is not True:
        return False, None
    revision = str(record.get("revision") or "")
    if not all((revision, record.get("sink"), record.get("endpoint"))):
        return False, f"{host}'s presentation record is incomplete"
    if lease is None:
        return False, f"{host}'s presentation is not confirmed by a current observer"
    if str(lease.get("host") or "") != host or str(lease.get("revision") or "") != revision:
        return False, f"{host}'s presentation observer confirms a different revision, so the record may be out of date"
    expires = lease.get("expires_at")
    if not isinstance(expires, (int, float)) or expires < now:
        when = f" at {_clock(expires)}" if isinstance(expires, (int, float)) else ""
        return False, f"{host}'s presentation record is stale: its observer stopped confirming it{when}"
    return True, None


class DisplayReader:
    def __init__(self, source: DisplaySource) -> None:
        self.source = source
        self._client = None

    async def read(self, point: str) -> DisplayReading:
        try:
            import redis.asyncio as aioredis
            from redis.exceptions import RedisError
        except ImportError:
            return DisplayReading(problems=["the redis package is not installed"])
        if self._client is None:
            self._client = aioredis.from_url(self.source.url, decode_responses=True)
        hosts = list(self.source.outputs)
        try:
            records = await asyncio.wait_for(
                self._client.hmget(self.source.key, [self.source.presentation.format(host=host) for host in hosts]),
                READ_TIMEOUT_SECONDS,
            )
            leases = await asyncio.wait_for(
                self._client.mget([self.source.lease.format(host=host) for host in hosts]), READ_TIMEOUT_SECONDS
            )
        except (RedisError, OSError, asyncio.TimeoutError) as exception:
            return DisplayReading(problems=[f"the display presentation records could not be read: {exception}"])
        reading = DisplayReading()
        now = time.time()
        words = set(re.findall(r"[a-z0-9]+", point.casefold()))
        displays: set[str] = set()
        for host, raw_record, raw_lease in zip(hosts, records, leases):
            record = _decode(raw_record)
            for entry in [record or {}, *((record or {}).get("candidates") or [])]:
                if isinstance(entry, dict) and entry.get("sink"):
                    displays.add(str(entry["sink"]).casefold())
            presented, problem = evaluate(host, record, _decode(raw_lease), now)
            if problem:
                reading.problems.append(problem)
            if not presented or record is None:
                continue
            presentation = Presentation(host, str(record["sink"]), str(record["endpoint"]), self.source.outputs[host])
            reading.presentations.append(presentation)
            state_key = record.get("display_state_key")
            if isinstance(state_key, str) and state_key:
                try:
                    state = _decode(
                        await asyncio.wait_for(self._client.hget(self.source.key, state_key), READ_TIMEOUT_SECONDS)
                    )
                except (RedisError, OSError, asyncio.TimeoutError) as exception:
                    logger.debug("Could not read the display state %s: %s", state_key, exception)
                    state = None
                if state is not None:
                    presentation.picture = PICTURE_STATES.get(str(state.get("state_name")))
        named = words & displays
        if named:
            reading.presentations = [
                presentation for presentation in reading.presentations if presentation.display.casefold() in named
            ]
            missing = sorted(named - {presentation.display.casefold() for presentation in reading.presentations})
            reading.problems.extend(
                f"the {display.upper()} is not showing any of {', '.join(hosts)}" for display in missing
            )
        return reading
