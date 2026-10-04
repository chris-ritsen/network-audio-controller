from __future__ import annotations

import math
import time
from array import array
from bisect import bisect_right
from collections.abc import Callable, Iterable
from datetime import datetime, timezone

from netaudio.dante.metering import normalize_metering_value

SECOND_SLOTS = 900
MINUTE_SLOTS = 1440
SILENCE = -math.inf
SIGNAL_FLOOR_DBFS = -80.0
TIMELINE_BUCKETS = 20
TIMELINE_STEPS = (1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600)
DANTE_SIGNAL_STATES = frozenset({"signal_present", "clipping"})
DANTE_LEVEL_STATES = frozenset({"signal_present", "clipping", "below_threshold"})
SHURE_LEVEL_OFFSET = 120

Key = tuple[str, str]


def iso_second(second: float) -> str:
    return datetime.fromtimestamp(second, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _Track:
    __slots__ = (
        "second_stamps",
        "second_peaks",
        "second_signal",
        "minute_stamps",
        "minute_peaks",
        "minute_signal",
        "minute_recorded",
    )

    def __init__(self) -> None:
        self.second_stamps = array("q", [0]) * SECOND_SLOTS
        self.second_peaks = array("f", [SILENCE]) * SECOND_SLOTS
        self.second_signal = array("b", [0]) * SECOND_SLOTS
        self.minute_stamps = array("q", [0]) * MINUTE_SLOTS
        self.minute_peaks = array("f", [SILENCE]) * MINUTE_SLOTS
        self.minute_signal = array("H", [0]) * MINUTE_SLOTS
        self.minute_recorded = array("H", [0]) * MINUTE_SLOTS

    def record(self, second: int, peak: float | None, signal: bool) -> None:
        value = SILENCE if peak is None else peak
        minute = second // 60
        minute_slot = minute % MINUTE_SLOTS
        if self.minute_stamps[minute_slot] != minute:
            self.minute_stamps[minute_slot] = minute
            self.minute_peaks[minute_slot] = SILENCE
            self.minute_signal[minute_slot] = 0
            self.minute_recorded[minute_slot] = 0
        slot = second % SECOND_SLOTS
        if self.second_stamps[slot] != second:
            self.second_stamps[slot] = second
            self.second_peaks[slot] = value
            self.second_signal[slot] = signal
            self.minute_recorded[minute_slot] += 1
            if signal:
                self.minute_signal[minute_slot] += 1
        else:
            if value > self.second_peaks[slot]:
                self.second_peaks[slot] = value
            if signal and not self.second_signal[slot]:
                self.second_signal[slot] = 1
                self.minute_signal[minute_slot] += 1
        if value > self.minute_peaks[minute_slot]:
            self.minute_peaks[minute_slot] = value

    def newest_minute(self) -> int:
        return max(self.minute_stamps)

    def dump(self) -> bytes:
        return b"".join(getattr(self, name).tobytes() for name in self.__slots__)

    @classmethod
    def load(cls, data: bytes) -> _Track:
        track = cls()
        offset = 0
        for name in cls.__slots__:
            empty = getattr(track, name)
            size = len(empty) * empty.itemsize
            values = array(empty.typecode)
            values.frombytes(data[offset : offset + size])
            if len(values) != len(empty):
                raise ValueError("saved level track has the wrong size")
            setattr(track, name, values)
            offset += size
        if offset != len(data):
            raise ValueError("saved level track has the wrong size")
        return track


class _Source:
    __slots__ = ("track", "member_starts", "member_sets", "first_second")

    def __init__(self, second: int) -> None:
        self.track = _Track()
        self.member_starts: list[int] = []
        self.member_sets: list[frozenset[str]] = []
        self.first_second = second

    def note_members(self, second: int, members: frozenset[str]) -> None:
        if self.member_sets and self.member_sets[-1] == members:
            return
        self.member_starts.append(second)
        self.member_sets.append(members)
        cutoff = second - MINUTE_SLOTS * 60
        while len(self.member_starts) > 1 and self.member_starts[1] <= cutoff:
            del self.member_starts[0]
            del self.member_sets[0]

    def member_at(self, second: int, point: str) -> bool:
        index = bisect_right(self.member_starts, second) - 1
        return index >= 0 and point in self.member_sets[index]


class LevelHistory:
    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._sources: dict[str, _Source] = {}
        self._tracks: dict[Key, _Track] = {}
        self.started = int(clock())

    def record(
        self,
        source: str,
        second: int,
        values: dict[str, tuple[float | None, bool]],
        members: Iterable[str] | None = None,
    ) -> None:
        state = self._sources.get(source)
        if state is None:
            state = self._sources[source] = _Source(second)
        state.track.record(second, None, False)
        if members is not None:
            state.note_members(second, frozenset(members))
        for point, (peak, signal) in values.items():
            track = self._tracks.get((source, point))
            if track is None:
                if peak is None:
                    continue
                track = self._tracks[(source, point)] = _Track()
            track.record(second, peak, signal)

    def dump(self, now: float) -> tuple[list[tuple[str, int, list, bytes]], list[tuple[str, str, bytes]]]:
        oldest = int(now) // 60 - MINUTE_SLOTS
        sources = [
            (
                name,
                source.first_second,
                [[start, sorted(members)] for start, members in zip(source.member_starts, source.member_sets)],
                source.track.dump(),
            )
            for name, source in self._sources.items()
            if source.track.newest_minute() > oldest
        ]
        tracks = [
            (source, point, track.dump())
            for (source, point), track in self._tracks.items()
            if track.newest_minute() > oldest
        ]
        return sources, tracks

    def restore(self, sources: Iterable[tuple[str, int, list, bytes]], tracks: Iterable[tuple[str, str, bytes]]) -> int:
        if self._sources or self._tracks:
            return 0
        restored_sources: dict[str, _Source] = {}
        for name, first_second, members, data in sources:
            source = restored_sources[name] = _Source(int(first_second))
            source.track = _Track.load(data)
            for start, points in members:
                source.member_starts.append(int(start))
                source.member_sets.append(frozenset(points))
        restored_tracks = {(source_name, point): _Track.load(data) for source_name, point, data in tracks}
        self._sources, self._tracks = restored_sources, restored_tracks
        if restored_sources:
            self.started = min(source.first_second for source in restored_sources.values())
        return len(restored_tracks)

    def recorded_since(self, keys: Iterable[Key]) -> int | None:
        firsts = [self._sources[source].first_second for source, _ in keys if source in self._sources]
        return min(firsts) if firsts else None

    def _second(self, key: Key, second: int) -> tuple[float, int, int]:
        slot = second % SECOND_SLOTS
        track = self._tracks.get(key)
        if track is not None and track.second_stamps[slot] == second:
            return track.second_peaks[slot], track.second_signal[slot], 1
        source = self._sources.get(key[0])
        if source is not None and source.track.second_stamps[slot] == second and source.member_at(second, key[1]):
            return SILENCE, 0, 1
        return SILENCE, 0, 0

    def _minute(self, key: Key, minute: int) -> tuple[float, int, int]:
        slot = minute % MINUTE_SLOTS
        source = self._sources.get(key[0])
        recorded = 0
        if (
            source is not None
            and source.track.minute_stamps[slot] == minute
            and source.member_at(minute * 60 + 59, key[1])
        ):
            recorded = source.track.minute_recorded[slot]
        track = self._tracks.get(key)
        if track is not None and track.minute_stamps[slot] == minute:
            return track.minute_peaks[slot], track.minute_signal[slot], max(recorded, track.minute_recorded[slot])
        return SILENCE, 0, recorded

    def _buckets(self, keys: list[Key], start: int, end: int) -> tuple[int, list[tuple[int, float, int, int]]]:
        now = int(self._clock())
        if end - start <= SECOND_SLOTS and start > now - SECOND_SLOTS:
            buckets = []
            for second in range(start, end):
                readings = [self._second(key, second) for key in keys]
                buckets.append(
                    (
                        second,
                        max(reading[0] for reading in readings),
                        max(reading[1] for reading in readings),
                        max(reading[2] for reading in readings),
                    )
                )
            return 1, buckets
        buckets = []
        for minute in range(start // 60, -(-end // 60)):
            readings = [self._minute(key, minute) for key in keys]
            buckets.append(
                (
                    minute * 60,
                    max(reading[0] for reading in readings),
                    max(reading[1] for reading in readings),
                    max(reading[2] for reading in readings),
                )
            )
        return 60, buckets

    def summary(self, keys: list[Key], start: float, end: float) -> dict:
        first, last = int(math.floor(start)), max(int(math.floor(end)), int(math.floor(start)) + 1)
        resolution, buckets = self._buckets(keys, first, last)
        recorded = sum(bucket[3] for bucket in buckets)
        result: dict = {
            "start": iso_second(first),
            "end": iso_second(last),
            "resolution_seconds": resolution,
            "recorded_seconds": recorded,
        }
        if not recorded:
            since = self.recorded_since(keys)
            result["recorded_since"] = iso_second(since) if since is not None else None
            return result
        loudest = max(buckets, key=lambda bucket: bucket[1])
        with_signal = [bucket for bucket in buckets if bucket[2]]
        result["signal_seconds"] = sum(bucket[2] for bucket in buckets)
        if loudest[1] > SILENCE:
            result["loudest_dbfs"] = round(loudest[1], 1)
            result["loudest_at"] = iso_second(loudest[0])
        if with_signal:
            result["first_signal_at"] = iso_second(with_signal[0][0])
            result["last_signal_at"] = iso_second(with_signal[-1][0] + resolution - 1)
            result["timeline"] = _timeline(buckets, resolution, first, last)
        return result


def _timeline(buckets: list[tuple[int, float, int, int]], resolution: int, start: int, end: int) -> dict:
    span = max(end - start, resolution)
    step = next(
        (value for value in TIMELINE_STEPS if value >= resolution and span / value <= TIMELINE_BUCKETS),
        TIMELINE_STEPS[-1],
    )
    groups: dict[int, list[tuple[int, float, int, int]]] = {}
    for bucket in buckets:
        groups.setdefault((bucket[0] - start) // step, []).append(bucket)
    marks = []
    for index in range(-(-span // step)):
        group = groups.get(index, [])
        if not any(bucket[3] for bucket in group):
            marks.append("?")
        elif any(bucket[2] for bucket in group):
            marks.append(str(round(max(bucket[1] for bucket in group))))
        else:
            marks.append("·")
    return {"start": iso_second(start), "step_seconds": step, "peaks_dbfs": " ".join(marks)}


def history_text(summary: dict) -> str:
    recorded = summary.get("recorded_seconds") or 0
    if not recorded and summary.get("not_recording"):
        return summary["not_recording"]
    if not recorded:
        since = summary.get("recorded_since")
        return "no levels recorded in that period" + (f"; recording began {since}" if since else "")
    signal = summary.get("signal_seconds") or 0
    loudest = summary.get("loudest_dbfs")
    if not signal:
        text = f"no signal in {recorded} recorded seconds"
        if loudest is not None:
            text += f" (loudest {loudest:g} dBFS, under the {SIGNAL_FLOOR_DBFS:g} dBFS floor)"
        return text
    text = (
        f"signal in {signal} of {recorded} recorded seconds, loudest {loudest:g} dBFS at {summary['loudest_at']}, "
        f"last signal {summary['last_signal_at']}"
    )
    timeline = summary.get("timeline")
    if timeline:
        text += f"; peak dBFS every {timeline['step_seconds']} s from {timeline['start']}: {timeline['peaks_dbfs']}"
    return text


def dante_values(sample: dict) -> dict[str, tuple[float | None, bool]]:
    source = sample.get("metering_source")
    values: dict[str, tuple[float | None, bool]] = {}
    for direction in ("tx", "rx"):
        for channel, raw in (sample.get(direction) or {}).items():
            reading = normalize_metering_value(raw, source)
            state = reading["state"]
            if state not in DANTE_LEVEL_STATES:
                values[f"{direction}:{channel}"] = (None, False)
                continue
            dbfs = 0.0 if state == "clipping" else reading["dbfs"]
            values[f"{direction}:{channel}"] = (dbfs, state in DANTE_SIGNAL_STATES)
    return values


def jack_values(levels: dict[str, list]) -> dict[str, tuple[float | None, bool]]:
    peaks = {port: values[0] for port, values in levels.items()}
    return {port: (peak, peak is not None and peak >= SIGNAL_FLOOR_DBFS) for port, peak in peaks.items()}


def shure_value(key: str, raw: object) -> tuple[float | None, bool] | None:
    if key not in {"AUDIO_LEVEL_RMS", "AUDIO_LEVEL_PEAK"}:
        return None
    try:
        dbfs = float(int(str(raw).strip())) - SHURE_LEVEL_OFFSET
    except ValueError:
        return None
    return dbfs, dbfs >= SIGNAL_FLOOR_DBFS
