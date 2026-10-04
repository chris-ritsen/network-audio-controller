from __future__ import annotations

from datetime import datetime
from typing import Any

from netaudio.daemon.http.mcp_views import _numbered_names, channels_text, compact
from netaudio.monitoring.level_history import MINUTE_SLOTS, history_text, iso_second

MAXIMUM_LISTEN_SECONDS = 30
MAXIMUM_HISTORY_SECONDS = MINUTE_SLOTS * 60
DEFAULT_HISTORY_SECONDS = 60
NETWORK_CHANNEL_LINES = 8

PERIOD_PROPERTIES = {
    "over_last_seconds": {
        "type": "integer",
        "minimum": 1,
        "maximum": MAXIMUM_HISTORY_SECONDS,
        "description": (
            "Summarize the levels recorded over this many seconds instead of sampling once: how many seconds had "
            "signal, the loudest moment, when signal was last heard and a peak timeline. Use it for sound that comes "
            "and goes, such as someone talking on a call. Levels are kept per second for 15 minutes and per minute "
            "for 24 hours."
        ),
    },
    "ending_at": {
        "type": "string",
        "description": (
            "End of that period as an ISO time, such as 2026-10-02T23:00 for last night (local time unless it has an "
            "offset or Z); defaults to now."
        ),
    },
    "listen_seconds": {
        "type": "integer",
        "minimum": 1,
        "maximum": MAXIMUM_LISTEN_SECONDS,
        "description": "Wait this many seconds, then summarize what was heard during them.",
    },
}


def _parse_time(text: str) -> float:
    try:
        moment = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    except ValueError as exception:
        raise ValueError("ending_at must be an ISO time, such as 2026-10-02T23:00") from exception
    return moment.timestamp()


def signal_period(arguments: dict, now: float) -> tuple[float, float] | None:
    listen = arguments.get("listen_seconds")
    over = arguments.get("over_last_seconds")
    ending = arguments.get("ending_at")
    if listen is None and over is None and ending is None:
        return None
    if listen is not None:
        if over is not None or ending is not None:
            raise ValueError("listen_seconds cannot be combined with over_last_seconds or ending_at")
        return now, now + listen
    end = _parse_time(ending) if ending else now
    if end > now + 1:
        raise ValueError("ending_at is in the future; use listen_seconds to wait for new sound")
    start = end - (over or DEFAULT_HISTORY_SECONDS)
    if start < now - MAXIMUM_HISTORY_SECONDS:
        raise ValueError("levels are kept for 24 hours")
    return start, end


def _short_text(summary: dict) -> str:
    signal = summary.get("signal_seconds") or 0
    text = f"signal in {signal} of {summary.get('recorded_seconds')} s"
    if summary.get("loudest_dbfs") is not None:
        text += f", loudest {summary['loudest_dbfs']:g} dBFS"
    return text


def dante_history_view(history: Any, record: dict, period: tuple[float, float], full: bool) -> dict:
    source = f"dante:{record.get('server_name')}"
    channels = record.get("channels") or {}
    view: dict[str, Any] = {}
    for direction, key in (("rx", "receivers"), ("tx", "transmitters")):
        names = _numbered_names(channels.get(key))
        heard, silent, unrecorded = [], [], []
        for number in sorted(names):
            summary = history.summary([(source, f"{direction}:{number}")], *period)
            label = f"{number} {names[number]}" if names[number] != str(number) else str(number)
            if not summary.get("recorded_seconds"):
                unrecorded.append(number)
            elif summary.get("signal_seconds"):
                heard.append(f"{label}: {history_text(summary) if full else _short_text(summary)}")
            else:
                silent.append(number)
        if not full and len(heard) > NETWORK_CHANNEL_LINES:
            heard = heard[:NETWORK_CHANNEL_LINES] + [f"and {len(heard) - NETWORK_CHANNEL_LINES} more with signal"]
        view[direction] = heard or None
        view[f"{direction}_silent"] = channels_text(silent) if silent and full else None
        view[f"{direction}_not_recorded"] = channels_text(unrecorded) if unrecorded and full else None
    if not full and not view.get("rx") and not view.get("tx"):
        return {}
    return compact(view)


def signal_history_view(history: Any, records: list[dict], period: tuple[float, float], full: bool) -> dict:
    view: dict[str, Any] = {"period": f"{iso_second(period[0])} to {iso_second(period[1])}"}
    quiet = []
    for record in sorted(records, key=lambda item: str(item.get("name"))):
        entry = dante_history_view(history, record, period, full)
        if entry:
            view[str(record.get("name"))] = entry
        else:
            quiet.append(str(record.get("name")))
    if quiet:
        view["no_signal"] = quiet
    return view
