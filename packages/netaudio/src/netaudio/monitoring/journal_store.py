from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from netaudio.monitoring.journal import MonitoringEventJournal

logger = logging.getLogger("netaudio")

LAYOUT = "2"
REMEMBER_SECONDS = 30 * 24 * 3600


def default_event_journal_path() -> Path:
    return Path.home() / ".local" / "share" / "netaudio" / "event_journal.sqlite"


def _iso(moment: float) -> str:
    return datetime.fromtimestamp(moment, timezone.utc).isoformat().replace("+00:00", "Z")


def save_event_journal(journal: MonitoringEventJournal, path: Path) -> None:
    events = journal.saved_events()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    temporary.unlink(missing_ok=True)
    connection = sqlite3.connect(temporary)
    try:
        with connection:
            connection.execute("CREATE TABLE layout (layout TEXT NOT NULL, saved_at REAL NOT NULL)")
            connection.execute("CREATE TABLE events (sequence INTEGER PRIMARY KEY, event TEXT NOT NULL)")
            connection.execute("CREATE TABLE gaps (stopped_at TEXT NOT NULL, started_at TEXT NOT NULL)")
            connection.execute("CREATE TABLE devices (identity TEXT PRIMARY KEY, seen_at TEXT, record TEXT NOT NULL)")
            connection.execute("INSERT INTO layout VALUES (?, ?)", (LAYOUT, time.time()))
            connection.executemany(
                "INSERT INTO events VALUES (?, ?)",
                [(event["sequence"], json.dumps(event, ensure_ascii=False)) for event in events],
            )
            connection.executemany("INSERT INTO gaps VALUES (?, ?)", journal.gaps)
            oldest = _iso(time.time() - REMEMBER_SECONDS)
            connection.executemany(
                "INSERT INTO devices VALUES (?, ?, ?)",
                [
                    (identity, seen_at, json.dumps(record, ensure_ascii=False, default=str))
                    for identity, (seen_at, record) in journal.last_known().items()
                    if seen_at is None or seen_at >= oldest
                ],
            )
    finally:
        connection.close()
    os.replace(temporary, path)
    logger.info("Saved the event journal: %d events to %s", len(events), path)


def load_event_journal(journal: MonitoringEventJournal, path: Path) -> None:
    if not path.exists():
        return
    try:
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        try:
            layout, saved_at = connection.execute("SELECT layout, saved_at FROM layout").fetchone()
            if layout != LAYOUT:
                logger.info("Not restoring an event journal saved in another layout: %s", layout)
                return
            events = [json.loads(row[0]) for row in connection.execute("SELECT event FROM events ORDER BY sequence")]
            gaps = [tuple(row) for row in connection.execute("SELECT stopped_at, started_at FROM gaps")]
            devices = {
                identity: {"seen_at": seen_at, "record": json.loads(record)}
                for identity, seen_at, record in connection.execute("SELECT identity, seen_at, record FROM devices")
            }
        finally:
            connection.close()
        restored = journal.restore(events, [*gaps, (_iso(saved_at), _iso(time.time()))])
        journal.remembered = devices
    except (sqlite3.Error, ValueError, TypeError, KeyError) as exception:
        logger.warning("Could not restore the event journal from %s: %s", path, exception)
        return
    logger.info(
        "Restored the event journal: %d events and %d remembered devices saved %.0f s ago",
        restored,
        len(devices),
        time.time() - saved_at,
    )
