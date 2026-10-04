from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import zlib
from pathlib import Path

from netaudio.monitoring.level_history import MINUTE_SLOTS, SECOND_SLOTS, LevelHistory

logger = logging.getLogger("netaudio")

LAYOUT = f"1 {SECOND_SLOTS} {MINUTE_SLOTS}"


def default_level_history_path() -> Path:
    return Path.home() / ".local" / "share" / "netaudio" / "level_history.sqlite"


def save_level_history(history: LevelHistory, path: Path) -> None:
    now = time.time()
    sources, tracks = history.dump(now)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    temporary.unlink(missing_ok=True)
    connection = sqlite3.connect(temporary)
    try:
        with connection:
            connection.execute("CREATE TABLE layout (layout TEXT NOT NULL, saved_at REAL NOT NULL)")
            connection.execute(
                "CREATE TABLE sources (name TEXT PRIMARY KEY, first_second INTEGER NOT NULL, "
                "members TEXT NOT NULL, track BLOB NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE tracks (source TEXT NOT NULL, point TEXT NOT NULL, track BLOB NOT NULL, "
                "PRIMARY KEY (source, point))"
            )
            connection.execute("INSERT INTO layout VALUES (?, ?)", (LAYOUT, now))
            connection.executemany(
                "INSERT INTO sources VALUES (?, ?, ?, ?)",
                [
                    (name, first_second, json.dumps(members), zlib.compress(data))
                    for name, first_second, members, data in sources
                ],
            )
            connection.executemany(
                "INSERT INTO tracks VALUES (?, ?, ?)",
                [(source, point, zlib.compress(data)) for source, point, data in tracks],
            )
    finally:
        connection.close()
    os.replace(temporary, path)
    logger.info("Saved signal level history: %d tracks, %d KB, to %s", len(tracks), path.stat().st_size // 1024, path)


def load_level_history(history: LevelHistory, path: Path) -> None:
    if not path.exists():
        return
    try:
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        try:
            layout, saved_at = connection.execute("SELECT layout, saved_at FROM layout").fetchone()
            if layout != LAYOUT:
                logger.info("Not restoring signal level history saved in another layout: %s", layout)
                return
            sources = [
                (name, first_second, json.loads(members), zlib.decompress(track))
                for name, first_second, members, track in connection.execute("SELECT * FROM sources")
            ]
            tracks = [
                (source, point, zlib.decompress(track))
                for source, point, track in connection.execute("SELECT * FROM tracks")
            ]
        finally:
            connection.close()
        restored = history.restore(sources, tracks)
    except (sqlite3.Error, zlib.error, ValueError, TypeError) as exception:
        logger.warning("Could not restore signal level history from %s: %s", path, exception)
        return
    logger.info("Restored signal level history: %d tracks saved %.0f s ago", restored, time.time() - saved_at)
