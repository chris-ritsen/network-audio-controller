from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import sqlite3
import struct
import time
import zlib

from netaudio.dante.debug_formatter import (
    PROTOCOL_NAMES,
    RESULT_NAMES,
    get_opcode_name,
    get_settings_message_type_name,
)
from netaudio.dante.packet_store_common import (
    SESSION_MEMBERSHIP_SQL,
    decompress_payload as _decompress_payload,
    extract_evidence_packet_ids,
    safe_name as _safe_name,
)
from netaudio.dante.packet_store_queries import PacketStoreQueries

logger = logging.getLogger("netaudio")


def _default_db_path():
    data_dir = os.path.join(os.path.expanduser("~"), ".local", "share", "netaudio")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "packet_capture.sqlite")


DEFAULT_DB_PATH = _default_db_path()

TEMPORAL_CORRELATION_WINDOW = 0.1

KNOWN_PROTOCOL_IDS = frozenset(PROTOCOL_NAMES.keys()) | {0x0008, 0x2729}


def _parse_header(data: bytes):
    if len(data) < 8:
        return None

    protocol_id = struct.unpack(">H", data[0:2])[0]
    length = struct.unpack(">H", data[2:4])[0]

    if protocol_id == 0xFFFF and len(data) >= 28:
        message_type = struct.unpack(">H", data[26:28])[0]
        message_type_name = get_settings_message_type_name(message_type)

        return {
            "protocol_id": protocol_id,
            "length": length,
            "transaction_id": None,
            "opcode": message_type,
            "result_code": None,
            "protocol_name": "PROTOCOL_SETTINGS",
            "opcode_name": message_type_name,
            "result_name": None,
        }

    if protocol_id == 0x0008 and len(data) >= 12:
        direction_field = struct.unpack(">H", data[6:8])[0]
        opcode = struct.unpack(">H", data[10:12])[0]
        sequence = struct.unpack(">H", data[16:18])[0] if len(data) >= 18 else None

        return {
            "protocol_id": protocol_id,
            "length": length,
            "transaction_id": sequence,
            "opcode": opcode,
            "result_code": direction_field,
            "protocol_name": "DDP_LOCK",
            "opcode_name": get_opcode_name(protocol_id, opcode) if opcode is not None else None,
            "result_name": None,
        }

    transaction_id = struct.unpack(">H", data[4:6])[0] if len(data) >= 6 else None
    opcode = struct.unpack(">H", data[6:8])[0] if len(data) >= 8 else None
    result_code = struct.unpack(">H", data[8:10])[0] if len(data) >= 10 else None

    return {
        "protocol_id": protocol_id,
        "length": length,
        "transaction_id": transaction_id,
        "opcode": opcode,
        "result_code": result_code,
        "protocol_name": PROTOCOL_NAMES.get(protocol_id),
        "opcode_name": get_opcode_name(protocol_id, opcode) if opcode is not None else None,
        "result_name": RESULT_NAMES.get(result_code) if result_code is not None else None,
    }


class PacketStore(PacketStoreQueries):
    def __init__(self, db_path=None):
        self._db_path = db_path or DEFAULT_DB_PATH
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.create_function("decompress_hex", 1, self._decompress_hex_func)
        self._create_tables()
        self._has_payload_hex = "payload_hex" in {
            row["name"] for row in self._conn.execute("PRAGMA table_info(packets)").fetchall()
        }

    @staticmethod
    def _decompress_hex_func(data):
        if not data:
            return ""
        try:
            return zlib.decompress(data).hex()
        except zlib.error:
            return data.hex() if isinstance(data, bytes) else ""

    def _create_tables(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS capture_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                source_host TEXT,
                description TEXT,
                category TEXT DEFAULT 'experiment',
                started_ns INTEGER NOT NULL,
                started_iso TEXT NOT NULL,
                ended_ns INTEGER,
                ended_iso TEXT,
                metadata_json TEXT
            );

            CREATE TABLE IF NOT EXISTS capture_markers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES capture_sessions(id),
                marker_type TEXT NOT NULL,
                label TEXT NOT NULL,
                summary TEXT,
                note TEXT,
                source_host TEXT,
                data_json TEXT,
                timestamp_ns INTEGER NOT NULL,
                timestamp_iso TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS packets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ns INTEGER NOT NULL,
                timestamp_iso TEXT NOT NULL,
                src_ip TEXT,
                src_port INTEGER,
                dst_ip TEXT,
                dst_port INTEGER,
                source_type TEXT NOT NULL,
                direction TEXT,
                device_name TEXT,
                device_ip TEXT,
                protocol_id INTEGER,
                protocol_name TEXT,
                transaction_id INTEGER,
                opcode INTEGER,
                opcode_name TEXT,
                result_code INTEGER,
                result_name TEXT,
                payload BLOB NOT NULL,
                correlated_packet_id INTEGER REFERENCES packets(id),
                multicast_group TEXT,
                multicast_port INTEGER,
                session_id INTEGER REFERENCES capture_sessions(id),
                source_host TEXT,
                interface TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_packets_transaction
                ON packets(transaction_id, device_ip, direction);
            CREATE INDEX IF NOT EXISTS idx_packets_opcode
                ON packets(opcode);
            CREATE INDEX IF NOT EXISTS idx_packets_source_type
                ON packets(source_type);
            CREATE INDEX IF NOT EXISTS idx_packets_device_ip_time
                ON packets(device_ip, timestamp_ns);
            CREATE INDEX IF NOT EXISTS idx_packets_correlated
                ON packets(correlated_packet_id);
            CREATE INDEX IF NOT EXISTS idx_capture_markers_session
                ON capture_markers(session_id, timestamp_ns);
            CREATE INDEX IF NOT EXISTS idx_capture_sessions_started
                ON capture_sessions(started_ns);

            CREATE TABLE IF NOT EXISTS capture_artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES capture_sessions(id),
                label TEXT NOT NULL,
                role TEXT NOT NULL,
                filename TEXT NOT NULL,
                media_type TEXT NOT NULL,
                note TEXT,
                source_path TEXT,
                source_host TEXT,
                source_modified_ns INTEGER,
                sha256 TEXT NOT NULL,
                size INTEGER NOT NULL,
                content BLOB NOT NULL,
                timestamp_ns INTEGER NOT NULL,
                timestamp_iso TEXT NOT NULL,
                UNIQUE(session_id, label, sha256)
            );

            CREATE INDEX IF NOT EXISTS idx_capture_artifacts_session
                ON capture_artifacts(session_id, timestamp_ns, id);

            CREATE TABLE IF NOT EXISTS packet_sessions (
                packet_id INTEGER NOT NULL REFERENCES packets(id),
                session_id INTEGER NOT NULL REFERENCES capture_sessions(id),
                PRIMARY KEY (packet_id, session_id)
            );
            CREATE INDEX IF NOT EXISTS idx_packet_sessions_session
                ON packet_sessions(session_id, packet_id);
        """)
        columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(packets)").fetchall()}
        if "session_id" not in columns:
            self._conn.execute("ALTER TABLE packets ADD COLUMN session_id INTEGER REFERENCES capture_sessions(id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_packets_session ON packets(session_id, timestamp_ns)")
        session_columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(capture_sessions)").fetchall()}
        if "category" not in session_columns:
            self._conn.execute("ALTER TABLE capture_sessions ADD COLUMN category TEXT DEFAULT 'experiment'")
        if "source_host" not in columns:
            self._conn.execute("ALTER TABLE packets ADD COLUMN source_host TEXT")
        if "interface" not in columns:
            self._conn.execute("ALTER TABLE packets ADD COLUMN interface TEXT")
        marker_columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(capture_markers)").fetchall()}
        if "summary" not in marker_columns:
            self._conn.execute("ALTER TABLE capture_markers ADD COLUMN summary TEXT")
        self._conn.commit()

    @staticmethod
    def _iso_from_ns(timestamp_ns: int) -> str:
        return datetime.datetime.fromtimestamp(timestamp_ns / 1e9).isoformat(timespec="microseconds")

    def start_session(
        self,
        name: str | None = None,
        source_host: str | None = None,
        description: str | None = None,
        metadata: dict | None = None,
        started_ns: int | None = None,
        category: str = "experiment",
    ) -> int:
        if started_ns is None:
            started_ns = time.time_ns()
        started_iso = self._iso_from_ns(started_ns)
        metadata_json = json.dumps(metadata, sort_keys=True) if metadata else None

        cursor = self._conn.execute(
            """INSERT INTO capture_sessions (
                name, source_host, description, category, started_ns, started_iso, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                name,
                source_host,
                description,
                category,
                started_ns,
                started_iso,
                metadata_json,
            ),
        )
        self._conn.commit()
        return int(cursor.lastrowid)

    def end_session(
        self,
        session_id: int,
        description: str | None = None,
        ended_ns: int | None = None,
    ) -> bool:
        if ended_ns is None:
            ended_ns = time.time_ns()
        ended_iso = self._iso_from_ns(ended_ns)
        if description:
            cursor = self._conn.execute(
                """UPDATE capture_sessions
                   SET ended_ns = ?, ended_iso = ?, description = ?
                   WHERE id = ?""",
                (ended_ns, ended_iso, description, session_id),
            )
        else:
            cursor = self._conn.execute(
                """UPDATE capture_sessions
                   SET ended_ns = ?, ended_iso = ?
                   WHERE id = ?""",
                (ended_ns, ended_iso, session_id),
            )
        self._conn.commit()
        return cursor.rowcount > 0

    def update_session(
        self,
        session_id: int,
        name: str | None = None,
        description: str | None = None,
        category: str | None = None,
    ) -> bool:
        updates = []
        params = []
        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if description is not None:
            updates.append("description = ?")
            params.append(description)
        if category is not None:
            updates.append("category = ?")
            params.append(category)
        if not updates:
            return False
        params.append(session_id)
        cursor = self._conn.execute(
            f"UPDATE capture_sessions SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        self._conn.commit()
        return cursor.rowcount > 0

    def add_marker(
        self,
        session_id: int,
        marker_type: str,
        label: str,
        summary: str | None = None,
        note: str | None = None,
        source_host: str | None = None,
        data: dict | None = None,
        timestamp_ns: int | None = None,
    ) -> int:
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()
        timestamp_iso = self._iso_from_ns(timestamp_ns)
        data_json = json.dumps(data, sort_keys=True) if data else None

        cursor = self._conn.execute(
            """INSERT INTO capture_markers (
                session_id, marker_type, label, summary, note, source_host, data_json, timestamp_ns, timestamp_iso
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session_id,
                marker_type,
                label,
                summary,
                note,
                source_host,
                data_json,
                timestamp_ns,
                timestamp_iso,
            ),
        )
        self._conn.commit()
        return int(cursor.lastrowid)

    def get_session(self, session_id: int) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM capture_sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        metadata_json = result.get("metadata_json")
        if metadata_json:
            try:
                result["metadata"] = json.loads(metadata_json)
            except Exception:
                result["metadata"] = None
        else:
            result["metadata"] = None
        return result

    def _decode_session_row(self, row) -> dict | None:
        if not row:
            return None

        result = dict(row)
        metadata_json = result.get("metadata_json")
        if metadata_json:
            try:
                result["metadata"] = json.loads(metadata_json)
            except Exception:
                result["metadata"] = None
        else:
            result["metadata"] = None
        return result

    def get_active_sessions(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM capture_sessions WHERE ended_ns IS NULL ORDER BY started_ns DESC"
        ).fetchall()
        return [self._decode_session_row(row) for row in rows if row]

    def link_packet_to_session(self, packet_id: int, session_id: int) -> None:
        try:
            self._conn.execute(
                "INSERT OR IGNORE INTO packet_sessions (packet_id, session_id) VALUES (?, ?)",
                (packet_id, session_id),
            )
            self._conn.commit()
        except sqlite3.Error:
            logger.exception(f"Failed to link packet {packet_id} to capture session {session_id}")
            raise

    def get_latest_session(self, active_only: bool = False) -> dict | None:
        query = "SELECT * FROM capture_sessions"
        params: list = []
        if active_only:
            query += " WHERE ended_ns IS NULL"
        query += " ORDER BY started_ns DESC, id DESC LIMIT 1"
        row = self._conn.execute(query, params).fetchone()
        return self._decode_session_row(row)

    def find_session_by_name(self, name: str, active_only: bool = False) -> dict | None:
        query = "SELECT * FROM capture_sessions WHERE name = ?"
        params: list = [name]
        if active_only:
            query += " AND ended_ns IS NULL"
        query += " ORDER BY started_ns DESC, id DESC LIMIT 1"
        row = self._conn.execute(query, params).fetchone()
        return self._decode_session_row(row)

    def list_sessions(self, limit: int = 100, category: str | None = None) -> list[dict]:
        query = "SELECT * FROM capture_sessions"
        params: list = []

        if category:
            query += " WHERE category = ?"
            params.append(category)

        query += " ORDER BY started_ns DESC LIMIT ?"
        params.append(limit)

        rows = self._conn.execute(query, params).fetchall()
        items = []
        for row in rows:
            result = self._decode_session_row(row)
            items.append(result)
        return items

    def get_markers(
        self,
        session_id: int,
        marker_types: list[str] | None = None,
        after_ns: int | None = None,
        before_ns: int | None = None,
        grep: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        query = "SELECT * FROM capture_markers WHERE session_id = ?"
        params: list = [session_id]

        if marker_types:
            placeholders = ",".join("?" for _ in marker_types)
            query += f" AND marker_type IN ({placeholders})"
            params.extend(marker_types)

        if after_ns is not None:
            query += " AND timestamp_ns >= ?"
            params.append(after_ns)

        if before_ns is not None:
            query += " AND timestamp_ns <= ?"
            params.append(before_ns)

        if grep:
            query += " AND (label LIKE ? OR summary LIKE ? OR note LIKE ?)"
            pattern = f"%{grep}%"
            params.extend([pattern, pattern, pattern])

        query += " ORDER BY timestamp_ns, id"

        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        rows = self._conn.execute(query, params).fetchall()
        items = []
        for row in rows:
            result = dict(row)
            data_json = result.get("data_json")
            if data_json:
                try:
                    result["data"] = json.loads(data_json)
                except Exception:
                    result["data"] = None
            else:
                result["data"] = None
            items.append(result)
        return items

    def get_marker(self, marker_id: int) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM capture_markers WHERE id = ?",
            (marker_id,),
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        data_json = result.get("data_json")
        if data_json:
            try:
                result["data"] = json.loads(data_json)
            except Exception:
                result["data"] = None
        else:
            result["data"] = None
        return result

    def add_artifact(
        self,
        session_id: int,
        label: str,
        role: str,
        filename: str,
        media_type: str,
        content: bytes,
        note: str | None = None,
        source_path: str | None = None,
        source_host: str | None = None,
        source_modified_ns: int | None = None,
        timestamp_ns: int | None = None,
    ) -> int:
        if self.get_session(session_id) is None:
            raise ValueError(f"capture session {session_id} does not exist")
        if not label.strip():
            raise ValueError("artifact label must not be empty")
        if not role.strip():
            raise ValueError("artifact role must not be empty")
        if not filename.strip() or os.path.basename(filename) != filename:
            raise ValueError("artifact filename must be one file name")
        if not media_type.strip():
            raise ValueError("artifact media type must not be empty")
        if not isinstance(content, bytes) or not content:
            raise ValueError("artifact content must be non-empty bytes")

        if timestamp_ns is None:
            timestamp_ns = time.time_ns()
        timestamp_iso = self._iso_from_ns(timestamp_ns)
        content_sha256 = hashlib.sha256(content).hexdigest()

        cursor = self._conn.execute(
            """INSERT INTO capture_artifacts (
                session_id, label, role, filename, media_type, note, source_path,
                source_host, source_modified_ns, sha256, size, content,
                timestamp_ns, timestamp_iso
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session_id,
                label.strip(),
                role.strip(),
                filename,
                media_type.strip(),
                note,
                source_path,
                source_host,
                source_modified_ns,
                content_sha256,
                len(content),
                zlib.compress(content),
                timestamp_ns,
                timestamp_iso,
            ),
        )
        self._conn.commit()
        return int(cursor.lastrowid)

    def _decode_artifact_row(self, row) -> dict | None:
        if not row:
            return None
        result = dict(row)
        result["content"] = _decompress_payload(result.get("content"))
        return result

    def get_artifact(self, artifact_id: int) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM capture_artifacts WHERE id = ?",
            (artifact_id,),
        ).fetchone()
        return self._decode_artifact_row(row)

    def get_session_artifacts(self, session_id: int) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM capture_artifacts WHERE session_id = ? ORDER BY timestamp_ns, id",
            (session_id,),
        ).fetchall()
        return [artifact for row in rows if (artifact := self._decode_artifact_row(row)) is not None]

    def store_packet(
        self,
        payload: bytes,
        source_type: str,
        src_ip: str = None,
        src_port: int = None,
        dst_ip: str = None,
        dst_port: int = None,
        device_name: str = None,
        device_ip: str = None,
        direction: str = None,
        multicast_group: str = None,
        multicast_port: int = None,
        session_id: int = None,
        timestamp_ns: int = None,
        source_host: str = None,
        interface: str = None,
    ) -> int | None:
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()

        timestamp_iso = self._iso_from_ns(timestamp_ns)

        header = _parse_header(payload)

        compressed_payload = zlib.compress(payload)

        if session_id is not None:
            dedup_window_ns = 1_000_000_000
            existing = self._conn.execute(
                """SELECT id FROM packets
                WHERE payload = ? AND src_ip IS ? AND dst_ip IS ?
                AND session_id = ? AND ABS(timestamp_ns - ?) < ?
                LIMIT 1""",
                (compressed_payload, src_ip, dst_ip, session_id, timestamp_ns, dedup_window_ns),
            ).fetchone()
            if existing:
                return existing["id"]

        try:
            if self._has_payload_hex:
                cursor = self._conn.execute(
                    """INSERT INTO packets (
                        timestamp_ns, timestamp_iso, src_ip, src_port, dst_ip, dst_port,
                        source_type, direction, device_name, device_ip,
                        protocol_id, protocol_name, transaction_id, opcode, opcode_name,
                        result_code, result_name, payload, payload_hex,
                        multicast_group, multicast_port, session_id, source_host, interface
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        timestamp_ns,
                        timestamp_iso,
                        src_ip,
                        src_port,
                        dst_ip,
                        dst_port,
                        source_type,
                        direction,
                        device_name,
                        device_ip,
                        header["protocol_id"] if header else None,
                        header["protocol_name"] if header else None,
                        header["transaction_id"] if header else None,
                        header["opcode"] if header else None,
                        header["opcode_name"] if header else None,
                        header["result_code"] if header else None,
                        header["result_name"] if header else None,
                        compressed_payload,
                        "",
                        multicast_group,
                        multicast_port,
                        session_id,
                        source_host,
                        interface,
                    ),
                )
            else:
                cursor = self._conn.execute(
                    """INSERT INTO packets (
                        timestamp_ns, timestamp_iso, src_ip, src_port, dst_ip, dst_port,
                        source_type, direction, device_name, device_ip,
                        protocol_id, protocol_name, transaction_id, opcode, opcode_name,
                        result_code, result_name, payload,
                        multicast_group, multicast_port, session_id, source_host, interface
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        timestamp_ns,
                        timestamp_iso,
                        src_ip,
                        src_port,
                        dst_ip,
                        dst_port,
                        source_type,
                        direction,
                        device_name,
                        device_ip,
                        header["protocol_id"] if header else None,
                        header["protocol_name"] if header else None,
                        header["transaction_id"] if header else None,
                        header["opcode"] if header else None,
                        header["opcode_name"] if header else None,
                        header["result_code"] if header else None,
                        header["result_name"] if header else None,
                        compressed_payload,
                        multicast_group,
                        multicast_port,
                        session_id,
                        source_host,
                        interface,
                    ),
                )
            self._conn.commit()
        except sqlite3.Error:
            logger.exception("Failed to store captured packet")
            raise

        packet_id = cursor.lastrowid

        if session_id is not None:
            try:
                self._conn.execute(
                    "INSERT OR IGNORE INTO packet_sessions (packet_id, session_id) VALUES (?, ?)",
                    (packet_id, session_id),
                )
                self._conn.commit()
            except sqlite3.Error:
                logger.exception(f"Failed to link packet {packet_id} to capture session {session_id}")
                raise

        if header and header["transaction_id"] is not None:
            self._correlate_by_transaction_id(packet_id, header, device_ip, direction)

        if source_type == "multicast" and device_ip:
            self._correlate_by_temporal_proximity(packet_id, device_ip, timestamp_ns)

        return packet_id

    def _correlate_by_transaction_id(self, packet_id, header, device_ip, direction):
        if not device_ip or not direction:
            return

        opposite = "response" if direction == "request" else "request"

        row = self._conn.execute(
            """SELECT id FROM packets
               WHERE transaction_id = ? AND device_ip = ? AND direction = ?
                 AND correlated_packet_id IS NULL AND id != ?
               ORDER BY timestamp_ns DESC LIMIT 1""",
            (header["transaction_id"], device_ip, opposite, packet_id),
        ).fetchone()

        if row:
            match_id = row["id"]
            self._conn.execute(
                "UPDATE packets SET correlated_packet_id = ? WHERE id = ?",
                (match_id, packet_id),
            )
            self._conn.execute(
                "UPDATE packets SET correlated_packet_id = ? WHERE id = ?",
                (packet_id, match_id),
            )
            self._conn.commit()

    def _correlate_by_temporal_proximity(self, packet_id, device_ip, timestamp_ns):
        window_ns = int(TEMPORAL_CORRELATION_WINDOW * 1e9)
        min_ts = timestamp_ns - window_ns

        row = self._conn.execute(
            """SELECT id FROM packets
               WHERE device_ip = ? AND direction = 'request'
                 AND timestamp_ns >= ? AND timestamp_ns <= ?
                 AND correlated_packet_id IS NULL AND id != ?
               ORDER BY timestamp_ns DESC LIMIT 1""",
            (device_ip, min_ts, timestamp_ns, packet_id),
        ).fetchone()

        if row:
            match_id = row["id"]
            self._conn.execute(
                "UPDATE packets SET correlated_packet_id = ? WHERE id = ?",
                (match_id, packet_id),
            )
            self._conn.execute(
                "UPDATE packets SET correlated_packet_id = ? WHERE id = ?",
                (packet_id, match_id),
            )
            self._conn.commit()

    def close(self):
        self._conn.close()
