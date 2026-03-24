import datetime
import json
import logging
import os
import sqlite3
import struct
import time
import zlib

from netaudio_lib.dante.debug_formatter import (
    PROTOCOL_NAMES,
    RESULT_NAMES,
    get_opcode_name,
    get_settings_message_type_name,
)

logger = logging.getLogger("netaudio")

def _default_db_path():
    data_dir = os.path.join(os.path.expanduser("~"), ".local", "share", "netaudio")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "packet_capture.sqlite")

DEFAULT_DB_PATH = _default_db_path()

TEMPORAL_CORRELATION_WINDOW = 0.1

KNOWN_PROTOCOL_IDS = frozenset(PROTOCOL_NAMES.keys()) | {0x0008, 0x2729}


def _decompress_payload(data):
    if not data:
        return b""
    if isinstance(data, str):
        return bytes.fromhex(data)
    try:
        return zlib.decompress(data)
    except zlib.error:
        return data


def _safe_name(name):
    return "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in name)


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


class PacketStore:
    def __init__(self, db_path=None):
        self._db_path = db_path or DEFAULT_DB_PATH
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.create_function("decompress_hex", 1, self._decompress_hex_func)
        self._create_tables()
        self._has_payload_hex = "payload_hex" in {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(packets)").fetchall()
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
        """)
        columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(packets)").fetchall()
        }
        if "session_id" not in columns:
            self._conn.execute(
                "ALTER TABLE packets ADD COLUMN session_id INTEGER REFERENCES capture_sessions(id)"
            )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_packets_session ON packets(session_id, timestamp_ns)"
        )
        session_columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(capture_sessions)").fetchall()
        }
        if "category" not in session_columns:
            self._conn.execute(
                "ALTER TABLE capture_sessions ADD COLUMN category TEXT DEFAULT 'experiment'"
            )
        if "source_host" not in columns:
            self._conn.execute(
                "ALTER TABLE packets ADD COLUMN source_host TEXT"
            )
        if "interface" not in columns:
            self._conn.execute(
                "ALTER TABLE packets ADD COLUMN interface TEXT"
            )
        marker_columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(capture_markers)").fetchall()
        }
        if "summary" not in marker_columns:
            self._conn.execute(
                "ALTER TABLE capture_markers ADD COLUMN summary TEXT"
            )
        self._conn.commit()

    @staticmethod
    def _iso_from_ns(timestamp_ns: int) -> str:
        return datetime.datetime.fromtimestamp(
            timestamp_ns / 1e9
        ).isoformat(timespec="microseconds")

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
        except sqlite3.Error as e:
            logger.error(f"Failed to store packet: {e}")
            return None

        packet_id = cursor.lastrowid

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

    def _decode_packet_row(self, row):
        if not row:
            return None
        result = dict(row)
        result["payload"] = _decompress_payload(result.get("payload"))
        return result

    def _decode_packet_rows(self, rows):
        return [self._decode_packet_row(row) for row in rows]

    def get_packet(self, packet_id):
        row = self._conn.execute(
            "SELECT * FROM packets WHERE id = ?", (packet_id,)
        ).fetchone()
        return self._decode_packet_row(row)

    def get_correlated_pairs(self, opcode=None):
        query = """
            SELECT r.*, resp.id as resp_id
            FROM packets r
            JOIN packets resp ON r.correlated_packet_id = resp.id
            WHERE r.direction = 'request' AND resp.direction IN ('response', NULL)
              AND r.id < resp.id
        """
        params = []

        if opcode is not None:
            query += " AND r.opcode = ?"
            params.append(opcode)

        query += " ORDER BY r.timestamp_ns"

        pairs = []
        for row in self._conn.execute(query, params).fetchall():
            request = self._decode_packet_row(row)
            resp_row = self._conn.execute(
                "SELECT * FROM packets WHERE id = ?", (request["resp_id"],)
            ).fetchone()
            if resp_row:
                pairs.append((request, self._decode_packet_row(resp_row)))

        return pairs

    def get_packets_by_opcode(self, opcode):
        rows = self._conn.execute(
            "SELECT * FROM packets WHERE opcode = ? ORDER BY timestamp_ns",
            (opcode,),
        ).fetchall()
        return self._decode_packet_rows(rows)

    def get_packets(self, limit=100, source_type=None, device_name=None):
        query = "SELECT * FROM packets WHERE 1=1"
        params = []

        if source_type:
            query += " AND source_type = ?"
            params.append(source_type)
        if device_name:
            query += " AND device_name = ?"
            params.append(device_name)

        query += " ORDER BY timestamp_ns DESC LIMIT ?"
        params.append(limit)

        rows = self._conn.execute(query, params).fetchall()
        return self._decode_packet_rows(rows)

    def get_session_packet_count(self, session_id: int, start_ns: int | None = None, end_ns: int | None = None) -> int:
        query = "SELECT COUNT(*) AS count FROM packets WHERE session_id = ?"
        params: list = [session_id]
        if start_ns is not None:
            query += " AND timestamp_ns >= ?"
            params.append(start_ns)
        if end_ns is not None:
            query += " AND timestamp_ns <= ?"
            params.append(end_ns)
        row = self._conn.execute(query, params).fetchone()
        return int(row["count"]) if row else 0

    def get_session_evidence_count(self, session_id: int) -> int:
        markers = self.get_markers(session_id, marker_types=["evidence"])
        packet_ids = set()
        for marker in markers:
            data = marker.get("data")
            if data and data.get("packet_ids"):
                for pid in data["packet_ids"]:
                    packet_ids.add(pid)
        return len(packet_ids)

    def _apply_packet_filters(
        self,
        query: str,
        params: list,
        device_ip: str | None = None,
        device_name: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        opcode: int | None = None,
        protocol_id: int | None = None,
        direction: str | None = None,
        payload_contains: str | None = None,
        src_ip: str | None = None,
        dst_ip: str | None = None,
        port: int | None = None,
    ) -> tuple[str, list]:
        if device_ip:
            query += " AND (src_ip = ? OR dst_ip = ?)"
            params.extend([device_ip, device_ip])

        if src_ip:
            query += " AND src_ip = ?"
            params.append(src_ip)

        if dst_ip:
            query += " AND dst_ip = ?"
            params.append(dst_ip)

        if port is not None:
            query += " AND (src_port = ? OR dst_port = ?)"
            params.extend([port, port])

        if device_name:
            query += " AND device_name = ?"
            params.append(device_name)

        if start_ns is not None:
            query += " AND timestamp_ns >= ?"
            params.append(start_ns)

        if end_ns is not None:
            query += " AND timestamp_ns <= ?"
            params.append(end_ns)

        if opcode is not None:
            query += " AND opcode = ?"
            params.append(opcode)

        if protocol_id is not None:
            query += " AND protocol_id = ?"
            params.append(protocol_id)

        if direction is not None:
            if direction == "__null__":
                query += " AND direction IS NULL"
            else:
                query += " AND direction = ?"
                params.append(direction)

        if payload_contains is not None:
            search_hex = payload_contains.encode().hex()
            query += " AND decompress_hex(payload) LIKE ?"
            params.append(f"%{search_hex}%")

        return query, params

    def get_session_packet_count_filtered(
        self,
        session_id: int,
        device_ip: str | None = None,
        device_name: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        opcode: int | None = None,
        protocol_id: int | None = None,
        direction: str | None = None,
        payload_contains: str | None = None,
        src_ip: str | None = None,
        dst_ip: str | None = None,
        port: int | None = None,
    ) -> int:
        query = "SELECT COUNT(*) AS count FROM packets WHERE session_id = ?"
        params: list = [session_id]
        query, params = self._apply_packet_filters(
            query, params,
            device_ip=device_ip,
            device_name=device_name,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=opcode,
            protocol_id=protocol_id,
            direction=direction,
            payload_contains=payload_contains,
            src_ip=src_ip,
            dst_ip=dst_ip,
            port=port,
        )
        row = self._conn.execute(query, params).fetchone()
        return int(row["count"]) if row else 0

    def get_session_packets(
        self,
        session_id: int,
        device_ip: str | None = None,
        device_name: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        opcode: int | None = None,
        protocol_id: int | None = None,
        direction: str | None = None,
        payload_contains: str | None = None,
        src_ip: str | None = None,
        dst_ip: str | None = None,
        port: int | None = None,
        limit: int = 200,
        offset: int = 0,
        ascending: bool = True,
    ) -> list[dict]:
        query = "SELECT * FROM packets WHERE session_id = ?"
        params: list = [session_id]
        query, params = self._apply_packet_filters(
            query, params,
            device_ip=device_ip,
            device_name=device_name,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=opcode,
            protocol_id=protocol_id,
            direction=direction,
            payload_contains=payload_contains,
            src_ip=src_ip,
            dst_ip=dst_ip,
            port=port,
        )
        order = "ASC" if ascending else "DESC"
        query += f" ORDER BY timestamp_ns {order}, id {order} LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = self._conn.execute(query, params).fetchall()
        return self._decode_packet_rows(rows)

    def search_packets(
        self,
        session_id: int | None = None,
        device_ip: str | None = None,
        device_name: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        opcode: int | None = None,
        protocol_id: int | None = None,
        direction: str | None = None,
        payload_contains: str | None = None,
        src_ip: str | None = None,
        dst_ip: str | None = None,
        port: int | None = None,
        limit: int = 200,
        offset: int = 0,
        ascending: bool = True,
    ) -> list[dict]:
        if session_id is not None:
            query = "SELECT * FROM packets WHERE session_id = ?"
            params: list = [session_id]
        else:
            query = "SELECT * FROM packets WHERE 1=1"
            params = []
        query, params = self._apply_packet_filters(
            query, params,
            device_ip=device_ip,
            device_name=device_name,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=opcode,
            protocol_id=protocol_id,
            direction=direction,
            payload_contains=payload_contains,
            src_ip=src_ip,
            dst_ip=dst_ip,
            port=port,
        )
        order = "ASC" if ascending else "DESC"
        query += f" ORDER BY timestamp_ns {order}, id {order} LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = self._conn.execute(query, params).fetchall()
        return self._decode_packet_rows(rows)

    def search_packets_count(
        self,
        session_id: int | None = None,
        device_ip: str | None = None,
        device_name: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        opcode: int | None = None,
        protocol_id: int | None = None,
        direction: str | None = None,
        payload_contains: str | None = None,
        src_ip: str | None = None,
        dst_ip: str | None = None,
        port: int | None = None,
    ) -> int:
        if session_id is not None:
            query = "SELECT COUNT(*) AS count FROM packets WHERE session_id = ?"
            params: list = [session_id]
        else:
            query = "SELECT COUNT(*) AS count FROM packets WHERE 1=1"
            params = []
        query, params = self._apply_packet_filters(
            query, params,
            device_ip=device_ip,
            device_name=device_name,
            start_ns=start_ns,
            end_ns=end_ns,
            opcode=opcode,
            protocol_id=protocol_id,
            direction=direction,
            payload_contains=payload_contains,
            src_ip=src_ip,
            dst_ip=dst_ip,
            port=port,
        )
        row = self._conn.execute(query, params).fetchone()
        return int(row["count"]) if row else 0

    def get_marker_timestamp(
        self,
        session_id: int,
        label: str,
        latest: bool = False,
    ) -> int | None:
        order = "DESC" if latest else "ASC"
        row = self._conn.execute(
            f"""SELECT timestamp_ns
                FROM capture_markers
                WHERE session_id = ? AND label = ?
                ORDER BY timestamp_ns {order}, id {order}
                LIMIT 1""",
            (session_id, label),
        ).fetchone()
        if not row:
            return None
        return int(row["timestamp_ns"])

    def export_fixture(self, packet_id, output_dir):
        row = self._conn.execute(
            "SELECT * FROM packets WHERE id = ?", (packet_id,)
        ).fetchone()
        if not row:
            return None

        row = self._decode_packet_row(row)
        os.makedirs(output_dir, exist_ok=True)

        ts = datetime.datetime.fromtimestamp(row["timestamp_ns"] / 1e9)
        timestamp_str = ts.strftime("%Y%m%d_%H%M%S_%f")

        device_id = row["device_name"] or row["device_ip"] or "unknown"
        safe_device = _safe_name(device_id)

        opcode_name = row["opcode_name"] or f"opcode_0x{row['opcode']:04X}" if row["opcode"] else "unknown"
        safe_opcode = _safe_name(opcode_name.lower().removeprefix("opcode_"))

        suffix = f"_{row['direction']}" if row["direction"] else ""
        filename = f"{timestamp_str}_{safe_device}_{safe_opcode}{suffix}.bin"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "wb") as f:
            f.write(row["payload"])

        return filepath

    def export_correlated_pair(self, request_id, output_dir):
        row = self._conn.execute(
            "SELECT * FROM packets WHERE id = ?", (request_id,)
        ).fetchone()
        if not row or not row["correlated_packet_id"]:
            return None

        req_path = self.export_fixture(request_id, output_dir)
        resp_path = self.export_fixture(row["correlated_packet_id"], output_dir)
        return (req_path, resp_path)

    def query_packets(
        self,
        device_ip: str | None = None,
        src_ip: str | None = None,
        dst_ip: str | None = None,
        opcode: int | None = None,
        protocol_id: int | None = None,
        direction: str | None = None,
        source_type: str | None = None,
        session_id: int | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        payload_hex_contains: str | None = None,
        min_length: int | None = None,
        max_length: int | None = None,
        limit: int = 10000,
        offset: int = 0,
        ascending: bool = True,
    ) -> list[dict]:
        query = "SELECT * FROM packets WHERE 1=1"
        params: list = []

        if device_ip:
            query += " AND (device_ip = ? OR src_ip = ? OR dst_ip = ?)"
            params.extend([device_ip, device_ip, device_ip])

        if src_ip:
            query += " AND src_ip = ?"
            params.append(src_ip)

        if dst_ip:
            query += " AND dst_ip = ?"
            params.append(dst_ip)

        if opcode is not None:
            query += " AND opcode = ?"
            params.append(opcode)

        if protocol_id is not None:
            query += " AND protocol_id = ?"
            params.append(protocol_id)

        if direction:
            query += " AND direction = ?"
            params.append(direction)

        if source_type:
            query += " AND source_type = ?"
            params.append(source_type)

        if session_id is not None:
            query += " AND session_id = ?"
            params.append(session_id)

        if start_ns is not None:
            query += " AND timestamp_ns >= ?"
            params.append(start_ns)

        if end_ns is not None:
            query += " AND timestamp_ns <= ?"
            params.append(end_ns)

        if payload_hex_contains:
            query += " AND decompress_hex(payload) LIKE ?"
            params.append(f"%{payload_hex_contains.lower()}%")

        if min_length is not None:
            query += " AND length(payload) >= ?"
            params.append(min_length)

        if max_length is not None:
            query += " AND length(payload) <= ?"
            params.append(max_length)

        order = "ASC" if ascending else "DESC"
        query += f" ORDER BY timestamp_ns {order}, id {order} LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = self._conn.execute(query, params).fetchall()
        return self._decode_packet_rows(rows)

    def get_stats(self):
        stats = {}

        row = self._conn.execute("SELECT COUNT(*) as total FROM packets").fetchone()
        stats["total"] = row["total"]

        rows = self._conn.execute(
            "SELECT source_type, COUNT(*) as count FROM packets GROUP BY source_type"
        ).fetchall()
        stats["by_source"] = {r["source_type"]: r["count"] for r in rows}

        rows = self._conn.execute(
            "SELECT opcode_name, direction, COUNT(*) as count FROM packets "
            "GROUP BY opcode_name, direction ORDER BY count DESC"
        ).fetchall()
        stats["by_opcode"] = [
            {"opcode_name": r["opcode_name"], "direction": r["direction"], "count": r["count"]}
            for r in rows
        ]

        row = self._conn.execute(
            "SELECT COUNT(*) as count FROM packets WHERE correlated_packet_id IS NOT NULL"
        ).fetchone()
        stats["correlated"] = row["count"]

        row = self._conn.execute(
            "SELECT COUNT(*) as count FROM packets WHERE correlated_packet_id IS NULL"
        ).fetchone()
        stats["uncorrelated"] = row["count"]

        return stats

    def close(self):
        self._conn.close()
