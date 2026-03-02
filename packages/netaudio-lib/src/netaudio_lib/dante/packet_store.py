import datetime
import logging
import os
import sqlite3
import struct
import time

from netaudio_lib.dante.debug_formatter import OPCODE_NAMES, PROTOCOL_NAMES, RESULT_NAMES

logger = logging.getLogger("netaudio")

def _default_db_path():
    data_dir = os.path.join(os.path.expanduser("~"), ".local", "share", "netaudio")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "packet_capture.sqlite")

DEFAULT_DB_PATH = _default_db_path()

TEMPORAL_CORRELATION_WINDOW = 0.1


def _safe_name(name):
    return "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in name)


def _parse_header(data: bytes):
    if len(data) < 8:
        return None

    protocol_id = struct.unpack(">H", data[0:2])[0]
    length = struct.unpack(">H", data[2:4])[0]

    if protocol_id in PROTOCOL_NAMES and protocol_id != 0xFFFF:
        transaction_id = struct.unpack(">H", data[4:6])[0]
        opcode = struct.unpack(">H", data[6:8])[0]
        result_code = struct.unpack(">H", data[8:10])[0] if len(data) >= 10 else None

        return {
            "protocol_id": protocol_id,
            "length": length,
            "transaction_id": transaction_id,
            "opcode": opcode,
            "result_code": result_code,
            "protocol_name": PROTOCOL_NAMES.get(protocol_id),
            "opcode_name": OPCODE_NAMES.get(opcode),
            "result_name": RESULT_NAMES.get(result_code) if result_code is not None else None,
        }

    if protocol_id == 0xFFFF and len(data) >= 28:
        message_type = struct.unpack(">H", data[26:28])[0]
        message_type_name = f"msg_type:{message_type}"

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

    return {
        "protocol_id": protocol_id,
        "length": length,
        "transaction_id": struct.unpack(">H", data[4:6])[0] if len(data) >= 6 else None,
        "opcode": struct.unpack(">H", data[6:8])[0] if len(data) >= 8 else None,
        "result_code": None,
        "protocol_name": None,
        "opcode_name": None,
        "result_name": None,
    }


class PacketStore:
    def __init__(self, db_path=None):
        self._db_path = db_path or DEFAULT_DB_PATH
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

    def _create_tables(self):
        self._conn.executescript("""
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
                payload_hex TEXT NOT NULL,
                correlated_packet_id INTEGER REFERENCES packets(id),
                multicast_group TEXT,
                multicast_port INTEGER
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
        """)
        self._conn.commit()

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
        timestamp_ns: int = None,
    ) -> int | None:
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()

        timestamp_iso = datetime.datetime.fromtimestamp(
            timestamp_ns / 1e9
        ).isoformat(timespec="microseconds")

        header = _parse_header(payload)

        payload_hex = payload.hex()

        try:
            cursor = self._conn.execute(
                """INSERT INTO packets (
                    timestamp_ns, timestamp_iso, src_ip, src_port, dst_ip, dst_port,
                    source_type, direction, device_name, device_ip,
                    protocol_id, protocol_name, transaction_id, opcode, opcode_name,
                    result_code, result_name, payload, payload_hex,
                    multicast_group, multicast_port
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    payload,
                    payload_hex,
                    multicast_group,
                    multicast_port,
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

    def get_packet(self, packet_id):
        row = self._conn.execute(
            "SELECT * FROM packets WHERE id = ?", (packet_id,)
        ).fetchone()
        return dict(row) if row else None

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
            request = dict(row)
            resp_row = self._conn.execute(
                "SELECT * FROM packets WHERE id = ?", (request["resp_id"],)
            ).fetchone()
            if resp_row:
                pairs.append((request, dict(resp_row)))

        return pairs

    def get_packets_by_opcode(self, opcode):
        rows = self._conn.execute(
            "SELECT * FROM packets WHERE opcode = ? ORDER BY timestamp_ns",
            (opcode,),
        ).fetchall()
        return [dict(r) for r in rows]

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
        return [dict(r) for r in rows]

    def export_fixture(self, packet_id, output_dir):
        row = self._conn.execute(
            "SELECT * FROM packets WHERE id = ?", (packet_id,)
        ).fetchone()
        if not row:
            return None

        row = dict(row)
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
