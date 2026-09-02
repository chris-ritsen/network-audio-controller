from __future__ import annotations

import datetime
import os
from dataclasses import dataclass

from netaudio.dante.packet_store_common import (
    SESSION_MEMBERSHIP_SQL,
    decompress_payload as _decompress_payload,
    extract_evidence_packet_ids,
    safe_name as _safe_name,
)


@dataclass(frozen=True)
class PacketQuery:
    ascending: bool = True
    device_ip: str | None = None
    device_name: str | None = None
    direction: str | None = None
    dst_ip: str | None = None
    end_ns: int | None = None
    limit: int = 10000
    max_length: int | None = None
    min_length: int | None = None
    offset: int = 0
    opcode: int | None = None
    payload_contains: str | None = None
    payload_hex_contains: str | None = None
    port: int | None = None
    protocol_id: int | None = None
    session_id: int | None = None
    source_type: str | None = None
    src_ip: str | None = None
    start_ns: int | None = None


class PacketStoreQueries:
    def _decode_packet_row(self, row):
        if not row:
            return None
        result = dict(row)
        result["payload"] = _decompress_payload(result.get("payload"))
        return result

    def _decode_packet_rows(self, rows):
        return [self._decode_packet_row(row) for row in rows]

    def get_packet(self, packet_id):
        row = self._conn.execute("SELECT * FROM packets WHERE id = ?", (packet_id,)).fetchone()
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
            resp_row = self._conn.execute("SELECT * FROM packets WHERE id = ?", (request["resp_id"],)).fetchone()
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
        query = f"SELECT COUNT(*) AS count FROM packets WHERE {SESSION_MEMBERSHIP_SQL}"
        params: list = [session_id, session_id]
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
        return len(extract_evidence_packet_ids(markers))

    def _apply_packet_filters(self, query: str, params: list, filters: PacketQuery) -> tuple[str, list]:
        if filters.device_ip:
            query += " AND (src_ip = ? OR dst_ip = ?)"
            params.extend([filters.device_ip, filters.device_ip])

        if filters.src_ip:
            query += " AND src_ip = ?"
            params.append(filters.src_ip)

        if filters.dst_ip:
            query += " AND dst_ip = ?"
            params.append(filters.dst_ip)

        if filters.port is not None:
            query += " AND (src_port = ? OR dst_port = ?)"
            params.extend([filters.port, filters.port])

        if filters.device_name:
            query += " AND device_name = ?"
            params.append(filters.device_name)

        if filters.start_ns is not None:
            query += " AND timestamp_ns >= ?"
            params.append(filters.start_ns)

        if filters.end_ns is not None:
            query += " AND timestamp_ns <= ?"
            params.append(filters.end_ns)

        if filters.opcode is not None:
            query += " AND opcode = ?"
            params.append(filters.opcode)

        if filters.protocol_id is not None:
            query += " AND protocol_id = ?"
            params.append(filters.protocol_id)

        if filters.direction is not None:
            if filters.direction == "__null__":
                query += " AND direction IS NULL"
            else:
                query += " AND direction = ?"
                params.append(filters.direction)

        if filters.payload_contains is not None:
            search_hex = filters.payload_contains.encode().hex()
            query += " AND decompress_hex(payload) LIKE ?"
            params.append(f"%{search_hex}%")

        return query, params

    def get_session_packet_count_filtered(self, filters: PacketQuery) -> int:
        query = f"SELECT COUNT(*) AS count FROM packets WHERE {SESSION_MEMBERSHIP_SQL}"
        params: list = [filters.session_id, filters.session_id]
        query, params = self._apply_packet_filters(query, params, filters)
        row = self._conn.execute(query, params).fetchone()
        return int(row["count"]) if row else 0

    def get_session_packets(self, filters: PacketQuery) -> list[dict]:
        query = f"SELECT * FROM packets WHERE {SESSION_MEMBERSHIP_SQL}"
        params: list = [filters.session_id, filters.session_id]
        query, params = self._apply_packet_filters(query, params, filters)
        order = "ASC" if filters.ascending else "DESC"
        query += f" ORDER BY timestamp_ns {order}, id {order} LIMIT ? OFFSET ?"
        params.extend([filters.limit, filters.offset])

        rows = self._conn.execute(query, params).fetchall()
        return self._decode_packet_rows(rows)

    def _session_scoped_query(self, selection: str, query: PacketQuery) -> tuple[str, list]:
        if query.session_id is not None:
            sql = f"SELECT {selection} FROM packets WHERE {SESSION_MEMBERSHIP_SQL}"
            params: list = [query.session_id, query.session_id]
        else:
            sql = f"SELECT {selection} FROM packets WHERE 1=1"
            params = []
        return self._apply_packet_filters(sql, params, query)

    def search_packets(self, query: PacketQuery) -> list[dict]:
        sql, params = self._session_scoped_query("*", query)
        order = "ASC" if query.ascending else "DESC"
        sql += f" ORDER BY timestamp_ns {order}, id {order} LIMIT ? OFFSET ?"
        params.extend([query.limit, query.offset])

        rows = self._conn.execute(sql, params).fetchall()
        return self._decode_packet_rows(rows)

    def search_packets_count(self, query: PacketQuery) -> int:
        sql, params = self._session_scoped_query("COUNT(*) AS count", query)
        row = self._conn.execute(sql, params).fetchone()
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
        row = self._conn.execute("SELECT * FROM packets WHERE id = ?", (packet_id,)).fetchone()
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
        row = self._conn.execute("SELECT * FROM packets WHERE id = ?", (request_id,)).fetchone()
        if not row or not row["correlated_packet_id"]:
            return None

        req_path = self.export_fixture(request_id, output_dir)
        resp_path = self.export_fixture(row["correlated_packet_id"], output_dir)
        return (req_path, resp_path)

    def query_packets(self, query: PacketQuery) -> list[dict]:
        sql = "SELECT * FROM packets WHERE 1=1"
        params: list = []

        if query.device_ip:
            sql += " AND (device_ip = ? OR src_ip = ? OR dst_ip = ?)"
            params.extend([query.device_ip, query.device_ip, query.device_ip])

        if query.src_ip:
            sql += " AND src_ip = ?"
            params.append(query.src_ip)

        if query.dst_ip:
            sql += " AND dst_ip = ?"
            params.append(query.dst_ip)

        if query.opcode is not None:
            sql += " AND opcode = ?"
            params.append(query.opcode)

        if query.protocol_id is not None:
            sql += " AND protocol_id = ?"
            params.append(query.protocol_id)

        if query.direction:
            sql += " AND direction = ?"
            params.append(query.direction)

        if query.source_type:
            sql += " AND source_type = ?"
            params.append(query.source_type)

        if query.session_id is not None:
            sql += f" AND {SESSION_MEMBERSHIP_SQL}"
            params.extend([query.session_id, query.session_id])

        if query.start_ns is not None:
            sql += " AND timestamp_ns >= ?"
            params.append(query.start_ns)

        if query.end_ns is not None:
            sql += " AND timestamp_ns <= ?"
            params.append(query.end_ns)

        if query.payload_hex_contains:
            sql += " AND decompress_hex(payload) LIKE ?"
            params.append(f"%{query.payload_hex_contains.lower()}%")

        if query.min_length is not None:
            sql += " AND length(payload) >= ?"
            params.append(query.min_length)

        if query.max_length is not None:
            sql += " AND length(payload) <= ?"
            params.append(query.max_length)

        order = "ASC" if query.ascending else "DESC"
        sql += f" ORDER BY timestamp_ns {order}, id {order} LIMIT ? OFFSET ?"
        params.extend([query.limit, query.offset])

        rows = self._conn.execute(sql, params).fetchall()
        return self._decode_packet_rows(rows)

    def get_stats(self):
        stats = {}

        row = self._conn.execute("SELECT COUNT(*) as total FROM packets").fetchone()
        stats["total"] = row["total"]

        rows = self._conn.execute("SELECT source_type, COUNT(*) as count FROM packets GROUP BY source_type").fetchall()
        stats["by_source"] = {r["source_type"]: r["count"] for r in rows}

        rows = self._conn.execute(
            "SELECT opcode_name, direction, COUNT(*) as count FROM packets "
            "GROUP BY opcode_name, direction ORDER BY count DESC"
        ).fetchall()
        stats["by_opcode"] = [
            {"opcode_name": r["opcode_name"], "direction": r["direction"], "count": r["count"]} for r in rows
        ]

        row = self._conn.execute(
            "SELECT COUNT(*) as count FROM packets WHERE correlated_packet_id IS NOT NULL"
        ).fetchone()
        stats["correlated"] = row["count"]

        row = self._conn.execute("SELECT COUNT(*) as count FROM packets WHERE correlated_packet_id IS NULL").fetchone()
        stats["uncorrelated"] = row["count"]

        return stats
