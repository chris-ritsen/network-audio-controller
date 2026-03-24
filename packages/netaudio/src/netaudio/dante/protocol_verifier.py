import json
import logging
import os
import socket

from pathlib import Path

from netaudio.common.config_loader import load_capture_profile, resolve_db_from_config
from netaudio.dante.const import DEVICE_ARC_PORT, SERVICE_ARC
from netaudio.dante.packet_store import PacketStore, _parse_header, _safe_name
from netaudio.dante.service import DanteUnicastService

logger = logging.getLogger("netaudio")

SERVICE_PORT_MAP = {
    SERVICE_ARC: DEVICE_ARC_PORT,
}


def export_session_bundle(store: PacketStore, session_id: int, output_dir: str | None = None) -> Path:
    import tarfile
    import io

    session = store.get_session(session_id)
    session_name = session["name"] if session else f"session_{session_id}"
    bundle_name = _safe_name(session_name)

    markers = store.get_markers(session_id)

    evidence_packet_ids = set()
    for marker_row in markers:
        if marker_row.get("marker_type") == "evidence":
            marker_data = marker_row.get("data")
            if marker_data and marker_data.get("packet_ids"):
                for pid in marker_data["packet_ids"]:
                    evidence_packet_ids.add(pid)

    evidence_packets = []
    for packet_id in sorted(evidence_packet_ids):
        packet = store.get_packet(packet_id)
        if packet:
            evidence_packets.append(packet)
    evidence_packets.sort(key=lambda p: (p["timestamp_ns"], p["id"]))

    if output_dir is None:
        if evidence_packet_ids:
            output_dir = os.path.join("tests", "fixtures", "provenance")
        else:
            output_dir = os.path.join("tests", "fixtures", "provenance", ".local")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    scope = {}
    if session and session.get("metadata"):
        scope = {
            "device_ip": session["metadata"].get("device_ip", ""),
            "device_name": session["metadata"].get("device_name", ""),
        }

    session_packet_count = store.get_session_packet_count(session_id)

    manifest = {
        "session_id": session_id,
        "session_name": session_name,
        "scope": scope,
        "started_iso": session.get("started_iso", "") if session else "",
        "ended_iso": session.get("ended_iso", "") if session else "",
        "count": len(evidence_packets),
        "session_packet_count": session_packet_count,
        "evidence_packet_count": len(evidence_packets),
        "markers": [],
        "samples": [],
    }

    for marker_row in markers:
        marker_entry = {
            "marker_type": marker_row["marker_type"],
            "label": marker_row["label"],
            "note": marker_row.get("note"),
            "timestamp_iso": marker_row["timestamp_iso"],
            "timestamp_ns": marker_row["timestamp_ns"],
        }
        marker_data = marker_row.get("data")
        if marker_data:
            marker_entry["data"] = marker_data
        manifest["markers"].append(marker_entry)

    def _build_sample_entry(packet_row):
        header = _parse_header(packet_row["payload"])
        protocol_id = header["protocol_id"] if header else packet_row.get("protocol_id")
        opcode = header["opcode"] if header else packet_row.get("opcode")
        direction = packet_row.get("direction", "unknown")
        protocol_hex = f"0x{protocol_id:04X}" if protocol_id is not None else "unknown"
        opcode_hex = f"0x{opcode:04X}" if opcode is not None else "unknown"
        if protocol_id == 0xFFFF:
            filename = f"protocol_{protocol_hex[2:]}_message_{opcode_hex[2:]}_id_{packet_row['id']}.bin"
        else:
            filename = f"protocol_{protocol_hex[2:]}_opcode_{opcode_hex[2:]}_id_{packet_row['id']}.bin"
        sample = {
            "file": filename,
            "packet_id": packet_row["id"],
            "timestamp_iso": packet_row["timestamp_iso"],
            "timestamp_ns": packet_row["timestamp_ns"],
            "src_ip": packet_row.get("src_ip"),
            "src_port": packet_row.get("src_port"),
            "dst_ip": packet_row.get("dst_ip"),
            "dst_port": packet_row.get("dst_port"),
            "direction": direction,
            "protocol_id": protocol_id,
            "protocol_hex": protocol_hex,
            "opcode": opcode,
            "opcode_hex": opcode_hex,
            "session_id": packet_row.get("session_id"),
            "device_ip": packet_row.get("device_ip"),
        }
        return sample, filename

    tar_path = output_path / f"{bundle_name}.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        for packet_row in evidence_packets:
            sample, filename = _build_sample_entry(packet_row)
            manifest["samples"].append(sample)
            data = packet_row["payload"]
            info = tarfile.TarInfo(name=f"{bundle_name}/{filename}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8") + b"\n"
        info = tarfile.TarInfo(name=f"{bundle_name}/manifest.json")
        info.size = len(manifest_bytes)
        tar.addfile(info, io.BytesIO(manifest_bytes))

    logger.info(f"Exported provenance bundle: {tar_path} ({len(evidence_packets)} evidence packets, {len(markers)} markers)")
    return tar_path


class ProtocolVerifier:
    def __init__(
        self,
        device_ip: str,
        device_name: str = "",
        session_name: str = "protocol_verification",
        config: str | None = None,
        profile: str | None = None,
        db: str | None = None,
        output_dir: str | None = None,
        record: bool = True,
        category: str = "experiment",
    ):
        self._device_ip = device_ip
        self._device_name = device_name
        self._session_name = session_name
        self._config = config
        self._profile = profile
        self._db_override = db
        self._output_dir = output_dir
        self._record = record
        self._category = category
        self._packet_store: PacketStore | None = None
        self._service: DanteUnicastService | None = None
        self._session_id: int | None = None
        self._source_host: str | None = None
        self._evidence_queries: list[dict] = []
        self._evidence_packet_ids: set[int] = set()

    @property
    def session_id(self) -> int | None:
        return self._session_id

    @property
    def packet_store(self) -> PacketStore | None:
        return self._packet_store

    @property
    def service(self) -> DanteUnicastService | None:
        return self._service

    async def __aenter__(self):
        try:
            self._source_host = socket.gethostname()
        except Exception:
            self._source_host = "unknown"

        if self._record:
            profile_cfg, _ = load_capture_profile(self._config, self._profile)
            db_path = resolve_db_from_config(self._db_override, profile_cfg)

            self._packet_store = PacketStore(db_path=db_path)

            self._session_id = self._packet_store.start_session(
                name=self._session_name,
                source_host=self._source_host,
                description=f"Protocol verification: {self._session_name}",
                category=self._category,
                metadata={
                    "device_ip": self._device_ip,
                    "device_name": self._device_name,
                    "verifier": "ProtocolVerifier",
                },
            )

        self._service = DanteUnicastService(packet_store=self._packet_store)
        self._service.session_id = self._session_id
        await self._service.start()

        self.marker("session_started", marker_type="system", note="ProtocolVerifier session started")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.marker("session_stopped", marker_type="system", note="ProtocolVerifier session ended")

        if self._service is not None:
            await self._service.stop()

        if self._packet_store is not None and self._session_id is not None:
            self._packet_store.end_session(self._session_id)

        if self._record:
            try:
                self.export_bundle()
            except Exception as exception:
                logger.error(f"Failed to export provenance bundle: {exception}")

        if self._packet_store is not None:
            self._packet_store.close()

        return False

    def marker(
        self,
        label: str,
        marker_type: str = "marker",
        note: str | None = None,
        data: dict | None = None,
    ) -> int | None:
        if self._packet_store is None or self._session_id is None:
            return None

        return self._packet_store.add_marker(
            session_id=self._session_id,
            marker_type=marker_type,
            label=label,
            note=note,
            source_host=self._source_host,
            data=data,
        )

    def hypothesis(self, label: str, note: str | None = None, data: dict | None = None) -> int | None:
        return self.marker(label, marker_type="hypothesis", note=note, data=data)

    def observation(self, label: str, note: str | None = None, data: dict | None = None) -> int | None:
        return self.marker(label, marker_type="observation", note=note, data=data)

    def include_evidence(
        self,
        label: str | None = None,
        note: str | None = None,
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
        time_window: bool = False,
        limit: int = 10000,
    ) -> list[dict]:
        if self._packet_store is None:
            return []

        if time_window and self._session_id is not None:
            session = self._packet_store.get_session(self._session_id)
            if session:
                if start_ns is None:
                    start_ns = session["started_ns"]
                if end_ns is None and session.get("ended_ns"):
                    end_ns = session["ended_ns"]

        packets = self._packet_store.query_packets(
            device_ip=device_ip,
            src_ip=src_ip,
            dst_ip=dst_ip,
            opcode=opcode,
            protocol_id=protocol_id,
            direction=direction,
            source_type=source_type,
            session_id=session_id,
            start_ns=start_ns,
            end_ns=end_ns,
            payload_hex_contains=payload_hex_contains,
            min_length=min_length,
            max_length=max_length,
            limit=limit,
        )

        for packet in packets:
            self._evidence_packet_ids.add(packet["id"])

        if label and self._session_id is not None:
            self.marker(
                f"evidence_{label}",
                marker_type="evidence",
                note=note or f"Included {len(packets)} evidence packets",
                data={
                    "query": {
                        "device_ip": device_ip,
                        "opcode": opcode,
                        "protocol_id": protocol_id,
                        "direction": direction,
                        "payload_hex_contains": payload_hex_contains,
                    },
                    "packet_count": len(packets),
                    "packet_ids": [p["id"] for p in packets],
                },
            )

        return packets

    async def send(
        self,
        packet: bytes,
        port: int,
        timeout: float = 2.0,
        label: str | None = None,
    ) -> bytes | None:
        if label:
            self.marker(f"send_{label}", marker_type="step", note=f"Sending packet: {label}")

        response = await self._service.request(
            packet=packet,
            device_ip=self._device_ip,
            port=port,
            timeout=timeout,
            device_name=self._device_name,
        )

        if label:
            received = response is not None
            self.marker(
                f"recv_{label}",
                marker_type="step",
                note=f"Response {'received' if received else 'timeout'}: {label}",
                data={"received": received, "response_len": len(response) if response else 0},
            )

        return response

    async def send_command(
        self,
        command_tuple: tuple,
        timeout: float = 2.0,
        label: str | None = None,
    ) -> bytes | None:
        packet = command_tuple[0]
        service_or_port = command_tuple[1] if len(command_tuple) > 1 else None
        explicit_port = command_tuple[2] if len(command_tuple) > 2 else None

        if explicit_port is not None:
            port = explicit_port
        elif isinstance(service_or_port, int):
            port = service_or_port
        elif isinstance(service_or_port, str):
            port = SERVICE_PORT_MAP.get(service_or_port, DEVICE_ARC_PORT)
        else:
            port = DEVICE_ARC_PORT

        return await self.send(packet, port, timeout=timeout, label=label)

    def _build_sample(self, packet_row, evidence=False):
        header = _parse_header(packet_row["payload"])
        protocol_id = header["protocol_id"] if header else packet_row.get("protocol_id")
        opcode = header["opcode"] if header else packet_row.get("opcode")
        direction = packet_row.get("direction", "unknown")

        protocol_hex = f"0x{protocol_id:04X}" if protocol_id is not None else "unknown"
        opcode_hex = f"0x{opcode:04X}" if opcode is not None else "unknown"

        prefix = "evidence_" if evidence else ""
        if protocol_id == 0xFFFF:
            filename = f"{prefix}protocol_{protocol_hex[2:]}_message_{opcode_hex[2:]}_id_{packet_row['id']}.bin"
        else:
            filename = f"{prefix}protocol_{protocol_hex[2:]}_opcode_{opcode_hex[2:]}_id_{packet_row['id']}.bin"

        sample = {
            "file": filename,
            "packet_id": packet_row["id"],
            "timestamp_iso": packet_row["timestamp_iso"],
            "timestamp_ns": packet_row["timestamp_ns"],
            "src_ip": packet_row.get("src_ip"),
            "src_port": packet_row.get("src_port"),
            "dst_ip": packet_row.get("dst_ip"),
            "dst_port": packet_row.get("dst_port"),
            "direction": direction,
            "protocol_id": protocol_id,
            "protocol_hex": protocol_hex,
            "opcode": opcode,
            "opcode_hex": opcode_hex,
            "opcode_name": packet_row.get("opcode_name") or opcode_hex,
            "evidence": evidence,
            "session_id": packet_row.get("session_id"),
            "device_ip": packet_row.get("device_ip"),
        }
        return sample, filename

    def export_bundle(self, output_dir: str | None = None) -> Path | None:
        if self._packet_store is None or self._session_id is None:
            return None

        target_dir = output_dir or self._output_dir
        if target_dir is None:
            target_dir = os.path.join(
                "tests",
                "fixtures",
                "provenance",
                f"session_{self._session_id}_{_safe_name(self._session_name)}",
            )

        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)

        session_packets = self._packet_store.get_session_packets(
            session_id=self._session_id,
            limit=10000,
            ascending=True,
        )

        session_packet_ids = {p["id"] for p in session_packets}

        evidence_packets = []
        for packet_id in self._evidence_packet_ids:
            if packet_id not in session_packet_ids:
                packet = self._packet_store.get_packet(packet_id)
                if packet:
                    evidence_packets.append(packet)
        evidence_packets.sort(key=lambda p: (p["timestamp_ns"], p["id"]))

        markers = self._packet_store.get_markers(self._session_id)

        manifest = {
            "db_path": self._packet_store._db_path,
            "session_id": self._session_id,
            "session_name": self._session_name,
            "scope": {
                "device_ip": self._device_ip,
                "device_name": self._device_name,
            },
            "count": len(session_packets) + len(evidence_packets),
            "session_packet_count": len(session_packets),
            "evidence_packet_count": len(evidence_packets),
            "markers": [],
            "samples": [],
        }

        for marker_row in markers:
            marker_entry = {
                "marker_type": marker_row["marker_type"],
                "label": marker_row["label"],
                "note": marker_row.get("note"),
                "timestamp_iso": marker_row["timestamp_iso"],
                "timestamp_ns": marker_row["timestamp_ns"],
            }
            marker_data = marker_row.get("data")
            if marker_data:
                marker_entry["data"] = marker_data
            manifest["markers"].append(marker_entry)

        for packet_row in session_packets:
            sample, filename = self._build_sample(packet_row, evidence=False)
            bin_path = target_path / filename
            with open(bin_path, "wb") as bin_file:
                bin_file.write(packet_row["payload"])
            manifest["samples"].append(sample)

        for packet_row in evidence_packets:
            sample, filename = self._build_sample(packet_row, evidence=True)
            bin_path = target_path / filename
            with open(bin_path, "wb") as bin_file:
                bin_file.write(packet_row["payload"])
            manifest["samples"].append(sample)

        manifest_path = target_path / "manifest.json"
        with open(manifest_path, "w") as manifest_file:
            json.dump(manifest, manifest_file, indent=2)
            manifest_file.write("\n")

        total = len(session_packets) + len(evidence_packets)
        logger.info(f"Exported provenance bundle: {target_path} ({total} packets, {len(evidence_packets)} evidence, {len(markers)} markers)")
        return target_path
