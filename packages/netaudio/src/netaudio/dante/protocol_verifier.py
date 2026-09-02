from __future__ import annotations

import gzip
import hashlib
import logging
import os
import socket
from dataclasses import replace
from pathlib import Path

from netaudio.common.config_loader import load_capture_profile, resolve_db_from_config
from netaudio.dante.const import DEVICE_ARC_PORT, SERVICE_ARC
from netaudio.common.manifest import manifest_bytes, write_manifest
from netaudio.dante.packet_header import parse_packet_header
from netaudio.dante.packet_store import PacketQuery, PacketRecord, PacketStore
from netaudio.dante.packet_store_common import extract_evidence_packet_ids, safe_name as _safe_name
from netaudio.dante.core_transport import CoreTransport

logger = logging.getLogger("netaudio")

SERVICE_PORT_MAP = {
    SERVICE_ARC: DEVICE_ARC_PORT,
}


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _artifact_bundle_filename(artifact: dict) -> str:
    source_name = Path(str(artifact["filename"])).name
    source_path = Path(source_name)
    safe_stem = _safe_name(source_path.stem) or "artifact"
    safe_suffix = "".join(character for character in source_path.suffix if character.isalnum() or character == ".")
    return f"artifact_{artifact['id']}_{safe_stem}{safe_suffix}"


def _artifact_manifest_entry(artifact: dict, filename: str) -> dict:
    return {
        "artifact_id": artifact["id"],
        "file": filename,
        "label": artifact["label"],
        "role": artifact["role"],
        "media_type": artifact["media_type"],
        "note": artifact.get("note"),
        "source_path": artifact.get("source_path"),
        "source_host": artifact.get("source_host"),
        "source_modified_ns": artifact.get("source_modified_ns"),
        "timestamp_iso": artifact["timestamp_iso"],
        "timestamp_ns": artifact["timestamp_ns"],
        "size": artifact["size"],
        "sha256": artifact["sha256"],
    }


def export_session_bundle(
    store: PacketStore,
    session_id: int,
    output_dir: str | None = None,
    packet_ids: set | None = None,
    bundle_format: str = "tar.gz",
) -> Path:
    import tarfile
    import zipfile
    import io

    session = store.get_session(session_id)
    session_name = session["name"] if session else f"session_{session_id}"
    bundle_name = _safe_name(session_name)

    markers = store.get_markers(session_id)

    if packet_ids is not None:
        evidence_packet_ids = set(packet_ids)
    else:
        evidence_packet_ids = extract_evidence_packet_ids(markers)

    evidence_packets = []
    missing_packet_ids = []
    for packet_id in sorted(evidence_packet_ids):
        packet = store.get_packet(packet_id)
        if packet:
            evidence_packets.append(packet)
        else:
            missing_packet_ids.append(packet_id)
    if missing_packet_ids:
        missing_list = ", ".join(str(packet_id) for packet_id in missing_packet_ids)
        raise ValueError(f"Evidence packets not found: {missing_list}")
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
    artifacts = store.get_session_artifacts(session_id)

    manifest = {
        "format_version": 2,
        "session_id": session_id,
        "session_name": session_name,
        "scope": scope,
        "started_iso": session.get("started_iso", "") if session else "",
        "ended_iso": session.get("ended_iso", "") if session else "",
        "count": len(evidence_packets),
        "session_packet_count": session_packet_count,
        "evidence_packet_count": len(evidence_packets),
        "artifact_count": len(artifacts),
        "markers": [],
        "samples": [],
        "artifacts": [],
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
        header = parse_packet_header(packet_row["payload"])
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
            "evidence": True,
            "session_id": packet_row.get("session_id"),
            "device_ip": packet_row.get("device_ip"),
            "source_type": packet_row.get("source_type"),
            "source_host": packet_row.get("source_host"),
            "interface": packet_row.get("interface"),
            "size": len(packet_row["payload"]),
            "sha256": _sha256(packet_row["payload"]),
        }
        return sample, filename

    file_entries = {}
    for packet_row in evidence_packets:
        sample, filename = _build_sample_entry(packet_row)
        manifest["samples"].append(sample)
        file_entries[filename] = packet_row["payload"]

    for artifact in artifacts:
        filename = _artifact_bundle_filename(artifact)
        manifest["artifacts"].append(_artifact_manifest_entry(artifact, filename))
        file_entries[filename] = artifact["content"]

    file_entries["manifest.json"] = manifest_bytes(manifest)

    if bundle_format == "zip":
        bundle_path = output_path / f"{bundle_name}.zip"
        with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for filename, data in file_entries.items():
                info = zipfile.ZipInfo(f"{bundle_name}/{filename}", date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o100644 << 16
                zf.writestr(info, data)
    else:
        bundle_path = output_path / f"{bundle_name}.tar.gz"
        with bundle_path.open("wb") as raw_bundle:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw_bundle, mtime=0) as compressed_bundle:
                with tarfile.open(fileobj=compressed_bundle, mode="w") as tar:
                    for filename, data in file_entries.items():
                        info = tarfile.TarInfo(name=f"{bundle_name}/{filename}")
                        info.size = len(data)
                        tar.addfile(info, io.BytesIO(data))

    logger.info(
        f"Exported provenance bundle: {bundle_path} "
        f"({len(evidence_packets)} evidence packets, {len(artifacts)} artifacts, {len(markers)} markers)"
    )
    return bundle_path


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
        self._transport: CoreTransport | None = None
        self._session_id: int | None = None
        self._source_host: str | None = None
        self._evidence_packet_ids: set[int] = set()

    @property
    def session_id(self) -> int | None:
        return self._session_id

    @property
    def packet_store(self) -> PacketStore | None:
        return self._packet_store

    @property
    def transport(self) -> CoreTransport | None:
        return self._transport

    async def __aenter__(self):
        self._source_host = socket.gethostname()

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

        self._transport = CoreTransport(observer=self._observe_wire if self._packet_store is not None else None)

        self.marker("session_started", marker_type="system", note="ProtocolVerifier session started")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.marker("session_stopped", marker_type="system", note="ProtocolVerifier session ended")

        if self._transport is not None:
            self._transport.close()
            self._transport = None

        if self._packet_store is not None and self._session_id is not None:
            self._packet_store.end_session(self._session_id)

        if self._record:
            self.export_bundle()

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
        query: PacketQuery,
        label: str | None = None,
        note: str | None = None,
        time_window: bool = False,
    ) -> list[dict]:
        if self._packet_store is None:
            return []

        if time_window and self._session_id is not None:
            session = self._packet_store.get_session(self._session_id)
            if session:
                query = replace(
                    query,
                    start_ns=session["started_ns"] if query.start_ns is None else query.start_ns,
                    end_ns=session.get("ended_ns") if query.end_ns is None else query.end_ns,
                )

        packets = self._packet_store.query_packets(query)

        for packet in packets:
            self._evidence_packet_ids.add(packet["id"])

        if label and self._session_id is not None:
            self.marker(
                f"evidence_{label}",
                marker_type="evidence",
                note=note or f"Included {len(packets)} evidence packets",
                data={
                    "query": {
                        "device_ip": query.device_ip,
                        "direction": query.direction,
                        "opcode": query.opcode,
                        "payload_hex_contains": query.payload_hex_contains,
                        "protocol_id": query.protocol_id,
                    },
                    "packet_count": len(packets),
                    "packet_ids": [p["id"] for p in packets],
                },
            )

        return packets

    def _observe_wire(self, payload: bytes, device_ip: str, port: int, direction: str) -> None:
        if direction == "request":
            self._packet_store.store_packet(
                PacketRecord(
                    payload=payload,
                    source_type="netaudio_request",
                    device_name=self._device_name,
                    device_ip=device_ip,
                    dst_ip=device_ip,
                    dst_port=port,
                    direction="request",
                    session_id=self._session_id,
                )
            )
            return
        self._packet_store.store_packet(
            PacketRecord(
                payload=payload,
                source_type="netaudio_response",
                device_name=self._device_name,
                device_ip=device_ip,
                src_ip=device_ip,
                src_port=port,
                direction="response",
                session_id=self._session_id,
            )
        )

    async def send(
        self,
        packet: bytes,
        port: int,
        timeout: float = 2.0,
        label: str | None = None,
    ) -> bytes | None:
        from netaudio import core

        if label:
            self.marker(f"send_{label}", marker_type="step", note=f"Sending packet: {label}")

        try:
            response = await self._transport.call(
                self._device_ip,
                lambda client: client.request(packet, port),
                timeout_milliseconds=int(timeout * 1000),
                attempts=1,
            )
        except core.NetaudioCoreError as exception:
            if exception.status != core.STATUS_TIMEOUT:
                raise
            response = None

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
        header = parse_packet_header(packet_row["payload"])
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
            "source_type": packet_row.get("source_type"),
            "source_host": packet_row.get("source_host"),
            "interface": packet_row.get("interface"),
            "size": len(packet_row["payload"]),
            "sha256": _sha256(packet_row["payload"]),
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
            PacketQuery(session_id=self._session_id, limit=10000, ascending=True)
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
        artifacts = self._packet_store.get_session_artifacts(self._session_id)

        manifest = {
            "format_version": 2,
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
            "artifact_count": len(artifacts),
            "markers": [],
            "samples": [],
            "artifacts": [],
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

        for artifact in artifacts:
            filename = _artifact_bundle_filename(artifact)
            artifact_path = target_path / filename
            with open(artifact_path, "wb") as artifact_file:
                artifact_file.write(artifact["content"])
            manifest["artifacts"].append(_artifact_manifest_entry(artifact, filename))

        write_manifest(target_path, manifest)

        total = len(session_packets) + len(evidence_packets)
        logger.info(
            f"Exported provenance bundle: {target_path} "
            f"({total} packets, {len(evidence_packets)} evidence, {len(artifacts)} artifacts, {len(markers)} markers)"
        )
        return target_path
