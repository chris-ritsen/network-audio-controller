from __future__ import annotations

import hashlib
import io
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from netaudio import core
from netaudio.core import _types

from netaudio.dante.conmon_export import (
    ConmonExport,
    ConmonExportError,
    decode_bounded_gzip,
    write_new_private_file,
)

DEFAULT_MAXIMUM_DEVICE_LOG_ARCHIVE_SIZE = 256 * 1024 * 1024


class DeviceLogExportError(ConmonExportError):
    pass


@dataclass(frozen=True)
class DeviceLogArchiveMember:
    name: str
    size: int
    kind: str


@dataclass(frozen=True)
class DeviceLogExport:
    encoded_payload: bytes
    archive_payload: bytes
    encoded_sha256: str
    archive_sha256: str
    fragment_count: int
    members: tuple[DeviceLogArchiveMember, ...]
    audio_capabilities: _types.DiagnosticAudioCapabilities | None


def parse_device_log_export(
    export: ConmonExport,
    maximum_archive_size: int = DEFAULT_MAXIMUM_DEVICE_LOG_ARCHIVE_SIZE,
) -> DeviceLogExport:
    if export.kind != "diagnostic_logs":
        raise DeviceLogExportError("response is not a diagnostic-log export")
    try:
        archive_payload = decode_bounded_gzip(export.encoded_payload, maximum_archive_size)
    except ConmonExportError as exception:
        raise DeviceLogExportError(str(exception)) from exception
    members = _read_archive_members(archive_payload)
    return DeviceLogExport(
        encoded_payload=export.encoded_payload,
        archive_payload=archive_payload,
        encoded_sha256=export.encoded_sha256,
        archive_sha256=hashlib.sha256(archive_payload).hexdigest(),
        fragment_count=export.fragment_count,
        members=members,
        audio_capabilities=parse_device_audio_capabilities(archive_payload),
    )


def _read_archive_members(archive_payload: bytes) -> tuple[DeviceLogArchiveMember, ...]:
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_payload), mode="r:") as archive:
            return tuple(
                DeviceLogArchiveMember(
                    name=member.name,
                    size=member.size,
                    kind="file" if member.isfile() else "directory" if member.isdir() else "other",
                )
                for member in archive.getmembers()
            )
    except (tarfile.TarError, OSError) as exception:
        raise DeviceLogExportError("LOGS payload does not contain a valid tar archive") from exception


def _read_regular_archive_member(archive_payload: bytes, member_name: str) -> Optional[bytes]:
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_payload), mode="r:") as archive:
            member = archive.getmember(member_name)
            if not member.isfile():
                return None
            member_file = archive.extractfile(member)
            if member_file is None:
                return None
            return member_file.read()
    except KeyError:
        return None
    except (tarfile.TarError, OSError) as exception:
        raise DeviceLogExportError("LOGS payload does not contain a valid tar archive") from exception


def parse_device_audio_capabilities(archive_payload: bytes) -> _types.DiagnosticAudioCapabilities | None:
    audio_log_payload = _read_regular_archive_member(archive_payload, "tmp/dante_data/apec.log")

    if audio_log_payload is None:
        return None

    return core.parse_diagnostic_audio(audio_log_payload)


def device_audio_capability_fields(capabilities: _types.DiagnosticAudioCapabilities | None) -> dict:
    fields: dict = {"diagnostic_log_export_supported": True}

    if capabilities is None:
        return fields

    if capabilities["license_signature_length_bytes"] is not None:
        fields["license_signature_length_bytes"] = capabilities["license_signature_length_bytes"]

    if capabilities["licensed_receive_channel_count"] is not None:
        fields["licensed_receive_channel_count"] = capabilities["licensed_receive_channel_count"]

    if capabilities["licensed_transmit_channel_count"] is not None:
        fields["licensed_transmit_channel_count"] = capabilities["licensed_transmit_channel_count"]

    if capabilities["licensed_redundancy_enabled"] is not None:
        fields["licensed_redundancy_enabled"] = capabilities["licensed_redundancy_enabled"]

    if capabilities["channel_capacities"]:
        fields["sample_rate_channel_capacities"] = [dict(capacity) for capacity in capabilities["channel_capacities"]]

    return fields


def write_device_log_archive(output_path: Path, archive_payload: bytes) -> None:
    write_new_private_file(output_path, archive_payload)
