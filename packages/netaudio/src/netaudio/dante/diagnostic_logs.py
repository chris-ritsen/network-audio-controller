from __future__ import annotations

import hashlib
import io
import re
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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
class DeviceAudioChannelCapacity:
    sample_rate_hertz: int
    receive_channel_count: int
    transmit_channel_count: int


@dataclass(frozen=True)
class DeviceAudioCapabilities:
    license_signature_length_bytes: Optional[int]
    licensed_receive_channel_count: Optional[int]
    licensed_transmit_channel_count: Optional[int]
    licensed_redundancy_enabled: Optional[bool]
    default_sample_rate_hertz: Optional[int]
    current_sample_rate_hertz: Optional[int]
    channel_capacities: tuple[DeviceAudioChannelCapacity, ...]


@dataclass(frozen=True)
class DeviceLogExport:
    encoded_payload: bytes
    archive_payload: bytes
    encoded_sha256: str
    archive_sha256: str
    fragment_count: int
    members: tuple[DeviceLogArchiveMember, ...]
    audio_capabilities: Optional[DeviceAudioCapabilities]


def parse_device_log_export(
    export: ConmonExport,
    maximum_archive_size: int = DEFAULT_MAXIMUM_DEVICE_LOG_ARCHIVE_SIZE,
) -> DeviceLogExport:
    if export.echoed_tag != b"LOGS" or export.selector_value != 1:
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


def _unique_integer(matches: list[str]) -> Optional[int]:
    values = {int(value) for value in matches}
    if len(values) != 1:
        return None
    return values.pop()


def parse_device_audio_capabilities(archive_payload: bytes) -> Optional[DeviceAudioCapabilities]:
    audio_log_payload = _read_regular_archive_member(archive_payload, "tmp/dante_data/apec.log")
    if audio_log_payload is None:
        return None
    audio_log = audio_log_payload.decode("utf-8", errors="replace")

    signature_length = _unique_integer(re.findall(r"(?m)^rsa\.len = (\d+)$", audio_log))
    licensed_counts = {
        (int(receive_count), int(transmit_count))
        for receive_count, transmit_count in re.findall(
            r"(?m)^license rx chans (\d+) tx chans (\d+)$",
            audio_log,
        )
    }
    if len(licensed_counts) == 1:
        licensed_receive_count, licensed_transmit_count = licensed_counts.pop()
    else:
        licensed_receive_count = None
        licensed_transmit_count = None

    redundancy_values = set()
    if re.search(r"(?m)^is redundant$", audio_log):
        redundancy_values.add(True)
    if re.search(r"(?m)^is non-redundant$", audio_log):
        redundancy_values.add(False)
    licensed_redundancy = redundancy_values.pop() if len(redundancy_values) == 1 else None

    default_sample_rate = _unique_integer(
        re.findall(
            r"(?m)^audio config dynamic \d+ audio_frame \d+ audio_align \d+ audio start \d+ "
            r"default srate (\d+) default enc \d+ num srates \d+ num enc \d+$",
            audio_log,
        )
    )
    current_sample_rate = _unique_integer(re.findall(r"(?m)^MCLK = \d+ sample_rate = (\d+) TDM = \d+$", audio_log))

    capacity_values: dict[int, set[tuple[int, int]]] = {}
    for sample_rate, receive_count, transmit_count in re.findall(
        r"(?m)^srate (\d+) rxchan (\d+) txchan (\d+) mclk \d+ tdm_chan \d+$",
        audio_log,
    ):
        capacity_values.setdefault(int(sample_rate), set()).add((int(receive_count), int(transmit_count)))
    channel_capacities = tuple(
        DeviceAudioChannelCapacity(
            sample_rate_hertz=sample_rate,
            receive_channel_count=next(iter(counts))[0],
            transmit_channel_count=next(iter(counts))[1],
        )
        for sample_rate, counts in sorted(capacity_values.items())
        if len(counts) == 1
    )

    if (
        signature_length is None
        and licensed_receive_count is None
        and licensed_transmit_count is None
        and licensed_redundancy is None
        and default_sample_rate is None
        and current_sample_rate is None
        and not channel_capacities
    ):
        return None
    return DeviceAudioCapabilities(
        license_signature_length_bytes=signature_length,
        licensed_receive_channel_count=licensed_receive_count,
        licensed_transmit_channel_count=licensed_transmit_count,
        licensed_redundancy_enabled=licensed_redundancy,
        default_sample_rate_hertz=default_sample_rate,
        current_sample_rate_hertz=current_sample_rate,
        channel_capacities=channel_capacities,
    )


def device_audio_capability_fields(capabilities: Optional[DeviceAudioCapabilities]) -> dict:
    fields = {"diagnostic_log_export_supported": True}
    if capabilities is None:
        return fields
    if capabilities.license_signature_length_bytes is not None:
        fields["license_signature_length_bytes"] = capabilities.license_signature_length_bytes
    if capabilities.licensed_receive_channel_count is not None:
        fields["licensed_receive_channel_count"] = capabilities.licensed_receive_channel_count
    if capabilities.licensed_transmit_channel_count is not None:
        fields["licensed_transmit_channel_count"] = capabilities.licensed_transmit_channel_count
    if capabilities.licensed_redundancy_enabled is not None:
        fields["licensed_redundancy_enabled"] = capabilities.licensed_redundancy_enabled
    if capabilities.channel_capacities:
        fields["sample_rate_channel_capacities"] = [
            {
                "sample_rate_hertz": capacity.sample_rate_hertz,
                "receive_channel_count": capacity.receive_channel_count,
                "transmit_channel_count": capacity.transmit_channel_count,
            }
            for capacity in capabilities.channel_capacities
        ]
    return fields


def write_device_log_archive(output_path: Path, archive_payload: bytes) -> None:
    write_new_private_file(output_path, archive_payload)
