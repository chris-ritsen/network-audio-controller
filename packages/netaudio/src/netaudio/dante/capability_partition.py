from __future__ import annotations

import hashlib
import io
import tarfile
from dataclasses import dataclass
from pathlib import Path

from netaudio.dante.conmon_export import (
    ConmonExport,
    ConmonExportError,
    decode_bounded_gzip,
    write_new_private_file,
)


DEFAULT_MAXIMUM_CAPABILITY_ARCHIVE_SIZE = 16 * 1024 * 1024
DEFAULT_MAXIMUM_CAPABILITY_PARTITION_SIZE = 16 * 1024 * 1024
CAPABILITY_PARTITION_MEMBER_NAME = "tmp/dante_data/capability.bin"


class CapabilityPartitionExportError(ConmonExportError):
    pass


@dataclass(frozen=True)
class CapabilityPartitionExport:
    encoded_payload: bytes
    archive_payload: bytes
    capability_partition: bytes
    encoded_sha256: str
    archive_sha256: str
    capability_partition_sha256: str
    fragment_count: int


def parse_capability_partition_export(
    export: ConmonExport,
    maximum_archive_size: int = DEFAULT_MAXIMUM_CAPABILITY_ARCHIVE_SIZE,
    maximum_partition_size: int = DEFAULT_MAXIMUM_CAPABILITY_PARTITION_SIZE,
) -> CapabilityPartitionExport:
    if export.echoed_tag != b"CAP1" or export.selector_value != 2:
        raise CapabilityPartitionExportError("response is not a CAP1 partition export")
    try:
        archive_payload = decode_bounded_gzip(export.encoded_payload, maximum_archive_size)
    except ConmonExportError as exception:
        raise CapabilityPartitionExportError(str(exception)) from exception
    capability_partition = _read_capability_partition(archive_payload, maximum_partition_size)
    return CapabilityPartitionExport(
        encoded_payload=export.encoded_payload,
        archive_payload=archive_payload,
        capability_partition=capability_partition,
        encoded_sha256=export.encoded_sha256,
        archive_sha256=hashlib.sha256(archive_payload).hexdigest(),
        capability_partition_sha256=hashlib.sha256(capability_partition).hexdigest(),
        fragment_count=export.fragment_count,
    )


def _read_capability_partition(archive_payload: bytes, maximum_partition_size: int) -> bytes:
    if maximum_partition_size < 1:
        raise ValueError("maximum capability partition size must be positive")
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_payload), mode="r:") as archive:
            matching_members = [
                member for member in archive.getmembers() if member.name == CAPABILITY_PARTITION_MEMBER_NAME
            ]
            if len(matching_members) != 1 or not matching_members[0].isfile():
                raise CapabilityPartitionExportError(
                    f"CAP1 archive must contain one regular {CAPABILITY_PARTITION_MEMBER_NAME} member"
                )
            member = matching_members[0]
            if not 1 <= member.size <= maximum_partition_size:
                raise CapabilityPartitionExportError("CAP1 partition size is outside the allowed range")
            member_file = archive.extractfile(member)
            if member_file is None:
                raise CapabilityPartitionExportError("CAP1 partition member cannot be read")
            payload = member_file.read(maximum_partition_size + 1)
    except CapabilityPartitionExportError:
        raise
    except (tarfile.TarError, OSError) as exception:
        raise CapabilityPartitionExportError("CAP1 payload does not contain a valid tar archive") from exception
    if len(payload) != member.size:
        raise CapabilityPartitionExportError("CAP1 partition length does not match its archive record")
    return payload


def write_capability_partition(output_path: Path, capability_partition: bytes) -> None:
    write_new_private_file(output_path, capability_partition)
