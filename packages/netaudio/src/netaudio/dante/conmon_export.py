from __future__ import annotations

import hashlib
import os
import zlib
from dataclasses import dataclass
from pathlib import Path

from netaudio import core
from netaudio.core import _requests, _types


DEFAULT_MAXIMUM_CONMON_EXPORT_ENCODED_SIZE = 64 * 1024 * 1024


class ConmonExportError(ValueError):
    pass


class ConmonExportUnavailableError(ConmonExportError):
    pass


@dataclass(frozen=True)
class ConmonExport:
    kind: _types.ExportKind
    encoded_payload: bytes
    encoded_sha256: str
    fragment_count: int


class ConmonExportCollector:
    def __init__(
        self,
        kind: _requests.ExportKind,
        maximum_encoded_size: int = DEFAULT_MAXIMUM_CONMON_EXPORT_ENCODED_SIZE,
    ):
        self._collector = core.ConmonExportCollector(
            {
                "kind": kind,
                "maximum_encoded_size": maximum_encoded_size,
            }
        )
        self.matched = False

    def close(self):
        self._collector.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exception_information):
        self.close()

    def observe(self, fragment: _requests.ConmonExportFragment) -> ConmonExport | None:
        try:
            progress = self._collector.accept(fragment)
        except core.NetaudioCoreError as error:
            raise ConmonExportError(error.detail or str(error)) from error

        self.matched = progress["matched"]
        result = progress["result"]

        if result is None:
            return None

        payload = bytes.fromhex(result["encoded_payload_hexadecimal"])

        return ConmonExport(
            kind=result["kind"],
            encoded_payload=payload,
            encoded_sha256=hashlib.sha256(payload).hexdigest(),
            fragment_count=result["fragment_count"],
        )


def decode_bounded_gzip(encoded_payload: bytes, maximum_decoded_size: int) -> bytes:
    if maximum_decoded_size < 1:
        raise ValueError("maximum decoded size must be positive")
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        decoded_payload = decompressor.decompress(encoded_payload, maximum_decoded_size + 1)
        if len(decoded_payload) > maximum_decoded_size or decompressor.unconsumed_tail:
            raise ConmonExportError("ConMon export exceeds the decoded size limit")
        decoded_payload += decompressor.flush(maximum_decoded_size + 1 - len(decoded_payload))
    except zlib.error as exception:
        raise ConmonExportError("ConMon export is not a valid gzip stream") from exception
    if len(decoded_payload) > maximum_decoded_size:
        raise ConmonExportError("ConMon export exceeds the decoded size limit")
    if not decompressor.eof:
        raise ConmonExportError("ConMon export gzip stream is incomplete")
    if decompressor.unused_data:
        raise ConmonExportError("ConMon export gzip stream has trailing data")
    return decoded_payload


def write_new_private_file(output_path: Path, payload: bytes) -> None:
    descriptor = os.open(
        output_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("ConMon export write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
    except BaseException:
        os.close(descriptor)
        output_path.unlink(missing_ok=True)
        raise
    os.close(descriptor)
