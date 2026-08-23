from __future__ import annotations

import binascii
import hashlib
import os
import zlib
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_MAXIMUM_CONMON_EXPORT_ENCODED_SIZE = 64 * 1024 * 1024


class ConmonExportError(ValueError):
    pass


class ConmonExportUnavailableError(ConmonExportError):
    pass


@dataclass(frozen=True)
class ConmonExport:
    echoed_tag: bytes
    selector_value: int
    record_protocol_identifier: int
    encoded_payload: bytes
    encoded_sha256: str
    fragment_count: int


@dataclass
class ConmonExportCollector:
    expected_echoed_tag: bytes
    expected_selector_value: int
    maximum_encoded_size: int = DEFAULT_MAXIMUM_CONMON_EXPORT_ENCODED_SIZE
    total_encoded_size: int | None = None
    record_protocol_identifier: int | None = None
    terminal_fragment_identifier: int | None = None
    received_encoded_size: int = 0
    fragments: dict[int, tuple[bytes, bool]] = field(default_factory=dict)
    result: ConmonExport | None = None

    def __post_init__(self) -> None:
        if len(self.expected_echoed_tag) != 4:
            raise ValueError("ConMon export tags must contain exactly four bytes")
        if not 0 <= self.expected_selector_value <= 65_535:
            raise ValueError("ConMon export selectors must fit in an unsigned 16-bit field")
        if self.maximum_encoded_size < 1:
            raise ValueError("maximum encoded size must be positive")

    def matches(self, fragment: dict) -> bool:
        return (
            fragment.get("echoed_tag_hexadecimal") == self.expected_echoed_tag.hex()
            and fragment.get("selector_value") == self.expected_selector_value
        )

    def observe(self, fragment: dict) -> ConmonExport | None:
        if self.result is not None:
            return self.result
        if not self.matches(fragment):
            raise ConmonExportError("fragment does not match the active ConMon export")
        total_encoded_size = fragment.get("total_encoded_size")
        record_protocol_identifier = fragment.get("record_protocol_identifier")
        fragment_identifier = fragment.get("fragment_identifier")
        has_more_fragments = fragment.get("has_more_fragments")
        fragment_size = fragment.get("fragment_size")
        data_hexadecimal = fragment.get("data_hexadecimal")
        if (
            isinstance(total_encoded_size, bool)
            or not isinstance(total_encoded_size, int)
            or not 1 <= total_encoded_size <= self.maximum_encoded_size
            or isinstance(record_protocol_identifier, bool)
            or not isinstance(record_protocol_identifier, int)
            or not 0 <= record_protocol_identifier <= 65_535
            or isinstance(fragment_identifier, bool)
            or not isinstance(fragment_identifier, int)
            or not 1 <= fragment_identifier <= 65_535
            or not isinstance(has_more_fragments, bool)
            or isinstance(fragment_size, bool)
            or not isinstance(fragment_size, int)
            or not 1 <= fragment_size <= total_encoded_size
            or not isinstance(data_hexadecimal, str)
        ):
            raise ConmonExportError("ConMon export fragment fields are invalid")
        try:
            fragment_data = binascii.unhexlify(data_hexadecimal)
        except (binascii.Error, ValueError) as exception:
            raise ConmonExportError("ConMon export fragment data is invalid") from exception
        if len(fragment_data) != fragment_size:
            raise ConmonExportError("ConMon export fragment size is invalid")
        if self.total_encoded_size is None:
            self.total_encoded_size = total_encoded_size
        elif self.total_encoded_size != total_encoded_size:
            raise ConmonExportError("ConMon export total encoded size changed")
        if self.record_protocol_identifier is None:
            self.record_protocol_identifier = record_protocol_identifier
        elif self.record_protocol_identifier != record_protocol_identifier:
            raise ConmonExportError("ConMon export record protocol identifier changed")
        existing = self.fragments.get(fragment_identifier)
        current = (fragment_data, has_more_fragments)
        if existing is not None:
            if existing != current:
                raise ConmonExportError("ConMon export fragment identifier conflicts")
            return self._complete_if_ready()
        if self.terminal_fragment_identifier is not None and fragment_identifier > self.terminal_fragment_identifier:
            raise ConmonExportError("ConMon export fragment follows the terminal fragment")
        if not has_more_fragments:
            if (
                self.terminal_fragment_identifier is not None
                and self.terminal_fragment_identifier != fragment_identifier
            ):
                raise ConmonExportError("ConMon export has multiple terminal fragments")
            if any(identifier > fragment_identifier for identifier in self.fragments):
                raise ConmonExportError("ConMon export fragment follows the terminal fragment")
            self.terminal_fragment_identifier = fragment_identifier
        if self.received_encoded_size + len(fragment_data) > total_encoded_size:
            raise ConmonExportError("ConMon export fragments exceed the declared encoded size")
        self.fragments[fragment_identifier] = current
        self.received_encoded_size += len(fragment_data)
        return self._complete_if_ready()

    def _complete_if_ready(self) -> ConmonExport | None:
        terminal = self.terminal_fragment_identifier
        if terminal is None or any(identifier not in self.fragments for identifier in range(1, terminal + 1)):
            return None
        if any(not self.fragments[identifier][1] for identifier in range(1, terminal)):
            raise ConmonExportError("ConMon export continuation sequence is invalid")
        if self.fragments[terminal][1]:
            raise ConmonExportError("ConMon export terminal fragment is marked for continuation")
        encoded_payload = b"".join(self.fragments[identifier][0] for identifier in range(1, terminal + 1))
        if len(encoded_payload) != self.total_encoded_size:
            raise ConmonExportError("ConMon export fragments do not cover the declared encoded size")
        if self.record_protocol_identifier is None:
            raise ConmonExportError("ConMon export has no record protocol identifier")
        self.result = ConmonExport(
            echoed_tag=self.expected_echoed_tag,
            selector_value=self.expected_selector_value,
            record_protocol_identifier=self.record_protocol_identifier,
            encoded_payload=encoded_payload,
            encoded_sha256=hashlib.sha256(encoded_payload).hexdigest(),
            fragment_count=terminal,
        )
        return self.result


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
