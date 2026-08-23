from __future__ import annotations

from binascii import Error as HexadecimalDecodeError
from binascii import unhexlify
from dataclasses import dataclass


A32_LINK_STATUS_LABELS = {
    0: "selected_link",
    1: "switch_port_0",
    2: "switch_port_3",
}
A32_LINK_STATUS_POINTERS = (0x002C, 0x0044, 0x005C)
A32_LINK_STATUS_RECORD_SIZE_BYTES = 24


def _is_ferrofish_a32(device) -> bool:
    if device is None:
        return False
    model_values = (
        getattr(device, "dante_model", None),
        getattr(device, "model", None),
        getattr(device, "board_name", None),
    )
    return any(
        isinstance(value, str) and value.casefold().startswith("a32 dante ad/da converter") for value in model_values
    )


def _has_exact_a32_link_status_layout(record_pointers: tuple[int, ...], parsed_records: list[dict]) -> bool:
    return record_pointers == A32_LINK_STATUS_POINTERS and all(
        isinstance(record, dict) and record.get("record_size_bytes") == A32_LINK_STATUS_RECORD_SIZE_BYTES
        for record in parsed_records
    )


@dataclass(frozen=True)
class LinkStatusRecord:
    record_index: int
    record_pointer: int
    record_size_bytes: int
    label: str | None
    unmapped_prefix_words: tuple[int, int, int, int]
    raw_link_status_word: int
    link_up: bool
    link_speed_megabits_per_second: int
    unmapped_trailing_hexadecimal: str
    raw_record_hexadecimal: str

    def to_dict(self) -> dict:
        return {
            "record_index": self.record_index,
            "record_pointer": self.record_pointer,
            "record_size_bytes": self.record_size_bytes,
            "label": self.label,
            "unmapped_prefix_words": list(self.unmapped_prefix_words),
            "raw_link_status_word": self.raw_link_status_word,
            "link_up": self.link_up,
            "link_speed_megabits_per_second": self.link_speed_megabits_per_second,
            "unmapped_trailing_hexadecimal": self.unmapped_trailing_hexadecimal,
            "raw_record_hexadecimal": self.raw_record_hexadecimal,
        }


@dataclass(frozen=True)
class LinkStatusObservation:
    record_count: int
    record_pointers: tuple[int, ...]
    records: tuple[LinkStatusRecord, ...]

    @classmethod
    def from_core(cls, parsed: dict, device=None) -> LinkStatusObservation:
        record_count = parsed["record_count"]
        record_pointers = tuple(parsed["record_pointers"])
        parsed_records = parsed["records"]
        if record_count != len(record_pointers) or record_count != len(parsed_records):
            raise ValueError("link-status record count does not match the pointer table")

        labels = (
            A32_LINK_STATUS_LABELS
            if _is_ferrofish_a32(device) and _has_exact_a32_link_status_layout(record_pointers, parsed_records)
            else {}
        )
        records = []
        for record_index, parsed_record in enumerate(parsed_records):
            record_pointer = parsed_record["record_pointer"]
            if record_pointer != record_pointers[record_index]:
                raise ValueError("link-status record pointer does not match the pointer table")
            prefix_words = tuple(parsed_record["unmapped_prefix_words"])
            if len(prefix_words) != 4:
                raise ValueError("link-status common prefix must contain four words")
            record_size_bytes = parsed_record["record_size_bytes"]
            raw_record_hexadecimal = parsed_record["raw_record_hexadecimal"]
            try:
                decoded_record_size = len(unhexlify(raw_record_hexadecimal))
            except (HexadecimalDecodeError, ValueError) as exception:
                raise ValueError("link-status raw record is not valid hexadecimal") from exception
            if decoded_record_size != record_size_bytes:
                raise ValueError("link-status raw record length does not match its declared size")
            records.append(
                LinkStatusRecord(
                    record_index=record_index,
                    record_pointer=record_pointer,
                    record_size_bytes=record_size_bytes,
                    label=labels.get(record_index),
                    unmapped_prefix_words=prefix_words,
                    raw_link_status_word=parsed_record["raw_link_status_word"],
                    link_up=parsed_record["link_up"],
                    link_speed_megabits_per_second=parsed_record["link_speed_megabits_per_second"],
                    unmapped_trailing_hexadecimal=parsed_record["unmapped_trailing_hexadecimal"],
                    raw_record_hexadecimal=raw_record_hexadecimal,
                )
            )
        return cls(
            record_count=record_count,
            record_pointers=record_pointers,
            records=tuple(records),
        )

    def to_dict(self) -> dict:
        return {
            "record_count": self.record_count,
            "record_pointers": list(self.record_pointers),
            "records": [record.to_dict() for record in self.records],
        }
