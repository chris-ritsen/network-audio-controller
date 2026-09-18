from __future__ import annotations


PTPV1_UUID_SIZE_BYTES = 6


def canonical_ptpv1_uuid(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        compact_value = value.replace(":", "").replace("-", "").lower()
        if len(compact_value) != PTPV1_UUID_SIZE_BYTES * 2 or any(
            character not in "0123456789abcdef" for character in compact_value
        ):
            return None
        return compact_value
    elif isinstance(value, (bytes, bytearray, memoryview)):
        identity_bytes = bytes(value)
    elif isinstance(value, (list, tuple)):
        if len(value) != PTPV1_UUID_SIZE_BYTES or any(
            type(item) is not int or item < 0 or item > 255 for item in value
        ):
            return None
        identity_bytes = bytes(value)
    else:
        return None
    if len(identity_bytes) != PTPV1_UUID_SIZE_BYTES:
        return None
    return identity_bytes.hex()
