from __future__ import annotations


from netaudio import core
from netaudio.core._requests import JsonValue


def canonical_ptpv1_uuid(value: object) -> str | None:
    supplied: JsonValue

    if isinstance(value, (bytes, bytearray, memoryview, tuple)):
        supplied = [item for item in value]
    elif isinstance(value, (str, list)):
        supplied = value
    else:
        return None

    try:
        return core.device_identity({"kind": "ptpv1", "value": supplied})
    except core.NetaudioCoreJsonError:
        return None
    except core.NetaudioCoreError as error:
        if error.category != "json_input":
            raise

        return None
