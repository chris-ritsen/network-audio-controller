from functools import lru_cache

from netaudio.dante.clean_labels import load_clean_labels


@lru_cache(maxsize=1)
def _external_labels():
    return load_clean_labels()


def opcode_label(header):
    if header["opcode_name"]:
        return header["opcode_name"]

    protocol = header["protocol_id"]
    opcode = header["opcode"]
    opcode_labels, _ = _external_labels()
    external_label = opcode_labels.get((protocol, opcode))
    if external_label:
        return external_label

    return "unknown"


def get_settings_message_type_name(message_type):
    _, message_labels = _external_labels()
    external_label = message_labels.get(message_type)
    if external_label:
        return external_label

    return f"msg:0x{message_type:04X}"
