from functools import lru_cache

from netaudio.common.app_config import settings as app_settings
from netaudio.dante.clean_labels import (
    load_clean_labels,
    load_clean_subscription_status_labels,
)
from netaudio.dante.const import (
    ARC_PROTOCOL_IDS,
    BUILTIN_ARC_OPCODE_NAMES,
    BUILTIN_OPCODE_NAMES,
    PROTOCOL_NAMES,
    RESULT_CODE_NAMES,
)

__all__ = [
    "Colors",
    "PROTOCOL_NAMES",
    "RESULT_CODE_NAMES",
    "get_opcode_name",
    "get_settings_message_type_name",
    "get_string_at_offset",
    "get_subscription_status_name",
    "get_subscription_status_state",
]


class Colors:
    @property
    def RESET(self):
        return "" if app_settings.no_color else "\033[0m"

    @property
    def BOLD(self):
        return "" if app_settings.no_color else "\033[1m"

    @property
    def DIM(self):
        return "" if app_settings.no_color else "\033[2m"

    @property
    def RED(self):
        return "" if app_settings.no_color else "\033[31m"

    @property
    def GREEN(self):
        return "" if app_settings.no_color else "\033[32m"

    @property
    def YELLOW(self):
        return "" if app_settings.no_color else "\033[33m"

    @property
    def BLUE(self):
        return "" if app_settings.no_color else "\033[34m"

    @property
    def MAGENTA(self):
        return "" if app_settings.no_color else "\033[35m"

    @property
    def CYAN(self):
        return "" if app_settings.no_color else "\033[36m"

    @property
    def WHITE(self):
        return "" if app_settings.no_color else "\033[37m"


C = Colors()


@lru_cache(maxsize=1)
def _external_labels():
    return load_clean_labels()


@lru_cache(maxsize=1)
def _external_subscription_status_labels():
    return load_clean_subscription_status_labels()


def get_opcode_name(protocol, opcode):
    opcode_labels, _ = _external_labels()
    external_label = opcode_labels.get((protocol, opcode))
    if external_label:
        return external_label

    if protocol in ARC_PROTOCOL_IDS:
        for fallback_protocol in ARC_PROTOCOL_IDS:
            if fallback_protocol != protocol:
                external_label = opcode_labels.get((fallback_protocol, opcode))
                if external_label:
                    return f"{external_label} [0x{fallback_protocol:04X} label]"

        builtin_label = BUILTIN_OPCODE_NAMES.get((protocol, opcode)) or BUILTIN_ARC_OPCODE_NAMES.get(opcode)
        if builtin_label:
            return builtin_label

    return f"0x{opcode:04X}"


def get_settings_message_type_name(message_type):
    _, message_labels = _external_labels()
    external_label = message_labels.get(message_type)
    if external_label:
        return external_label

    return f"msg:0x{message_type:04X}"


def get_subscription_status_name(status_code):
    status_labels = _external_subscription_status_labels()
    entry = status_labels.get(status_code)
    if isinstance(entry, dict):
        label = entry.get("label")
        if isinstance(label, str) and label.strip():
            return label.strip()

        labels = entry.get("labels")
        if isinstance(labels, list):
            for value in labels:
                if isinstance(value, str) and value.strip():
                    return value.strip()

    from netaudio.dante.const import subscription_status_label

    return subscription_status_label(status_code)


def get_subscription_status_state(status_code):
    status_labels = _external_subscription_status_labels()
    entry = status_labels.get(status_code)
    if isinstance(entry, dict):
        state = entry.get("state")
        if isinstance(state, str) and state.strip():
            return state.strip()

    if status_code == 0:
        return "none"
    return "unknown"


def get_string_at_offset(data: bytes, offset: int) -> str:
    if offset >= len(data):
        return ""

    end = data.find(b"\x00", offset)

    if end == -1:
        end = len(data)

    return data[offset:end].decode("utf-8", errors="replace")
