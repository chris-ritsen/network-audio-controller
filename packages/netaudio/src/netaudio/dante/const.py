from netaudio.dante.clean_labels import load_clean_subscription_status_labels


SERVICE_ARC: str = "_netaudio-arc._udp.local."
SERVICE_CHAN: str = "_netaudio-chan._udp.local."
SERVICE_CMC: str = "_netaudio-cmc._udp.local."
SERVICE_DBC: str = "_netaudio-dbc._udp.local."

MULTICAST_GROUP_HEARTBEAT = "224.0.0.233"
MULTICAST_GROUP_CONTROL_MONITORING = "224.0.0.231"

SERVICES = [SERVICE_ARC, SERVICE_CHAN, SERVICE_CMC, SERVICE_DBC]

BLUETOOTH_MODEL_IDS = {"DIOBT"}

HEARTBEAT_LOCK_UNRELIABLE_MODEL_IDS = {"DIOBT"}

RESULT_CODE_LOCK_REJECTION = 0x0600

DANTE_CONTROLLER_METERING_PORT = 8751
DEFAULT_MULTICAST_METERING_PORT = 8752
DEVICE_ARC_PORT: int = 4440
DEVICE_CONTROL_PORT: int = 8800
DEVICE_HEARTBEAT_PORT: int = 8708
DEVICE_INFO_PORT: int = 8702
DEVICE_INFO_SRC_PORT1 = 1029
DEVICE_INFO_SRC_PORT2 = 1030
DEVICE_LOCK_PORT: int = 8002
DEVICE_SETTINGS_PORT: int = 8700
MESSAGE_TYPE_METERING_LEVELS = 0

PORTS = [DEVICE_ARC_PORT, DEVICE_CONTROL_PORT, DEVICE_INFO_PORT, DEVICE_SETTINGS_PORT]

DEVICE_SETTINGS_INFO_SAMPLE_RATE = 0x8020
DEVICE_SETTINGS_INFO_LATENCY = 0x8204

PROTOCOL_ID = 0x27FF

RESULT_CODE_SUCCESS = 0x0001
RESULT_CODE_SUCCESS_EXTENDED = 0x8112

OPCODE_CHANNEL_COUNT = 0x1000
OPCODE_DEVICE_NAME = 0x1002
OPCODE_TX_CHANNEL_INFO = 0x2000
OPCODE_RX_CHANNELS = 0x3000

FLOW_PROTOCOL_IDS = (0x2729, 0x2801, 0x2809)

OPCODE_QUERY_TX_FLOWS = 0x2200
OPCODE_CREATE_TX_FLOW = 0x2201
OPCODE_DELETE_TX_FLOW = 0x2202

OPCODE_QUERY_TX_FLOWS_2809 = 0x2600
OPCODE_CREATE_TX_FLOW_2809 = 0x2601
OPCODE_DELETE_TX_FLOW_2809 = 0x2602

FLOW_TYPE_MULTICAST = 0x0002

SUBSCRIPTION_STATUS_NONE = 0x0000

_CLEAN_SUBSCRIPTION_STATUS_LABELS = load_clean_subscription_status_labels()


def _default_status_entry(code: int) -> dict[str, object]:
    if code == SUBSCRIPTION_STATUS_NONE:
        return {
            "state": "none",
            "label": "status:none",
            "detail": None,
            "labels": ("status:none",),
        }
    return {
        "state": "unknown",
        "label": f"status:{code}",
        "detail": None,
        "labels": (f"status:{code}",),
    }


def _normalize_status_entry(code: int, entry: dict[str, object] | None) -> dict[str, object]:
    default = _default_status_entry(code)
    if not isinstance(entry, dict):
        return default

    state = entry.get("state")
    if isinstance(state, str) and state.strip():
        state = state.strip()
    else:
        state = default["state"]

    label = entry.get("label")
    if isinstance(label, str) and label.strip():
        label = label.strip()
    else:
        label = default["label"]

    detail = entry.get("detail")
    if not isinstance(detail, str) or not detail:
        detail = None

    labels_value = entry.get("labels")
    labels: tuple[str, ...] = ()
    if isinstance(labels_value, list):
        labels = tuple(value.strip() for value in labels_value if isinstance(value, str) and value.strip())
    if not labels:
        labels = (label,)

    return {
        "state": state,
        "label": label,
        "detail": detail,
        "labels": labels,
    }


def _load_status_catalog() -> dict[int, dict[str, object]]:
    catalog = {
        code: _normalize_status_entry(code, entry) for code, entry in sorted(_CLEAN_SUBSCRIPTION_STATUS_LABELS.items())
    }
    if SUBSCRIPTION_STATUS_NONE not in catalog:
        catalog[SUBSCRIPTION_STATUS_NONE] = _default_status_entry(SUBSCRIPTION_STATUS_NONE)
    return catalog


_SUBSCRIPTION_STATUS_CATALOG = _load_status_catalog()

SUBSCRIPTION_STATUSES = list(_SUBSCRIPTION_STATUS_CATALOG.keys())
SUBSCRIPTION_STATUS_ACTIVE = [
    code for code, entry in _SUBSCRIPTION_STATUS_CATALOG.items() if entry["state"] == "connected"
]
SUBSCRIPTION_STATUS_INFO = {
    code: (entry["state"], entry["label"], entry["detail"]) for code, entry in _SUBSCRIPTION_STATUS_CATALOG.items()
}
SUBSCRIPTION_STATUS_LABELS = {code: entry["labels"] for code, entry in _SUBSCRIPTION_STATUS_CATALOG.items()}
