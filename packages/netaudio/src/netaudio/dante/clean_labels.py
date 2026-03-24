from __future__ import annotations

import json
import os
from pathlib import Path

ENV_LABELS_PATH = "NETAUDIO_LABELS_PATH"


def _parse_u16(value) -> int:
    if isinstance(value, int):
        return value & 0xFFFF

    if isinstance(value, str):
        token = value.strip()
        if token.lower().startswith("0x"):
            return int(token, 16) & 0xFFFF
        return int(token, 10) & 0xFFFF

    raise ValueError(f"unsupported numeric value: {value!r}")


def _default_labels_path() -> Path:
    env_path = os.environ.get(ENV_LABELS_PATH)
    if env_path:
        return Path(env_path).expanduser()

    cwd = Path.cwd()
    if (cwd / "pyproject.toml").exists() and (cwd / "tests" / "fixtures").exists():
        return cwd / "tests" / "fixtures" / "provenance" / "labels.json"

    return Path.home() / ".local" / "share" / "netaudio" / "clean_labels.json"


def resolve_clean_labels_path(path: str | Path | None = None) -> Path:
    if path is None:
        return _default_labels_path()
    return Path(path).expanduser()


def load_clean_labels(
    path: str | Path | None = None,
) -> tuple[dict[tuple[int, int], str], dict[int, str]]:
    resolved = resolve_clean_labels_path(path)
    if not resolved.exists():
        return {}, {}

    try:
        data = json.loads(resolved.read_text())
    except Exception:
        return {}, {}

    opcode_labels: dict[tuple[int, int], str] = {}
    message_labels: dict[int, str] = {}

    opcode_section = data.get("opcode_labels", {})
    if isinstance(opcode_section, dict):
        for key, label in opcode_section.items():
            if not isinstance(label, str) or not label.strip():
                continue

            try:
                protocol_token, opcode_token = str(key).split(":", 1)
                protocol_id = _parse_u16(protocol_token)
                opcode = _parse_u16(opcode_token)
            except Exception:
                continue

            opcode_labels[(protocol_id, opcode)] = label.strip()
    elif isinstance(opcode_section, list):
        for entry in opcode_section:
            if not isinstance(entry, dict):
                continue

            label = entry.get("label")
            if not isinstance(label, str) or not label.strip():
                continue

            try:
                protocol_id = _parse_u16(entry.get("protocol"))
                opcode = _parse_u16(entry.get("opcode"))
            except Exception:
                continue

            opcode_labels[(protocol_id, opcode)] = label.strip()

    message_section = data.get("message_labels", {})
    if isinstance(message_section, dict):
        for key, label in message_section.items():
            if not isinstance(label, str) or not label.strip():
                continue

            try:
                message_type = _parse_u16(key)
            except Exception:
                continue

            message_labels[message_type] = label.strip()
    elif isinstance(message_section, list):
        for entry in message_section:
            if not isinstance(entry, dict):
                continue

            label = entry.get("label")
            if not isinstance(label, str) or not label.strip():
                continue

            try:
                message_type = _parse_u16(entry.get("message_type"))
            except Exception:
                continue

            message_labels[message_type] = label.strip()

    return opcode_labels, message_labels


def load_clean_subscription_status_labels(
    path: str | Path | None = None,
) -> dict[int, dict[str, object]]:
    resolved = resolve_clean_labels_path(path)
    if not resolved.exists():
        return {}

    try:
        data = json.loads(resolved.read_text())
    except Exception:
        return {}

    status_section = data.get("subscription_status_labels", {})
    if not isinstance(status_section, dict):
        return {}

    result: dict[int, dict[str, object]] = {}
    for key, entry in status_section.items():
        try:
            code = _parse_u16(key)
        except Exception:
            continue

        if isinstance(entry, str):
            label = entry.strip()
            if not label:
                continue
            result[code] = {
                "state": "unknown",
                "label": label,
                "detail": None,
                "labels": [label],
            }
            continue

        if not isinstance(entry, dict):
            continue

        state = entry.get("state")
        if not isinstance(state, str) or not state.strip():
            state = "unknown"
        else:
            state = state.strip()

        detail = entry.get("detail")
        if detail is not None and not isinstance(detail, str):
            detail = None

        labels: list[str] = []
        raw_labels = entry.get("labels")
        if isinstance(raw_labels, list):
            for value in raw_labels:
                if isinstance(value, str) and value.strip():
                    labels.append(value.strip())

        label = entry.get("label")
        if isinstance(label, str) and label.strip():
            canonical_label = label.strip()
        elif labels:
            canonical_label = labels[0]
        else:
            canonical_label = f"status:{code}"

        if not labels:
            labels = [canonical_label]

        result[code] = {
            "state": state,
            "label": canonical_label,
            "detail": detail,
            "labels": labels,
        }

    return result


def save_clean_labels(
    opcode_labels: dict[tuple[int, int], str],
    message_labels: dict[int, str],
    subscription_status_labels: dict[int, dict[str, object]] | None = None,
    path: str | Path | None = None,
) -> Path:
    resolved = resolve_clean_labels_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)

    opcode_section = {
        f"0x{protocol_id:04X}:0x{opcode:04X}": label
        for (protocol_id, opcode), label in sorted(opcode_labels.items())
        if isinstance(label, str) and label.strip()
    }
    message_section = {
        f"0x{message_type:04X}": label
        for message_type, label in sorted(message_labels.items())
        if isinstance(label, str) and label.strip()
    }

    if subscription_status_labels is None:
        subscription_status_labels = load_clean_subscription_status_labels(resolved)

    status_section = {}
    for code, entry in sorted(subscription_status_labels.items()):
        if not isinstance(entry, dict):
            continue

        state = entry.get("state")
        if not isinstance(state, str) or not state.strip():
            state = "unknown"
        else:
            state = state.strip()

        detail = entry.get("detail")
        if detail is not None and not isinstance(detail, str):
            detail = None

        labels: list[str] = []
        raw_labels = entry.get("labels")
        if isinstance(raw_labels, list):
            for value in raw_labels:
                if isinstance(value, str) and value.strip():
                    labels.append(value.strip())

        label = entry.get("label")
        if isinstance(label, str) and label.strip():
            canonical_label = label.strip()
        elif labels:
            canonical_label = labels[0]
        else:
            continue

        if not labels:
            labels = [canonical_label]

        status_section[f"0x{code:04X}"] = {
            "state": state,
            "label": canonical_label,
            "detail": detail,
            "labels": labels,
        }

    payload = {
        "opcode_labels": opcode_section,
        "message_labels": message_section,
        "subscription_status_labels": status_section,
    }

    resolved.write_text(json.dumps(payload, indent=2) + "\n")
    return resolved
