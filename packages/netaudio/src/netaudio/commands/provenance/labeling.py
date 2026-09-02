from __future__ import annotations

import sqlite3

from netaudio.commands.capture.options import _valid_label


def _interactive_label_opcodes(
    rows: list[sqlite3.Row],
    opcode_labels: dict[tuple[int, int], str],
) -> bool:
    changed = False
    for index, row in enumerate(rows, start=1):
        protocol_id = int(row["protocol_id"])
        opcode = int(row["opcode"])
        seen = int(row["seen"])
        sample_id = int(row["sample_id"])
        key = (protocol_id, opcode)
        if key in opcode_labels:
            continue

        prompt = (
            f"[{index}/{len(rows)}] protocol=0x{protocol_id:04X} opcode=0x{opcode:04X} "
            f"seen={seen} sample_id={sample_id}\n"
            "label (blank=skip, q=quit): "
        )
        label = input(prompt).strip()
        if not label:
            continue
        if label.lower() in {"q", "quit", "exit"}:
            return changed
        if not _valid_label(label):
            print("invalid label, skipping")
            continue

        opcode_labels[key] = label
        changed = True
    return changed


def _interactive_label_messages(
    rows: list[sqlite3.Row],
    message_labels: dict[int, str],
) -> bool:
    changed = False
    for index, row in enumerate(rows, start=1):
        message_type = int(row["message_type"])
        seen = int(row["seen"])
        sample_id = int(row["sample_id"])
        if message_type in message_labels:
            continue

        prompt = (
            f"[{index}/{len(rows)}] message_type=0x{message_type:04X} "
            f"seen={seen} sample_id={sample_id}\n"
            "label (blank=skip, q=quit): "
        )
        label = input(prompt).strip()
        if not label:
            continue
        if label.lower() in {"q", "quit", "exit"}:
            return changed
        if not _valid_label(label):
            print("invalid label, skipping")
            continue

        message_labels[message_type] = label
        changed = True
    return changed


def _interactive_label_statuses(
    rows: list[dict[str, int]],
    status_labels: dict[int, dict[str, object]],
) -> bool:
    changed = False
    for index, row in enumerate(rows, start=1):
        status_code = int(row["status_code"])
        seen = int(row["seen"])
        sample_id = int(row["sample_id"])
        if status_code in status_labels:
            continue

        prompt = (
            f"[{index}/{len(rows)}] subscription_status=0x{status_code:04X} ({status_code}) "
            f"seen={seen} sample_id={sample_id}\n"
            "state,label (blank=skip, q=quit): "
        )
        value = input(prompt).strip()
        if not value:
            continue
        if value.lower() in {"q", "quit", "exit"}:
            return changed

        if "," in value:
            state, label = value.split(",", 1)
            state = state.strip() or "unknown"
            label = label.strip()
        else:
            state = "unknown"
            label = value

        if not _valid_label(label):
            print("invalid status label, skipping")
            continue

        status_labels[status_code] = {
            "state": state,
            "label": label,
            "detail": None,
            "labels": [label],
        }
        changed = True
    return changed
