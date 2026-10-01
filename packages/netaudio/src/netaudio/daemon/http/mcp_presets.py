from __future__ import annotations

from typing import Any

from netaudio.daemon.http.mcp_schema import close_matches
from netaudio.daemon.http.mcp_views import availability_verdict, channel_label
from netaudio.presets.parsing import parse_preset_xml
from netaudio.presets.storage import (
    list_presets,
    preset_directory,
    preset_reference,
    stored_preset_path,
    write_preset_atomic,
)

PRESET_TOOLS = frozenset({"list_presets", "save_preset", "preview_preset", "apply_preset"})
DEFAULT_PRESET_SECTIONS = ["audio", "routing"]
PRESET_NEXT_STEP = (
    "Nothing has changed. Tell the user which devices this changes and how; after they agree, call again with the "
    "same arguments and confirmed=true."
)


def _record_key(records: dict, selector) -> str:
    if not isinstance(selector, str) or not selector:
        raise ValueError("devices must be named")
    folded = selector.casefold()
    matches = [
        key
        for key, record in records.items()
        if isinstance(record, dict)
        and any(
            isinstance(value, str) and value.casefold() == folded
            for value in (key, record.get("server_name"), record.get("name"), record.get("inventory_id"))
        )
    ]
    if len(matches) > 1:
        raise ValueError(f"device {selector!r} is ambiguous; use its inventory ID")
    if matches:
        return matches[0]
    names = sorted(str(record.get("name")) for record in records.values() if isinstance(record, dict))
    suggestions = close_matches(selector, names)
    hint = f"; did you mean {' or '.join(suggestions)}?" if suggestions else ""
    raise ValueError(f"no device named {selector!r} is on the network{hint}")


def _device_name(records: dict, key: str) -> str:
    record = records.get(key)
    return str(record.get("name") or key) if isinstance(record, dict) else key


def _settings(entry: dict) -> list[str]:
    return [
        f"{item.get('label')}: {item.get('value')}" for item in entry.get("settings") or [] if isinstance(item, dict)
    ]


def _assignments(preview: dict, arguments: dict, records: dict) -> tuple[dict, list[dict], list[str]]:
    entries = {entry["name"]: entry for entry in preview.get("devices") or [] if isinstance(entry, dict)}
    requested = arguments.get("targets") or {}
    skip = set(arguments.get("skip") or [])
    for name in sorted(set(requested) | skip):
        if name not in entries:
            suggestions = close_matches(name, entries)
            hint = (
                f"did you mean {' or '.join(suggestions)}?"
                if suggestions
                else f"its devices are {', '.join(sorted(entries))}"
            )
            raise ValueError(f"the preset has no device named {name!r}; {hint}")
    overlap = sorted(set(requested) & skip)
    if overlap:
        raise ValueError(f"{', '.join(overlap)} cannot be both targeted and skipped")
    targets: dict[str, str] = {}
    skipped: list[dict] = []
    warnings: list[str] = []
    for name, entry in sorted(entries.items()):
        candidates = [target for target in entry.get("targets") or [] if isinstance(target, dict)]
        if name in skip:
            skipped.append({"preset_device": name, "reason": "skipped as requested"})
            continue
        if name in requested:
            key = _record_key(records, requested[name])
            if all(target.get("id") != key for target in candidates):
                matching = ", ".join(str(target.get("name")) for target in candidates) or "none"
                raise ValueError(
                    f"{_device_name(records, key)} does not match preset device {name}; a preset device applies only "
                    f"to a device with the same name or identity (matching devices: {matching})"
                )
            targets[name] = key
            continue
        if len(candidates) == 1:
            targets[name] = candidates[0]["id"]
            continue
        reason = (
            "no device on the network matches it"
            if not candidates
            else f"{len(candidates)} devices match it ({', '.join(str(target.get('name')) for target in candidates)}); name one in targets"
        )
        skipped.append({"preset_device": name, "reason": reason})
    return targets, skipped, warnings


def _preset_xml(arguments: dict) -> str:
    if arguments.get("preset") and arguments.get("xml"):
        raise ValueError("give either preset or xml, not both")
    if arguments.get("preset"):
        path = stored_preset_path(arguments["preset"])
        if not path.exists():
            presets, _ = list_presets()
            names = [entry["preset"] for entry in presets]
            suggestions = close_matches(arguments["preset"], names)
            hint = (
                f"did you mean {' or '.join(suggestions)}?"
                if suggestions
                else f"saved presets: {', '.join(names) or 'none'}"
            )
            raise ValueError(f"there is no saved preset {arguments['preset']!r}; {hint}")
        return path.read_text(encoding="utf-8")
    if arguments.get("xml"):
        return arguments["xml"]
    raise ValueError("give preset, the name of a saved preset, or xml")


PRESET_SCALARS = (
    ("device_name", "name", "name"),
    ("sample_rate", "sample_rate_hz", "sample rate"),
    ("encoding", "encoding", "encoding"),
    ("latency", "latency_ms", "latency"),
    ("preferred_leader", "preferred_leader", "preferred leader"),
)
COMPARED_PRESET_FIELDS = {field for field, _, _ in PRESET_SCALARS} | {
    "name",
    "device_identity",
    "receiver_channel_names",
    "transmitter_channel_names",
    "rx_subscriptions",
}


PERFORMANCE_PRESET_FIELDS = (
    "receive_flow_performance",
    "transmit_flow_performance",
    "unicast_performance",
    "receive_flow_default_slots",
)


def preset_blockers(config: dict, record: dict) -> list[str]:
    availability = record.get("performance_operation_availability") or {}
    blocked = []
    for field in PERFORMANCE_PRESET_FIELDS:
        state = availability.get(field)
        if field not in config or not isinstance(state, dict):
            continue
        verdict, reasons = availability_verdict(field, state)
        if verdict == "refused":
            blocked.append(f"{field.replace('_', ' ')} ({reasons})")
    if not blocked:
        return []
    return [
        f"{record.get('name')} cannot take {', '.join(blocked)}; loading stops at the first setting that fails, so "
        "later settings such as channel labels and routing would not be sent. A preset saved with sections "
        "['routing'] leaves these settings out."
    ]


def _labels(record: dict, direction: str) -> dict[str, str]:
    channels = ((record.get("channels") or {}).get(direction)) or {}
    return {str(number): channel_label(value) for number, value in channels.items()}


def _current_routes(record: dict) -> dict[str, str]:
    routes = {}
    for subscription in record.get("subscriptions") or []:
        if not isinstance(subscription, dict) or not subscription.get("tx_device"):
            continue
        status = subscription.get("status") if isinstance(subscription.get("status"), dict) else {}
        if status.get("state") == "none":
            continue
        routes[str(subscription.get("rx_channel_number"))] = (
            f"{subscription['tx_device']}:{subscription.get('tx_channel')}"
        )
    return routes


def preset_changes(config: dict, record: dict) -> dict:
    changes = []
    for field, record_field, label in PRESET_SCALARS:
        if field in config and config[field] != record.get(record_field):
            changes.append(f"{label}: {record.get(record_field)} -> {config[field]}")
    for field, direction, kind in (
        ("receiver_channel_names", "receivers", "rx"),
        ("transmitter_channel_names", "transmitters", "tx"),
    ):
        current = _labels(record, direction)
        for number, name in sorted((config.get(field) or {}).items(), key=lambda item: int(item[0])):
            if current.get(str(number)) != name:
                changes.append(f"{kind} {number} label: {current.get(str(number))} -> {name}")
    if "rx_subscriptions" in config:
        current = _current_routes(record)
        labels = _labels(record, "receivers")
        wanted = {}
        for number, route in (config.get("rx_subscriptions") or {}).items():
            if isinstance(route, dict) and route.get("tx_device"):
                wanted[str(number)] = f"{route['tx_device']}:{route.get('tx_channel')}"
        for number in sorted(set(current) | set(wanted), key=int):
            if current.get(number) != wanted.get(number):
                changes.append(
                    f"rx {number} ({labels.get(number)}) route: {current.get(number) or 'nothing'} -> "
                    f"{wanted.get(number) or 'nothing'}"
                )
    unchecked = sorted(field for field in config if field not in COMPARED_PRESET_FIELDS)
    return {"changes": changes or ["nothing compared differs from the device now"], "also_sends": unchecked or None}


class McpPresetTools:
    async def _save_preset(self, arguments: dict, records: dict, writer) -> tuple[int, Any]:
        reference = preset_reference(arguments["name"])
        path = stored_preset_path(reference)
        if path.exists() and not arguments.get("replace"):
            return 409, {
                "error": f"a preset named {reference!r} is already saved; choose another name or set replace=true"
            }
        request = {
            "name": arguments["name"],
            "devices": [_record_key(records, selector) for selector in arguments["devices"]],
            "sections": arguments.get("sections") or DEFAULT_PRESET_SECTIONS,
        }
        status, payload = await self._post_captured("/presets/save", request, writer)
        if status >= 400 or not isinstance(payload, dict):
            return status, payload
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            write_preset_atomic(path, payload["xml"], force=bool(arguments.get("replace")))
        except OSError as exception:
            return 500, {"error": f"the preset was read but could not be saved to {path}: {exception}"}
        return status, {
            "preset": reference,
            "name": payload.get("name"),
            "path": str(path),
            "devices": [_device_name(records, key) for key in request["devices"]],
            "sections": request["sections"],
            "saved_at": payload.get("saved_at"),
            "next": f"preview_preset with preset={reference!r} lists what it holds; apply_preset with the same restores it.",
        }

    async def _preset_tool(self, name: str, arguments: dict, writer) -> tuple[int, Any]:
        records = self._serialized_devices()
        if name == "list_presets":
            presets, problems = list_presets()
            return 200, {"directory": str(preset_directory()), "presets": presets, "unreadable": problems or None}
        if name == "save_preset":
            return await self._save_preset(arguments, records, writer)
        xml = _preset_xml(arguments)
        keys = (
            [_record_key(records, selector) for selector in arguments["devices"]]
            if arguments.get("devices")
            else [key for key, record in records.items() if isinstance(record, dict)]
        )
        status, preview = await self._post_captured("/presets/preview", {"xml": xml, "devices": keys}, writer)
        if status >= 400 or not isinstance(preview, dict):
            return status, preview
        if arguments.get("digest") and arguments["digest"] != preview.get("digest"):
            return 409, {"error": "this preset XML does not match the digest; preview it again"}
        if name == "preview_preset":
            return status, {
                "preset": preview.get("name"),
                "digest": preview.get("digest"),
                "devices": [
                    {
                        "preset_device": entry.get("name"),
                        "settings": _settings(entry),
                        "preserved": entry.get("preserved"),
                        "matches": [target.get("name") for target in entry.get("targets") or []],
                    }
                    for entry in preview.get("devices") or []
                ],
            }
        targets, skipped, warnings = _assignments(preview, arguments, records)
        _, configs = parse_preset_xml(xml)
        for preset_device, key in sorted(targets.items()):
            warnings.extend(preset_blockers(configs.get(preset_device) or {}, records.get(key) or {}))
        if not targets:
            return 409, {"error": "no preset device has a matching device on the network", "skipped": skipped}
        if arguments.get("confirmed") is not True:
            result = {
                "changed": False,
                "confirmation_required": True,
                "operation": "apply_preset",
                "preset": preview.get("name"),
                "apply": [
                    {
                        "preset_device": preset_device,
                        "to": _device_name(records, key),
                        **preset_changes(configs.get(preset_device) or {}, records.get(key) or {}),
                    }
                    for preset_device, key in sorted(targets.items())
                ],
                "skipped": skipped or None,
                "warnings": warnings or None,
                "next": PRESET_NEXT_STEP,
            }
            return 200, result
        request = {
            "xml": xml,
            "digest": preview.get("digest"),
            "confirmed": True,
            "targets": targets,
            "excluded": [entry["preset_device"] for entry in skipped],
            "confirm_destructive": arguments.get("confirm_destructive", False),
            "store_current_configuration": arguments.get("store_current_configuration", False),
        }
        status, payload = await self._post_captured("/presets/load", request, writer)
        if isinstance(payload, dict):
            payload = {**payload, "applied_to": {key: _device_name(records, value) for key, value in targets.items()}}
            if skipped:
                payload["skipped"] = skipped
        return status, payload
