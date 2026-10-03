from __future__ import annotations

import difflib
import json
import re
from datetime import datetime, timezone
from typing import Any

from netaudio.dante.device_serializer import DEVICE_SCALAR_FIELDS
from netaudio.dante.metering import normalize_metering_value

DEVICE_SECTIONS = ("availability", "channels", "flows", "full", "network", "subscriptions", "summary")
DEVICE_SUMMARY_FIELDS = (
    "encoding",
    "inventory_id",
    "ipv4",
    "is_locked",
    "kind",
    "latency_ms",
    "manufacturer",
    "model",
    "name",
    "online",
    "rx_count",
    "sample_rate_hz",
    "tx_count",
)
DEVICE_NETWORK_FIELDS = ("interfaces", "link_speed_mbps", "mac_address", "network_redundancy")
EVENT_FIELDS = (
    "channel_identity",
    "device_name",
    "flow_identity",
    "interface_identity",
    "kind",
    "sequence",
    "severity",
    "timestamp",
)
OPERATION_EVENT_KINDS = frozenset({"configuration_operation", "preset_run"})
CHANGE_EVENT_KINDS = frozenset({"setting_changed", "channel_renamed", "route_changed"})
ISSUE_FIELDS = (
    "first_seen",
    "issue_id",
    "kind",
    "last_seen",
    "resolved_at",
    "severity",
    "state",
    "summary",
    "title",
)
PROBLEM_SEVERITIES = {"error", "warning"}
FIND_LIMIT = 60
UNCONFIRMED_REASONS = frozenset({"capability_unknown", "lock_state_unknown", "update_mode_unknown"})
FULL_FIELD_LIMIT = 16000


def compact(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: compact(item) for key, item in value.items() if key != "icon" and item is not None and item != ""}
    if isinstance(value, list):
        return [compact(item) for item in value]
    return value


def _is_route(subscription: dict) -> bool:
    if not subscription.get("tx_device"):
        return False
    status = subscription.get("status")
    return not (isinstance(status, dict) and status.get("state") == "none")


def _subscription_list(device: dict) -> list[dict]:
    subscriptions = device.get("subscriptions") or []
    if isinstance(subscriptions, dict):
        subscriptions = list(subscriptions.values())
    return [
        subscription for subscription in subscriptions if isinstance(subscription, dict) and _is_route(subscription)
    ]


def _subscription_line(subscription: dict) -> str:
    number = subscription.get("rx_channel_number")
    receiver = f"rx {number} {subscription.get('rx_channel')}" if number is not None else subscription.get("rx_channel")
    return f"{receiver} \u2190 {subscription.get('tx_channel')} on {subscription.get('tx_device')}"


def _subscription_view(subscription: dict) -> dict:
    status = subscription.get("status") if isinstance(subscription.get("status"), dict) else {}
    view = {
        "rx_channel": {"number": subscription.get("rx_channel_number"), "name": subscription.get("rx_channel")},
        "status": status.get("label"),
        "tx_channel": subscription.get("tx_channel"),
        "tx_device": subscription.get("tx_device"),
    }
    if status.get("severity") in PROBLEM_SEVERITIES:
        view["problem"] = status.get("detail")
    return view


def _subscription_problems(device: dict, records: dict[str, dict] | None = None) -> tuple[list[str], list[str]]:
    problems = []
    waiting: dict[str, list] = {}
    for subscription in _subscription_list(device):
        status = subscription.get("status")
        if not isinstance(status, dict) or status.get("severity") not in PROBLEM_SEVERITIES:
            continue
        source = subscription.get("tx_device")
        wait = waiting_text(str(source), records) if records is not None and source else None
        if wait:
            number = subscription.get("rx_channel_number")
            waiting.setdefault(wait, []).append(number if isinstance(number, int) else subscription.get("rx_channel"))
            continue
        detail = status.get("detail")
        problems.append(f"{_subscription_line(subscription)}: {detail or status.get('label')}")
    return problems, [f"rx {channels_text(channels)} {wait}" for wait, channels in waiting.items()]


def iso_time(timestamp: Any) -> str | None:
    if isinstance(timestamp, (int, float)) and not isinstance(timestamp, bool):
        return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z")
    return timestamp if isinstance(timestamp, str) else None


def is_off(device: Any) -> bool:
    return isinstance(device, dict) and device.get("online") is False


def is_enrolled(device: Any) -> bool:
    return isinstance(device, dict) and device.get("management_state") == "managed"


def last_seen_text(device: Any) -> str | None:
    if not is_off(device):
        return None
    seen = iso_time(device.get("last_seen"))
    return f"last seen {seen}" if seen else "last seen at an unknown time"


def off_devices(devices: dict) -> dict[str, str]:
    off = {}
    for server_name, device in devices.items():
        text = last_seen_text(device)
        if text:
            for key in (device.get("name"), server_name, device.get("server_name")):
                if key:
                    off[key] = text
    return off


def waiting_text(source: str, records: dict[str, dict]) -> str | None:
    record = records.get(source)
    if record is None:
        return f"waiting for {source}, which has not been seen since the server started"
    if is_off(record) and not is_enrolled(record):
        return f"waiting for {source}, {last_seen_text(record)}"
    return None


def routes_by_source(devices: dict) -> dict[str, dict[str, list]]:
    sources: dict[str, dict[str, list]] = {}
    for server_name, device in devices.items():
        if not isinstance(device, dict) or device.get("online") is False:
            continue
        receiver = device.get("name") or server_name
        for subscription in _subscription_list(device):
            source = subscription.get("tx_device")
            if not source:
                continue
            number = subscription.get("rx_channel_number")
            channel = number if isinstance(number, int) else subscription.get("rx_channel")
            sources.setdefault(str(source), {}).setdefault(receiver, []).append(channel)
    return sources


def channels_text(channels: list) -> str:
    numbers = [channel for channel in channels if isinstance(channel, int)]
    names = [str(channel) for channel in channels if not isinstance(channel, int)]
    return ", ".join(([number_ranges(numbers)] if numbers else []) + names)


def receive_channels_text(receivers: dict[str, list]) -> str:
    return "; ".join(f"{receiver} rx {channels_text(channels)}" for receiver, channels in sorted(receivers.items()))


def ddm_inventory_known(ddm_status: Any) -> bool:
    if not isinstance(ddm_status, dict):
        return False
    return not ddm_status.get("enabled") or bool(ddm_status.get("fresh"))


def management_state(device: dict, ddm_known: bool) -> str | None:
    state = device.get("management_state")
    if state == "managed":
        return "managed"
    if state == "unenrolled" or ddm_known:
        return "unmanaged"
    return None


def device_model(device: dict) -> str | None:
    model = device.get("model") or device.get("dante_model") or device.get("model_id")
    if isinstance(model, str) and model.startswith("_"):
        return None
    return model


def device_summary(device: Any, ddm_known: bool = False, devices: dict | None = None) -> Any:
    if not isinstance(device, dict):
        return device
    summary = {key: device.get(key) for key in DEVICE_SUMMARY_FIELDS}
    summary["model"] = device_model(device)
    summary["product"] = device.get("product_name")
    summary["management"] = management_state(device, ddm_known) or "unknown"
    if device.get("server_name") != summary.get("inventory_id"):
        summary["server_name"] = device.get("server_name")
    routes = _subscription_list(device)
    summary["last_seen"] = iso_time(device.get("last_seen")) if is_off(device) else None
    summary["subscription_count"] = len(routes)
    problems, waiting = _subscription_problems(device, _device_records(devices) if devices is not None else None)
    summary["subscription_problems"] = problems
    summary["routes_waiting"] = waiting or None
    return compact(summary)


def clock_view(device: dict) -> dict:
    clocking = device.get("ddm_clocking_state") if isinstance(device.get("ddm_clocking_state"), dict) else {}
    preferences = device.get("ddm_clock_preferences") if isinstance(device.get("ddm_clock_preferences"), dict) else {}
    role = device.get("clock_role")
    if role is None and "grand_leader" in clocking:
        role = "Leader" if clocking.get("grand_leader") else "Follower"
    preferred = device.get("preferred_leader")
    if preferred is None:
        preferred = preferences.get("leader")
    managed = device.get("management_state") == "managed"
    status = device.get("clock_status") if isinstance(device.get("clock_status"), dict) else {}
    synchronization = status.get("synchronization")
    if synchronization in (None, "unknown") and clocking.get("locked") == "LOCKED":
        synchronization = "synchronized"
    return compact(
        {
            "clock_observed_at": device.get("clock_observed_at"),
            "ddm_frequency_offset": clocking.get("frequency_offset") if managed else None,
            "frequency_offset_parts_per_billion": device.get("clock_frequency_offset_parts_per_billion"),
            "synchronization": synchronization,
            "clock_state": status.get("clock_state"),
            "clock_source": status.get("clock_source"),
            "mute_state": status.get("mute_state"),
            "leader_evidence": "reported" if device.get("ptpv1_master_uuid") else None,
            "locked": None if status.get("clock_state") else clocking.get("locked"),
            "mute_status": None if status.get("mute_state") else clocking.get("mute_status"),
            "preferred_leader": preferred,
            "role": role,
            "role_source": "ddm" if device.get("clock_role") is None and role else ("direct" if role else None),
        }
    )


def _clock_domain(device: dict) -> str:
    if device.get("management_state") != "managed":
        return "unmanaged"
    domain = device.get("ddm_domain_name") or device.get("ddm_domain_id")
    parts = str(device.get("inventory_id") or "").split(":")
    server = device.get("ddm_server_profile") or (parts[1] if parts[0] == "ddm" and len(parts) > 1 else None)
    if domain is None and parts[0] == "ddm" and len(parts) > 2 and parts[2] != "unenrolled":
        domain = parts[2]
    return f"ddm:{server}:{domain}" if domain else f"ddm:{server}"


def clock_status_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    devices = [
        {**device, "name": device.get("name") or key} for key, device in payload.items() if isinstance(device, dict)
    ]
    identities = {device["ptpv1_device_uuid"]: device["name"] for device in devices if device.get("ptpv1_device_uuid")}
    domains: dict[str, dict] = {}
    for device in sorted(devices, key=lambda device: device["name"]):
        if is_off(device) and not is_enrolled(device):
            continue
        clock = clock_view(device)
        leader_identity = device.get("ptpv1_master_uuid")
        if leader_identity in identities:
            clock["leader"] = identities[leader_identity]
        entry = compact(
            {
                "name": device["name"],
                "online": False if device.get("online") is False else None,
                "role": clock.get("role"),
                "leader": clock.get("leader"),
                "preferred": clock.get("preferred_leader"),
                "synchronization": clock.get("synchronization"),
                "locked": clock.get("locked"),
                "offset_ppb": clock.get("frequency_offset_parts_per_billion"),
                "ddm_frequency_offset": clock.get("ddm_frequency_offset"),
                "leader_evidence": clock.get("leader_evidence"),
            }
        )
        domain = domains.setdefault(_clock_domain(device), {"devices": [], "leaders": []})
        domain["devices"].append(entry)
        if clock.get("role") == "Leader":
            domain["leaders"].append(device["name"])
    for domain in domains.values():
        for entry in domain["devices"]:
            if entry.get("role") == "Leader" or "leader" in entry:
                continue
            if len(domain["leaders"]) == 1:
                entry["leader"] = domain["leaders"][0]
                entry["leader_evidence"] = "domain"
            else:
                entry["leader_evidence"] = "unknown"
        preferred = [
            entry["name"] for entry in domain["devices"] if entry.get("preferred") and entry.get("online", True)
        ]
        if len(preferred) > 1:
            leading = [name for name in preferred if name in domain["leaders"]]
            domain["note"] = f"{len(preferred)} devices are set as preferred leader ({', '.join(preferred)})" + (
                f"; {leading[0]} won the election, so the others follow it" if len(leading) == 1 else ""
            )
        if len(domain["leaders"]) > 1:
            domain["problem"] = f"more than one leader in one clock domain: {', '.join(domain['leaders'])}"
        elif not domain["leaders"] and any(entry.get("online", True) for entry in domain["devices"]):
            domain["problem"] = "no device in this domain reports being the leader"
    view = {"domains": dict(sorted(domains.items()))}
    if arguments.get("detail") == "debug":
        view["notes"] = [
            "offset_ppb comes from the device's own clock status. ddm_frequency_offset is "
            "Dante Domain Manager's figure, reported only for devices it manages, and has not been shown to be the "
            "same quantity; do not compare the two.",
            "leader_evidence reported means the device named its leader; domain means it was inferred from the only "
            "leader in its domain; unknown means the device did not say.",
        ]
        view["raw_clock_records"] = {
            server_name: compact(
                {
                    "clock_status": device.get("clock_status"),
                    "ptpv1_device_uuid": device.get("ptpv1_device_uuid"),
                    "ptpv1_master_uuid": device.get("ptpv1_master_uuid"),
                    "ptpv1_grandmaster_uuid": device.get("ptpv1_grandmaster_uuid"),
                }
            )
            for server_name, device in payload.items()
            if isinstance(device, dict)
        }
    return view


def channel_label(channel: Any) -> Any:
    if isinstance(channel, dict):
        return channel.get("friendly_name") or channel.get("name")
    return channel


def _channel_names(channels: Any) -> dict:
    if not isinstance(channels, dict):
        return {}
    return {
        number: channel_label(channel)
        for number, channel in sorted(channels.items(), key=lambda item: int(item[0]) if str(item[0]).isdigit() else 0)
    }


COLLAPSE_MINIMUM = 4
FACTORY_NAME_PATTERN = re.compile(r"(?:(?:input|output|in|out|ch|channel|rx|tx)\s*)?0*(\d+)", re.IGNORECASE)
SIGNAL_WORDS = {
    "clipping": "clipping",
    "signal_present": "signal",
    "below_threshold": "very quiet",
    "mute_or_floor": "silent",
    "muted": "silent",
}


def factory_named(number: Any, channel: Any) -> bool:
    if not isinstance(channel, dict):
        return False
    name = channel_label(channel)
    if not isinstance(name, str) or not name.strip():
        return True
    factory = channel.get("factory_name") or (channel.get("name") if channel.get("friendly_name") else None)
    if isinstance(factory, str) and factory:
        return name == factory
    match = FACTORY_NAME_PATTERN.fullmatch(name.strip())
    return bool(match) and str(number).isdigit() and int(match.group(1)) == int(number)


def number_ranges(numbers: list[int]) -> str:
    ranges = []
    for number in sorted(numbers):
        if ranges and number == ranges[-1][1] + 1:
            ranges[-1][1] = number
        else:
            ranges.append([number, number])
    return ", ".join(str(start) if start == end else f"{start}-{end}" for start, end in ranges)


def compact_channel_names(channels: Any) -> dict:
    if not isinstance(channels, dict):
        return {}
    view: dict[str, Any] = {}
    factory: list[int] = []
    for number, channel in sorted(channels.items(), key=lambda item: int(item[0]) if str(item[0]).isdigit() else 0):
        if str(number).isdigit() and factory_named(number, channel):
            factory.append(int(number))
        else:
            view[str(number)] = channel_label(channel)
    if factory:
        view["factory names"] = number_ranges(factory)
    return view


def signal_text(reading: Any) -> str | None:
    if not isinstance(reading, dict):
        return None
    state = reading.get("state")
    word = SIGNAL_WORDS.get(state, state or "unknown")
    dbfs = reading.get("dbfs")
    if word == "signal" and isinstance(dbfs, (int, float)):
        return f"{dbfs:g} dBFS"
    if isinstance(dbfs, (int, float)) and word in {"very quiet", "clipping"}:
        return f"{word} ({dbfs:g} dBFS)"
    return word


def compact_channel_levels(readings: Any, channels: Any) -> Any:
    if not isinstance(readings, dict):
        return readings
    records = channels if isinstance(channels, dict) else {}
    with_signal: dict[str, str] = {}
    groups: dict[str, tuple[list[str], list[int]]] = {"very_quiet": ([], []), "silent": ([], [])}
    for number, reading in sorted(readings.items(), key=lambda item: int(item[0]) if str(item[0]).isdigit() else 0):
        text = signal_text(reading)
        record = records.get(str(number)) or records.get(number)
        name = reading.get("name") if isinstance(reading, dict) else None
        custom = name is not None and not factory_named(number, record or {"name": name})
        label = f"{number} {name}" if custom else str(number)
        group = "silent" if text == "silent" else "very_quiet" if str(text).startswith("very quiet") else None
        if group is None:
            with_signal[label] = text
        elif custom:
            groups[group][0].append(label)
        elif str(number).isdigit():
            groups[group][1].append(int(number))
    summary = {
        group: ", ".join(named + ([number_ranges(numbers)] if numbers else [])) or None
        for group, (named, numbers) in groups.items()
    }
    return compact({"with_signal": with_signal or None, **summary})


def with_channel_signal(payload: Any, cache: dict, records: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    servers = {}
    for server_name, record in records.items():
        if isinstance(record, dict):
            servers[record.get("name") or server_name] = record.get("server_name") or server_name
    off = off_devices(records)
    for entry in payload.get("channels") or ():
        if not isinstance(entry, dict):
            continue
        if entry.get("device") in off:
            entry["signal"] = f"none, {entry['device']} is off the network ({off[entry['device']]})"
            continue
        if "number" not in entry:
            continue
        levels = cache.get(servers.get(entry.get("device"))) or {}
        normalized = levels.get(f"{entry.get('channel_type')}_normalized") or {}
        number = entry.get("number")
        text = signal_text(normalized.get(str(number), normalized.get(number)))
        entry["signal"] = text or "unknown"
    if any(isinstance(entry, dict) and entry.get("signal") == "unknown" for entry in payload.get("channels") or ()):
        payload["signal_note"] = "unknown means no recent meter reading; get_signal_levels samples the device"
    return payload


def redundancy_summary(redundancy: Any) -> Any:
    if not isinstance(redundancy, dict):
        return redundancy
    return compact(
        {
            "mode": redundancy.get("current_mode"),
            "configured_mode": redundancy.get("configured_mode"),
            "supported": redundancy.get("advertised_support"),
            "read_only": redundancy.get("read_only"),
            "choices": [
                choice.get("label") for choice in redundancy.get("available_modes") or () if isinstance(choice, dict)
            ]
            or None,
            "current": redundancy.get("state_fresh"),
        }
    )


def availability_verdict(operation: str, state: dict) -> tuple[str, str]:
    reasons = [str(reason) for reason in state.get("reasons") or []]
    if state.get("writable"):
        return "writable", ""
    if reasons and operation != "redundancy" and all(reason in UNCONFIRMED_REASONS for reason in reasons):
        return "unconfirmed", ", ".join(reasons)
    return "refused", ", ".join(reasons) or "no reason given"


def _availability_view(availability: Any) -> dict:
    if not isinstance(availability, dict):
        return {}
    view = {}
    for operation, state in sorted(availability.items()):
        if not isinstance(state, dict):
            view[operation] = state
            continue
        verdict, reasons = availability_verdict(operation, state)
        if verdict == "writable":
            view[operation] = "writable"
        elif verdict == "unconfirmed":
            view[operation] = f"unconfirmed, a write will be tried: {reasons}"
        else:
            view[operation] = f"refused: {reasons}"
    return view


def device_view(
    device: Any,
    sections: list[str],
    ddm_known: bool = False,
    addresses: dict | None = None,
    devices: dict | None = None,
) -> Any:
    if not isinstance(device, dict):
        return device
    if "full" in sections:
        record = compact(device)
        omitted = {}
        for key, value in list(record.items()):
            size = len(json.dumps(value, default=str))
            if size > FULL_FIELD_LIMIT:
                omitted[key] = size
                del record[key]
        if omitted:
            record["omitted_fields"] = {
                "bytes": omitted,
                "read_with": "get_device_record with these field names",
            }
        return record
    view: dict[str, Any] = {}
    if "summary" in sections:
        view.update(device_summary(device, ddm_known, devices))
        if "subscriptions" in sections:
            view.pop("subscription_problems", None)
        view["clock"] = clock_view(device)
    if "channels" in sections:
        channels = device.get("channels") if isinstance(device.get("channels"), dict) else {}
        view["channels"] = {
            "rx": compact_channel_names(channels.get("receivers")),
            "tx": compact_channel_names(channels.get("transmitters")),
        }
    if "subscriptions" in sections:
        view["subscriptions"] = [_subscription_view(subscription) for subscription in _subscription_list(device)]
    if "network" in sections:
        network = {key: device.get(key) for key in DEVICE_NETWORK_FIELDS}
        network["network_redundancy"] = redundancy_summary(network.get("network_redundancy"))
        view["network"] = compact(network)
    if "availability" in sections:
        view["operation_availability"] = _availability_view(device.get("operation_availability"))
        view["performance_operation_availability"] = _availability_view(
            device.get("performance_operation_availability")
        )
    if "flows" in sections:
        view["flows"] = flows_view(device, addresses or {})
    seen = last_seen_text(device)
    if seen:
        view = {
            "off_the_network": f"{seen}; everything below is how it was then"
            + ("; it is enrolled in Dante Domain Manager, so its domain expects it" if is_enrolled(device) else ""),
            **view,
        }
    return compact(view)


def record_fields_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    fields = arguments.get("fields") or []
    view = {field: payload[field] for field in fields if field in payload}
    empty = [field for field in fields if field not in payload and field in DEVICE_SCALAR_FIELDS]
    if empty:
        view["not_known_yet"] = empty
    unknown = [field for field in fields if field not in payload and field not in DEVICE_SCALAR_FIELDS]
    if unknown:
        view["unknown_fields"] = {
            field: difflib.get_close_matches(field, list(payload), n=3, cutoff=0.5) for field in unknown
        }
    return view


def find_channels_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    words = re.findall(r"[a-z0-9]+", str(arguments.get("query", "")).casefold())
    wanted = arguments.get("channel_type", "any")
    devices = {server_name: device for server_name, device in payload.items() if isinstance(device, dict)}
    records = _device_records(devices)
    feeds: dict[tuple[str, str], list[str]] = {}
    for server_name, device in devices.items():
        receiver = device.get("name") or server_name
        for subscription in _subscription_list(device):
            source = (str(subscription.get("tx_device")), str(subscription.get("tx_channel")))
            feeds.setdefault(source, []).append(f"{receiver}:{subscription.get('rx_channel')}")
    matches = []
    for server_name, device in sorted(devices.items()):
        name = device.get("name") or server_name
        device_text = f"{name} {device.get('product_name') or ''}".casefold()
        channels = device.get("channels") if isinstance(device.get("channels"), dict) else {}
        routes = {}
        for subscription in _subscription_list(device):
            routes[str(subscription.get("rx_channel"))] = subscription
        for side, key in (("rx", "receivers"), ("tx", "transmitters")):
            if wanted not in ("any", side):
                continue
            for number, label in _channel_names(channels.get(key)).items():
                text = f"{device_text} {str(label).casefold()}"
                if not words or not all(word in text for word in words):
                    continue
                entry = {"device": name, "channel_type": side, "number": int(number), "name": label}
                if side == "rx":
                    subscription = routes.get(str(label))
                    if subscription is None:
                        entry["fed_by"] = "nothing"
                    else:
                        status = subscription.get("status") if isinstance(subscription.get("status"), dict) else {}
                        entry["fed_by"] = f"{subscription.get('tx_device')}:{subscription.get('tx_channel')}"
                        if status.get("severity") in PROBLEM_SEVERITIES:
                            wait = waiting_text(str(subscription.get("tx_device")), records)
                            if wait:
                                entry["waiting"] = wait
                            else:
                                entry["problem"] = status.get("detail") or status.get("label")
                else:
                    entry["feeds"] = sorted(feeds.get((str(name), str(label)), []))
                record = (channels.get(key) or {}).get(str(number)) or (channels.get(key) or {}).get(number)
                unused = entry.get("fed_by") == "nothing" or entry.get("feeds") == []
                entry["_collapsible"] = unused and factory_named(number, record or {"name": label})
                matches.append(entry)
    collapsed: dict[tuple, list[int]] = {}
    for entry in matches:
        if entry.get("_collapsible"):
            collapsed.setdefault((entry["device"], entry["channel_type"]), []).append(entry["number"])
    collapse = {group for group, numbers in collapsed.items() if len(numbers) >= COLLAPSE_MINIMUM}
    shown = []
    for entry in matches:
        group = (entry["device"], entry["channel_type"])
        if entry.pop("_collapsible") and group in collapse:
            if collapsed[group] is not None:
                shown.append(
                    {
                        "device": entry["device"],
                        "channel_type": entry["channel_type"],
                        "numbers": number_ranges(collapsed[group]),
                        "name": "factory names, not routed",
                    }
                )
                collapsed[group] = None
            continue
        shown.append(entry)
    return {
        "count": len(matches),
        "channels": shown[:FIND_LIMIT],
        "omitted": max(0, len(shown) - FIND_LIMIT) or None,
    }


def _ddm_problems(status: Any) -> dict | None:
    if not isinstance(status, dict):
        return None
    problems = {
        key: value for key, value in status.items() if key not in {"id", "summary", "alert_message"} and value != "OK"
    }
    alert = status.get("alert_message")
    if isinstance(alert, dict):
        problems.update({f"{key}_alert": value for key, value in alert.items() if key != "id" and value})
    return problems or None


def ddm_domains_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, list) or arguments.get("detail") == "debug":
        return payload
    domains = []
    for domain in payload:
        if not isinstance(domain, dict):
            continue
        devices = []
        for device in domain.get("devices") or []:
            if not isinstance(device, dict):
                continue
            identity = device.get("identity") if isinstance(device.get("identity"), dict) else {}
            connection = device.get("connection") if isinstance(device.get("connection"), dict) else {}
            status = device.get("status") if isinstance(device.get("status"), dict) else {}
            devices.append(
                compact(
                    {
                        "name": device.get("name"),
                        "product": identity.get("product_model_name"),
                        "enrollment": device.get("enrolment_state"),
                        "connection": connection.get("state"),
                        "status": status.get("summary"),
                        "problems": _ddm_problems(status),
                    }
                )
            )
        status = domain.get("status") if isinstance(domain.get("status"), dict) else {}
        domains.append(
            compact(
                {
                    "name": domain.get("name"),
                    "id": domain.get("id"),
                    "server": domain.get("ddm_server_profile"),
                    "context": domain.get("ddm_context"),
                    "status": status.get("summary"),
                    "problems": _ddm_problems(status),
                    "devices": devices,
                }
            )
        )
    return {"domains": domains}


def _matches_device(record: dict, device: str | None) -> bool:
    if not device:
        return True
    candidates = {record.get("device_name"), record.get("server_name"), record.get("device_identity")}
    scope = record.get("scope")
    if isinstance(scope, dict):
        candidates |= {scope.get("device_name"), scope.get("server_name"), scope.get("device_identity")}
    return device in candidates or f"{device}.local." in candidates


def _number_runs(numbers: list[int]) -> str:
    runs = []
    start = previous = None
    for number in sorted(set(numbers)):
        if start is None:
            start = previous = number
        elif number == previous + 1:
            previous = number
        else:
            runs.append(f"{start}" if start == previous else f"{start}–{previous}")
            start = previous = number
    if start is not None:
        runs.append(f"{start}" if start == previous else f"{start}–{previous}")
    return ",".join(runs)


def compress_items(items: list[str]) -> list[str]:
    groups: dict[str, list[int]] = {}
    order: list[str] = []
    for item in items:
        numbers = re.findall(r"\d+", item)
        template = re.sub(r"\d+", "#", item, count=1) if len(numbers) == 1 else item
        if template not in groups:
            groups[template] = []
            order.append(template)
        if len(numbers) == 1:
            groups[template].append(int(numbers[0]))
    compressed = []
    for template in order:
        numbers = groups[template]
        if "#" in template and len(numbers) > 1:
            compressed.append(template.replace("#", _number_runs(numbers)))
        elif "#" in template:
            compressed.append(template.replace("#", str(numbers[0])))
        else:
            compressed.append(template)
    return compressed


def readable_summary(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    items = value.split(", ")
    if len(items) <= 8:
        return value
    return ", ".join(compress_items(items))


def _text_change(before: Any, after: Any) -> dict:
    if isinstance(before, str) and isinstance(after, str) and ", " in before + after:
        old_items, new_items = before.split(", "), after.split(", ")
        if len(old_items) > 8 or len(new_items) > 8:
            added = [item for item in new_items if item not in old_items]
            removed = [item for item in old_items if item not in new_items]
            return compact({"added": compress_items(added) or None, "removed": compress_items(removed) or None})
    return {"from": readable_summary(before), "to": readable_summary(after)}


def issues_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    state = arguments.get("state", "open")
    limit = arguments.get("limit", 50)
    offset = arguments.get("offset", 0)
    issues = payload.get("issues") or []
    if isinstance(issues, dict):
        issues = list(issues.values())
    selected = [
        issue
        for issue in issues
        if isinstance(issue, dict)
        and (state == "all" or issue.get("state") == state)
        and _matches_device(issue, arguments.get("device"))
    ]
    selected.sort(key=lambda issue: str(issue.get("last_seen") or ""), reverse=True)
    views = []
    for issue in selected[offset : offset + limit]:
        view = {key: issue.get(key) for key in ISSUE_FIELDS}
        view["summary"] = readable_summary(view.get("summary"))
        scope = issue.get("scope") if isinstance(issue.get("scope"), dict) else {}
        view["device"] = scope.get("device_name") or scope.get("server_name")
        view["channel"] = scope.get("channel_identity")
        view["flow"] = scope.get("flow_identity")
        view["interface"] = scope.get("interface_identity")
        views.append(view)
    return compact(
        {
            "active_count": payload.get("active_count"),
            "issues": views,
            "matched": len(selected),
            "returned": len(views),
            "next_offset": offset + len(views) if offset + len(views) < len(selected) else None,
            "total": payload.get("count"),
        }
    )


def events_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    limit = arguments.get("limit", 20)
    kind = arguments.get("kind")
    events = payload.get("events") or []
    selected = [
        event
        for event in events
        if isinstance(event, dict)
        and (not kind or event.get("kind") == kind)
        and _matches_device(event, arguments.get("device"))
        and (arguments.get("before_sequence") is None or (event.get("sequence") or 0) < arguments["before_sequence"])
    ]
    selected.sort(key=lambda event: event.get("sequence") or 0, reverse=True)
    observation_count = len(selected)
    if arguments.get("collapse_repeats", True):
        grouped: dict[tuple, dict] = {}
        for event in selected:
            value = event.get("current_value")
            previous = event.get("previous_value")
            issue_id = (value.get("issue_id") if isinstance(value, dict) else None) or (
                previous.get("issue_id") if isinstance(previous, dict) else None
            )
            if event.get("kind") in OPERATION_EVENT_KINDS and event.get("operation_id"):
                identity_fields = (event.get("kind"), event.get("operation_id"))
            elif event.get("kind") in CHANGE_EVENT_KINDS:
                identity_fields = (event.get("kind"), event.get("sequence"))
            elif event.get("kind") == "issue_opened" and arguments.get("detail") != "debug" and isinstance(value, dict):
                identity_fields = (
                    event.get("kind"),
                    event.get("device_name"),
                    value.get("kind"),
                    value.get("state"),
                    value.get("severity"),
                    value.get("title"),
                    value.get("summary"),
                )
            else:
                identity_fields = (
                    event.get("kind"),
                    event.get("device_name"),
                    event.get("flow_identity"),
                    event.get("interface_identity"),
                    event.get("channel_identity"),
                    issue_id,
                )
            identity = tuple(json.dumps(value, sort_keys=True, default=str) for value in identity_fields)
            if identity not in grouped:
                grouped[identity] = {
                    **event,
                    "_observation_count": 1,
                    "_first_observed_at": event.get("timestamp"),
                    "_targets": [event],
                }
            else:
                grouped[identity]["_observation_count"] += 1
                grouped[identity]["_first_observed_at"] = event.get("timestamp")
                grouped[identity]["_targets"].append(event)
        selected = list(grouped.values())
    views = []
    for event in selected[:limit]:
        if arguments.get("detail") == "debug":
            views.append({key: value for key, value in event.items() if not key.startswith("_")})
            continue
        view = {key: event.get(key) for key in EVENT_FIELDS}
        if event.get("kind") in CHANGE_EVENT_KINDS and arguments.get("detail") != "debug":
            before = event.get("previous_value") if isinstance(event.get("previous_value"), dict) else {}
            after = event.get("current_value") if isinstance(event.get("current_value"), dict) else {}
            field = next(iter(after), None) or next(iter(before), None)
            view["change"] = {
                "what": field,
                "from": before.get(field) if field else None,
                "to": after.get(field) if field else None,
            }
            views.append(compact(view))
            continue
        if event.get("kind") in OPERATION_EVENT_KINDS:
            value = event.get("current_value") if isinstance(event.get("current_value"), dict) else {}
            raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
            view.update(
                {
                    "operation": event.get("operation_name") or value.get("operation"),
                    "requested": event.get("requested_values"),
                    "result": event.get("final_operation_state") or value.get("state"),
                    "phase": event.get("lifecycle_phase") or value.get("phase"),
                    "message": raw.get("message"),
                }
            )
            views.append(compact(view))
            continue
        targets = event.get("_targets", [event])
        issue_ids = {
            value.get("issue_id")
            for target in targets
            if isinstance(value := target.get("current_value"), dict) and value.get("issue_id")
        }
        multi_issue_group = event.get("kind") == "issue_opened" and len(issue_ids) > 1
        if multi_issue_group:
            for field in ("channel_identity", "flow_identity", "interface_identity"):
                view.pop(field, None)
            for field, plural in (
                ("channel_identity", "channels"),
                ("flow_identity", "flows"),
                ("interface_identity", "interfaces"),
            ):
                values = sorted({str(target[field]) for target in targets if target.get(field)})
                if values:
                    view[plural] = values
            view["issue_count"] = len(issue_ids)
            view["event_count"] = event["_observation_count"]
        elif event.get("_observation_count", 1) > 1:
            view["observation_count"] = event["_observation_count"]
        if event.get("_observation_count", 1) > 1:
            view["first_observed_at"] = event["_first_observed_at"]
        for key in ("previous_value", "current_value"):
            value = event.get(key)
            if isinstance(value, dict):
                visible = compact(
                    {
                        field: value.get(field)
                        for field in ("issue_id", "kind", "state", "severity", "title", "summary", "synchronization")
                    }
                )
                if multi_issue_group:
                    visible.pop("issue_id", None)
                view[key] = visible
                view["debug_detail_available"] = True
            else:
                view[key] = value
        if event.get("kind") == "issue_updated" and view.get("previous_value") == view.get("current_value"):
            view.pop("previous_value", None)
            previous = event.get("previous_value")
            current = event.get("current_value")
            if isinstance(previous, dict) and isinstance(current, dict):
                previous_scope = previous.get("scope") if isinstance(previous.get("scope"), dict) else {}
                current_scope = current.get("scope") if isinstance(current.get("scope"), dict) else {}
                changed = {
                    field: {"from": previous_scope.get(field), "to": current_scope.get(field)}
                    for field in ("device_name", "channel_identity", "flow_identity", "interface_identity")
                    if previous_scope.get(field) != current_scope.get(field)
                }
                if changed:
                    view["attribution_change"] = changed
        previous_view, current_view = view.get("previous_value"), view.get("current_value")
        if isinstance(previous_view, dict) and isinstance(current_view, dict) and previous_view and current_view:
            changed = {
                field: _text_change(previous_view.get(field), current_view.get(field))
                for field in sorted(set(previous_view) | set(current_view))
                if previous_view.get(field) != current_view.get(field)
            }
            view.pop("previous_value")
            view.pop("current_value")
            view["value"] = {
                field: readable_summary(value) for field, value in current_view.items() if field not in changed
            } or None
            view["change"] = changed or None
        elif isinstance(current_view, dict) and "summary" in current_view:
            view["current_value"] = {**current_view, "summary": readable_summary(current_view["summary"])}
        views.append(view)
    return compact(
        {
            "events": views,
            "matched": len(selected),
            "observation_count": observation_count,
            "returned": len(views),
            "next_before_sequence": views[-1].get("sequence") if len(selected) > limit and views else None,
            "total": payload.get("count"),
        }
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def name_meter_channels(levels: Any, devices: dict) -> Any:
    if not isinstance(levels, dict):
        return levels
    by_name = {}
    for server_name, device in devices.items():
        if isinstance(device, dict):
            by_name[device.get("name") or server_name] = device
            by_name[server_name] = device
    named = {}
    for name, entry in levels.items():
        device = by_name.get(name)
        if not isinstance(entry, dict) or device is None:
            named[name] = entry
            continue
        channels = device.get("channels") if isinstance(device.get("channels"), dict) else {}
        result = dict(entry)
        for direction, key, count_key in (("rx", "receivers", "rx_count"), ("tx", "transmitters", "tx_count")):
            meters = entry.get(direction)
            if not isinstance(meters, dict):
                continue
            names = {str(number): label for number, label in _channel_names(channels.get(key)).items()}
            count = device.get(count_key)
            result[direction] = {
                number: {"name": names[str(number)], **reading} if str(number) in names else reading
                for number, reading in meters.items()
                if not isinstance(count, int) or int(number) <= count
            }
        named[device.get("name") or name] = result
    return named


def signal_levels_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    device = arguments.get("device")
    view = {}
    for server_name, levels in sorted(payload.items()):
        if not isinstance(levels, dict):
            continue
        if device and server_name not in {device, f"{device}.local."} and levels.get("device_name") != device:
            continue
        source = levels.get("metering_source")
        channels = {}
        for direction in ("rx", "tx"):
            values = levels.get(direction)
            if not isinstance(values, dict):
                continue
            channels[direction] = {}
            for number, raw in values.items():
                if not isinstance(raw, int) or isinstance(raw, bool):
                    continue
                reading = normalize_metering_value(raw, source)
                state = "mute_or_floor" if reading["state"] == "muted" else reading["state"]
                fields = {"state": state, "dbfs": reading["dbfs"]}
                if arguments.get("detail") == "debug":
                    fields["raw"] = raw
                channels[direction][number] = compact(fields)
        wall_time = levels.get("wall_time")
        observed_at = (
            datetime.fromtimestamp(wall_time, timezone.utc).isoformat().replace("+00:00", "Z")
            if isinstance(wall_time, (int, float))
            else None
        )
        view[levels.get("device_name") or server_name.removesuffix(".local.")] = compact(
            {
                "metering_source": source,
                **channels,
                "observed_at": observed_at,
            }
        )
    return view


def routing_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    routes: dict[str, list[str]] = {}
    records = _device_records(payload)
    for server_name, device in sorted(payload.items()):
        if not isinstance(device, dict):
            continue
        receiver = device.get("name") or server_name
        if arguments.get("device") not in (None, receiver, server_name):
            continue
        if arguments.get("device") is None and is_off(device) and not is_enrolled(device):
            continue
        channels = device.get("channels") if isinstance(device.get("channels"), dict) else {}
        rx_names = _channel_names(channels.get("receivers"))
        name_numbers: dict[str, list[str]] = {}
        for number, name in rx_names.items():
            name_numbers.setdefault(str(name), []).append(str(number))
        for subscription in _subscription_list(device):
            channel_name = subscription.get("rx_channel")
            numbers = name_numbers.get(str(channel_name), [])
            channel_number = numbers[0] if len(numbers) == 1 else None
            requested_channel = arguments.get("rx_channel")
            if requested_channel is not None and str(requested_channel) not in {str(channel_name), str(channel_number)}:
                continue
            status = subscription.get("status") if isinstance(subscription.get("status"), dict) else {}
            severity = status.get("severity")
            wait = (
                waiting_text(str(subscription.get("tx_device")), records)
                if severity in PROBLEM_SEVERITIES and subscription.get("tx_device")
                else None
            )
            wanted = arguments.get("status", "all")
            if wanted == "problem" and (severity not in PROBLEM_SEVERITIES or wait):
                continue
            if wanted == "waiting" and not wait:
                continue
            if wanted == "ok" and severity != "ok":
                continue
            line = f"{channel_number} {channel_name}" if channel_number is not None else str(channel_name)
            line += f" \u2190 {subscription.get('tx_channel')} on {subscription.get('tx_device')}"
            label = str(status.get("label") or "")
            if wait:
                line += f" ({wait})"
            elif severity in PROBLEM_SEVERITIES:
                line += f": {label}, {status.get('detail')}" if status.get("detail") else f": {label}"
            elif label.startswith("Subscribed (") and label != "Subscribed (unicast)":
                line += f" ({label.removeprefix('Subscribed (').removesuffix(')')})"
            elif label and not label.startswith("Subscribed"):
                line += f" ({label})"
            routes.setdefault(receiver, []).append(line)
    off = off_devices(payload)
    receivers_off = {
        receiver: f"{off[receiver]}; these are its routes as they were then" for receiver in routes if receiver in off
    }
    return compact(
        {
            "count": sum(len(lines) for lines in routes.values()),
            "receivers_off_the_network": receivers_off or None,
            "routes": routes,
        }
    )


def _mac(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value.casefold().replace(":", "").replace("-", "")


def renamed_devices(devices: dict, journal: Any) -> dict[str, dict]:
    names_by_mac = {
        _mac(device.get("mac_address")): device.get("name")
        for device in devices.values()
        if isinstance(device, dict) and _mac(device.get("mac_address")) and device.get("online")
    }
    renamed: dict[str, dict] = {}
    events = journal.get("events") if isinstance(journal, dict) else None
    for event in events or []:
        if not isinstance(event, dict) or event.get("kind") != "device_disappeared":
            continue
        raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
        evidence = raw.get("previous_observation") or raw.get("current_observation") or {}
        name = event.get("device_name")
        current = names_by_mac.get(_mac(evidence.get("mac_address")))
        if name and current and current != name:
            renamed[name] = {"renamed_to": current, "renamed_at": event.get("timestamp")}
    return renamed


def missing_devices(devices: dict, journal: Any) -> list[dict]:
    present = set(devices)
    for server_name, device in devices.items():
        if isinstance(device, dict):
            present |= {device.get("name"), device.get("server_name")}
    renamed = renamed_devices(devices, journal)
    sources = routes_by_source(devices)
    last_seen: dict[str, dict] = {}
    events = journal.get("events") if isinstance(journal, dict) else None
    for event in sorted(events or [], key=lambda item: item.get("sequence") or 0 if isinstance(item, dict) else 0):
        if not isinstance(event, dict):
            continue
        name = event.get("device_name") or event.get("server_name")
        if event.get("kind") == "device_disappeared" and name:
            last_seen[name] = {"disappeared_at": event.get("timestamp")}
        elif event.get("kind") == "device_reappeared" and name:
            last_seen.pop(name, None)
    waiting: dict[str, int] = {}
    connected: dict[str, int] = {}
    for device in devices.values():
        if not isinstance(device, dict):
            continue
        for subscription in _subscription_list(device):
            source = subscription.get("tx_device")
            if not source or source in present:
                continue
            status = subscription.get("status") if isinstance(subscription.get("status"), dict) else {}
            counts = connected if status.get("state") == "connected" else waiting
            counts[source] = counts.get(source, 0) + 1
    missing = []
    for name in sorted(set(last_seen) | set(waiting) | set(connected)):
        if name in present or (name in renamed and not waiting.get(name) and not connected.get(name)):
            continue
        entry = {
            "name": name,
            **last_seen.get(name, {}),
            **renamed.get(name, {}),
            "routes_waiting": waiting.get(name, 0),
        }
        if waiting.get(name) and name in sources:
            entry["waiting_routes"] = receive_channels_text(sources[name])
        if name in renamed:
            entry.pop("disappeared_at", None)
        if connected.get(name):
            new_name = renamed.get(name, {}).get("renamed_to")
            entry["routes_still_connected"] = connected[name]
            entry["note"] = (
                f"routes to this name are still receiving audio because the device was renamed; they fail the next "
                f"time they are re-established unless they are re-pointed to {new_name}"
                if new_name
                else "routes to this name are still receiving audio, so the device was most likely renamed; they "
                "fail the next time they are re-established unless they are re-pointed to its new name"
            )
        if name not in last_seen:
            entry["seen"] = "not since the server started"
        missing.append(entry)
    return missing


def device_list_view(devices: dict, ddm_known: bool) -> dict:
    entries = []
    for server_name, device in sorted(devices.items()):
        if not isinstance(device, dict):
            continue
        summary = device_summary(device, ddm_known, devices)
        problems = summary.get("subscription_problems") or []
        if is_off(device) and not is_enrolled(device):
            entries.append(
                (
                    summary.get("name") or server_name,
                    server_name,
                    compact(
                        {
                            "product": summary.get("product") or summary.get("model"),
                            "last_seen": summary.get("last_seen"),
                        }
                    ),
                )
            )
            continue
        entries.append(
            (
                summary.get("name") or server_name,
                server_name,
                compact(
                    {
                        "product": summary.get("product") or summary.get("model"),
                        "ipv4": summary.get("ipv4"),
                        "online": summary.get("online"),
                        "last_seen": summary.get("last_seen"),
                        "management": summary.get("management"),
                        "domain": device.get("ddm_domain_name") if summary.get("management") == "managed" else None,
                        "sample_rate_hz": summary.get("sample_rate_hz"),
                        "latency_ms": summary.get("latency_ms"),
                        "channels": (
                            f"{summary.get('rx_count')} rx, {summary.get('tx_count')} tx"
                            if summary.get("rx_count") is not None or summary.get("tx_count") is not None
                            else None
                        ),
                        "routes": summary.get("subscription_count") or None,
                        "route_problems": problems or None,
                        "routes_waiting": summary.get("routes_waiting"),
                        "locked": True if summary.get("is_locked") is True else None,
                    }
                ),
            )
        )
    names = [name for name, _, _ in entries]
    return {(name if names.count(name) == 1 else server_name): entry for name, server_name, entry in entries}


def ddm_domains_summary(devices: dict) -> list[dict] | None:
    domains: dict[str, dict] = {}
    for device in devices.values():
        if not isinstance(device, dict) or device.get("management_state") != "managed":
            continue
        name = device.get("ddm_domain_name") or device.get("ddm_domain_id")
        if not name:
            continue
        domain = domains.setdefault(name, {"name": name, "server": device.get("ddm_server_profile"), "devices": []})
        label = device.get("name") or device.get("server_name")
        domain["devices"].append(label if device.get("online") else f"{label} (offline)")
    return [compact({**domain, "devices": sorted(domain["devices"])}) for domain in domains.values()] or None


OVERVIEW_ISSUE_GROUPS = 10
SEVERITY_ORDER = {"critical": 0, "error": 1, "warning": 2, "info": 3}
CHANNEL_IDENTITY = re.compile(r"^(rx|tx):(\d+)$")


def _identities_text(identities: set) -> str | None:
    numbers: dict[str, list[int]] = {}
    others = []
    for identity in sorted(identities, key=str):
        match = CHANNEL_IDENTITY.match(str(identity))
        if match:
            numbers.setdefault(match.group(1), []).append(int(match.group(2)))
        else:
            others.append(str(identity))
    parts = [f"{direction} {number_ranges(values)}" for direction, values in sorted(numbers.items())] + others
    return "; ".join(parts) or None


def _device_records(devices: dict) -> dict[str, dict]:
    records = {}
    for server_name, device in devices.items():
        if isinstance(device, dict):
            for key in (device.get("name"), server_name, device.get("server_name")):
                if key:
                    records[key] = device
    return records


def _route_source(record: dict | None, identity: Any) -> str | None:
    match = CHANNEL_IDENTITY.match(str(identity or ""))
    if record is None or match is None or match.group(1) != "rx":
        return None
    for subscription in _subscription_list(record):
        if subscription.get("rx_channel_number") == int(match.group(2)):
            return subscription.get("tx_device")
    return None


def _enrolled_off_group(name: str, record: dict) -> dict:
    domain = record.get("ddm_domain_name") or record.get("ddm_domain_id")
    where = f"Dante Domain Manager domain {domain}" if domain else "Dante Domain Manager"
    return {
        "kind": "enrolled_device_off",
        "severity": "error",
        "device": name,
        "summary": f"enrolled in {where} but off the network ({last_seen_text(record)})",
        "issue_count": 1,
    }


def issue_groups(
    issues: Any, devices: dict, state: str = "open", device: str | None = None
) -> tuple[list[dict], list[str]]:
    raw_issues = issues.get("issues") if isinstance(issues, dict) else None
    if isinstance(raw_issues, dict):
        raw_issues = list(raw_issues.values())
    records = _device_records(devices)
    groups: dict[tuple, dict] = {}
    for issue in raw_issues or ():
        if not isinstance(issue, dict) or (state != "all" and issue.get("state") != state):
            continue
        if not _matches_device(issue, device):
            continue
        scope = issue.get("scope") if isinstance(issue.get("scope"), dict) else {}
        name = scope.get("device_name") or scope.get("server_name")
        record = records.get(name)
        if is_off(record) and not is_enrolled(record):
            continue
        identity = (issue.get("kind"), issue.get("severity"), name, issue.get("summary"), issue.get("state"))
        group = groups.setdefault(
            identity,
            {
                "kind": issue.get("kind"),
                "severity": issue.get("severity"),
                "state": issue.get("state") if state == "all" else None,
                "device": name,
                "summary": readable_summary(issue.get("summary")),
                "issue_count": 0,
                "first_seen": issue.get("first_seen"),
                "last_seen": issue.get("last_seen"),
                "resolved_at": issue.get("resolved_at"),
                "channels": set(),
                "flows": set(),
                "interfaces": set(),
                "sources": {},
                "waiting": {},
            },
        )
        group["issue_count"] += 1
        for field, latest in (("first_seen", False), ("last_seen", True), ("resolved_at", True)):
            value = issue.get(field)
            if isinstance(value, str) and (
                not group[field] or (value > group[field] if latest else value < group[field])
            ):
                group[field] = value
        for field, target in (
            ("channel_identity", "channels"),
            ("flow_identity", "flows"),
            ("interface_identity", "interfaces"),
        ):
            if scope.get(field):
                group[target].add(scope[field])
        source = _route_source(record, scope.get("channel_identity"))
        if source:
            wait = waiting_text(source, records) if issue.get("state") == "open" else None
            target = group["waiting"] if wait else group["sources"]
            target.setdefault(wait or source, set()).add(scope["channel_identity"])
    problems = []
    waiting_lines = []
    for group in groups.values():
        waiting = group.pop("waiting")
        sources = group.pop("sources")
        waiting_channels = set().union(*waiting.values()) if waiting else set()
        for wait, identities in sorted(waiting.items()):
            waiting_lines.append(f"{group['device']} {_identities_text(identities)}: {wait}")
        remaining = group["channels"] - waiting_channels
        if waiting and not remaining:
            continue
        group["issue_count"] -= len(waiting_channels)
        problems.append(
            compact(
                {
                    **group,
                    "channels": _identities_text(remaining),
                    "flows": _identities_text(group["flows"]),
                    "interfaces": _identities_text(group["interfaces"]),
                    "from": "; ".join(
                        f"{source}: {_identities_text(identities)}" for source, identities in sorted(sources.items())
                    )
                    or None,
                    "device_state": (
                        f"{last_seen_text(records.get(group['device']))}; this is its last report"
                        if is_off(records.get(group["device"]))
                        else None
                    ),
                }
            )
        )
    if state in ("open", "all"):
        for server_name, record in sorted(devices.items()):
            name = record.get("name") or server_name if isinstance(record, dict) else None
            if not name or not (is_off(record) and is_enrolled(record)):
                continue
            if device and not _matches_device({"device_name": name, "server_name": record.get("server_name")}, device):
                continue
            problems.append(_enrolled_off_group(name, record))
    problems.sort(
        key=lambda group: (
            SEVERITY_ORDER.get(group.get("severity"), 4),
            -group.get("issue_count", 0),
            str(group.get("kind")),
            str(group.get("device")),
        )
    )
    return problems, sorted(waiting_lines)


def issue_groups_view(payload: Any, arguments: dict, devices: dict) -> Any:
    if not isinstance(payload, dict) or arguments.get("detail") == "debug":
        return issues_view(payload, arguments)
    limit = arguments.get("limit", 50)
    offset = arguments.get("offset", 0)
    problems, waiting = issue_groups(payload, devices, arguments.get("state", "open"), arguments.get("device"))
    selected = problems[offset : offset + limit]
    return compact(
        {
            "problems": selected,
            "problem_count": sum(group["issue_count"] for group in problems),
            "next_offset": offset + len(selected) if offset + len(selected) < len(problems) else None,
            "routes_waiting_for_devices_that_are_off": waiting or None,
            "detail": "detail=debug lists each issue separately with its identifier",
        }
    )


def network_overview_view(devices: dict, issues: dict, ddm: dict, journal: Any = None) -> dict:
    summaries = []
    ddm_known = ddm_inventory_known(ddm)
    for server_name, device in sorted(devices.items()):
        if not isinstance(device, dict):
            continue
        summary = device_summary(device, ddm_known, devices)
        if is_off(device) and not is_enrolled(device):
            summaries.append(
                compact(
                    {
                        "name": summary.get("name") or server_name,
                        "product": summary.get("product") or summary.get("model"),
                        "last_seen": summary.get("last_seen"),
                    }
                )
            )
            continue
        summaries.append(
            compact(
                {
                    "name": summary.get("name") or server_name,
                    "product": summary.get("product") or summary.get("model"),
                    "online": summary.get("online"),
                    "last_seen": summary.get("last_seen"),
                    "management": summary.get("management"),
                    "ipv4": summary.get("ipv4"),
                    "sample_rate_hz": summary.get("sample_rate_hz"),
                    "latency_ms": summary.get("latency_ms"),
                    "subscription_count": summary.get("subscription_count"),
                    "subscription_problem_count": len(summary.get("subscription_problems", [])),
                }
            )
        )
    clocks = clock_status_view(devices, {})["domains"]
    problems, _ = issue_groups(issues, devices)
    return compact(
        {
            "as_of": utc_now(),
            "devices": summaries,
            "online_count": sum(device.get("online") is True for device in summaries),
            "clock_leaders": {name: domain["leaders"] for name, domain in clocks.items()},
            "open_issue_count": sum(group["issue_count"] for group in problems),
            "renamed_devices": [
                entry for entry in missing_devices(devices, journal) if entry.get("renamed_to") or entry.get("note")
            ]
            or None,
            "issue_groups": problems[:OVERVIEW_ISSUE_GROUPS],
            "omitted_issue_count": sum(group["issue_count"] for group in problems[OVERVIEW_ISSUE_GROUPS:]) or None,
            "ddm": compact(
                {
                    **{key: ddm.get(key) for key in ("enabled", "state", "fresh", "server_count", "domain_count")},
                    "domains": ddm_domains_summary(devices),
                }
            ),
        }
    )


def device_addresses(records: dict) -> dict[str, str]:
    return {
        record["ipv4"]: str(record.get("name") or key)
        for key, record in records.items()
        if isinstance(record, dict) and isinstance(record.get("ipv4"), str)
    }


def _numbered_names(channels: Any) -> dict[int, str]:
    return {int(number): label for number, label in _channel_names(channels).items()}


def _channel_labels(numbers: list, names: dict[int, str]) -> list[str]:
    return [f"{number} {names[number]}" if number in names else str(number) for number in numbers]


def _endpoint(destination: Any, addresses: dict[str, str]) -> str | None:
    if not isinstance(destination, dict) or not destination.get("address"):
        return None
    address = destination["address"]
    if address in addresses:
        return addresses[address]
    return f"{address}:{destination['port']}" if destination.get("port") else address


def transmit_flow_view(flow: dict, names: dict[int, str], addresses: dict[str, str]) -> dict:
    identity = flow.get("identity") if isinstance(flow.get("identity"), dict) else {}
    if isinstance(flow.get("channel_slots"), list):
        numbers = [slot.get("transmitter_channel") for slot in flow["channel_slots"] if isinstance(slot, dict)]
    else:
        numbers = list(flow.get("channels") or [])
    return compact(
        {
            "id": flow.get("flow_number") or identity.get("global_flow_id"),
            "type": flow.get("flow_type"),
            "to": _endpoint(flow.get("primary_destination"), addresses),
            "channels": _channel_labels([number for number in numbers if number], names),
            "sample_rate_hz": flow.get("sample_rate_hz") or flow.get("sample_rate"),
            "encoding_bits": flow.get("encoding_bits") or flow.get("encoding"),
        }
    )


def receive_flow_view(flow: dict, device: dict) -> dict:
    names = _numbered_names(((device.get("channels") or {}).get("receivers")))
    numbers = [number for group in flow.get("receiver_channel_numbers_by_flow_channel") or [] for number in group]
    sources = sorted(
        {
            str(subscription.get("tx_device"))
            for subscription in _subscription_list(device)
            if subscription.get("rx_channel_number") in numbers
        }
    )
    latency = flow.get("latency_nanoseconds")
    return compact(
        {
            "id": flow.get("flow_number"),
            "type": flow.get("flow_type"),
            "from": sources or None,
            "channels": _channel_labels(numbers, names),
            "latency_ms": latency / 1_000_000 if isinstance(latency, (int, float)) else None,
        }
    )


def flows_view(device: dict, addresses: dict[str, str]) -> dict:
    names = _numbered_names(((device.get("channels") or {}).get("transmitters")))
    transmit: Any = [
        transmit_flow_view(flow, names, addresses)
        for flow in device.get("transmitter_flows") or []
        if isinstance(flow, dict)
    ]
    if device.get("transmitter_flows") is None:
        transmit = "not read since the server started; get_transmit_flows reads them"
    receive: Any = [
        receive_flow_view(flow, device) for flow in device.get("receiver_flows") or [] if isinstance(flow, dict)
    ]
    if device.get("receiver_flows") is None:
        receive = "not read since the server started"
    return {"transmit": transmit, "receive": receive}


def free_flow_ids(device: dict, flows: list | None = None) -> list[int]:
    authoring = device.get("transmit_flow_authoring") if isinstance(device.get("transmit_flow_authoring"), dict) else {}
    maximum = authoring.get("identifier_max")
    if authoring.get("identity_field") != "global_flow_id" or not isinstance(maximum, int):
        return []
    flows = device.get("transmitter_flows") if flows is None else flows
    used = {
        flow.get("flow_number") or (flow.get("identity") or {}).get("global_flow_id")
        for flow in flows or []
        if isinstance(flow, dict)
    }
    return [number for number in range(1, maximum + 1) if number not in used]


def transmit_flows_view(payload: Any, arguments: dict, device: dict | None, addresses: dict[str, str]) -> Any:
    if not isinstance(payload, dict) or arguments.get("detail") == "debug":
        return payload
    device = device or {}
    names = _numbered_names(((device.get("channels") or {}).get("transmitters")))
    free = free_flow_ids(device, payload.get("flows") or [])
    return compact(
        {
            "device": device.get("name") or payload.get("device"),
            "flow_count": payload.get("reported_flow_count"),
            "maximum_flows": device.get("maximum_transmit_flows"),
            "flows": [
                transmit_flow_view(flow, names, addresses)
                for flow in payload.get("flows") or []
                if isinstance(flow, dict)
            ],
            "next_free_flow_id": free[0] if free else None,
        }
    )


def flow_result_view(payload: Any, device: dict | None, addresses: dict[str, str]) -> Any:
    if not isinstance(payload, dict):
        return payload
    device = device or {}
    names = _numbered_names(((device.get("channels") or {}).get("transmitters")))
    acknowledgement = (
        payload.get("request_acknowledgement") if isinstance(payload.get("request_acknowledgement"), dict) else {}
    )
    comparison = payload.get("comparison") if isinstance(payload.get("comparison"), dict) else {}
    effective = payload.get("effective")
    return compact(
        {
            "state": payload.get("state"),
            "message": payload.get("message"),
            "error": payload.get("error") if payload.get("error") != payload.get("message") else None,
            "device_accepted": acknowledgement.get("accepted"),
            "flow": transmit_flow_view(effective, names, addresses) if isinstance(effective, dict) else None,
            "not_read_back": comparison.get("unavailable_fields") or None,
            "differences": comparison.get("differences") or None,
            "media_confirmed": payload.get("media_packet_reception_confirmed"),
        }
    )


def _iso_from_epoch(value: Any) -> Any:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")
    return value


def _dante_match(device_type: Any, records: dict) -> dict | None:
    if not isinstance(device_type, str) or not device_type:
        return None
    wanted = device_type.casefold()
    matches = [
        record
        for record in records.values()
        if isinstance(record, dict)
        and any(
            isinstance(value, str) and value.casefold() == wanted
            for value in (record.get("model"), record.get("product_name"), record.get("dante_model"))
        )
    ]
    return matches[0] if len(matches) == 1 else None


def wireless_view(payload: Any, records: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    feeds: dict[tuple[str, str], list[str]] = {}
    for record in records.values():
        if not isinstance(record, dict):
            continue
        for subscription in _subscription_list(record):
            key = (str(subscription.get("tx_device")), str(subscription.get("tx_channel")))
            feeds.setdefault(key, []).append(f"{record.get('name')}:{subscription.get('rx_channel')}")
    devices = []
    for mac, device in sorted(payload.items(), key=lambda item: str((item[1] or {}).get("name"))):
        if not isinstance(device, dict):
            continue
        dante = _dante_match(device.get("device_type"), records)
        outputs = _numbered_names(((dante or {}).get("channels") or {}).get("transmitters"))
        channels = []
        for number, channel in sorted((device.get("channels") or {}).items(), key=lambda item: int(item[0])):
            if not isinstance(channel, dict):
                continue
            entry: dict[str, Any] = {"number": int(number), "name": channel.get("name")}
            if "active" in channel:
                entry["transmitter"] = (
                    "receiving a transmitter" if channel["active"] else "no transmitter signal (antenna status XX)"
                )
            if isinstance(channel.get("frequency"), (int, float)):
                entry["frequency_mhz"] = channel["frequency"] / 1000
            entry.update({key: value for key, value in channel.items() if key not in {"name", "active", "frequency"}})
            label = outputs.get(int(number))
            if dante is not None and label is not None:
                entry["dante_output"] = f"{dante.get('name')}:{label}"
                entry["feeds"] = sorted(feeds.get((str(dante.get("name")), str(label)), [])) or "nothing"
            channels.append(entry)
        devices.append(
            compact(
                {
                    "name": device.get("name"),
                    "type": device.get("device_type"),
                    "model": device.get("model"),
                    "online": device.get("online"),
                    "ip": device.get("ip"),
                    "mac": device.get("mac") or mac,
                    "firmware": device.get("firmware_version"),
                    "dante_device": (dante or {}).get("name"),
                    "last_seen": _iso_from_epoch(device.get("last_seen")),
                    "settings": {
                        key: value
                        for key, value in device.items()
                        if key
                        not in {
                            "name",
                            "device_type",
                            "model",
                            "online",
                            "ip",
                            "mac",
                            "firmware_version",
                            "last_seen",
                            "channels",
                        }
                    }
                    or None,
                    "channels": channels,
                }
            )
        )
    return {"devices": devices}


def wireless_link(shure_payload: Any, records: dict, device_name: Any) -> dict | None:
    view = wireless_view(shure_payload, records)
    for device in view.get("devices") or [] if isinstance(view, dict) else []:
        if device.get("dante_device") != device_name:
            continue
        return {
            "name": device.get("name"),
            "channels": [
                f"{channel.get('number')} {channel.get('name')}: {channel.get('transmitter') or 'state unknown'}"
                for channel in device.get("channels") or []
            ],
            "details": "get_wireless_devices",
        }
    return None


def _without_history(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _without_history(item) for key, item in value.items() if key != "history"}
    if isinstance(value, list):
        return [_without_history(item) for item in value]
    return value


def _series_summary(series: Any, scale: float = 1.0, digits: int = 3) -> dict | None:
    if not isinstance(series, dict):
        return None
    current = series.get("current") if isinstance(series.get("current"), dict) else {}
    statistics = series.get("statistics") if isinstance(series.get("statistics"), dict) else {}

    def scaled(value):
        return round(value * scale, digits) if isinstance(value, (int, float)) else None

    return compact(
        {
            "now": scaled(current.get("value")),
            "mean": scaled(statistics.get("mean")),
            "min": scaled(statistics.get("minimum")),
            "max": scaled(statistics.get("maximum")),
            "samples": statistics.get("count"),
            "since": statistics.get("window_start"),
        }
    )


def diagnostics_view(payload: Any, arguments: dict, records: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    if arguments.get("detail") == "debug":
        return _without_history(payload)
    record = next(
        (
            candidate
            for candidate in records.values()
            if isinstance(candidate, dict) and candidate.get("server_name") == payload.get("device")
        ),
        {},
    )
    uuid_names = {
        str(candidate.get("ptpv1_device_uuid")): candidate.get("name")
        for candidate in records.values()
        if isinstance(candidate, dict) and candidate.get("ptpv1_device_uuid")
    }
    clock_state = payload.get("clock_state") if isinstance(payload.get("clock_state"), dict) else {}
    ports = [port for port in clock_state.get("base_ports") or [] if isinstance(port, dict)]
    clock = payload.get("clock") if isinstance(payload.get("clock"), dict) else {}
    variation = clock.get("variation") if isinstance(clock.get("variation"), dict) else {}
    leader_uuid = clock_state.get("ptpv1_master_uuid")
    view: dict[str, Any] = {
        "device": record.get("name") or payload.get("device"),
        "clock": compact(
            {
                "state": clock_state.get("clock_state"),
                "servo": clock_state.get("servo_state"),
                "source": clock_state.get("clock_source"),
                "role": ports[0].get("role") if ports else None,
                "preferred_leader": clock_state.get("preferred_leader"),
                "leader": uuid_names.get(str(leader_uuid)) or leader_uuid,
                "offset_ppm": _series_summary(clock.get("heartbeat")) or _series_summary(clock.get("conmon")),
                "offset_variation_warning": any(
                    isinstance(entry, dict) and entry.get("active") for entry in variation.values()
                )
                or None,
            }
        ),
    }
    if "role" not in view["clock"] and record:
        fallback = clock_view(record)
        view["clock"] = compact(
            {
                "role": fallback.get("role"),
                "synchronization": fallback.get("synchronization"),
                "preferred_leader": fallback.get("preferred_leader"),
                "role_source": fallback.get("role_source"),
                **view["clock"],
            }
        )
    receiver = payload.get("receiver") if isinstance(payload.get("receiver"), dict) else {}
    routes = {subscription.get("rx_channel_number"): subscription for subscription in _subscription_list(record)}
    names = _numbered_names(((record.get("channels") or {}).get("receivers")))
    flows, idle = [], 0
    interfaces = len(
        {path.get("network_interface_index") for path in receiver.get("paths") or [] if isinstance(path, dict)}
    )
    for path in receiver.get("paths") or []:
        if not isinstance(path, dict):
            continue
        evidence = path.get("evidence") if isinstance(path.get("evidence"), dict) else {}
        flow = evidence.get("flow") if isinstance(evidence.get("flow"), dict) else None
        if path.get("attribution_status") != "resolved" or flow is None:
            idle += 1
            continue
        channels = [number for group in flow.get("receiver_channel_numbers_by_flow_channel") or [] for number in group]
        sources = sorted({str(routes[number].get("tx_device")) for number in channels if number in routes})
        budget = evidence.get("configured_latency_nanoseconds")
        latency = _series_summary(path.get("latency"), 1e-6) or {}
        late = path.get("late_packets") if isinstance(path.get("late_packets"), dict) else {}
        late_current = (late.get("current") or {}).get("value") if isinstance(late.get("current"), dict) else None
        interface = path.get("network_interface_index")
        recent = late.get("increase_since_baseline")
        entry = compact(
            {
                "flow": path.get("audio_receiver_flow_id"),
                "interface": ("primary" if interface == 0 else "secondary") if interfaces > 1 else None,
                "type": flow.get("flow_type"),
                "from": sources or None,
                "channels": _channel_labels(channels, names),
                "latency_budget_ms": budget / 1e6 if isinstance(budget, (int, float)) else None,
                "latency_ms": latency or None,
                "late_packets_device_total": int(late_current) if isinstance(late_current, (int, float)) else None,
                "late_packets_since_watching": recent,
            }
        )
        worst = latency.get("max")
        if (
            isinstance(worst, (int, float))
            and isinstance(budget, (int, float))
            and budget
            and worst * 1e6 > 0.8 * budget
        ):
            entry["problem"] = "packets arrive close to the latency budget; raise the latency or check the network"
        if isinstance(recent, (int, float)) and recent > 0:
            entry["problem"] = f"{int(recent)} late packets since the server started watching: audio is being dropped"
        flows.append(entry)
    silent = [
        entry
        for entry in flows
        if entry.get("interface") == "secondary" and not (entry.get("latency_ms") or {}).get("max")
    ]
    if silent and len(silent) == sum(1 for entry in flows if entry.get("interface") == "secondary"):
        flows = [entry for entry in flows if entry not in silent]
        view["secondary_interface"] = "no receive traffic measured on it"
    view["receive_flows"] = flows
    if not isinstance(payload.get("receiver"), dict):
        view["receive_flows"] = "no receive telemetry seen from this device"
    elif not flows and routes:
        view["receive_flows"] = "not measured yet; latency telemetry arrives a few seconds after the server starts"
    if idle:
        view["unused_receive_flow_slots"] = idle
    view["detail"] = "detail=debug returns the raw evidence without history samples"
    return compact(view)


BLUETOOTH_CONNECTION_STATES = {0: "undefined", 1: "connected", 2: "disconnected", 3: "connected but link lost"}
BLUETOOTH_NAME_SOURCES = {1: "uses the Dante device name", 2: "uses the custom name"}
BLUETOOTH_DISCOVERY = {1: "discoverable", 2: "not discoverable"}


def _bluetooth_meaning(category: str, value: Any) -> Any:
    if category == "bluetooth_connection" and isinstance(value, dict):
        state = BLUETOOTH_CONNECTION_STATES.get(value.get("state"))
        peer = value.get("peer_name")
        return f"{state}{f' to {peer}' if peer and state and state.startswith('connected') else ''}" if state else None
    if category == "bluetooth_identification" and isinstance(value, dict):
        return BLUETOOTH_NAME_SOURCES.get(value.get("name_source"))
    if category == "bluetooth_discovery" and isinstance(value, int):
        return BLUETOOTH_DISCOVERY.get(value)
    if category == "bluetooth_pairing" and isinstance(value, int) and not isinstance(value, bool):
        return f"{value} remembered paired device{'s' if value != 1 else ''}"
    return None


CONTROL_INPUTS = {
    "bluetooth_identification": 'a custom Bluetooth name (up to 32 characters), or "Dante device name"',
    "bluetooth_discovery": '"discoverable" or "not discoverable" (on/off also work)',
    "bluetooth_pairing": '"clear" forgets every remembered phone; it needs confirm_clear=true and cannot be undone',
}
CODEC_CONTROL_UNSUPPORTED = "Codec-control support is unavailable."
NO_PANEL_REASONS = {
    "Managed panel transport is not established.": (
        "Panel settings such as Bluetooth or video cannot be read through Dante Domain Manager."
    ),
    "Device platform identity has not been read.": "The device has not reported which control panel it has.",
}
PLAN_RESULTS = {
    "change": "can be applied",
    "unchanged": "already set; nothing to change",
    "unsupported": "refused",
    "unavailable": "cannot be checked right now",
}
BYTE_FIELDS = frozenset({"raw_record", "application_payload", "source_identifier", "raw_response"})


def hexadecimal_byte_fields(value: Any) -> Any:
    if isinstance(value, list):
        return [hexadecimal_byte_fields(item) for item in value]
    if not isinstance(value, dict):
        return value
    converted = {}
    for key, item in value.items():
        if (
            key in BYTE_FIELDS
            and isinstance(item, list)
            and all(isinstance(byte, int) and not isinstance(byte, bool) and 0 <= byte <= 255 for byte in item)
        ):
            converted[f"{key}_hexadecimal"] = bytes(item).hex()
        else:
            converted[key] = hexadecimal_byte_fields(item)
    return converted


def _control_meaning(category: Any, value: Any) -> Any:
    if category == "bluetooth_identification" and isinstance(value, dict):
        if value.get("name_source") == 2 and value.get("custom_name") is not None:
            return f"custom name {value['custom_name']!r}"
        if value.get("name_source") == 1:
            return "the Dante device name"
    if category == "analog_level" and isinstance(value, dict):
        if "channel_levels" in value:
            return {"levels": value["channel_levels"]}
        if "level" in value:
            return {key: value.get(key) for key in ("channel", "level", "direction") if value.get(key) is not None}
    meaning = _bluetooth_meaning(str(category), value)
    return value if meaning is None else meaning


def control_plan_view(payload: Any) -> Any:
    if not isinstance(payload, dict) or "action" not in payload:
        return payload
    category = payload.get("category")
    action = payload.get("action")
    view = {
        "category": category,
        "result": PLAN_RESULTS.get(action, action),
        "from": _control_meaning(category, payload.get("before")),
        "to": (
            _control_meaning(category, payload.get("expected", payload.get("requested")))
            if action in {"change", "unchanged"}
            else None
        ),
        "reason": payload.get("reason"),
    }
    if action == "change":
        view["next"] = "apply_device_control with the same arguments applies it after the user confirms"
    return compact(view)


def control_apply_view(payload: Any) -> Any:
    if not isinstance(payload, dict) or not isinstance(payload.get("plan"), dict):
        return payload
    plan = payload["plan"]
    category = plan.get("category")
    verified = payload.get("effective_state_confirmed") is True
    view = {
        "category": category,
        "changed": verified and plan.get("action") == "change",
        "verified": verified,
        "from": _control_meaning(category, plan.get("before")),
        "now": _control_meaning(category, payload.get("status")) if verified else None,
        "requested": None if verified else _control_meaning(category, plan.get("expected", plan.get("requested"))),
        "sent": payload.get("request_sent"),
        "reason": payload.get("reason") or payload.get("error") or plan.get("reason"),
    }
    if payload.get("request_sent") and not verified:
        view["note"] = (
            "The request was sent but the device has not confirmed the new value; "
            "read inspect_device_controls again before retrying."
        )
    return compact(view)


def controls_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    if arguments.get("detail") == "debug":
        return hexadecimal_byte_fields(payload)
    if not payload.get("panels") and not payload.get("family"):
        reason = payload.get("read_unavailable_reason")
        return compact(
            {
                "panel": "none known",
                "why": NO_PANEL_REASONS.get(reason, reason),
                "analog": _analog_controls(payload.get("analog")),
                "detail": "detail=debug returns the raw panel records",
            }
        )
    presentation = payload.get("presentation") if isinstance(payload.get("presentation"), dict) else {}
    editors = presentation.get("editors") if isinstance(presentation.get("editors"), dict) else {}
    settings = {}
    for category, observation in (payload.get("observations") or {}).items():
        if not isinstance(observation, dict):
            continue
        entry: dict[str, Any] = {"value": observation.get("value")}
        editor = editors.get(category) if isinstance(editors.get(category), dict) else {}
        labels = {
            name: field.get("label")
            for name, field in (editor.get("initial_fields") or {}).items()
            if isinstance(field, dict) and field.get("label")
        }
        if labels:
            entry["meaning"] = labels
        meaning = _bluetooth_meaning(category, observation.get("value"))
        if meaning:
            entry["meaning"] = meaning
        if not observation.get("available"):
            entry["current"] = False
        settings[category] = entry
    changeable = {
        category: CONTROL_INPUTS[category] for category in payload.get("categories") or () if category in CONTROL_INPUTS
    }
    for category, editor in editors.items():
        if not isinstance(editor, dict) or category in changeable:
            continue
        choices = [
            ", ".join(
                str(field.get("label")) for field in (variant.get("fields") or {}).values() if isinstance(field, dict)
            )
            or json.dumps(variant.get("requested"), default=str)
            for variant in editor.get("variants") or []
            if isinstance(variant, dict)
        ]
        changeable[category] = choices or "on/off or a value; plan_device_control shows the accepted values"
    return compact(
        {
            "panel": payload.get("family"),
            "readable": payload.get("readable"),
            "writable": payload.get("writable"),
            "unavailable": payload.get("write_unavailable_reason") or payload.get("read_unavailable_reason"),
            "summary": presentation.get("summary") or None,
            "settings": settings or None,
            "changeable": (changeable or None) if payload.get("writable") is not False else None,
            "analog": _analog_controls(payload.get("analog")),
            "detail": "detail=debug returns the raw panel records",
        }
    )


def _analog_controls(analog: Any) -> Any:
    if not isinstance(analog, dict) or analog.get("read_unavailable_reason") == CODEC_CONTROL_UNSUPPORTED:
        return None
    view = {
        "direction": analog.get("direction"),
        "levels": analog.get("levels"),
        "choices": {
            choice.get("value"): choice.get("label")
            for choice in analog.get("choices") or ()
            if isinstance(choice, dict)
        }
        or None,
        "unavailable": analog.get("read_unavailable_reason"),
        "not_writable": analog.get("write_unavailable_reason"),
    }
    if analog.get("levels") is not None and not analog.get("write_unavailable_reason"):
        view["change_with"] = 'category "analog_level" and requested {"channel": N, "level": one of the choices}'
    return compact(view) or None


def network_levels_view(levels: Any) -> Any:
    if not isinstance(levels, dict):
        return levels
    summary = {}
    for name, entry in levels.items():
        if not isinstance(entry, dict):
            summary[name] = entry
            continue
        device: dict[str, Any] = {}
        for direction in ("rx", "tx"):
            meters = entry.get(direction)
            if not isinstance(meters, dict):
                continue
            active, silent = [], 0
            for number, reading in sorted(
                meters.items(), key=lambda item: int(item[0]) if str(item[0]).isdigit() else 0
            ):
                if not isinstance(reading, dict):
                    continue
                if reading.get("state") == "signal_present":
                    label = f"{number} {reading.get('name')}" if reading.get("name") else str(number)
                    level = reading.get("dbfs")
                    active.append(f"{label} {level} dBFS" if isinstance(level, (int, float)) else label)
                else:
                    silent += 1
            if active or silent:
                device[direction] = compact({"signal": active or None, "silent_channels": silent or None})
        device["observed_at"] = entry.get("observed_at")
        summary[name] = compact(device)
    return {
        "devices": summary,
        "detail": "get_signal_levels with a device lists every channel's state and level",
    }
