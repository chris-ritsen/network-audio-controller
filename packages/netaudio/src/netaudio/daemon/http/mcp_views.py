from __future__ import annotations

from typing import Any

DEVICE_SECTIONS = ("availability", "channels", "flows", "full", "network", "subscriptions", "summary")
DEVICE_SUMMARY_FIELDS = (
    "encoding",
    "inventory_id",
    "ipv4",
    "is_locked",
    "kind",
    "latency_ms",
    "management_state",
    "manufacturer",
    "model",
    "name",
    "online",
    "rx_count",
    "sample_rate_hz",
    "tx_count",
)
DEVICE_NETWORK_FIELDS = ("interfaces", "link_speed_mbps", "mac_address", "network_redundancy")
DEVICE_FLOW_FIELDS = ("receiver_flows", "rx_flow_count", "transmitter_flows", "tx_flow_count")
EVENT_FIELDS = (
    "channel_identity",
    "current_value",
    "device_name",
    "flow_identity",
    "interface_identity",
    "kind",
    "previous_value",
    "sequence",
    "severity",
    "timestamp",
)
ISSUE_FIELDS = (
    "first_seen",
    "issue_id",
    "kind",
    "last_seen",
    "occurrence_count",
    "resolved_at",
    "severity",
    "state",
    "summary",
    "title",
)
PROBLEM_SEVERITIES = {"error", "warning"}


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
    source = f"{subscription.get('tx_device')}:{subscription.get('tx_channel')}"
    return f"{subscription.get('rx_channel')} <- {source}"


def _subscription_view(subscription: dict) -> dict:
    status = subscription.get("status") if isinstance(subscription.get("status"), dict) else {}
    view = {
        "route": _subscription_line(subscription),
        "rx_channel": subscription.get("rx_channel"),
        "status": status.get("label"),
        "tx_channel": subscription.get("tx_channel"),
        "tx_device": subscription.get("tx_device"),
    }
    if status.get("severity") in PROBLEM_SEVERITIES:
        view["problem"] = status.get("detail")
    return view


def _subscription_problems(device: dict) -> list[str]:
    problems = []
    for subscription in _subscription_list(device):
        status = subscription.get("status")
        if isinstance(status, dict) and status.get("severity") in PROBLEM_SEVERITIES:
            problems.append(f"{_subscription_line(subscription)}: {status.get('label')} ({status.get('detail')})")
    return problems


def device_summary(device: Any) -> Any:
    if not isinstance(device, dict):
        return device
    summary = {key: device.get(key) for key in DEVICE_SUMMARY_FIELDS}
    summary["model"] = device.get("model") or device.get("dante_model") or device.get("model_id")
    if device.get("server_name") != summary.get("inventory_id"):
        summary["server_name"] = device.get("server_name")
    routes = _subscription_list(device)
    summary["subscription_count"] = len(routes)
    summary["subscription_problems"] = _subscription_problems(device)
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
    return compact(
        {
            "clock_status": device.get("clock_status"),
            "clock_observed_at": device.get("clock_observed_at"),
            "ptpv1_device_uuid": device.get("ptpv1_device_uuid"),
            "ptpv1_grandmaster_uuid": device.get("ptpv1_grandmaster_uuid"),
            "ddm_frequency_offset": clocking.get("frequency_offset") if managed else None,
            "frequency_offset_parts_per_billion": device.get("clock_frequency_offset_parts_per_billion"),
            "ptpv1_master_uuid": device.get("ptpv1_master_uuid"),
            "leader_evidence": "reported" if device.get("ptpv1_master_uuid") else None,
            "locked": clocking.get("locked"),
            "mute_status": clocking.get("mute_status"),
            "preferred_leader": preferred,
            "role": role,
            "role_source": "ddm" if device.get("clock_role") is None and role else ("direct" if role else None),
        }
    )


def _clock_domain(device: dict) -> str:
    inventory_id = str(device.get("inventory_id") or "")
    parts = inventory_id.split(":")
    if (
        parts[0] == "ddm"
        and len(parts) > 2
        and parts[2] != "unenrolled"
        and device.get("management_state") == "managed"
    ):
        return f"ddm:{parts[1]}"
    return "unmanaged"


def clock_status_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    devices = [
        {**device, "name": device.get("name") or key} for key, device in payload.items() if isinstance(device, dict)
    ]
    identities = {device["ptpv1_device_uuid"]: device["name"] for device in devices if device.get("ptpv1_device_uuid")}
    domains: dict[str, dict] = {}
    for device in sorted(devices, key=lambda device: device["name"]):
        clock = clock_view(device)
        leader_identity = clock.get("ptpv1_master_uuid")
        if leader_identity in identities:
            clock["leader"] = identities[leader_identity]
        entry = compact({"name": device["name"], "online": device.get("online"), **clock})
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
    return {
        "domains": dict(sorted(domains.items())),
        "notes": [
            "frequency_offset_parts_per_billion comes from the device's own clock status. ddm_frequency_offset is "
            "Dante Domain Manager's figure, reported only for devices it manages, and has not been shown to be the "
            "same quantity; do not compare the two.",
            "leader_evidence reported means the device named its leader; domain means it was inferred from the only "
            "leader in its domain; unknown means the device did not say.",
            "role_source says where the role came from and nothing else; other fields may come from the other path.",
        ],
    }


def _channel_names(channels: Any) -> dict:
    if not isinstance(channels, dict):
        return {}
    return {
        number: channel.get("name") if isinstance(channel, dict) else channel
        for number, channel in sorted(channels.items(), key=lambda item: int(item[0]) if str(item[0]).isdigit() else 0)
    }


def _availability_view(availability: Any) -> dict:
    if not isinstance(availability, dict):
        return {}
    view = {}
    for operation, state in sorted(availability.items()):
        if not isinstance(state, dict):
            view[operation] = state
        elif state.get("writable"):
            view[operation] = "writable"
        else:
            reasons = state.get("reasons") or []
            view[operation] = "read-only: " + ", ".join(str(reason) for reason in reasons) if reasons else "read-only"
    return view


def device_view(device: Any, sections: list[str]) -> Any:
    if not isinstance(device, dict):
        return device
    if "full" in sections:
        return compact(device)
    view: dict[str, Any] = {}
    if "summary" in sections:
        view.update(device_summary(device))
        view["clock"] = clock_view(device)
    if "channels" in sections:
        channels = device.get("channels") if isinstance(device.get("channels"), dict) else {}
        view["channels"] = {
            "rx": _channel_names(channels.get("receivers")),
            "tx": _channel_names(channels.get("transmitters")),
        }
    if "subscriptions" in sections:
        view["subscriptions"] = [_subscription_view(subscription) for subscription in _subscription_list(device)]
    if "network" in sections:
        view["network"] = compact({key: device.get(key) for key in DEVICE_NETWORK_FIELDS})
    if "availability" in sections:
        view["operation_availability"] = _availability_view(device.get("operation_availability"))
        view["performance_operation_availability"] = _availability_view(
            device.get("performance_operation_availability")
        )
    if "flows" in sections:
        view["flows"] = compact({key: device.get(key) for key in DEVICE_FLOW_FIELDS})
    return compact(view)


def _matches_device(record: dict, device: str | None) -> bool:
    if not device:
        return True
    candidates = {record.get("device_name"), record.get("server_name"), record.get("device_identity")}
    scope = record.get("scope")
    if isinstance(scope, dict):
        candidates |= {scope.get("device_name"), scope.get("server_name"), scope.get("device_identity")}
    return device in candidates or f"{device}.local." in candidates


def issues_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    state = arguments.get("state", "open")
    limit = arguments.get("limit", 50)
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
    for issue in selected[:limit]:
        view = {key: issue.get(key) for key in ISSUE_FIELDS}
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
            "total": payload.get("count"),
        }
    )


def events_view(payload: Any, arguments: dict) -> Any:
    if not isinstance(payload, dict):
        return payload
    limit = arguments.get("limit", 50)
    kind = arguments.get("kind")
    events = payload.get("events") or []
    selected = [
        event
        for event in events
        if isinstance(event, dict)
        and (not kind or event.get("kind") == kind)
        and _matches_device(event, arguments.get("device"))
    ]
    selected.sort(key=lambda event: event.get("sequence") or 0, reverse=True)
    views = [{key: event.get(key) for key in EVENT_FIELDS} for event in selected[:limit]]
    return compact({"events": views, "matched": len(selected), "returned": len(views), "total": payload.get("count")})


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
        view[server_name] = compact(
            {
                "metering_source": levels.get("metering_source"),
                "rx": levels.get("rx"),
                "tx": levels.get("tx"),
                "wall_time": levels.get("wall_time"),
            }
        )
    return view
