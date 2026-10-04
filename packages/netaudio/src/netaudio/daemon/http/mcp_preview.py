from __future__ import annotations

from netaudio.daemon.http.mcp_views import (
    availability_verdict,
    channel_label,
    device_addresses,
    is_off,
    last_seen_text,
    transmit_flow_view,
)

NEXT_STEP = (
    "Nothing has changed. Tell the user what this will do; after they agree, call again with the same "
    "arguments and confirmed=true."
)
SETTINGS = {
    "set_aes67": ("enabled", "aes67_configured", None),
    "set_encoding": ("encoding", "encoding", "supported_encodings"),
    "set_latency": ("latency", "latency_ms", "standard_latency_choices_ms"),
    "set_preferred_leader": ("preferred", "preferred_leader", None),
    "set_sample_rate": ("sample_rate", "sample_rate_hz", "supported_sample_rates_hz"),
}
INTERRUPTING = {
    "clear_configuration": "clears its stored configuration and reboots",
    "factory_reset": "returns to factory settings, losing its name, routes and network settings, and reboots",
    "reboot": "reboots",
}
AFTER_REBOOT = {"set_aes67", "set_interface", "set_redundancy"}
AVAILABILITY_KEYS = {
    "configure_flow_performance": None,
    "identify": ("operation_availability", "identify"),
    "lock_device": ("operation_availability", "locking"),
    "set_aes67": ("operation_availability", "aes67"),
    "set_encoding": ("operation_availability", "encoding"),
    "set_gain": ("operation_availability", "codec_control"),
    "set_interface": ("operation_availability", "static_ipv4"),
    "set_receive_flow_default_slots": ("performance_operation_availability", "receive_flow_default_slots"),
    "set_redundancy": ("operation_availability", "redundancy"),
    "set_sample_rate": ("operation_availability", "sample_rate"),
    "set_sample_rate_pullup": ("operation_availability", "sample_rate_pullup"),
    "store_current_configuration": ("performance_operation_availability", "store_current_configuration"),
    "unlock_device": ("operation_availability", "locking"),
}
LOCK_EXEMPT = {"identify", "unlock_device", "lock_device", "start_metering", "stop_metering", "refresh"}


SECRET_ARGUMENTS = frozenset({"api_key", "password", "pin", "secret", "token"})


def _plain_arguments(request: dict, excluded: set) -> dict:
    return {
        key: "(hidden)" if key in SECRET_ARGUMENTS else value for key, value in request.items() if key not in excluded
    }


def _display(record: dict | None, fallback) -> str:
    if not isinstance(record, dict):
        return str(fallback)
    return str(record.get("name") or record.get("server_name") or fallback)


def _record(records: dict, selector) -> dict | None:
    if selector in records and isinstance(records[selector], dict):
        return records[selector]
    for record in records.values():
        if isinstance(record, dict) and selector in {record.get("name"), record.get("server_name")}:
            return record
    return None


def _channel_label(record: dict | None, direction: str, number) -> str | None:
    channels = ((record or {}).get("channels") or {}).get(direction) or {}
    value = next((channel for key, channel in channels.items() if str(key) == str(number)), None)
    return channel_label(value)


def _subscriptions(record: dict | None) -> list[dict]:
    subscriptions = (record or {}).get("subscriptions") or []
    if isinstance(subscriptions, dict):
        subscriptions = list(subscriptions.values())
    return [
        subscription
        for subscription in subscriptions
        if isinstance(subscription, dict)
        and subscription.get("tx_device")
        and not (isinstance(subscription.get("status"), dict) and subscription["status"].get("state") == "none")
    ]


def _current_route(record: dict | None, number) -> dict | None:
    label = _channel_label(record, "receivers", number)
    for subscription in _subscriptions(record):
        if subscription.get("rx_channel_number") == number or (
            subscription.get("rx_channel_number") is None and subscription.get("rx_channel") == label
        ):
            return subscription
    return None


def _routes_from(records: dict, device_name: str, tx_channel: str | None = None) -> list[str]:
    routes = []
    for record in records.values():
        if not isinstance(record, dict):
            continue
        for subscription in _subscriptions(record):
            if subscription.get("tx_device") != device_name:
                continue
            if tx_channel is not None and subscription.get("tx_channel") != tx_channel:
                continue
            routes.append(
                f"{_display(record, '?')}:{subscription.get('rx_channel')} <- {device_name}:{subscription.get('tx_channel')}"
            )
    return sorted(routes)


def _forget_change(request: dict, records: dict, warnings: list[str]) -> dict:
    if request.get("every_device_off"):
        targets = [record for record in records.values() if is_off(record)]
    else:
        record = _record(records, request.get("device"))
        if record is None:
            warnings.append(f"{request.get('device')} is not in the device list, so there is nothing to forget")
            return {}
        if not is_off(record):
            warnings.append(f"{_display(record, '?')} is on the network; only a device that is off can be forgotten")
            return {}
        targets = [record]
    for record in targets:
        routes = _routes_from(records, _display(record, "?"))
        if routes:
            warnings.append(f"routes that point at {_display(record, '?')} stay as they are: {', '.join(routes)}")
    return {
        "forget": sorted(f"{_display(record, '?')} ({last_seen_text(record)})" for record in targets)
        or "nothing is off the network",
        "comes_back": "a forgotten device returns by itself if it comes back on the network",
    }


def _route_change(route: dict, records: dict, warnings: list[str]) -> dict:
    receiver = _record(records, route.get("rx_device"))
    number = route.get("rx_channel")
    label = _channel_label(receiver, "receivers", number)
    current = _current_route(receiver, number)
    before = f"{current.get('tx_device')}:{current.get('tx_channel')}" if current else "nothing"
    after = f"{route['tx_device']}:{route['tx_channel']}" if route.get("tx_device") else "nothing"
    target = f"{_display(receiver, route.get('rx_device'))} receive channel {number}" + (f" ({label})" if label else "")
    rx_count = (receiver or {}).get("rx_count")
    if isinstance(rx_count, int) and isinstance(number, int) and number > rx_count:
        warnings.append(f"{_display(receiver, route.get('rx_device'))} has only {rx_count} receive channels")
    if before == after:
        warnings.append(
            f"{target} is already {'unrouted' if after == 'nothing' else 'fed by ' + after}; nothing would change"
        )
    if route.get("tx_device") and _record(records, route["tx_device"]) is None:
        warnings.append(f"{route['tx_device']} is not on the network now; the route stays unresolved until it appears")
    return {"receiver": target, "from": before, "to": after}


def _rate_effects(device_name: str, record: dict | None, records: dict, rate) -> tuple[list[str], list[str]]:
    peers: dict[str, int] = {}
    routes: list[str] = []
    for subscription in _subscriptions(record):
        transmitter = _record(records, subscription.get("tx_device"))
        peer_rate = (transmitter or {}).get("sample_rate_hz")
        if transmitter is not record and peer_rate not in (None, rate):
            peer = _display(transmitter, subscription.get("tx_device"))
            peers[peer] = peers.get(peer, 0) + 1
            routes.append(
                f"{peer}:{subscription.get('tx_channel')} -> {device_name}:{subscription.get('rx_channel')} "
                f"({peer} at {peer_rate} Hz)"
            )
    for other in records.values():
        if not isinstance(other, dict) or other is record or other.get("sample_rate_hz") in (None, rate):
            continue
        peer = _display(other, "?")
        for subscription in _subscriptions(other):
            if subscription.get("tx_device") != device_name:
                continue
            peers[peer] = peers.get(peer, 0) + 1
            routes.append(
                f"{device_name}:{subscription.get('tx_channel')} -> {peer}:{subscription.get('rx_channel')} "
                f"({peer} at {other.get('sample_rate_hz')} Hz)"
            )
    effects = []
    if peers:
        listing = ", ".join(f"{peer} ({count})" for peer, count in sorted(peers.items()))
        effects.append(
            f"{sum(peers.values())} route(s) between {device_name} and devices running at another rate stop until "
            f"those devices also run at {rate} Hz: {listing}"
        )
    effects.append(
        f"if the server cannot tell whether {device_name}'s transmit flows keep all their channels at {rate} Hz, it "
        "refuses the change unless confirm_destructive=true is also given"
    )
    return effects, routes


def _device_change(name: str, request: dict, record: dict | None, records: dict, warnings: list[str]) -> dict:
    device_name = _display(record, request.get("device"))
    if name in SETTINGS:
        argument, field, choices_field = SETTINGS[name]
        before = (record or {}).get(field)
        after = request.get(argument)
        choices = (record or {}).get(choices_field) if choices_field else None
        if isinstance(choices, list) and choices and after not in choices:
            warnings.append(f"{device_name} supports {argument} values {choices}; {after} will be rejected")
        if before == after:
            warnings.append(f"{device_name} already has {argument} {after}; nothing would change")
        elif name == "set_sample_rate":
            effects, routes = _rate_effects(device_name, record, records, after)
            warnings.extend(effects)
            if routes:
                return {argument: {"from": before, "to": after}, "routes_that_stop": routes}
        return {argument: {"from": before, "to": after}}
    if name == "rename_device":
        routes = _routes_from(records, device_name)
        if routes:
            warnings.append(
                f"{len(routes)} route(s) subscribe to {device_name} by name: {'; '.join(routes)}. They keep running "
                "on their current flows, but fail the next time they are re-established (either device restarts, "
                f"or the route is re-made) unless they are re-pointed to {request.get('name')}"
            )
        return {"name": {"from": device_name, "to": request.get("name")}}
    if name == "rename_channel":
        direction = "transmitters" if request.get("channel_type") == "tx" else "receivers"
        number = request.get("channel_number")
        before = _channel_label(record, direction, number)
        if before is None:
            warnings.append(f"{device_name} has no {request.get('channel_type')} channel {number}")
        after = request.get("name") or "the device default"
        if direction == "transmitters" and before:
            routes = _routes_from(records, device_name, before)
            if routes:
                warnings.append(
                    f"{len(routes)} route(s) subscribe to this channel by name and will go unresolved: "
                    f"{'; '.join(routes)}"
                )
        return {f"{request.get('channel_type')} channel {number}": {"from": before, "to": after}}
    if name == "set_gain":
        choices = {
            choice.get("value"): choice.get("label")
            for choice in (record or {}).get("gain_level_choices") or []
            if isinstance(choice, dict)
        }
        levels = (record or {}).get("gain_levels") or []
        number = request.get("channel_number")
        before = levels[number - 1] if isinstance(number, int) and 0 < number <= len(levels) else None
        after = request.get("gain_level")
        if before == after:
            warnings.append(
                f"{device_name} channel {number} is already at {choices.get(after, after)}; nothing would change"
            )
        return {f"channel {number} level": {"from": choices.get(before, before), "to": choices.get(after, after)}}
    if name == "delete_transmit_flow":
        flows = [flow for flow in (record or {}).get("transmitter_flows") or [] if isinstance(flow, dict)]
        flow = next((flow for flow in flows if flow.get("flow_number") == request.get("flow_id")), None)
        if flow is None:
            known = ", ".join(str(flow.get("flow_number")) for flow in flows) or "none"
            warnings.append(f"{device_name} has no transmit flow {request.get('flow_id')}; its flows are {known}")
            return {"remove flow": request.get("flow_id")}
        names = {
            int(key): channel_label(value)
            for key, value in (((record or {}).get("channels") or {}).get("transmitters") or {}).items()
            if str(key).isdecimal()
        }
        view = transmit_flow_view(flow, names, device_addresses(records))
        if flow.get("flow_type") == "unicast":
            warnings.append(
                f"flow {request.get('flow_id')} is a unicast flow {device_name} made for a route to {view.get('to')}; "
                "deleting it interrupts that route until the device rebuilds it"
            )
        else:
            warnings.append("devices receiving this multicast flow lose its channels until they are routed another way")
        return {"remove flow": view}
    if name in {"lock_device", "unlock_device"}:
        locked = (record or {}).get("is_locked")
        after = name == "lock_device"
        if locked is after:
            warnings.append(f"{device_name} is already {'locked' if after else 'unlocked'}; nothing would change")
        return {"locked": {"from": locked, "to": after}}
    return _plain_arguments(request, {"device", "confirmed"})


def _enrollment_change(request: dict, arguments: dict, records: dict, warnings: list[str], preview: dict) -> dict:
    record = next(
        (
            candidate
            for candidate in records.values()
            if isinstance(candidate, dict)
            and candidate.get("ddm_device_id") == request.get("device_id")
            and candidate.get("ddm_server_profile") in {None, request.get("server")}
        ),
        None,
    )
    device = _display(record, request.get("device_id"))
    preview["device"] = device
    managed = (record or {}).get("management_state") == "managed"
    before = (record or {}).get("ddm_domain_name") or (record or {}).get("ddm_domain_id") if managed else None
    after = (arguments.get("domain") or request.get("domain_id")) if request.get("action") == "enroll" else None
    if before == after or (
        before is not None and after is not None and str(before).casefold() == str(after).casefold()
    ):
        warnings.append(f"{device} is already {'in ' + str(after) if after else 'unenrolled'}; nothing would change")
    elif before is not None and after is not None:
        warnings.append(f"{device} is enrolled in {before}; this unenrolls it from {before} and then enrolls it")
    inbound = len(_subscriptions(record))
    outbound = _routes_from(records, device)
    if inbound or outbound:
        preview["effect"] = (
            f"{device} changes domain; its {inbound} receive route(s) and {len(outbound)} route(s) it feeds stop "
            "wherever the other device is no longer in the same domain"
        )
    return {"domain": {"from": before or "none (unmanaged)", "to": after or "none (unmanaged)"}}


def write_preview(name: str, request: dict, records: dict, arguments: dict | None = None) -> dict:
    warnings: list[str] = []
    preview: dict = {"changed": False, "confirmation_required": True, "operation": name}
    if name == "set_ddm_enrollment":
        preview["change"] = _enrollment_change(request, arguments or {}, records, warnings, preview)
    elif name == "update_ddm_domain":
        members = sorted(
            _display(record, "?")
            for record in records.values()
            if isinstance(record, dict)
            and record.get("ddm_domain_id") == request.get("domain_id")
            and record.get("management_state") == "managed"
        )
        label = (arguments or {}).get("domain") or request.get("domain_id")
        if request.get("action") == "remove":
            preview["change"] = {"remove_domain": label}
            if members:
                warnings.append(
                    f"{len(members)} enrolled device(s) leave the domain and become unmanaged: {', '.join(members)}"
                )
        else:
            preview["change"] = {"domain_name": {"from": label, "to": request.get("name")}}
    elif name in {"subscribe", "unsubscribe"}:
        preview["change"] = _route_change(request, records, warnings)
    elif name == "set_subscriptions":
        preview["changes"] = [_route_change(route, records, warnings) for route in request.get("routes") or []]
    elif name == "forget_device":
        preview["change"] = _forget_change(request, records, warnings)
    elif "device" in request:
        record = _record(records, request["device"])
        preview["device"] = _display(record, request["device"])
        change = _device_change(name, request, record, records, warnings)
        if change:
            preview["change"] = change
        if record is not None:
            if record.get("online") is False:
                warnings.append(f"{preview['device']} is offline; the change will fail")
            key = AVAILABILITY_KEYS.get(name)
            if name == "configure_flow_performance":
                kind = request.get("kind")
                key = (
                    "performance_operation_availability",
                    "unicast_performance" if kind == "unicast" else f"{kind}_flow_performance",
                )
            availability = ((record.get(key[0]) or {}).get(key[1]) if key else None) or {}
            if isinstance(availability, dict) and availability.get("writable") is False:
                verdict, reasons = availability_verdict(key[1], availability)
                if verdict == "refused":
                    warnings.append(f"the server will refuse this on {preview['device']}: {reasons}")
                else:
                    warnings.append(
                        f"{preview['device']} has not confirmed it supports this ({reasons}); the server will try it"
                    )
            if record.get("is_locked") is True and name not in LOCK_EXEMPT:
                warnings.append(f"{preview['device']} is locked; unlock it first or the device will refuse the change")
            if name in AFTER_REBOOT:
                preview["effect"] = f"{preview['device']} applies this only after it reboots"
            if name in INTERRUPTING and (name != "clear_configuration" or request.get("reboot")):
                inbound = len(_subscriptions(record))
                outbound = _routes_from(records, preview["device"])
                preview["effect"] = (
                    f"{preview['device']} {INTERRUPTING[name]}; audio stops on its {inbound} receive route(s) and "
                    f"{len(outbound)} route(s) it feeds until it is back"
                )
    elif name == "login_ddm":
        preview["change"] = _plain_arguments(request, {"confirmed"})
        server = request.get("server") or request.get("url")
        profile = next(
            (
                record
                for record in records.values()
                if isinstance(record, dict) and record.get("ddm_server_profile") == server
            ),
            None,
        )
        preview["effect"] = (
            f"replaces the saved login for {server}; its managed inventory reconnects with the new credentials, and "
            "if they are wrong the inventory stops until a working login is saved"
            if profile is not None
            else f"saves a login for {server} and starts reading its managed inventory"
        )
    else:
        preview["change"] = _plain_arguments(request, {"confirmed"})
    if warnings:
        preview["warnings"] = warnings
    preview["next"] = NEXT_STEP
    return preview


READBACK_TOOLS = frozenset(SETTINGS) | {"rename_channel", "rename_device", "set_gain", "lock_device", "unlock_device"}
ROUTE_TOOLS = frozenset({"subscribe", "unsubscribe", "set_subscriptions"})
PENDING_ROUTE_STATES = {"in_progress", "pending", "unknown"}


def _routes_of(request: dict) -> list[dict]:
    return list(request.get("routes") or []) if "routes" in request else [request]


def _route_now(route: dict, records: dict) -> dict:
    receiver = _record(records, route.get("rx_device"))
    number = route.get("rx_channel")
    label = _channel_label(receiver, "receivers", number)
    current = _current_route(receiver, number) or {}
    status = current.get("status")
    status = status if isinstance(status, dict) else {}
    view: dict = {
        "receiver": f"{_display(receiver, route.get('rx_device'))} receive channel {number}"
        + (f" ({label})" if label else ""),
        "requested": f"{route['tx_device']}:{route['tx_channel']}" if route.get("tx_device") else "nothing",
        "now": f"{current.get('tx_device')}:{current.get('tx_channel')}" if current else "nothing",
    }
    if current:
        view["status"] = status.get("label")
        if status.get("severity") in {"error", "warning"}:
            view["problem"] = status.get("detail")
    view["verified"] = view["now"] == view["requested"] and str(status.get("state")) not in PENDING_ROUTE_STATES
    return view


def _device_after(name: str, request: dict, records: dict) -> dict | None:
    record = _record(records, request.get("device"))
    if record is None and name == "rename_device":
        record = _record(records, request.get("name"))
    return record


def write_settled(name: str, request: dict, records: dict, before: dict | None = None) -> bool:
    if name in ROUTE_TOOLS:
        return all(_route_now(route, records)["verified"] for route in _routes_of(request))
    if name == "rename_channel" and not request.get("name"):
        if before is None:
            return True
        now = _device_change(name, request, _device_after(name, request, records), records, [])
        was = _device_change(name, request, _record(before, request.get("device")), before, [])
        return all(
            isinstance(entry, dict) and entry.get("from") != (was.get(label) or {}).get("from")
            for label, entry in now.items()
        )
    if name in READBACK_TOOLS:
        record = _device_after(name, request, records)
        change = _device_change(name, request, record, records, [])
        return all(entry.get("from") == entry.get("to") for entry in change.values() if isinstance(entry, dict))
    return True


def _sample_rate_refusal(payload: dict, records: dict) -> str | None:
    preflight = payload.get("preflight")
    if not isinstance(preflight, dict) or not preflight.get("requires_destructive_confirmation"):
        return None
    device = preflight.get("device_name") or "the device"
    target = preflight.get("target_sample_rate_hertz")
    flows = ((preflight.get("current_topology") or {}).get("transmitter_flows")) or []
    losses = preflight.get("destructive_transmitter_membership_loss") or []
    names = _channel_names_by_number(_record(records, device))
    described = []
    for flow in flows:
        channels = ", ".join(
            f"{number} ({names[number]})" if number in names else str(number)
            for number in flow.get("channel_members") or []
        )
        described.append(f"flow {flow.get('flow_number')} ({flow.get('flow_type')}: {channels})")
    if preflight.get("capacity_known"):
        cause = f"at {target} Hz {device}'s transmit flows would lose channels ({losses})"
    else:
        cause = (
            f"{device} did not report how many channels it has at {target} Hz, so the server cannot tell whether "
            f"its transmit flows keep all their channels"
        )
    flows_text = f": {'; '.join(described)}" if described else ""
    return (
        f"Nothing was changed. {cause}{flows_text}. If the user accepts that risk, call again with "
        "confirm_destructive=true."
    )


def _channel_names_by_number(record: dict | None) -> dict[int, str]:
    channels = ((record or {}).get("channels") or {}).get("transmitters") or {}
    names = {}
    for key, value in channels.items():
        name = channel_label(value)
        if str(key).isdecimal() and isinstance(name, str):
            names[int(key)] = name
    return names


def write_outcome(name: str, request: dict, before: dict, after: dict, status: int, payload) -> tuple[dict, bool]:
    failed = status >= 400
    if name == "set_sample_rate" and failed and isinstance(payload, dict):
        refusal = _sample_rate_refusal(payload, before)
        if refusal:
            return {"changed": False, "error": refusal}, True
    if name in ROUTE_TOOLS:
        result: dict = {"changed": not failed, "operation": name}
        routes = [_route_now(route, after) for route in _routes_of(request)]
        backend = payload.get("routes") if isinstance(payload, dict) else None
        if isinstance(backend, list) and len(backend) == len(routes):
            for view, outcome in zip(routes, backend):
                if isinstance(outcome, dict) and outcome.get("ok") is False:
                    view["error"] = outcome.get("error")
        if name == "set_subscriptions":
            result["routes"] = routes
            if isinstance(payload, dict):
                result["applied"] = payload.get("applied")
                result["failed"] = payload.get("failed")
        else:
            result.update(routes[0])
        if failed and isinstance(payload, dict) and payload.get("error"):
            result["error"] = payload["error"]
        return result, failed
    if name in READBACK_TOOLS:
        before_record = _record(before, request.get("device"))
        after_record = _device_after(name, request, after)
        device = _display(before_record, request.get("device"))
        before_change = _device_change(name, request, before_record, before, [])
        after_change = _device_change(name, request, after_record, after, [])
        change = {}
        for label, entry in before_change.items():
            if not isinstance(entry, dict):
                continue
            now = (after_change.get(label) or {}).get("from")
            change[label] = {"from": entry.get("from"), "to": entry.get("to"), "now": now}
        if name == "rename_channel" and not request.get("name"):
            verified = all(entry["now"] and entry["now"] != entry["from"] for entry in change.values())
        else:
            verified = all(entry["now"] == entry["to"] for entry in change.values())
        result = {"changed": not failed or verified, "operation": name, "device": device, "change": change}
        if failed:
            error = payload.get("error") if isinstance(payload, dict) else None
            result["error"] = error or f"the server answered {status}"
            if verified:
                result["note"] = (
                    f"{device} reports the requested value, so the change took effect although the server could "
                    "not confirm it in time"
                )
            else:
                result["note"] = (
                    f"{device} may still apply it; some devices stop answering for several seconds after a "
                    "change. Read the value again before retrying."
                )
            return result, not verified
        result["verified"] = verified
        if not verified and name == "rename_channel" and not request.get("name"):
            result["note"] = "the label did not change; it may already be the device default"
        elif not verified:
            result["note"] = f"{device} has not reported the new value yet; read it again in a few seconds"
        return result, False
    return payload if isinstance(payload, dict) else {"result": payload}, failed
