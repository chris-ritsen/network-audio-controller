"""Correlated panel requests. Every mutation is sent once; verification uses queries."""

from __future__ import annotations

import asyncio
from collections import defaultdict

from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.panel_plan import matches, plan_panel
from netaudio.dante.panel_state import (
    QUERY_SELECTORS,
    observation_time,
    observe_panel,
    panel_family,
    panel_permission,
    panel_snapshot,
)


class PanelTransport:
    def __init__(self, application):
        self.application = application
        self.sequence = 0
        self.pending = set()
        self.locks = defaultdict(asyncio.Lock)

    def publish(self, device):
        self.application.dispatcher.emit_nowait(
            DanteEvent(
                type=EventType.DEVICE_UPDATED,
                server_name=device.server_name,
                device_name=device.name or "",
                data={"reason": "panel_status"},
            )
        )

    def unavailable(self, device, category):
        observation = device.device_controls.get("observations", {}).get(category)
        if observation and observation.get("available"):
            observation["available"] = False
            self.publish(device)

    def next_sequence(self):
        while True:
            self.sequence = (self.sequence + 1) & 0xFFFFFFFF
            if self.sequence and self.sequence not in self.pending:
                return self.sequence

    def require(self, device, *, write=False):
        reason = panel_permission(device, write=write)
        if reason:
            raise RuntimeError(reason)

    async def exchange(self, device, request, category, timeout, *, write=False):
        self.require(device, write=write)
        sequence = self.next_sequence()
        self.pending.add(sequence)
        requester = 0  # Only the established direct transport is supported here.
        key = self.application._control_key(device)

        def accepts(status):
            return (
                status.get("requester") == requester
                and status.get("sequence") == sequence
                and not status.get("diagnostic_error")
                and any(o["category"] == category for o in status.get("observations", []))
            )

        waiter = self.application.notifications.register_waiter("panel_status", key, accept=accepts)
        try:
            self.require(device, write=write)
            await self.application._send_settings(
                device, self.application.commands.panel_control(request, requester, sequence)
            )
            try:
                await asyncio.wait_for(waiter.wait(), timeout)
            except asyncio.TimeoutError:
                return None
            status = waiter.latest_result
            if not accepts(status):
                return None
            status = {**status, "correlated": True}
            status.setdefault("observed_at_unix", observation_time())
            if observe_panel(device, status):
                self.application.dispatcher.emit_nowait(
                    DanteEvent(
                        type=EventType.DEVICE_UPDATED,
                        server_name=device.server_name,
                        device_name=device.name or "",
                        data={"reason": "panel_status"},
                    )
                )
            return status
        finally:
            self.pending.discard(sequence)
            self.application.notifications.unregister_waiter(waiter)

    async def query(self, device, category, timeout=1.0):
        family = panel_family(device)
        selector = QUERY_SELECTORS.get(family or "", {}).get(category)
        if selector is None:
            raise RuntimeError("No query is established for that device and setting.")
        return await self.exchange(
            device,
            {"operation": "bluetooth_query" if family == "bluetooth" else "video_query", "selector": selector},
            category,
            timeout,
        )

    async def inspect(self, device, *, timeout=1.0):
        self.require(device)
        async with self.locks[device.server_name]:
            for category in QUERY_SELECTORS.get(panel_family(device) or "", {}):
                result = await self.query(device, category, timeout)
                if result is None:
                    self.unavailable(device, category)
                elif category == "visca":
                    value = device.device_controls.get("observations", {}).get("visca", {}).get("value", {})
                    if value.get("capability") == 1:
                        await self.exchange(device, {"operation": "video_visca_query"}, "visca", timeout)
            return panel_snapshot(device)

    async def plan(self, device, category, requested, *, confirm_clear=False, timeout=1.0):
        self.require(device)
        async with self.locks[device.server_name]:
            return await self._plan(device, category, requested, confirm_clear, timeout)

    async def _plan(self, device, category, requested, confirm_clear, timeout):
        needed = [category]
        if category in {"video_format", "bandwidth"}:
            needed += ["video_format", "visca"] if category == "video_format" else ["video_format"]
        for item in dict.fromkeys(needed):
            result = await self.query(device, item, timeout)
            if result is None:
                self.unavailable(device, item)
        return plan_panel(device, category, requested, confirm_clear=confirm_clear)

    async def apply(self, device, category, requested, *, confirm_clear=False, timeout=2.0):
        self.require(device, write=True)
        async with self.locks[device.server_name]:
            plan = await self._plan(device, category, requested, confirm_clear, min(timeout, 1.0))
            result = {
                "plan": plan,
                "request_sent": False,
                "request_acknowledged": None,
                "transport_reply": False,
                "correlated_status": False,
                "effective_state_confirmed": plan["action"] == "unchanged",
                "persistence": "unknown",
                "media_readiness": "unknown",
                "sent_operations": [],
            }
            if plan["action"] != "change":
                return result
            family = panel_family(device)
            for message in plan["requests"]:
                reason = panel_permission(device, write=True)
                if reason or panel_family(device) != family:
                    return {**result, "reason": reason or "Panel identity changed during the operation."}
                sequence = self.next_sequence()
                self.pending.add(sequence)
                key = self.application._control_key(device)
                waiter = self.application.notifications.register_waiter(
                    "panel_status",
                    key,
                    accept=lambda status, seq=sequence: status.get("requester") == 0 and status.get("sequence") == seq,
                )
                try:
                    # The mutation never retries. Polling below uses new query sequences.
                    result["request_attempted"] = True
                    await self.application._send_settings(
                        device, self.application.commands.panel_control(message, 0, sequence)
                    )
                    result["request_sent"] = True
                    result["sent_operations"].append(message["operation"])
                    try:
                        await asyncio.wait_for(waiter.wait(), min(timeout, 0.1))
                        result["transport_reply"] = True
                    except asyncio.TimeoutError:
                        pass
                except (RuntimeError, OSError) as exc:
                    return {
                        **result,
                        "reason": str(exc),
                        "outcome": "partial" if result["request_sent"] else "unavailable",
                    }
                finally:
                    self.application.notifications.unregister_waiter(waiter)
                    self.pending.discard(sequence)
            deadline = asyncio.get_running_loop().time() + timeout
            while asyncio.get_running_loop().time() < deadline:
                remaining = deadline - asyncio.get_running_loop().time()
                try:
                    status = await self.query(device, category, min(remaining, 0.5))
                except (RuntimeError, OSError) as exc:
                    return {**result, "reason": str(exc)}
                if status:
                    result["correlated_status"] = True
                    for observation in status["observations"]:
                        if observation["category"] == category and matches(observation["value"], plan["expected"]):
                            result["effective_state_confirmed"] = True
                            return result
                await asyncio.sleep(min(0.05, max(0, deadline - asyncio.get_running_loop().time())))
            return result


def audit_control_result(result):
    return {
        "state": "confirmed" if result["effective_state_confirmed"] else "unverified",
        "requested_values": result["plan"]["requested"],
        "effective_values": result["plan"].get("expected") if result["effective_state_confirmed"] else None,
        "effective_state_confirmation": True if result["effective_state_confirmed"] else None,
        "persistence_confirmation": None,
        "request_sent": result["request_sent"],
        "transport_reply": result["transport_reply"],
        "correlated_status": result["correlated_status"],
    }
