from __future__ import annotations

import asyncio
import logging
import math
import time

from netaudio import core
from netaudio.common.app_config import settings as app_settings
from netaudio.core.binding import STATUS_TIMEOUT, NetaudioCoreError
from netaudio.dante.flows import FlowValidationError
from netaudio.dante.discovery import discovery_destination
from netaudio.dante.lock import validate_pin
from netaudio.dante.application import CapabilityProbeTimeout
from netaudio.dante.arc_protocol import ArcProtocolError, require_arc_protocol_for_device
from netaudio.dante.channel import channels_by_number
from netaudio.dante.channel_frontend import ChannelRenameCapabilityError, require_channel_rename_supported
from netaudio.dante.network_configuration import (
    network_snapshot,
    probe_switch_configuration_if_reported,
)
from netaudio.dante.operation_availability import operation_availability
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyError,
    SampleRateTopologyMutationOutcomeUnknownError,
    SampleRateTopologyReadbackError,
)
from netaudio.dante.self_connection import (
    SelfConnectionCapabilityUnavailableError,
    SelfConnectionUnsupportedError,
)
from netaudio.dante.subscription_operations import plan_receiver_subscription_commands
from netaudio.daemon.subscription_batching import (
    parse_routes,
    plan_batches,
)

logger = logging.getLogger("netaudio")

FORGET_SELECTIONS = frozenset({"emulated", "offline"})


class DaemonDeviceHandlers:
    async def _handle_diagnostics(
        self, writer, name, *, include_clock=True, receiver_history=True, reset=False, warning_enabled=None
    ):
        device = self._find_device(name)
        if device is None:
            await self._send_json(writer, {"error": "Device not found"}, 404)
            return
        if self.diagnostics is None:
            await self._send_json(writer, {"error": "Diagnostics are not available"}, 503)
            return
        await self._send_json(
            writer,
            self.diagnostics.diagnostics_snapshot(
                device,
                include_clock=include_clock,
                receiver_history=receiver_history,
                reset=reset,
                warning_enabled=warning_enabled,
            ),
        )

    async def _handle_reset_diagnostics(self, writer, data):
        await self._handle_diagnostics(writer, data.get("device", ""), reset=True)

    async def _handle_diagnostics_policy(self, writer, data):
        enabled = data.get("clock_variation_warnings")
        if type(enabled) is not bool:
            await self._send_json(writer, {"error": "clock_variation_warnings must be a boolean"}, 400)
            return
        await self._handle_diagnostics(writer, data.get("device", ""), warning_enabled=enabled)

    async def _send_self_connection_capability_error(self, writer, error):
        unavailable = isinstance(error, SelfConnectionCapabilityUnavailableError)
        await self._send_json(
            writer,
            {
                "error": str(error),
                "self_connection_capability": "unavailable" if unavailable else "unsupported",
                "mutation_sent": False,
            },
            503 if unavailable else 409,
        )

    async def _handle_get_shure_devices(self, writer):
        if not self.shure:
            await self._send_json(writer, {})
            return
        result = {}
        for mac, device in self.shure.devices.items():
            result[mac] = device.to_json()
        await self._send_json(writer, result)

    async def _handle_get_shure_device(self, writer, mac):
        if not self.shure:
            await self._send_json(writer, {"error": "shure not available"}, 404)
            return
        device = self.shure.devices.get(mac)
        if not device:
            for m, d in self.shure.devices.items():
                if d.name and d.name.lower() == mac.lower():
                    device = d
                    break
                if d.ip == mac:
                    device = d
                    break
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return
        await self._send_json(writer, device.to_json())

    async def _handle_get_devices(self, writer, context_name=None):
        await self._send_json(writer, self._serialized_devices(context_name))

    async def _handle_get_device(self, writer, server_name, context_name=None):
        devices = self._serialized_devices(context_name)
        device_json = devices.get(server_name)
        if device_json is None:
            lowered = server_name.lower()
            matches = []
            for candidate in devices.values():
                identifiers = (
                    candidate.get("ddm_device_id"),
                    candidate.get("inventory_id"),
                    candidate.get("ipv4"),
                    candidate.get("name"),
                )
                if any(isinstance(value, str) and value.lower() == lowered for value in identifiers):
                    matches.append(candidate)
            if len(matches) > 1:
                await self._send_json(
                    writer,
                    {"error": "multiple devices matched; use the context-qualified inventory ID or server name"},
                    409,
                )
                return
            if matches:
                device_json = matches[0]

        if device_json is None:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        await self._send_json(writer, device_json)

    async def _handle_forget_device(self, writer, device_name):
        device = self._find_device(device_name)
        if not device:
            records = (
                self._serialized_devices()
                if self.managed_inventory is not None and self.managed_inventory.enabled
                else {}
            )
            matches = [
                (key, record)
                for key, record in records.items()
                if device_name.lower()
                in {str(record.get(field, "")).lower() for field in ("server_name", "inventory_id", "name")}
                or key == device_name
            ]
            if len(matches) > 1:
                await self._send_json(writer, {"error": "multiple devices matched; use the inventory ID"}, 409)
                return
            if matches and matches[0][1].get("online") is False:
                forgotten = self._dismiss_inventory_records(matches)
                await self.publish_inventory_snapshot()
                await self._send_json(writer, {"forgotten": forgotten})
                return
            await self._send_json(writer, {"error": "device not found"}, 404)
            return
        records = (
            self._serialized_devices() if self.managed_inventory is not None and self.managed_inventory.enabled else {}
        )
        forgotten = self._forget_devices([device])
        record = records.get(device.server_name)
        if record and record.get("online") is False and record.get("ddm_device_id"):
            self._dismiss_inventory_records([(device.server_name, record)])
            await self.publish_inventory_snapshot()
        await self._send_json(writer, {"forgotten": forgotten})

    async def _handle_forget_devices(self, writer, query):
        selections = {
            selection.strip()
            for raw_value in query.get("selection", [])
            for selection in raw_value.split(",")
            if selection.strip()
        }
        unknown_selections = sorted(selections - FORGET_SELECTIONS)
        if not selections or unknown_selections:
            await self._send_json(
                writer,
                {"error": f"selection must be one or more of: {', '.join(sorted(FORGET_SELECTIONS))}"},
                400,
            )
            return
        matched = [
            device
            for device in self.application.devices.values()
            if ("offline" in selections and not device.online)
            or ("emulated" in selections and device.kind == "emulated")
        ]
        records = (
            self._serialized_devices() if self.managed_inventory is not None and self.managed_inventory.enabled else {}
        )
        forgotten = self._forget_devices(matched)
        if "offline" in selections:
            extra = [
                (key, record)
                for key, record in records.items()
                if record.get("online") is False and record.get("ddm_device_id")
            ]
            existing = {entry["server_name"] for entry in forgotten}
            dismissed = self._dismiss_inventory_records(extra)
            forgotten.extend(entry for entry in dismissed if entry["server_name"] not in existing)
        await self.publish_inventory_snapshot()
        await self._send_json(writer, {"forgotten": forgotten})

    def _dismiss_inventory_records(self, records):
        forgotten = []
        for key, record in records:
            self._dismissed_offline_inventory.add(key)
            forgotten.append({field: record.get(field) for field in ("ipv4", "kind", "name", "online", "server_name")})
        return forgotten

    def _forget_devices(self, devices):
        forgotten = []
        for device in devices:
            forgotten.append(
                {
                    "ipv4": str(device.ipv4) if device.ipv4 else None,
                    "kind": device.kind,
                    "name": device.name,
                    "online": device.online,
                    "server_name": device.server_name,
                }
            )
            logger.info(f"Forgetting cached device {device.server_name}")
            self.forget_device(device.server_name)
        return forgotten

    async def _handle_get_interfaces(self, writer, device_name):
        device = await self._require_device(writer, device_name)
        if not device:
            return
        if not device.online:
            await self._send_json(writer, {"error": "device is offline"}, 409)
            return
        if not getattr(device, "requires_managed_control", False) and device.ipv4 is None:
            await self._send_json(writer, {"error": "device has no IP address"}, 409)
            return

        try:
            interfaces = await self.application.probe_interface_status(device)
            await probe_switch_configuration_if_reported(self.application, device)
        except (CapabilityProbeTimeout, TimeoutError):
            await self._send_json(writer, {"error": "The device did not respond to the network settings query"}, 504)
            return
        if interfaces is None:
            await self._send_json(writer, {"error": "interface status was not reported"}, 504)
            return

        device.interfaces = interfaces
        await self._send_json(
            writer,
            {
                "device": device.server_name,
                **network_snapshot(device),
            },
        )

    async def _handle_get_lock_status(self, writer, device_name):
        device = await self._require_lock_device(writer, device_name)
        if not device:
            return

        observation = await self.application.probe_lock_status(
            str(device.ipv4),
            timeout=app_settings.lock_state_timeout,
        )
        if observation is None:
            self._invalidate_cached_lock_status(device)
            await self._send_json(
                writer,
                {
                    "error": "lock status was not reported",
                    "device": device.server_name,
                    "is_locked": None,
                },
                504,
            )
            return

        self._apply_lock_status_observation(device, observation)
        await self._send_json(writer, self._lock_status_payload(device, observation))

    async def _handle_subscribe(self, writer, params):
        rx_device_name = params.get("rx_device")
        device = await self._require_device(writer, rx_device_name, "rx device not found")
        if not device:
            return

        batch = "subscriptions" in params
        entries = (
            params["subscriptions"]
            if batch
            else [{key: params.get(key) for key in ("rx_channel", "tx_channel", "tx_device")}]
        )

        try:
            if not isinstance(entries, list):
                raise ValueError("subscriptions must be a non-empty list")

            for entry in entries:
                if not isinstance(entry, dict) or set(entry) - {"rx_channel", "tx_channel", "tx_device"}:
                    raise ValueError("invalid subscription entry")

            routes = parse_routes([{**entry, "rx_device": rx_device_name} for entry in entries])

            if any(route.clears for route in routes):
                raise ValueError("tx_channel and tx_device are required")
        except ValueError as error:
            await self._send_json(writer, {"error": str(error)}, 400)
            return

        records = [(route.rx_channel, route.tx_channel, route.tx_device) for route in routes]

        try:
            response = await self.application.add_subscriptions(device, records)
        except ArcProtocolError as error:
            await self._send_json(writer, {"error": str(error)}, 409)
            return
        except (SelfConnectionCapabilityUnavailableError, SelfConnectionUnsupportedError) as error:
            await self._send_self_connection_capability_error(writer, error)
            return
        if not await self._require_arc_write_success(writer, response, "subscription change"):
            return

        for rx_channel_number, tx_channel_name, tx_device_name in records:
            await self._broadcast_sse(
                {
                    "event": "subscription_pending",
                    "action": "add",
                    "rx_device": rx_device_name,
                    "rx_channel": rx_channel_number,
                    "tx_channel": tx_channel_name,
                    "tx_device": tx_device_name,
                }
            )

        self.subscription_readback.request(device, records)
        result = {"success": True}

        if batch:
            result["count"] = len(records)

        await self._send_json(writer, result)

    async def _handle_subscribe_external_rtp(self, writer, params):
        device = await self._require_device(writer, params.get("rx_device"), "rx device not found")
        if not device:
            return
        try:
            session_id = params.get("session_id")
            if not isinstance(session_id, str) or not session_id.isascii() or not session_id.isdecimal():
                raise ValueError("external session ID must be a decimal string")
            flow = self.application.external_flows.get(params.get("source_ipv4"), int(session_id))
        except (TypeError, ValueError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        if flow is None:
            await self._send_json(writer, {"error": "external flow not found"}, 404)
            return
        if flow.expires_monotonic <= time.monotonic() or params.get("content_sha256") != flow.content_sha256:
            await self._send_json(writer, {"error": "source announcement expired or changed; refresh the source"}, 409)
            return
        try:
            result = await self.application.subscribe_external_rtp(
                device,
                flow,
                params.get("receiver_channel_ids"),
                params.get("flow_slot_assignments"),
                receiver_supports_multiple_interfaces=getattr(device, "switch_redundancy_supported", None) is True,
            )
        except FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return
        except (NetaudioCoreError, OSError, RuntimeError, TimeoutError, ValueError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 502)
            return
        if not result.get("mutation_sent", True):
            await self._send_json(writer, {"error": result["message"], **result}, 504)
            return
        if not result["request_acknowledged"]:
            status = 504 if result["result_code"] is None else 409
            await self._send_json(writer, {"error": "external subscription was not acknowledged", **result}, status)
            return
        if result.get("arc_effective_state_confirmed") is False:
            await self._send_json(writer, {"error": result["message"], **result}, 502)
            return
        status = 200 if result.get("arc_effective_state_confirmed") is True else 202
        await self._send_json(writer, {"success": True, **result}, status)

    async def _handle_unsubscribe(self, writer, params):
        device = await self._require_device(writer, params.get("rx_device"), "rx device not found")
        if not device:
            return

        batch = "rx_channels" in params
        numbers = params["rx_channels"] if batch else [params.get("rx_channel")]

        try:
            if not isinstance(numbers, list):
                raise ValueError("rx_channels must be a non-empty list")

            routes = parse_routes([{"rx_device": params["rx_device"], "rx_channel": number} for number in numbers])
        except ValueError as error:
            await self._send_json(writer, {"error": str(error)}, 400)
            return

        try:
            channels = channels_by_number(device.rx_channels.values())
        except RuntimeError as error:
            await self._send_json(writer, {"error": str(error)}, 409)
            return

        for route in routes:
            if route.rx_channel not in channels:
                await self._send_json(writer, {"error": f"rx channel {route.rx_channel} not found"}, 404)
                return

        numbers = [route.rx_channel for route in routes]

        try:
            response = await self.application.remove_subscriptions(device, numbers)
        except ArcProtocolError as error:
            await self._send_json(writer, {"error": str(error)}, 409)
            return

        if not await self._require_arc_write_success(writer, response, "subscription removal"):
            return

        self.subscription_readback.request(device, [(number, "", "") for number in numbers])
        result = {"success": True}

        if batch:
            result["count"] = len(numbers)

        await self._send_json(writer, result)

    async def _handle_apply_subscriptions(self, writer, params):
        try:
            routes = parse_routes(params.get("routes"))
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return

        devices = {route.rx_device: self._find_device(route.rx_device) for route in routes}
        receivers = set()

        for route in routes:
            device = devices[route.rx_device]

            if device is None:
                continue

            receiver = (device.server_name, route.rx_channel)

            if receiver in receivers:
                await self._send_json(writer, {"error": "routes address the same receiver channel more than once"}, 400)
                return

            receivers.add(receiver)

        results = {(route.rx_device, route.rx_channel): {**route.to_dict(), "ok": False} for route in routes}
        for route in routes:
            if devices[route.rx_device] is None:
                results[(route.rx_device, route.rx_channel)]["error"] = "rx device not found"
        try:
            plan = plan_batches(
                [route for route in routes if devices[route.rx_device] is not None],
                lambda name: require_arc_protocol_for_device(devices[name])["subscription_batch_limit"],
            )

            for rx_device, batches in plan.items():
                device = devices[rx_device]

                if getattr(device, "requires_managed_control", False):
                    continue

                plan_receiver_subscription_commands(
                    device,
                    [
                        (
                            {"action": "clear", "rx_channel": route.rx_channel}
                            if route.clears
                            else {
                                "action": "set",
                                "rx_channel": route.rx_channel,
                                "tx_channel": route.tx_channel,
                                "tx_device": route.tx_device,
                            }
                        )
                        for batch in batches
                        for route in batch.routes
                    ],
                )
        except (ArcProtocolError, NetaudioCoreError) as error:
            await self._send_json(writer, {"error": str(error)}, 409)
            return

        async def apply_device(rx_device, batches):
            device = devices[rx_device]
            for batch in batches:
                for route in batch.routes:
                    await self._broadcast_sse(
                        {
                            "event": "subscription_pending",
                            "action": "remove" if batch.action == "clear" else "add",
                            "rx_device": rx_device,
                            "rx_channel": route.rx_channel,
                            "tx_channel": route.tx_channel or "",
                            "tx_device": route.tx_device or "",
                        }
                    )
                records = [(route.rx_channel, route.tx_channel or "", route.tx_device or "") for route in batch.routes]
                try:
                    if batch.action == "clear":
                        response = await self.application.remove_subscriptions(device, [r[0] for r in records])
                    else:
                        response = await self.application.add_subscriptions(device, records)
                except (NetaudioCoreError, OSError, RuntimeError, TimeoutError, ValueError) as exception:
                    failure = str(exception)
                else:
                    failure = self._arc_write_failure(response, f"subscription {batch.action}")
                for route in batch.routes:
                    result = results[(rx_device, route.rx_channel)]
                    if failure:
                        result["error"] = failure
                    else:
                        result["ok"] = True
                if not failure:
                    self.subscription_readback.request(device, records)

        await asyncio.gather(*(apply_device(rx_device, batches) for rx_device, batches in plan.items()))
        ordered = [results[(route.rx_device, route.rx_channel)] for route in routes]
        applied = sum(1 for result in ordered if result["ok"])
        await self._send_json(
            writer,
            {
                "success": applied == len(ordered),
                "applied": applied,
                "failed": len(ordered) - applied,
                "batches": {rx_device: len(batches) for rx_device, batches in plan.items()},
                "routes": ordered,
            },
            200 if applied else 409,
        )

    async def _handle_identify(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        await self.application.identify(device)
        await self._broadcast_sse(
            {
                "event": "identify_started",
                "server_name": device.server_name,
                "duration": 6,
            }
        )
        await self._send_json(writer, {"accepted": True, "verified": False}, 202)

    async def _handle_rename_device(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        name = params.get("name")
        if not isinstance(name, str):
            await self._send_json(writer, {"error": "name must be a string"}, 400)
            return
        if name.strip():
            response = await self.application.set_device_name(device, name)
        else:
            response = await self.application.reset_device_name(device)
        if not await self._require_arc_write_success(writer, response, "device name change"):
            return
        await self._send_json(writer, {"success": True})

    async def _handle_rename_channel(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        name = params.get("name")
        if not isinstance(name, str):
            await self._send_json(writer, {"error": "name must be a string"}, 400)
            return
        channel_type = params.get("channel_type")
        channel_number = params.get("channel_number")
        try:
            require_channel_rename_supported(device, channel_type, channel_number)
        except ChannelRenameCapabilityError as error:
            await self._send_json(
                writer,
                {"error": str(error), "mutation_sent": False},
                409 if error.prohibited else 503,
            )

            return

        if name.strip():
            response = await self.application.set_channel_name(device, channel_type, channel_number, name)
        else:
            response = await self.application.reset_channel_name(device, channel_type, channel_number)
        if not await self._require_arc_write_success(writer, response, "channel name change"):
            return
        await self._send_json(writer, {"success": True})

    @staticmethod
    def _arc_write_error(response, operation) -> tuple[int, dict] | None:
        from netaudio.ddm.device_transport import ManagedOperationResult

        if isinstance(response, ManagedOperationResult):
            if response.successful:
                return None

            return 409, {"error": f"device rejected {operation}"}

        if not response:
            return 504, {"error": "device did not respond"}

        try:
            from netaudio import core

            acknowledgement = core.parse_response("command_acknowledgement", response)
        except NetaudioCoreError as exception:
            return 500, {"error": f"invalid device response: {exception}"}

        if not acknowledgement["accepted"]:
            return 409, {"error": f"device rejected {operation}", "result_code": acknowledgement["result_code"]}

        return None

    @classmethod
    def _arc_write_failure(cls, response, operation) -> str | None:
        error = cls._arc_write_error(response, operation)

        return error[1]["error"] if error is not None else None

    async def _require_arc_write_success(self, writer, response, operation):
        error = self._arc_write_error(response, operation)

        if error is None:
            return True

        status, body = error
        await self._send_json(writer, body, status)

        return False

    async def _handle_set_latency(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        latency = params.get("latency")
        if (
            isinstance(latency, bool)
            or not isinstance(latency, (int, float))
            or not math.isfinite(latency)
            or latency < 0
        ):
            await self._send_json(
                writer, {"error": "latency must be a finite, nonnegative number of milliseconds"}, 400
            )
            return
        result = await self.application.set_latency(device, latency)

        if result["state"] == "rejected":
            await self._send_json(writer, {"error": "device rejected latency change"}, 409)
            return

        if result["state"] == "unavailable":
            await self._send_json(writer, {"error": "latency readback was unavailable; refresh before retrying"}, 504)
            return

        self._emit_device_updated(device)

        if not result["effective_state_confirmed"]:
            await self._send_json(
                writer,
                {"error": "latency change was not applied", "configured_latency_ms": result["configured_latency_ms"]},
                409,
            )
            return
        await self._send_json(writer, {"success": True})

    async def _handle_lock(self, writer, params):
        await self._handle_lock_operation(writer, params, locking=True)

    async def _handle_unlock(self, writer, params):
        await self._handle_lock_operation(writer, params, locking=False)

    async def _handle_lock_operation(self, writer, params, locking):
        device = await self._require_lock_device(writer, params.get("device"))
        if not device:
            return

        lock_key = self._get_lock_key()
        if not lock_key:
            await self._send_json(writer, {"error": "device_lock_key not configured"}, 503)
            return

        pin = params.get("pin")
        error = validate_pin(pin or "")
        if error:
            await self._send_json(writer, {"error": error}, 400)
            return

        device_ip_address = str(device.ipv4)
        async with self._device_lock_operation_lock(device_ip_address):
            if locking:
                result = await self.application.lock_device(device, pin, lock_key)
            else:
                result = await self.application.unlock_device(device, pin, lock_key)

            if not isinstance(result, dict):
                self._invalidate_cached_lock_status(device)
                await self._send_json(writer, {"error": "invalid device response"}, 500)
                return
            if result.get("success") is not True:
                status = 504 if result.get("status") == STATUS_TIMEOUT else 409
                if status == 504:
                    self._invalidate_cached_lock_status(device)
                await self._send_json(writer, result, status)
                return

            # The operation acknowledgement is not authoritative lock state.
            # Once a mutation succeeds, only a subsequent 0x1009 observation can
            # make the cached state known again.
            self._invalidate_cached_lock_status(device)
            observation = await self.application.probe_lock_status(
                device_ip_address,
                timeout=app_settings.lock_state_timeout,
            )
            if observation is None:
                await self._send_json(
                    writer,
                    {
                        "error": "lock status readback was not reported",
                        "device": device.server_name,
                        "is_locked": None,
                        "operation_result": result,
                    },
                    504,
                )
                return

            self._apply_lock_status_observation(device, observation)
            lock_status = self._lock_status_payload(device, observation)
            if observation.is_locked is not locking:
                await self._send_json(
                    writer,
                    {
                        "error": "lock operation did not reach the requested state",
                        "requested_is_locked": locking,
                        **lock_status,
                        "operation_result": result,
                    },
                    409,
                )
                return

            await self._send_json(writer, {**result, **lock_status})

    def _device_lock_operation_lock(self, device_ip_address: str) -> asyncio.Lock:
        lock = self._device_lock_operation_locks.get(device_ip_address)
        if lock is None:
            lock = asyncio.Lock()
            self._device_lock_operation_locks[device_ip_address] = lock
        return lock

    def _invalidate_cached_lock_status(self, device) -> None:
        changed = device.is_locked is not None or getattr(device, "lock_reset_status", None) is not None
        device.is_locked = None
        device.lock_reset_status = None
        if changed:
            self._emit_device_updated(device)

    def _apply_lock_status_observation(self, device, observation) -> None:
        lock_reset_status = observation.lock_reset_status
        changed = (
            device.is_locked is not observation.is_locked
            or getattr(device, "lock_reset_status", None) != lock_reset_status
        )
        device.is_locked = observation.is_locked
        device.lock_reset_status = lock_reset_status
        if changed:
            self._emit_device_updated(device)

    def _emit_device_updated(self, device) -> None:
        self.application.dispatcher.emit_nowait(
            DanteEvent(
                type=EventType.DEVICE_UPDATED,
                device_name=device.name,
                server_name=device.server_name,
            )
        )

    async def _require_online_device(self, writer, device_name):
        device = await self._require_device(writer, device_name)
        if not device:
            return None
        if not device.online:
            await self._send_json(writer, {"error": "device is offline"}, 409)
            return None
        if device.ipv4 is None and not getattr(device, "requires_managed_control", False):
            await self._send_json(writer, {"error": "device has no IP address"}, 409)
            return None
        return device

    async def _require_lock_device(self, writer, device_name):
        device = await self._require_online_device(writer, device_name)
        if device is not None and getattr(device, "requires_managed_control", False):
            await self._send_json(writer, {"error": "device lock is not available through DDM"}, 409)
            return None
        return device

    @staticmethod
    def _lock_status_payload(device, observation):
        return {
            "device": device.server_name,
            "is_locked": observation.is_locked,
            "lock_state_code": observation.lock_state_code,
            "status_code": observation.status_code,
            "observed_at": observation.observed_at,
            "observation_source": "observed_after_0x1008",
            "operation_availability": operation_availability(device, "locking").to_dict(),
        }

    def _get_lock_key(self):
        if app_settings.device_lock_key:
            return app_settings.device_lock_key
        from netaudio.common.key_extract import extract_lock_key

        key = extract_lock_key()
        if key:
            app_settings.device_lock_key = key
            logger.info("Extracted device lock key from Dante Controller")
        return key

    async def _handle_refresh(self, writer, params):
        device_name = params.get("device")
        if device_name:
            device = await self._require_device(writer, device_name)
            if not device:
                return
            await self.state.refresh_device(device.server_name)
        else:
            await self.state.refresh_all_devices()
        await self._send_json(writer, {"success": True})

    async def _handle_discovery_refresh(self, writer, params):
        try:
            address = discovery_destination(params.get("address"))
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        if self.refresh_discovery is None:
            await self._send_json(writer, {"error": "mDNS discovery is not running"}, 503)
            return
        try:
            result = await self.refresh_discovery(address)
        except (RuntimeError, OSError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 503)
            return
        await self._send_json(writer, {"success": True, **result})

    @staticmethod
    def _peer_is_loopback(writer):
        peername = writer.get_extra_info("peername")
        if not peername:
            return False
        return peername[0] in ("127.0.0.1", "::1", "::ffff:127.0.0.1")

    async def _handle_shutdown(self, writer, params):
        await self._send_json(writer, {"success": True})
        if self.on_shutdown is not None:
            self.on_shutdown()

    async def _handle_report_unresponsive(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        if device.online:
            logger.info(f"Device reported unresponsive, marking offline candidate: {device.server_name}")
            self.mark_offline(device.server_name)
        await self._send_json(writer, {"success": True})

    async def _handle_metering_status(self, writer):
        if not self.metering:
            await self._send_json(writer, {})
            return
        await self._send_json(writer, self.metering.get_status())

    async def _handle_metering_cache(self, writer):
        """Return fresh cache contents without starting detailed metering."""
        if not self.metering:
            await self._send_json(writer, {})
            return
        await self._send_json(writer, self.metering.get_cached_levels_by_server())

    async def _handle_metering_snapshot(self, writer, name):
        device = self._find_device(name)
        if not device or not device.ipv4:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return
        if not self.metering:
            await self._send_json(writer, {"error": "metering not available"}, 503)
            return

        levels = await self.metering.snapshot(device.server_name, timeout=3.0)
        if levels is None:
            await self._send_json(writer, {"error": "no metering data"}, 504)
            return

        tx_names = {}
        if device.tx_channels:
            for channel in device.tx_channels.values():
                tx_names[channel.number] = channel.friendly_name or channel.name
        rx_names = {}
        if device.rx_channels:
            for channel in device.rx_channels.values():
                rx_names[channel.number] = channel.friendly_name or channel.name

        response = {
            "tx": {},
            "rx": {},
            "wall_time": levels.get("wall_time"),
            "source_ip": levels.get("source_ip"),
            "source_port": levels.get("source_port"),
            "metering_source": levels.get("metering_source"),
        }
        tx_signal_presence = levels.get("tx_signal_presence", {})
        rx_signal_presence = levels.get("rx_signal_presence", {})
        for channel_number, level in levels.get("tx", {}).items():
            response["tx"][channel_number] = {
                "name": tx_names.get(channel_number, ""),
                "level": level,
            }
            if channel_number in tx_signal_presence:
                response["tx"][channel_number]["signal_presence"] = tx_signal_presence[channel_number]
        for channel_number, level in levels.get("rx", {}).items():
            response["rx"][channel_number] = {
                "name": rx_names.get(channel_number, ""),
                "level": level,
            }
            if channel_number in rx_signal_presence:
                response["rx"][channel_number]["signal_presence"] = rx_signal_presence[channel_number]
        await self._send_json(writer, response)

    async def _handle_metering_start(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        if getattr(device, "requires_managed_control", False):
            await self._send_json(
                writer,
                {
                    "error": "Detailed metering through DDM is not implemented yet. DDM login enables inventory and supported device controls, not this meter stream."
                },
                409,
            )
            return
        client_id = params.get("client_id", "daemon_http")
        if not self.metering:
            await self._send_json(writer, {"error": "metering not available"}, 503)
            return
        self.metering.add_persistent(device.server_name, client_id)
        await self._send_json(writer, {"success": True})

    async def _handle_metering_stop(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        client_id = params.get("client_id", "daemon_http")
        if not self.metering:
            await self._send_json(writer, {"error": "metering not available"}, 503)
            return
        self.metering.remove_persistent(device.server_name, client_id)
        await self._send_json(writer, {"success": True})

    async def _handle_set_sample_rate(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        requested_sample_rate = params.get("sample_rate")
        if not await self._require_audio_capability_value(writer, requested_sample_rate, "sample_rate"):
            return
        confirm_destructive = params.get("confirm_destructive", False)
        if not isinstance(confirm_destructive, bool):
            await self._send_json(writer, {"error": "confirm_destructive must be a boolean"}, 400)
            return
        try:
            result = await self.application.set_sample_rate(
                device,
                requested_sample_rate,
                confirm_destructive=confirm_destructive,
                timeout=self.audio_capability_verification_timeout,
            )
        except SampleRateTopologyChangedButUnverifiedError as exception:
            payload = {
                "error": str(exception),
                "change_sent": True,
                "state_verified": False,
                "observed_sample_rate_hertz": exception.observed_sample_rate_hertz,
            }
            if exception.preflight is not None:
                payload["preflight"] = exception.preflight.to_dict()
            await self._send_json(writer, payload, 502)
            return
        except SampleRateTopologyMutationOutcomeUnknownError as exception:
            payload = {
                "error": str(exception),
                "mutation_attempted": True,
                "state_verified": False,
            }
            if exception.preflight is not None:
                payload["preflight"] = exception.preflight.to_dict()
            await self._send_json(writer, payload, 502)
            return
        except SampleRateTopologyReadbackError as exception:
            payload = {"error": str(exception)}
            if exception.preflight is not None:
                payload["preflight"] = exception.preflight.to_dict()
            await self._send_json(writer, payload, 504)
            return
        except (SampleRateTopologyError, ValueError) as exception:
            payload = {"error": str(exception)}
            if isinstance(exception, SampleRateTopologyError) and exception.preflight is not None:
                payload["preflight"] = exception.preflight.to_dict()
            await self._send_json(writer, payload, 409)
            return
        await self._send_json(writer, result.to_dict())

    async def _handle_set_encoding(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        requested_encoding = params.get("encoding")

        if not await self._require_audio_capability_value(writer, requested_encoding, "encoding"):
            return

        try:
            status = await self.application.set_encoding(
                device, requested_encoding, timeout=self.audio_capability_verification_timeout
            )
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return

        result = core.audio_capability_readback(status, requested_encoding)

        if result["state"] == "unavailable":
            await self._send_json(writer, {"error": "encoding readback was unavailable"}, 504)
            return

        if not result["effective_state_confirmed"]:
            await self._send_json(
                writer,
                {
                    "error": "encoding change was not applied",
                    "observed": result["current_value"],
                    "supported": status["available_values"],
                },
                409,
            )
            return

        await self._send_json(writer, {"success": True})

    async def _require_audio_capability_value(self, writer, value, field_name):
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 0xFFFFFFFF:
            await self._send_json(writer, {"error": f"{field_name} must be an integer from 1 through 4294967295"}, 400)
            return False

        return True
