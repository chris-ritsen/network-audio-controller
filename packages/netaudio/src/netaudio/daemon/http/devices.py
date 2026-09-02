from __future__ import annotations

import asyncio
import logging

from netaudio.common.app_config import settings as app_settings
from netaudio.core.binding import STATUS_TIMEOUT, NetaudioCoreError
from netaudio.dante.const import RESULT_CODE_SUCCESS
from netaudio.dante.lock import validate_pin
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.sample_rate_topology import (
    SampleRateTopologyChangedButUnverifiedError,
    SampleRateTopologyError,
    SampleRateTopologyMutationOutcomeUnknownError,
    SampleRateTopologyReadbackError,
)
from netaudio.dante.services.notification import mutate_and_wait_for_capability_value

logger = logging.getLogger("netaudio")

FORGET_SELECTIONS = frozenset({"emulated", "offline"})


class DaemonDeviceHandlers:
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

    async def _handle_get_devices(self, writer):
        await self._send_json(writer, self._serialized_devices())

    async def _handle_get_device(self, writer, server_name):
        devices = self._serialized_devices()
        device_json = devices.get(server_name)
        if device_json is None:
            lowered = server_name.lower()
            for candidate in devices.values():
                identifiers = (
                    candidate.get("ddm_device_id"),
                    candidate.get("inventory_id"),
                    candidate.get("ipv4"),
                    candidate.get("name"),
                )
                if any(isinstance(value, str) and value.lower() == lowered for value in identifiers):
                    device_json = candidate
                    break

        if device_json is None:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        await self._send_json(writer, device_json)

    async def _handle_forget_device(self, writer, device_name):
        device = self._find_device(device_name)
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return
        await self._send_json(writer, {"forgotten": self._forget_devices([device])})

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
        await self._send_json(writer, {"forgotten": self._forget_devices(matched)})

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
        if device.ipv4 is None:
            await self._send_json(writer, {"error": "device has no IP address"}, 409)
            return

        interfaces = await self.application.probe_interface_status(str(device.ipv4))
        if interfaces is None:
            await self._send_json(writer, {"error": "interface status was not reported"}, 504)
            return

        device.interfaces = interfaces
        await self._send_json(
            writer,
            {
                "device": device.server_name,
                "interfaces": interfaces,
                "link_speed_mbps": device.link_speed_mbps,
                "reboot_required": device.interface_reboot_required,
                "pending_config": device.interface_pending_config,
            },
        )

    async def _handle_get_lock_status(self, writer, device_name):
        device = await self._require_online_device(writer, device_name)
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

        subscriptions = params.get("subscriptions")
        if subscriptions is not None:
            try:
                records = [(entry["rx_channel"], entry["tx_channel"], entry["tx_device"]) for entry in subscriptions]
            except (KeyError, TypeError) as exception:
                await self._send_json(writer, {"error": f"invalid subscription entry: {exception}"}, 400)
                return
            if not records:
                await self._send_json(writer, {"error": "subscriptions list is empty"}, 400)
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

            response = await self.application.add_subscriptions(device, records)
            if not await self._require_arc_write_success(writer, response, "subscription change"):
                return
            await self._send_json(writer, {"success": True, "count": len(records)})
            return

        rx_channel_number = params.get("rx_channel")
        tx_channel_name = params.get("tx_channel")
        tx_device_name = params.get("tx_device")
        if rx_channel_number is None or not tx_channel_name or not tx_device_name:
            await self._send_json(writer, {"error": "rx_channel, tx_channel, tx_device required"}, 400)
            return

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

        response = await self.application.add_subscriptions(
            device,
            [(rx_channel_number, tx_channel_name, tx_device_name)],
        )
        if not await self._require_arc_write_success(writer, response, "subscription change"):
            return
        await self._send_json(writer, {"success": True})

    async def _handle_unsubscribe(self, writer, params):
        device = await self._require_device(writer, params.get("rx_device"), "rx device not found")
        if not device:
            return

        rx_channel_numbers = params.get("rx_channels")
        if rx_channel_numbers:
            rx_channels = []
            for number in rx_channel_numbers:
                channel = device.rx_channels.get(number)
                if not channel:
                    await self._send_json(writer, {"error": f"rx channel {number} not found"}, 404)
                    return
                rx_channels.append(channel)

            response = await self.application.remove_subscriptions(
                device,
                [channel.number for channel in rx_channels],
            )
            if not await self._require_arc_write_success(writer, response, "subscription removal"):
                return
            await self._send_json(writer, {"success": True, "count": len(rx_channels)})
            return

        rx_channel = device.rx_channels.get(params.get("rx_channel"))
        if not rx_channel:
            await self._send_json(writer, {"error": "rx channel not found"}, 404)
            return

        response = await self.application.remove_subscriptions(device, [rx_channel.number])
        if not await self._require_arc_write_success(writer, response, "subscription removal"):
            return
        await self._send_json(writer, {"success": True})

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
        if name.strip():
            response = await self.application.set_channel_name(device, channel_type, channel_number, name)
        else:
            response = await self.application.reset_channel_name(device, channel_type, channel_number)
        if not await self._require_arc_write_success(writer, response, "channel name change"):
            return
        await self._send_json(writer, {"success": True})

    async def _require_arc_write_success(self, writer, response, operation):
        if not response:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
            return False
        try:
            from netaudio import core

            result_code = core.parse_response("result_code", response)
        except NetaudioCoreError as exception:
            await self._send_json(writer, {"error": f"invalid device response: {exception}"}, 500)
            return False
        if not isinstance(result_code, int):
            await self._send_json(writer, {"error": "invalid device response: missing result code"}, 500)
            return False
        if result_code != RESULT_CODE_SUCCESS:
            await self._send_json(
                writer,
                {
                    "error": f"device rejected {operation} with result 0x{result_code:04X}",
                    "result_code": result_code,
                },
                409,
            )
            return False
        return True

    async def _handle_set_latency(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        response = await self.application.set_latency(device, params.get("latency"))
        if not await self._require_arc_write_success(writer, response, "latency change"):
            return
        await self._send_json(writer, {"success": True})

    async def _handle_lock(self, writer, params):
        await self._handle_lock_operation(writer, params, locking=True)

    async def _handle_unlock(self, writer, params):
        await self._handle_lock_operation(writer, params, locking=False)

    async def _handle_lock_operation(self, writer, params, locking):
        device = await self._require_online_device(writer, params.get("device"))
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
        if device.ipv4 is None:
            await self._send_json(writer, {"error": "device has no IP address"}, 409)
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
        await self._set_and_verify_audio_capability(
            writer,
            device,
            requested_encoding,
            lambda value: self.application.send_set_encoding(device, value),
            self.application.probe_encoding_status,
            "encoding",
            "supported_encodings",
            "encoding",
        )

    async def _require_audio_capability_value(self, writer, value, field_name):
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 0xFFFFFFFF:
            await self._send_json(writer, {"error": f"{field_name} must be an integer from 1 through 4294967295"}, 400)
            return False
        return True

    async def _set_and_verify_audio_capability(
        self,
        writer,
        device,
        requested_value,
        set_value,
        probe_status,
        current_value_field,
        supported_values_field,
        capability_description,
    ):
        device_ip_address = str(device.ipv4)

        async def mutate() -> None:
            await set_value(requested_value)

        async def probe():
            return await probe_status(device_ip_address)

        try:
            status = await mutate_and_wait_for_capability_value(
                self.application.notifications,
                current_value_field,
                device_ip_address,
                requested_value,
                mutate,
                probe,
                self.audio_capability_verification_timeout,
            )
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return

        if status is None:
            await self._send_json(writer, {"error": f"{capability_description} readback was unavailable"}, 504)
            return

        observed_value, supported_values = status
        setattr(device, current_value_field, observed_value)
        setattr(device, supported_values_field, supported_values)
        if observed_value != requested_value:
            await self._send_json(
                writer,
                {
                    "error": f"{capability_description} change was not applied",
                    "observed": observed_value,
                    "supported": supported_values,
                },
                409,
            )
            return

        await self._send_json(writer, {"success": True})
