from __future__ import annotations

import ipaddress
import json

from netaudio.dante import flows
from netaudio.dante.const import RESULT_CODE_SUCCESS


STATUS_TEXT = {
    200: "OK",
    202: "Accepted",
    400: "Bad Request",
    404: "Not Found",
    409: "Conflict",
    500: "Internal Server Error",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}


class DaemonConfigurationHandlers:
    async def _handle_set_gain(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        channel_number = params.get("channel_number")
        gain_level = params.get("gain_level")
        device_type = params.get("device_type", "")
        try:
            status = await device.operations.set_gain_level(channel_number, gain_level, device_type)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        if status is None:
            await self._send_json(writer, {"error": "gain readback was unavailable"}, 504)
            return
        observed_device_type, channel_levels = status
        channel_index = channel_number - 1
        observed_level = channel_levels[channel_index] if 0 <= channel_index < len(channel_levels) else None
        if observed_device_type != device_type or observed_level != gain_level:
            await self._send_json(
                writer,
                {
                    "error": "gain change was not applied",
                    "observed_device_type": observed_device_type,
                    "observed_level": observed_level,
                },
                409,
            )
            return
        await self._send_json(writer, {"success": True})

    async def _handle_set_preferred_leader(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        expected = params.get("preferred")
        if not isinstance(expected, bool):
            await self._send_json(writer, {"error": "preferred must be a boolean"}, 400)
            return
        observed = await self.application.set_preferred_leader(device, expected)
        if observed is None:
            await self._send_json(writer, {"error": "preferred leader readback was unavailable"}, 504)
            return
        if observed != expected:
            await self._send_json(
                writer,
                {"error": "preferred leader change was not applied", "observed": observed},
                409,
            )
            return
        await self._send_json(writer, {"success": True})

    async def _handle_set_clock_source(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        clock_source = params.get("clock_source")
        if isinstance(clock_source, bool) or not isinstance(clock_source, int) or not 0 <= clock_source <= 0xFFFF:
            await self._send_json(writer, {"error": "clock_source must be an integer from 0 through 65535"}, 400)
            return
        try:
            observed = await self.application.set_clock_source(device, clock_source)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        if observed is None:
            await self._send_json(writer, {"error": "clock source readback was unavailable"}, 504)
            return
        if observed != clock_source:
            await self._send_json(
                writer,
                {"error": "clock source change was not applied", "observed": observed},
                409,
            )
            return
        await self._send_json(writer, {"success": True, "clock_source": observed})

    async def _handle_set_clock_subdomain(self, writer, params):
        from netaudio.dante.clock_config import format_clock_subdomain, parse_clock_subdomain_selection

        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        subdomain = params.get("subdomain")
        if not isinstance(subdomain, str):
            await self._send_json(writer, {"error": "subdomain must be an ASCII string, hex:<bytes>, or unset"}, 400)
            return
        try:
            requested = parse_clock_subdomain_selection(subdomain)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        try:
            observed = await self.application.set_clock_subdomain(device, requested)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        if observed is None:
            await self._send_json(writer, {"error": "clock subdomain readback was unavailable"}, 504)
            return
        if observed != requested:
            await self._send_json(
                writer,
                {
                    "error": "clock subdomain change was not applied",
                    "observed": format_clock_subdomain(observed),
                },
                409,
            )
            return
        await self._send_json(
            writer,
            {"success": True, "subdomain": format_clock_subdomain(observed)},
        )

    async def _handle_refresh_clock(self, writer, params):
        from netaudio.dante.clock_config import format_clock_subdomain

        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        parsed = await self.application.probe_clocking_status(device)
        if parsed is None:
            await self._send_json(writer, {"error": "clock status readback was unavailable"}, 504)
            return
        clock_subdomain = parsed.get("clock_subdomain")
        await self._send_json(
            writer,
            {
                "success": True,
                "clock_source_code": parsed["clock_source_code"],
                "clock_subdomain": list(clock_subdomain) if clock_subdomain is not None else None,
                "clock_subdomain_label": format_clock_subdomain(clock_subdomain),
                "preferred_leader": parsed.get("preferred_leader"),
                "clock_role": parsed.get("clock_role"),
                "clock_identity": parsed.get("clock_identity"),
                "leader_clock_identity": parsed.get("leader_clock_identity"),
            },
        )

    async def _handle_set_aes67(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        if device.aes67_supported is False:
            await self._send_json(writer, {"error": "device does not support AES67 configuration"}, 409)
            return
        expected = params.get("enabled")
        if not isinstance(expected, bool):
            await self._send_json(writer, {"error": "enabled must be a boolean"}, 400)
            return
        result = await self.application.set_aes67_enabled(device, expected)
        configured = result[1] if result is not None else None
        if configured is None:
            await self._send_json(writer, {"error": "AES67 readback was unavailable"}, 504)
            return
        if configured != expected:
            await self._send_json(
                writer,
                {"error": "AES67 change was not applied", "observed": configured},
                409,
            )
            return
        await self._send_json(writer, {"success": True})

    async def _handle_set_aes67_multicast_prefix(self, writer, params):
        from netaudio.dante.device import device_advertises_aes67_multicast_prefix

        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        if not device_advertises_aes67_multicast_prefix(device):
            await self._send_json(writer, {"error": "device does not advertise an AES67 multicast prefix"}, 409)
            return
        prefix = params.get("prefix")
        if not isinstance(prefix, str) or not prefix:
            await self._send_json(writer, {"error": "prefix must be an IPv4 address"}, 400)
            return
        try:
            prefix = str(ipaddress.IPv4Address(prefix))
        except (ipaddress.AddressValueError, ValueError):
            await self._send_json(writer, {"error": "prefix must be an IPv4 address"}, 400)
            return
        try:
            observed = await self.application.set_aes67_multicast_prefix(device, prefix)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        if observed is None:
            await self._send_json(writer, {"error": "AES67 multicast prefix readback was unavailable"}, 504)
            return
        if observed != prefix:
            await self._send_json(
                writer,
                {"error": "AES67 multicast prefix change was not applied", "observed": observed},
                409,
            )
            return
        await self._send_json(writer, {"success": True, "prefix": observed})

    async def _handle_set_sample_rate_pullup(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        raw_value = params.get("raw_value")
        if raw_value is None and params.get("value") is not None:
            from netaudio.dante.sample_rate_pullup import parse_sample_rate_pullup_selection

            try:
                raw_value = parse_sample_rate_pullup_selection(str(params.get("value")))
            except ValueError as exception:
                await self._send_json(writer, {"error": str(exception)}, 400)
                return
        if isinstance(raw_value, bool) or not isinstance(raw_value, int) or not 0 <= raw_value <= 0xFFFFFFFF:
            await self._send_json(
                writer,
                {"error": "raw_value must be an integer from 0 through 4294967295"},
                400,
            )
            return
        try:
            result = await self.application.set_sample_rate_pullup(device, raw_value)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        if result is None:
            await self._send_json(writer, {"error": "sample-rate pull-up readback was unavailable"}, 504)
            return
        observed_raw_value, supported_raw_values = result
        if observed_raw_value != raw_value:
            await self._send_json(
                writer,
                {
                    "error": "sample-rate pull-up change was not applied",
                    "observed": observed_raw_value,
                    "supported": supported_raw_values,
                },
                409,
            )
            return
        await self._send_json(
            writer,
            {
                "success": True,
                "raw_value": observed_raw_value,
                "supported": supported_raw_values,
            },
        )

    async def _handle_reboot(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        await device.operations.reboot()
        await self._send_json(writer, {"accepted": True, "verified": False}, 202)

    async def _handle_set_interface(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        mode = params.get("mode", "").lower()
        if mode not in ("dhcp", "static"):
            await self._send_json(writer, {"error": "mode must be 'dhcp' or 'static'"}, 400)
            return

        device_ip = str(device.ipv4)
        ip_address = ""
        netmask = ""
        if mode == "dhcp":
            result = await self.application.set_interface_dhcp(device_ip)
        else:
            ip_address = params.get("ip")
            netmask = params.get("netmask")
            if not isinstance(ip_address, str) or not ip_address or not isinstance(netmask, str) or not netmask:
                await self._send_json(writer, {"error": "static mode requires ip, netmask"}, 400)
                return
            result = await self.application.set_interface_static(
                device_ip, ip_address, netmask, params.get("dns") or "", params.get("gateway") or ""
            )
        if result is None:
            await self._send_json(writer, {"error": "interface readback was unavailable"}, 504)
            return
        expected_mode = "dynamic" if mode == "dhcp" else "static"
        candidates = list(result)
        pending_config = device.interface_pending_config
        if isinstance(pending_config, dict):
            candidates.append(pending_config)
        expected_fields = {"mode": expected_mode}
        if mode == "static":
            expected_fields.update(
                {
                    "ip_address": ip_address,
                    "netmask": netmask,
                    "dns_server": params.get("dns") or "",
                    "gateway": params.get("gateway") or "",
                }
            )
        matched = any(
            all(candidate.get(key) == value for key, value in expected_fields.items()) for candidate in candidates
        )
        if not matched:
            await self._send_json(
                writer,
                {"error": "interface change was not applied", "interfaces": result},
                409,
            )
            return
        await self._send_json(
            writer,
            {
                "success": True,
                "reboot_required": device.interface_reboot_required,
                "interfaces": result,
            },
        )

    async def _handle_get_tx_flows(self, writer, device_name):
        snapshot = await self._tx_flow_snapshot(writer, device_name)
        if snapshot is None:
            return

        device, flow_protocol_id, flow_inventory = snapshot
        await self._send_json(
            writer,
            {
                "device": device.server_name,
                "flow_protocol_id": flow_protocol_id,
                "max_flow_slots": flow_inventory["max_flow_slots"],
                "flows": flow_inventory["flows"],
            },
        )

    async def _handle_create_tx_flow(self, writer, params):
        if params.get("confirmed") is not True:
            await self._send_json(writer, {"error": "confirmed must be true"}, 400)
            return

        try:
            flow_slot = flows.validate_flow_slot(params.get("flow_slot"))
            channel_numbers = flows.validate_flow_channels(params.get("channels"))
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return

        device_name = params.get("device")
        snapshot = await self._tx_flow_snapshot(writer, device_name)
        if snapshot is None:
            return
        device, flow_protocol_id, flow_inventory = snapshot
        device_flows = flow_inventory["flows"]

        available_channels = {int(number) for number in (device.tx_channels or {}).keys()}
        try:
            flows.require_creatable_flow_protocol(flow_protocol_id)
            flows.require_supported_flow_slot(flow_slot, flow_inventory["max_flow_slots"])
            flows.require_available_tx_channels(channel_numbers, available_channels)
            flows.require_available_flow_slot(device_flows, flow_slot)
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return

        async with device.topology_mutation_lock:
            result_code = await flows.create_tx_flow(
                str(device.ipv4), self._flow_arc_port(device), flow_protocol_id, flow_slot, channel_numbers
            )
        if result_code is None:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
            return
        if result_code != RESULT_CODE_SUCCESS:
            await self._send_json(
                writer,
                {
                    "error": f"device rejected flow creation with result 0x{result_code:04X}",
                    "result_code": result_code,
                },
                409,
            )
            return

        await self._send_json(
            writer,
            {
                "success": True,
                "flow_protocol_id": flow_protocol_id,
                "flow_slot": flow_slot,
                "channels": channel_numbers,
            },
        )

    async def _handle_delete_tx_flow(self, writer, params):
        if params.get("confirmed") is not True:
            await self._send_json(writer, {"error": "confirmed must be true"}, 400)
            return

        try:
            flow_slot = flows.validate_flow_slot(params.get("flow_slot"))
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return

        snapshot = await self._tx_flow_snapshot(writer, params.get("device"))
        if snapshot is None:
            return
        device, flow_protocol_id, flow_inventory = snapshot
        device_flows = flow_inventory["flows"]

        try:
            flows.require_deletable_flow_protocol(flow_protocol_id, flow_slot)
            flows.require_multicast_flow(device_flows, flow_slot)
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return

        async with device.topology_mutation_lock:
            result_code = await flows.delete_tx_flow(
                str(device.ipv4), self._flow_arc_port(device), flow_protocol_id, flow_slot
            )
        if result_code is None:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
            return
        if result_code != RESULT_CODE_SUCCESS:
            await self._send_json(
                writer,
                {
                    "error": f"device rejected flow deletion with result 0x{result_code:04X}",
                    "result_code": result_code,
                },
                409,
            )
            return

        await self._send_json(
            writer,
            {
                "success": True,
                "flow_protocol_id": flow_protocol_id,
                "flow_slot": flow_slot,
            },
        )

    async def _tx_flow_snapshot(self, writer, device_name):
        device = await self._require_device(writer, device_name)
        if not device:
            return None
        if not device.online:
            await self._send_json(writer, {"error": "device is offline"}, 503)
            return None
        if not device.ipv4:
            await self._send_json(writer, {"error": "device has no control address"}, 503)
            return None

        flow_protocol_id = device.flow_protocol_id
        if flow_protocol_id is None:
            flow_protocol_id = await flows.detect_flow_protocol(str(device.ipv4), self._flow_arc_port(device))
            if flow_protocol_id is None:
                await self._send_json(writer, {"error": "flow protocol is not supported or did not respond"}, 503)
                return None
            device.flow_protocol_id = flow_protocol_id

        flow_inventory = await flows.query_preferred_tx_flow_inventory(
            str(device.ipv4), self._flow_arc_port(device), flow_protocol_id
        )
        if flow_inventory is None:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
            return None
        return device, flow_protocol_id, flow_inventory

    @staticmethod
    def _flow_arc_port(device) -> int:
        return device._arc_port()

    async def _require_device(self, writer, name, error="device not found"):
        device = self._find_device(name)
        if not device:
            await self._send_json(writer, {"error": error}, 404)
        return device

    def _find_device(self, name):
        if not name:
            return None
        device = self.application.devices.get(name)
        if device:
            return device
        for server_name, candidate in self.application.devices.items():
            if candidate.name and candidate.name.lower() == name.lower():
                return candidate
            if candidate.ipv4 and str(candidate.ipv4) == name:
                return candidate
        return None

    async def _send_json(self, writer, data, status=200):
        body = json.dumps(data, default=str).encode()
        status_text = STATUS_TEXT.get(status, "Error")
        response = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Cache-Control: no-store\r\n"
            f"Access-Control-Allow-Origin: *\r\n"
            f"\r\n"
        ).encode() + body
        writer.write(response)
        await writer.drain()
