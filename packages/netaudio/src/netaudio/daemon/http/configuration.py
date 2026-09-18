from __future__ import annotations

import ipaddress
import json

from netaudio.dante import flows
from netaudio.dante.flow_lifecycle import (
    create_transmit_flow,
    delete_transmit_flow,
    inspect_transmit_flows,
    plan_create_transmit_flow,
)
from netaudio.dante.network_configuration import (
    NetworkConfigurationError,
    NetworkConfigurationUnverified,
    interface_configuration,
    network_snapshot,
    validate_interface_configuration,
)
from netaudio.dante.transmit_flow import FlowLifecycleState, TransmitFlowSpecification

STATUS_TEXT = {
    200: "OK",
    202: "Accepted",
    400: "Bad Request",
    404: "Not Found",
    409: "Conflict",
    500: "Internal Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}


class DaemonConfigurationHandlers:
    async def _handle_device_controls(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        action = params.get("action", "inspect")
        try:
            if action == "inspect":
                result = await self.application.inspect_device_controls(device)
            elif action in {"plan", "apply"}:
                operation = (
                    self.application.plan_device_control if action == "plan" else self.application.apply_device_control
                )
                result = await operation(
                    device,
                    params.get("category"),
                    params.get("requested"),
                    confirm_clear=params.get("confirm_clear") is True,
                )
            else:
                raise ValueError("Action must be inspect, plan or apply.")
        except (ValueError, TypeError, RuntimeError) as exc:
            await self._send_json(writer, {"error": str(exc)}, 409)
            return
        success = action != "apply" or result.get("effective_state_confirmed") is True
        await self._send_json(writer, {"success": success, **result}, 200 if success else 409)

    @staticmethod
    def _performance_result_status(result) -> int:
        return {
            "confirmed": 200,
            "request_acknowledged": 202,
            "contradicted": 409,
            "rejected": 409,
            "unverified": 504,
        }.get(result.state, 500)

    async def _handle_performance_operation(self, writer, params, operation_name, *arguments):
        device = await self._require_online_device(writer, params.get("device"))
        if not device:
            return
        try:
            result = await getattr(self.application, operation_name)(device, *arguments)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        except RuntimeError as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        payload = result.to_dict()
        status = self._performance_result_status(result)
        if status >= 400:
            payload["error"] = result.message
        await self._send_json(writer, payload, status)

    async def _handle_set_receive_flow_performance(self, writer, params):
        await self._handle_performance_operation(
            writer,
            params,
            "set_receive_flow_performance",
            params.get("latency_microseconds"),
            params.get("frames_per_packet"),
        )

    async def _handle_set_transmit_flow_performance(self, writer, params):
        await self._handle_performance_operation(
            writer,
            params,
            "set_transmit_flow_performance",
            params.get("latency_microseconds"),
            params.get("frames_per_packet"),
        )

    async def _handle_set_unicast_performance(self, writer, params):
        await self._handle_performance_operation(
            writer,
            params,
            "set_unicast_performance",
            params.get("latency_microseconds"),
            params.get("frames_per_packet"),
        )

    async def _handle_set_receive_flow_default_slots(self, writer, params):
        await self._handle_performance_operation(
            writer,
            params,
            "set_receive_flow_default_slots",
            params.get("default_slots"),
        )

    async def _handle_store_current_configuration(self, writer, params):
        await self._handle_performance_operation(writer, params, "store_current_configuration")

    async def _handle_get_transmit_flows(self, writer, device_name):
        device = await self._require_online_device(writer, device_name)
        if not device:
            return
        try:
            inventory = await inspect_transmit_flows(device)
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return
        await self._send_json(writer, {"device": device.server_name, **inventory})

    async def _handle_plan_transmit_flow(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        try:
            specification = TransmitFlowSpecification.from_dict(params.get("specification"))
            plan = plan_create_transmit_flow(device, specification)
        except (TypeError, ValueError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        await self._send_json(writer, {"device": device.server_name, "plan": plan.to_dict()})

    @staticmethod
    def _transmit_flow_result_status(result) -> int:
        return {
            FlowLifecycleState.CONFIRMED: 200,
            FlowLifecycleState.DELETED: 200,
            FlowLifecycleState.PARTIAL: 202,
            FlowLifecycleState.PENDING: 202,
            FlowLifecycleState.INCONSISTENT: 502,
            FlowLifecycleState.REJECTED: 409,
            FlowLifecycleState.UNSUPPORTED: 409,
        }.get(result.state, 500)

    async def _handle_create_transmit_flow(self, writer, params):
        if params.get("confirmed") is not True:
            await self._send_json(writer, {"error": "confirmed must be true"}, 400)
            return
        device = await self._require_online_device(writer, params.get("device"))
        if not device:
            return
        try:
            operation = getattr(self.application, "create_transmit_flow", None)
            if operation is None:
                specification = TransmitFlowSpecification.from_dict(params.get("specification"))
                result = await self.operation_recorder.run_operation(
                    device,
                    "create_transmit_flow",
                    specification.to_dict(),
                    lambda: create_transmit_flow(device, specification),
                )
            else:
                result = await operation(device, params.get("specification"))
        except (TypeError, ValueError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        payload = result.to_dict()
        status = self._transmit_flow_result_status(result)
        if status >= 400:
            payload["error"] = result.message
        await self._send_json(writer, payload, status)

    async def _handle_delete_transmit_flow(self, writer, params):
        if params.get("confirmed") is not True:
            await self._send_json(writer, {"error": "confirmed must be true"}, 400)
            return
        device = await self._require_online_device(writer, params.get("device"))
        if not device:
            return
        try:
            operation = getattr(self.application, "delete_transmit_flow", None)
            if operation is None:
                flow_id = flows.validate_flow_slot(params.get("flow_id"))
                result = await self.operation_recorder.run_operation(
                    device,
                    "delete_transmit_flow",
                    {"flow_id": flow_id},
                    lambda: delete_transmit_flow(device, flow_id),
                )
            else:
                result = await operation(device, params.get("flow_id"))
        except flows.FlowValidationError as exception:
            await self._send_json(writer, {"error": str(exception)}, exception.status)
            return
        payload = result.to_dict()
        status = self._transmit_flow_result_status(result)
        if status >= 400:
            payload["error"] = result.message
        await self._send_json(writer, payload, status)

    async def _handle_set_gain(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        channel_number = params.get("channel_number")
        gain_level = params.get("gain_level")
        device_type = params.get("device_type", "")
        try:
            status = await self.application.set_gain_level(device, channel_number, gain_level, device_type)
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

    async def _handle_set_clock_configuration(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        changes = params.get("changes")
        if not isinstance(changes, dict):
            await self._send_json(writer, {"error": "changes must be an object"}, 400)
            return
        from netaudio.monitoring.model import _json_safe

        result = await self.application.set_clock_configuration(
            device, changes, record_revision=params.get("record_revision")
        )
        result["success"] = result["effective_state_confirmed"]
        await self._send_json(writer, _json_safe(result), 200 if result["success"] else 409)

    async def _handle_refresh_clock(self, writer, params):
        from netaudio.monitoring.model import _json_safe

        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        parsed = await self.application.probe_clocking_status(device, record_revision=params.get("record_revision"))
        await self._send_json(writer, {"success": True, **_json_safe(parsed)})

    async def _handle_set_aes67(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        if device.aes67_configuration_supported is False:
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
        await self.application.reboot(device)
        await self._send_json(writer, {"accepted": True, "verified": False}, 202)

    async def _handle_set_interface(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        mode = params.get("mode")
        if mode not in ("dhcp", "static"):
            await self._send_json(writer, {"error": "mode must be 'dhcp' or 'static'"}, 400)
            return

        interface = params.get("interface", "primary")
        configuration = {
            "ip_address": params.get("ip"),
            "netmask": params.get("netmask"),
            "dns_server": params.get("dns"),
            "gateway": params.get("gateway"),
        }
        try:
            expected = validate_interface_configuration(mode, configuration)
            result = await self.application.set_interface(device, mode, configuration, interface=interface)
            configured = interface_configuration(result, interface).get("configured") or {}
            if not all(configured.get(key) == value for key, value in expected.items()):
                raise NetworkConfigurationUnverified("Interface change could not be verified")
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        except NetworkConfigurationUnverified as exception:
            await self._send_json(
                writer,
                {"error": str(exception), **({"mutation": exception.evidence} if exception.evidence else {})},
                502,
            )
            return
        except (NetworkConfigurationError, RuntimeError, TimeoutError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        device.interfaces = result
        await self._send_json(writer, {"success": True, **network_snapshot(device)})

    async def _handle_get_redundancy(self, writer, device_name):
        device = await self._require_online_device(writer, device_name)
        if not device:
            return
        try:
            status = await self.application.probe_dante_redundancy(device)
        except (NetworkConfigurationError, RuntimeError, TimeoutError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        await self._send_json(writer, {"device": device.server_name, "redundancy": status})

    async def _handle_set_redundancy(self, writer, params):
        device = await self._require_online_device(writer, params.get("device"))
        if not device:
            return
        try:
            status = await self.application.set_dante_redundancy(device, params.get("mode"))
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        except NetworkConfigurationUnverified as exception:
            await self._send_json(
                writer,
                {"error": str(exception), **({"mutation": exception.evidence} if exception.evidence else {})},
                502,
            )
            return
        except (NetworkConfigurationError, RuntimeError, TimeoutError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 409)
            return
        await self._send_json(
            writer,
            {
                "success": status.get("effective_state_confirmation") is True,
                "redundancy": status.get("effective_readback"),
                "mutation": status,
            },
        )

    async def _require_device(self, writer, name, error="device not found"):
        device = self._find_device(name)
        if not device:
            await self._send_json(writer, {"error": error}, 404)
        return device

    def _direct_device_for_record(self, record):
        mac_address = str(record.get("mac_address") or "").replace(":", "").replace("-", "").lower()
        name = str(record.get("name") or "").lower()
        ipv4 = str(record.get("ipv4") or "")
        matches = [
            candidate
            for candidate in self.application.devices.values()
            if (
                mac_address
                and str(candidate.mac_address or "").replace(":", "").replace("-", "").lower() == mac_address
            )
            or (name and candidate.name and candidate.name.lower() == name)
            or (ipv4 and candidate.ipv4 and str(candidate.ipv4) == ipv4)
        ]
        return matches[0] if len(matches) == 1 else None

    def _find_device(self, name):
        if not name:
            return None

        managed_inventory = getattr(self, "managed_inventory", None)
        if managed_inventory is not None and managed_inventory.enabled:
            records = self._serialized_devices()
            record = records.get(name)
            if record is None:
                lowered = name.lower()
                matches = [
                    candidate
                    for candidate in records.values()
                    if any(
                        isinstance(value, str) and value.lower() == lowered
                        for value in (
                            candidate.get("ddm_device_id"),
                            candidate.get("inventory_id"),
                            candidate.get("ipv4"),
                            candidate.get("name"),
                        )
                    )
                ]
                record = matches[0] if len(matches) == 1 else None
            if record is not None:
                device = self.application.devices.get(record["server_name"]) or self._direct_device_for_record(record)
                if device is not None or record.get("management_state") == "managed":
                    return device

        device = self.application.devices.get(name)
        if device:
            return device
        lowered = name.lower()
        matches = [
            candidate
            for candidate in self.application.devices.values()
            if (candidate.name and candidate.name.lower() == lowered)
            or (candidate.ipv4 and str(candidate.ipv4) == name)
        ]
        return matches[0] if len(matches) == 1 else None

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
