from __future__ import annotations

import logging

from netaudio.ddm import ManagedAPIError

logger = logging.getLogger("netaudio")


class DaemonManagedHandlers:
    async def _handle_ddm_create_domain(self, writer, params):
        server_name = params.get("server")
        name = params.get("name")
        if not isinstance(server_name, str) or not isinstance(name, str) or not name.strip():
            await self._send_json(writer, {"error": "Choose a DDM server and enter a domain name"}, 400)
            return
        registry = self.managed_inventory
        service = registry.services.get(server_name) if registry is not None else None
        if service is None or not service.enabled:
            await self._send_json(writer, {"error": "Log in to the selected DDM server first"}, 409)
            return
        try:
            result = await service.client.execute_async(
                "mutation DomainAdd($input: DomainAddInput!) { DomainAdd(input: $input) { ok domain { id name } } }",
                {"input": {"name": name.strip(), "icon": "OTHER"}},
                "DomainAdd",
            )
        except ManagedAPIError:
            await self._send_json(writer, {"error": "DDM request failed. Check the domain list before retrying."}, 502)
            return
        payload = (result.data or {}).get("DomainAdd") or {}
        if result.errors or not payload.get("ok"):
            await self._send_json(
                writer, {"error": "DDM rejected domain creation. Check your permissions and the domain name."}, 409
            )
            return
        await service.refresh()
        await self.publish_inventory_snapshot()
        await self._send_json(writer, {"accepted": True, "domain": payload.get("domain")}, 201)

    async def _handle_ddm_enrollment(self, writer, params):
        server_name = params.get("server")
        device_id = params.get("device_id")
        action = params.get("action")
        if not isinstance(server_name, str) or not isinstance(device_id, str) or action not in {"enroll", "unenroll"}:
            await self._send_json(writer, {"error": "Select a server, device, and enrollment action"}, 400)
            return
        registry = self.managed_inventory
        service = registry.services.get(server_name) if registry is not None else None
        if service is None or not service.enabled:
            await self._send_json(writer, {"error": "Log in to the selected DDM server first"}, 409)
            return
        if not await service.refresh():
            await self._send_json(writer, {"error": "Could not verify current DDM inventory"}, 502)
            return
        matches = [
            device
            for device in self._serialized_devices().values()
            if device.get("ddm_server_profile") == server_name and device.get("ddm_device_id") == device_id
        ]
        if len(matches) != 1 or not matches[0].get("online"):
            await self._send_json(writer, {"error": "Device is not uniquely available on that server"}, 409)
            return
        enrolled = bool(matches[0].get("ddm_domain_id"))
        if enrolled != (action == "unenroll"):
            await self._send_json(writer, {"error": "Device enrollment has changed; use its current state"}, 409)
            return
        operation = "DevicesEnroll" if action == "enroll" else "DevicesUnenroll"
        values = {"deviceIds": [device_id], "clearConfig": False}
        if action == "enroll":
            domain_id = params.get("domain_id")
            if not isinstance(domain_id, str) or not any(
                domain.get("id") == domain_id and domain.get("ddm_server_profile") == server_name
                for domain in registry.domains()
            ):
                await self._send_json(writer, {"error": "Choose a domain on the device's DDM server"}, 400)
                return
            values["domainId"] = domain_id
        try:
            result = await service.client.execute_async(
                f"mutation {operation}($input: {operation}Input!) {{ {operation}(input: $input) {{ ok }} }}",
                {"input": values},
                operation,
            )
        except ManagedAPIError:
            await self._send_json(
                writer, {"error": "DDM enrollment request failed; check the current device state before retrying"}, 502
            )
            return
        if result.errors or not ((result.data or {}).get(operation) or {}).get("ok"):
            await self._send_json(writer, {"error": "DDM rejected the enrollment request"}, 409)
            return
        await service.refresh()
        await self.publish_inventory_snapshot()
        await self._send_json(writer, {"accepted": True})

    async def _handle_ddm_graphql(self, writer, params):
        if self.managed_inventory is None or not self.managed_inventory.enabled:
            await self._send_json(writer, {"error": "Managed API is not configured"}, 409)
            return
        query = params.get("query")
        variables = params.get("variables")
        operation_name = params.get("operation_name")
        context_name = params.get("context")
        if not isinstance(query, str) or not query.strip():
            await self._send_json(writer, {"error": "query must be a non-empty string"}, 400)
            return
        if variables is not None and not isinstance(variables, dict):
            await self._send_json(writer, {"error": "variables must be an object"}, 400)
            return
        if operation_name is not None and not isinstance(operation_name, str):
            await self._send_json(writer, {"error": "operation_name must be a string"}, 400)
            return
        if context_name is not None and not isinstance(context_name, str):
            await self._send_json(writer, {"error": "context must be a string"}, 400)
            return
        try:
            client = self.managed_inventory.client_for_context(context_name)
            result = await client.execute_async(query, variables, operation_name)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        except ManagedAPIError as exception:
            await self._send_json(writer, {"error": str(exception)}, 502)
            return
        await self._send_json(writer, result.to_json())

    async def _handle_ddm_refresh(self, writer, params):
        if self.managed_inventory is None or not self.managed_inventory.enabled:
            await self._send_json(writer, {"error": "Managed API is not configured"}, 409)
            return
        context_name = params.get("context")
        if context_name is not None and not isinstance(context_name, str):
            await self._send_json(writer, {"error": "context must be a string"}, 400)
            return
        try:
            refreshed = await self.managed_inventory.refresh(context_name)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        await self._send_json(writer, self.managed_inventory.status(), 200 if refreshed else 502)

    async def _handle_get_ddm_devices(self, writer, context_name=None):
        devices = {
            key: value
            for key, value in self._serialized_devices(context_name).items()
            if "ddm" in (value.get("inventory_sources") or [])
        }
        await self._send_json(writer, devices)

    async def _handle_get_ddm_domains(self, writer, context_name=None):
        if self.managed_inventory is None or not self.managed_inventory.enabled:
            await self._send_json(writer, [])
            return
        domains = self.managed_inventory.domains()
        if context_name is not None:
            domains = [domain for domain in domains if domain.get("ddm_context") == context_name]
        await self._send_json(writer, domains)

    async def _handle_get_ddm_status(self, writer):
        if self.managed_inventory is None:
            await self._send_json(writer, {"enabled": False, "state": "disabled"})
            return
        await self._send_json(writer, self.managed_inventory.status())
