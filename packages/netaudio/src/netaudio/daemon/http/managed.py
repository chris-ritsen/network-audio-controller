from __future__ import annotations

import logging

from netaudio.ddm import ManagedAPIError

logger = logging.getLogger("netaudio")


class DaemonManagedHandlers:
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
