from __future__ import annotations

import asyncio
import re

from netaudio.common.config_loader import default_config_path, load_config_document
from netaudio.common.ddm_config_store import (
    logout_ddm_server,
    save_ddm_context,
    save_ddm_server,
    set_default_ddm_context,
)
from netaudio.common.managed_api import DDM_NAME_PATTERN, resolve_ddm_configuration
from netaudio.ddm.client import ManagedAPIClient, ManagedAPIError, authenticate_with_password


class DaemonConnectionHandlers:
    def _connection_configuration(self):
        path = default_config_path()
        return resolve_ddm_configuration(load_config_document(path), base_directory=path.parent), path

    def _connection_state(self, configuration=None):
        if configuration is None:
            configuration, _ = self._connection_configuration()
        return {
            "default_context": configuration.default_context,
            "servers": [
                {"name": server.name, "url": server.url, "configured": server.enabled}
                for server in configuration.servers.values()
            ],
            "contexts": [
                {
                    "name": context.name,
                    "server": context.server,
                    "domain_id": context.domain_id,
                    "domain_name": context.domain_name,
                }
                for context in configuration.contexts.values()
            ],
        }

    async def _reload_connections(self, changed_server=None):
        configuration, _ = self._connection_configuration()
        await self.managed_inventory.reconfigure(configuration, changed_server)
        self.application._managed_transports.clear()
        await self.publish_inventory_snapshot()

    async def _handle_get_connections(self, writer):
        try:
            state = self._connection_state()
        except (OSError, ValueError):
            await self._send_json(writer, {"error": "Saved DDM configuration could not be read"}, 500)
            return
        await self._send_json(writer, state)

    async def _handle_ddm_login(self, writer, params):
        if self.managed_inventory is None:
            await self._send_json(writer, {"error": "DDM support is unavailable"}, 503)
            return
        async with self._connection_lock:
            await self._login_and_save(writer, params)

    async def _handle_ddm_logout(self, writer, params):
        if self.managed_inventory is None:
            await self._send_json(writer, {"error": "DDM support is unavailable"}, 503)
            return
        async with self._connection_lock:
            try:
                configuration, path = self._connection_configuration()
                server = configuration.server(params.get("server"))
                await asyncio.to_thread(logout_ddm_server, path, server.name)
                await self._reload_connections(server.name)
            except (ValueError, TypeError):
                await self._send_json(writer, {"error": "Select a saved DDM server"}, 400)
                return
            except OSError:
                await self._send_json(writer, {"error": "DDM logout could not be saved"}, 500)
                return
            await self._send_json(writer, self._connection_state())

    async def _login_and_save(self, writer, params):
        try:
            configuration, path = self._connection_configuration()
            name = params.get("server")
            if not isinstance(name, str) or DDM_NAME_PATTERN.fullmatch(name) is None:
                raise ValueError("Choose a server profile name using letters, numbers, dots, dashes, or underscores")
            existing = configuration.servers.get(name)
            url = params.get("url") or (existing.url if existing else None)
            if not isinstance(url, str) or not url.strip():
                raise ValueError("Enter the DDM server URL")
            if existing is not None and url != existing.url:
                raise ValueError("That profile belongs to a different server; choose a new profile name")
            method = params.get("method")
            if method == "password":
                credential = await asyncio.to_thread(
                    authenticate_with_password, url, params.get("username"), params.get("password")
                )
            elif method == "api_key":
                credential = params.get("api_key")
            elif method == "saved" and existing is not None:
                credential = ManagedAPIClient(
                    existing.url, credential=existing.credential, credential_file=existing.credential_file
                ).read_credential()
            else:
                raise ValueError("Choose saved credentials, username/password, or API key")
            client = ManagedAPIClient(url, credential=credential)
            inventory = await client.inventory_async()
            if inventory.data is None or inventory.errors:
                raise ValueError("DDM did not grant access to its device inventory")
            await asyncio.to_thread(save_ddm_server, path, name=name, url=url, credential=credential)
            await self._reload_connections(name)
        except ValueError as error:
            await self._send_json(writer, {"error": str(error)}, 400)
            return
        except ManagedAPIError:
            await self._send_json(
                writer, {"error": "DDM authentication failed or the server could not be reached"}, 502
            )
            return
        except OSError:
            await self._send_json(writer, {"error": "DDM credentials could not be saved on the backend"}, 500)
            return
        await self._send_json(writer, self._connection_state())

    async def _handle_ddm_context(self, writer, params):
        if self.managed_inventory is None:
            await self._send_json(writer, {"error": "DDM support is unavailable"}, 503)
            return
        async with self._connection_lock:
            await self._select_context(writer, params)

    async def _select_context(self, writer, params):
        try:
            configuration, path = self._connection_configuration()
            selected = params.get("context")
            if selected:
                configuration.context(selected)
                await asyncio.to_thread(set_default_ddm_context, path, selected)
            else:
                server = configuration.server(params.get("server"))
                domain_id = params.get("domain_id")
                domains = [
                    domain
                    for domain in self.managed_inventory.domains()
                    if domain.get("ddm_server_profile") == server.name and domain.get("id") == domain_id
                ]
                if len(domains) != 1:
                    raise ValueError("Select a domain reported by the chosen server")
                domain = domains[0]
                selected = next(
                    (
                        context.name
                        for context in configuration.contexts.values()
                        if context.server == server.name and context.domain_id == domain_id
                    ),
                    None,
                )
                if selected is None:
                    base = re.sub(r"[^A-Za-z0-9_.-]", "-", f"{server.name}-{domain.get('name') or 'domain'}")
                    selected = base
                    suffix = 2
                    while selected in configuration.contexts:
                        selected = f"{base}-{suffix}"
                        suffix += 1
                if server.credential_file is None:
                    await asyncio.to_thread(
                        save_ddm_server, path, name=server.name, url=server.url, credential=server.credential
                    )
                    configuration, _ = self._connection_configuration()
                    server = configuration.server(server.name)
                await asyncio.to_thread(
                    save_ddm_context,
                    path,
                    server_name=server.name,
                    url=server.url,
                    credential_file=server.credential_file,
                    context_name=selected,
                    domain_id=domain_id,
                    domain_name=domain.get("name"),
                    make_default=True,
                )
            await self._reload_connections()
        except (ValueError, TypeError):
            await self._send_json(writer, {"error": "Select a saved context or a domain available on that server"}, 400)
            return
        except OSError:
            await self._send_json(writer, {"error": "DDM selection could not be saved on the backend"}, 500)
            return
        await self._send_json(writer, self._connection_state())
