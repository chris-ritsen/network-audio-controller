from __future__ import annotations

import asyncio
import json
import logging
import re
import secrets
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger("netaudio.mcp")

UI_EXTENSION = "io.modelcontextprotocol/ui"
APP_MIME_TYPE = "text/html;profile=mcp-app"
APP_DIRECTORY = Path(__file__).parent / "webapp" / "mcp-app"
TRANSLATION_DIRECTORY = Path(__file__).parent / "webapp" / "i18n"
LANGUAGE_PATTERN = re.compile(r"[A-Za-z]{2,3}(-[A-Za-z]{2,4})?")

ROUTING_VIEW_URI = "ui://netaudio/routing"
SHOW_ROUTING_MATRIX = "show_routing_matrix"
APP_EVENTS = "app_events"
APP_REQUEST = "app_request"
APP_TRANSLATIONS = "app_translations"

MAILBOX_QUEUE_SIZE = 4096
SESSION_IDLE_SECONDS = 90.0
MAXIMUM_SESSIONS = 32
MAXIMUM_WAIT_SECONDS = 25

APP_WRITES = frozenset({"/subscribe", "/unsubscribe", "/external-flows/subscribe"})

CLIENT_CAPABILITIES: ContextVar[dict | None] = ContextVar("mcp_client_capabilities", default=None)
INVOKED_INDIRECTLY: ContextVar[bool] = ContextVar("mcp_invoked_indirectly", default=False)


@dataclass(frozen=True)
class AppView:
    uri: str
    name: str
    description: str
    document: str
    omitted_fields: frozenset[str]
    omitted_snapshot: frozenset[str]
    events: frozenset[str]


APP_VIEWS = (
    AppView(
        uri=ROUTING_VIEW_URI,
        name="Routing matrix",
        description="The netaudio web interface's Dante routing matrix, live, with subscriptions the user can change.",
        document="routing.html",
        omitted_fields=frozenset(
            {
                "clock_control_availability",
                "clock_observed_at",
                "clock_port_records",
                "clock_status",
                "ddm_identity",
                "ddm_last_sync",
                "device_controls",
                "field_sources",
                "lock_reset_status",
                "manufacturer_versions_record",
                "platform_versions_record",
                "receiver_flow_status_page",
                "receiver_telemetry_capacity",
                "settings_properties",
            }
        ),
        omitted_snapshot=frozenset({"metering", "metering_scale", "shure_devices"}),
        events=frozenset(
            {
                "device_discovered",
                "device_patch",
                "device_removed",
                "device_updated",
                "external_flow_changed",
                "snapshot",
                "subscription_pending",
            }
        ),
    ),
)
APP_VIEWS_BY_URI = {view.uri: view for view in APP_VIEWS}

APP_ONLY = {"ui": {"visibility": ["app"]}}

VIEW_TOOL_SPECS = (
    {
        "name": SHOW_ROUTING_MATRIX,
        "path": "",
        "description": (
            "Show the user the interactive routing matrix of every Dante device, where they can see and change "
            "subscriptions themselves, in apps that display MCP Apps views. Use it when the user wants to see or "
            "edit routing visually; it also returns the current routes as text. receivers and transmitters narrow "
            "the matrix to devices or channels whose names contain the text."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "receivers": {"type": "string", "description": "Show only receivers whose names contain this text."},
                "transmitters": {
                    "type": "string",
                    "description": "Show only transmitters whose names contain this text.",
                },
            },
            "required": [],
            "additionalProperties": False,
        },
        "read_only": True,
        "meta": {"ui": {"resourceUri": ROUTING_VIEW_URI}},
    },
)

APP_TOOLS = (
    {
        "name": APP_EVENTS,
        "description": (
            "Used by netaudio's routing view, not for direct use: device events for the open view. Without a "
            "session it opens one and returns a snapshot; with one it waits up to wait_seconds for the next changes."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "session": {"type": "string"},
                "wait_seconds": {"type": "integer", "minimum": 0, "maximum": MAXIMUM_WAIT_SECONDS},
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "destructiveHint": False},
        "_meta": APP_ONLY,
    },
    {
        "name": APP_REQUEST,
        "description": (
            "Used by netaudio's routing view, not for direct use: sends a route change the user made in the view. "
            "To change routes yourself, use set_subscriptions."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "method": {"type": "string", "enum": ["POST"]},
                "path": {"type": "string", "enum": sorted(APP_WRITES)},
                "body": {"type": "object"},
            },
            "required": ["method", "path"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": False, "destructiveHint": False},
        "_meta": APP_ONLY,
    },
    {
        "name": APP_TRANSLATIONS,
        "description": "Used by netaudio's routing view, not for direct use: its text in one language.",
        "inputSchema": {
            "type": "object",
            "properties": {"language": {"type": "string"}},
            "required": ["language"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "destructiveHint": False},
        "_meta": APP_ONLY,
    },
)
APP_TOOL_NAMES = frozenset(tool["name"] for tool in APP_TOOLS)


def shows_apps(capabilities) -> bool:
    if not isinstance(capabilities, dict):
        return False
    extensions = capabilities.get("extensions")
    settings = extensions.get(UI_EXTENSION) if isinstance(extensions, dict) else None
    mime_types = settings.get("mimeTypes") if isinstance(settings, dict) else None
    return isinstance(mime_types, list) and APP_MIME_TYPE in mime_types


def app_resources() -> list[dict]:
    return [
        {"uri": view.uri, "name": view.name, "description": view.description, "mimeType": APP_MIME_TYPE}
        for view in APP_VIEWS
    ]


@lru_cache(maxsize=None)
def _document(name: str) -> str:
    return (APP_DIRECTORY / name).read_text(encoding="utf-8")


def read_app_view(uri: str) -> dict | None:
    view = APP_VIEWS_BY_URI.get(uri)
    if view is None:
        return None
    return {
        "contents": [
            {
                "uri": uri,
                "mimeType": APP_MIME_TYPE,
                "text": _document(view.document),
                "_meta": {"ui": {"prefersBorder": True}},
            }
        ]
    }


@lru_cache(maxsize=None)
def translation_catalog(language: str) -> dict:
    if not LANGUAGE_PATTERN.fullmatch(language):
        raise ValueError(f"no translations for {language!r}")
    path = TRANSLATION_DIRECTORY / f"{language}.js"
    if not path.is_file():
        raise ValueError(f"no translations for {language!r}")
    text = path.read_text(encoding="utf-8").strip()
    prefix = "export default "
    if not text.startswith(prefix):
        raise ValueError(f"the {language} translations are not a default-exported object")
    return json.loads(text[len(prefix) :].rstrip(";"))


def app_result(payload: dict, summary: str) -> dict:
    return {"content": [{"type": "text", "text": summary}], "structuredContent": payload, "isError": False}


def app_error(message: str) -> dict:
    return {"content": [{"type": "text", "text": message}], "isError": True}


class _MailboxWriter:
    transport = None

    def write(self, data) -> None:
        raise RuntimeError("an MCP app session is read by polling, not written to")

    def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass


@dataclass(eq=False)
class _AppSession:
    identifier: str
    client: Any
    expiry: asyncio.TimerHandle | None = None


class McpAppTools:
    def _app_sessions(self) -> dict[str, _AppSession]:
        sessions = getattr(self, "_mcp_app_sessions", None)
        if sessions is None:
            sessions = {}
            self._mcp_app_sessions = sessions
        return sessions

    def _open_app_session(self) -> _AppSession:
        from netaudio.daemon.http.api import _encode_sse, _SseClient
        from netaudio.daemon.http.sse_view import SseDeviceView

        sessions = self._app_sessions()
        while len(sessions) >= MAXIMUM_SESSIONS:
            self._close_app_session(next(iter(sessions.values())), "too many open views")
        shown = APP_VIEWS_BY_URI[ROUTING_VIEW_URI]
        view = SseDeviceView(patches=True, telemetry=False, meters=False, notices=False, omitted=shown.omitted_fields)
        client = _SseClient(writer=_MailboxWriter(), view=view, queue=asyncio.Queue(maxsize=MAILBOX_QUEUE_SIZE))
        snapshot = {key: value for key, value in self._snapshot_payload().items() if key not in shown.omitted_snapshot}
        client.queue.put_nowait(_encode_sse(view.initial_snapshot(snapshot)))
        self.sse_clients[client.writer] = client
        session = _AppSession(secrets.token_urlsafe(16), client)
        sessions[session.identifier] = session
        return session

    def _close_app_session(self, session: _AppSession, reason: str) -> None:
        if session.expiry is not None:
            session.expiry.cancel()
        self._app_sessions().pop(session.identifier, None)
        self._drop_sse_client(session.client, reason)

    def _keep_app_session(self, session: _AppSession) -> None:
        sessions = self._app_sessions()
        sessions.pop(session.identifier, None)
        sessions[session.identifier] = session
        if session.expiry is not None:
            session.expiry.cancel()
        session.expiry = asyncio.get_running_loop().call_later(
            SESSION_IDLE_SECONDS, self._close_app_session, session, "MCP app view closed"
        )

    async def _app_events(self, arguments: dict) -> dict:
        session = self._app_sessions().get(arguments.get("session") or "")
        if session is None or session.client.closed.is_set():
            session = self._open_app_session()
        self._keep_app_session(session)
        queue = session.client.queue
        wait = min(int(arguments.get("wait_seconds") or 0), MAXIMUM_WAIT_SECONDS)
        items = []
        if queue.empty() and wait > 0:
            getter = asyncio.ensure_future(queue.get())
            closed = asyncio.ensure_future(session.client.closed.wait())
            done, pending = await asyncio.wait({getter, closed}, timeout=wait, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if getter in done:
                items.append(getter.result())
        while not queue.empty():
            items.append(queue.get_nowait())
        self._keep_app_session(session)
        shown = APP_VIEWS_BY_URI[ROUTING_VIEW_URI].events
        decoded = (json.loads(self._sse_payload(session.client, item).decode()[len("data: ") :]) for item in items)
        events = [event for event in decoded if event.get("event") in shown]
        return app_result(
            {"session": session.identifier, "events": events}, f"{len(events)} device events for an open view"
        )

    async def _app_request(self, arguments: dict, writer, grant) -> dict:
        from netaudio.daemon.http.mcp import CapturedResponse

        path = arguments["path"]
        if arguments["method"] != "POST" or path not in APP_WRITES:
            return app_error(f"an open view cannot send {arguments['method']} {path}")
        if grant is not None and not grant.can_write:
            return app_error(f"{grant.client_name} was granted read-only access, so the view cannot change routes")
        body = arguments.get("body") or {}
        logger.info("MCP %s view sent %s %s", grant.client_name if grant else "unknown client", path, body)
        captured = CapturedResponse(writer.get_extra_info("peername"))
        try:
            await self.post_handlers[path](captured, body)
        except TimeoutError:
            return app_result({"status": 504, "payload": {"error": "device did not respond"}}, "device did not respond")
        status, payload = captured.result()
        return app_result({"status": status, "payload": payload}, f"{path} answered {status}")

    def _app_translations(self, arguments: dict) -> dict:
        language = arguments["language"]
        try:
            catalog = translation_catalog(language)
        except (OSError, ValueError) as exception:
            return app_error(str(exception))
        return app_result({"language": language, "catalog": catalog}, f"{language} translations")

    async def _show_routing_matrix(self, arguments: dict, writer, grant) -> dict:
        routing = await self._mcp_tool_call({"name": "get_routing", "arguments": {}}, writer)
        if routing.get("isError"):
            return routing
        narrowed = [
            f"{side} matching {arguments[side]!r}" for side in ("receivers", "transmitters") if arguments.get(side)
        ]
        indirect = INVOKED_INDIRECTLY.get()
        logger.info(
            "MCP %s called %s%s",
            grant.client_name if grant else "unknown client",
            SHOW_ROUTING_MATRIX,
            " through invoke_tool" if indirect else "",
        )
        if indirect:
            heading = (
                "The routing matrix was not shown: apps open it only when show_routing_matrix is called directly, "
                "not through invoke_tool. If it is missing from your tools, the app's list of netaudio tools is "
                "out of date and needs refreshing."
            )
        elif self._client_shows_apps(grant):
            heading = "The routing matrix is open for the user"
            heading += f", showing {' and '.join(narrowed)}." if narrowed else "."
        else:
            heading = "This app cannot display the interactive routing matrix."
        text = f"{heading} Current routes: {routing['content'][0]['text']}"
        return {"content": [{"type": "text", "text": text}], "isError": False}

    async def _app_tool_call(self, name: str, arguments: dict, writer, grant) -> dict:
        if name == APP_REQUEST and not self._client_shows_apps(grant):
            return app_error(f"{name} is only for netaudio views open in an app that displays MCP Apps")
        if name == APP_EVENTS:
            return await self._app_events(arguments)
        if name == APP_REQUEST:
            return await self._app_request(arguments, writer, grant)
        return self._app_translations(arguments)

    def _legacy_app_clients(self) -> set[str]:
        clients = getattr(self, "_mcp_legacy_app_clients", None)
        if clients is None:
            clients = set()
            self._mcp_legacy_app_clients = clients
        return clients

    def _client_shows_apps(self, grant) -> bool:
        capabilities = CLIENT_CAPABILITIES.get()
        if capabilities is not None:
            return shows_apps(capabilities)
        return grant is not None and grant.client_id in self._legacy_app_clients()

    def _note_app_client(self, capabilities, grant, *, initializing: bool = False) -> None:
        shown = shows_apps(capabilities)
        if initializing and grant is not None:
            if shown:
                self._legacy_app_clients().add(grant.client_id)
            else:
                self._legacy_app_clients().discard(grant.client_id)
        seen = getattr(self, "_mcp_app_clients", None)
        if seen is None:
            seen = set()
            self._mcp_app_clients = seen
        name = grant.client_name if grant else "unknown client"
        if (name, shown) not in seen:
            seen.add((name, shown))
            logger.info(
                "MCP %s %s MCP Apps views; it advertises %s",
                name,
                "shows" if shown else "does not show",
                json.dumps(capabilities, sort_keys=True),
            )
