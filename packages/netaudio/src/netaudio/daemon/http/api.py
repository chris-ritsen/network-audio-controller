from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import socket
import ssl
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, unquote, urlsplit

import ifaddr
from zeroconf import Error as ZeroconfError
from zeroconf import IPVersion, ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.common.app_config import DEFAULT_DAEMON_PORT
from netaudio.common.app_config import settings as app_settings
from netaudio.common.managed_api import DDMConfiguration
from netaudio.daemon.api_contract import API_VERSION, api_contract
from netaudio.daemon.http.configuration import DaemonConfigurationHandlers
from netaudio.daemon.http.connections import DaemonConnectionHandlers
from netaudio.daemon.http.devices import DaemonDeviceHandlers
from netaudio.daemon.http.host_audio import DaemonHostAudioHandlers
from netaudio.daemon.http.host_audio_control import DaemonHostAudioControlHandlers
from netaudio.daemon.http.managed import DaemonManagedHandlers
from netaudio.daemon.http.mcp import MCP_PATH, DaemonMcpHandlers
from netaudio.daemon.http.oauth import DaemonOAuthHandlers
from netaudio.daemon.http.presets import DaemonPresetHandlers
from netaudio.daemon.http.request_scope import REQUEST_INVENTORY
from netaudio.daemon.http.settings import DaemonSettingsHandlers
from netaudio.daemon.http.shure import DaemonShureHandlers
from netaudio.daemon.http.sse_view import TELEMETRY_FIELDS, SseDeviceView, substantive_record
from netaudio.daemon.http.tls import (
    TLSConfigurationError,
    certificate_fingerprint,
    TLSSettings,
    build_ssl_context,
    ensure_generated_identity,
    identity_coverage,
    identity_is_current,
)
from netaudio.daemon.http.web import DaemonWebHandlers, is_application_route, prefers_web_page
from netaudio.daemon.mcp_access import ensure_mcp_token
from netaudio.daemon.mcp_oauth import OAuthStore
from netaudio.daemon.server_info import server_info
from netaudio.daemon.subscription_readback import SubscriptionReadback
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.metering import metering_scale
from netaudio.network_path import interface_supports_multicast
from netaudio.monitoring import (
    DerivationStatus,
    EventSeverity,
    MonitoringEvent,
    MonitoringEventJournal,
    MonitoringEventKind,
    MutationAuditRecorder,
)
from netaudio.monitoring.level_history import LevelHistory

logger = logging.getLogger("netaudio")

if TYPE_CHECKING:
    from netaudio.dante.services.heartbeat import DanteHeartbeatService

DAEMON_SERVICE_TYPE = "_netaudio-relay._tcp.local."
BONJOUR_MONITOR_INTERVAL_SECONDS = 5
TLS_RENEWAL_RETRY_SECONDS = 300
BONJOUR_REFRESH_INTERVAL_SECONDS = 60
BONJOUR_SLEEP_GAP_MULTIPLIER = 3
SSE_CLIENT_QUEUE_SIZE = 128
SSE_DRAIN_TIMEOUT_SECONDS = 5
SSE_CLOSE_TIMEOUT_SECONDS = 1
AUDIO_CAPABILITY_VERIFICATION_TIMEOUT_SECONDS = 20
SERVICE_LABEL_MAXIMUM_BYTES = 63
DAEMON_SERVICE_INSTANCE_PREFIX = "netaudio-daemon ("
DAEMON_SERVICE_INSTANCE_SUFFIX = ")"


def advertisement_addresses(selected_interface: str | None = None) -> tuple[str, ...]:
    addresses = set()
    for adapter in ifaddr.get_adapters():
        if selected_interface and adapter.nice_name != selected_interface:
            continue
        if interface_supports_multicast(adapter.nice_name) is False:
            continue
        for adapter_ip in adapter.ips:
            address = adapter_ip.ip
            if not isinstance(address, str):
                continue
            try:
                parsed = ipaddress.IPv4Address(address)
            except ipaddress.AddressValueError:
                continue
            if parsed.is_loopback or parsed.is_unspecified or parsed.is_multicast:
                continue
            addresses.add(str(parsed))

    return tuple(sorted(addresses))


def _bounded_service_label(value: str, maximum_bytes: int) -> str:
    encoded_value = value.encode("utf-8")
    if len(encoded_value) <= maximum_bytes:
        return value
    digest_suffix = f"-{hashlib.sha256(encoded_value).hexdigest()[:12]}"
    prefix_byte_count = maximum_bytes - len(digest_suffix.encode("ascii"))
    if prefix_byte_count <= 0:
        raise ValueError("Service label limit is too small for a stable identity suffix")
    bounded_prefix = encoded_value[:prefix_byte_count].decode("utf-8", errors="ignore")
    return f"{bounded_prefix}{digest_suffix}"


def _daemon_service_instance_label(hostname: str) -> str:
    hostname_byte_limit = SERVICE_LABEL_MAXIMUM_BYTES - len(
        f"{DAEMON_SERVICE_INSTANCE_PREFIX}{DAEMON_SERVICE_INSTANCE_SUFFIX}".encode("ascii")
    )
    bounded_hostname = _bounded_service_label(hostname, hostname_byte_limit)
    return f"{DAEMON_SERVICE_INSTANCE_PREFIX}{bounded_hostname}{DAEMON_SERVICE_INSTANCE_SUFFIX}"


@dataclass(eq=False)
class _MeterUpdate:
    key: str
    payload: bytes


@dataclass(eq=False)
class _SseClient:
    writer: asyncio.StreamWriter
    queue: asyncio.Queue[bytes | _MeterUpdate] = field(
        default_factory=lambda: asyncio.Queue(maxsize=SSE_CLIENT_QUEUE_SIZE)
    )
    pending_meters: dict[str, _MeterUpdate] = field(default_factory=dict)
    closed: asyncio.Event = field(default_factory=asyncio.Event)
    sender_task: asyncio.Task | None = None
    view: SseDeviceView | None = None


def _encode_sse(data) -> bytes:
    from netaudio.daemon.http.json_values import browser_values

    return f"data: {json.dumps(browser_values(data), default=str)}\n\n".encode()


async def _bounded(awaitable, timeout: float):
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except BaseException:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise
    if not done:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise asyncio.TimeoutError
    return task.result()


UNENROLLED_DISCOVERY_GRACE_SECONDS = 10.0


def _awaiting_direct_discovery(record: dict) -> bool:
    return record.get("inventory_sources") == ["ddm"] and record.get("management_state") != "managed"


def _without_telemetry(record: dict) -> dict:
    return {key: value for key, value in record.items() if key not in TELEMETRY_FIELDS}


class DaemonHTTPServer(
    DaemonPresetHandlers,
    DaemonSettingsHandlers,
    DaemonConnectionHandlers,
    DaemonConfigurationHandlers,
    DaemonDeviceHandlers,
    DaemonManagedHandlers,
    DaemonHostAudioHandlers,
    DaemonHostAudioControlHandlers,
    DaemonShureHandlers,
    DaemonMcpHandlers,
    DaemonOAuthHandlers,
    DaemonWebHandlers,
):
    def __init__(
        self,
        application,
        state,
        metering=None,
        shure=None,
        port=None,
        on_shutdown=None,
        mark_offline=None,
        forget_device=None,
        managed_inventory=None,
        refresh_discovery=None,
        event_journal=None,
        tls: TLSSettings | None = None,
        mcp_token: str | None = None,
        host_audio=None,
        level_history=None,
    ):
        self.application = application
        self.level_history = level_history or LevelHistory()
        self.diagnostics: DanteHeartbeatService | None = None
        self.mcp_token = mcp_token if mcp_token is not None else ensure_mcp_token()
        self.oauth_store = OAuthStore()
        self.server_info = {**server_info(), "mcp": self.mcp_server_info()}
        from netaudio.daemon.managed_controls import ManagedDeviceControls

        self.managed_controls = ManagedDeviceControls(application)
        self.managed_inventory = managed_inventory
        self.refresh_discovery = refresh_discovery
        self.event_journal = event_journal or MonitoringEventJournal()
        self.operation_recorder = MutationAuditRecorder.from_journal(
            self.event_journal,
            publish=self.publish_journal_event,
        )
        configure_recorder = getattr(self.application, "set_operation_recorder", None)
        if configure_recorder is None:
            self.application.operation_recorder = self.operation_recorder
        else:
            configure_recorder(self.operation_recorder)
        self._dismissed_offline_inventory: set[str] = set()
        self._published_inventory: dict[str, dict] = {}
        self._published_managed = None
        self.discovery_started_monotonic: float | None = None
        self._discovery_republish: asyncio.TimerHandle | None = None
        self.state = state
        self.subscription_readback = SubscriptionReadback(self._emit_device_updated, application)
        self.metering = metering
        self.shure = shure
        self.host_audio = host_audio
        self._host_audio_broadcasts: set[asyncio.Task] = set()
        if host_audio is not None:
            host_audio.add_listener(self._on_host_audio_changed)
        self.on_shutdown = on_shutdown
        self.mark_offline = mark_offline or application.mark_device_offline
        self.forget_device = forget_device or application.unregister_device
        self.port: int = int(port if port is not None else DEFAULT_DAEMON_PORT)
        if tls is not None and tls.port == self.port:
            raise TLSConfigurationError(f"[daemon] tls_port must differ from the plain HTTP port {self.port}")
        self.tls = tls
        self.tcp_server = None
        self.tls_server = None
        self._tls_context: ssl.SSLContext | None = None
        self._tls_coverage = None
        self._tls_fingerprint: str | None = None
        self._tls_renewal_retry_wall_time = 0.0
        self.zeroconf = None
        self.service_info = None
        self.sse_clients: dict[asyncio.StreamWriter, _SseClient] = {}
        self._events_registered = False
        self._stop_lock: asyncio.Lock | None = None
        self._bonjour_addresses: tuple[str, ...] = ()
        self._bonjour_monitor_task: asyncio.Task | None = None
        self._bonjour_registered_monotonic: float | None = None
        self._last_bonjour_probe_wall_time: float | None = None
        self._device_lock_operation_locks: dict[str, asyncio.Lock] = {}
        self._connection_lock = DeferredAsyncioLock()
        self._preset_operation_lock = DeferredAsyncioLock()
        self.audio_capability_verification_timeout = AUDIO_CAPABILITY_VERIFICATION_TIMEOUT_SECONDS
        self.post_handlers = {
            "/presets/save": self._handle_save_preset,
            "/presets/preview": self._handle_preview_preset,
            "/presets/load": self._handle_load_preset,
            "/subscribe": self._handle_subscribe,
            "/subscriptions/apply": self._handle_apply_subscriptions,
            "/unsubscribe": self._handle_unsubscribe,
            "/identify": self._handle_identify,
            "/rename-device": self._handle_rename_device,
            "/rename-channel": self._handle_rename_channel,
            "/set-latency": self._handle_set_latency,
            "/set-receive-flow-performance": self._handle_set_receive_flow_performance,
            "/set-transmit-flow-performance": self._handle_set_transmit_flow_performance,
            "/set-unicast-performance": self._handle_set_unicast_performance,
            "/set-receive-flow-default-slots": self._handle_set_receive_flow_default_slots,
            "/store-current-configuration": self._handle_store_current_configuration,
            "/lock": self._handle_lock,
            "/unlock": self._handle_unlock,
            "/refresh": self._handle_refresh,
            "/discovery/refresh": self._handle_discovery_refresh,
            "/set-sample-rate": self._handle_set_sample_rate,
            "/set-encoding": self._handle_set_encoding,
            "/set-gain": self._handle_set_gain,
            "/set-aes67": self._handle_set_aes67,
            "/set-aes67-multicast-prefix": self._handle_set_aes67_multicast_prefix,
            "/set-sample-rate-pullup": self._handle_set_sample_rate_pullup,
            "/set-preferred-leader": self._handle_set_preferred_leader,
            "/set-clock-source": self._handle_set_clock_source,
            "/device-controls": self._handle_device_controls,
            "/set-clock-configuration": self._handle_set_clock_configuration,
            "/set-clock-subdomain": self._handle_set_clock_subdomain,
            "/refresh-clock": self._handle_refresh_clock,
            "/reboot": self._handle_reboot,
            "/clear-configuration": self._handle_clear_configuration,
            "/factory-reset": self._handle_factory_reset,
            "/interface": self._handle_set_interface,
            "/redundancy": self._handle_set_redundancy,
            "/metering/start": self._handle_metering_start,
            "/metering/stop": self._handle_metering_stop,
            "/report-unresponsive": self._handle_report_unresponsive,
            "/transmit-flows/plan": self._handle_plan_transmit_flow,
            "/transmit-flows/create": self._handle_create_transmit_flow,
            "/transmit-flows/delete": self._handle_delete_transmit_flow,
            "/external-flows/subscribe": self._handle_subscribe_external_rtp,
            "/ddm/graphql": self._handle_ddm_graphql,
            "/ddm/refresh": self._handle_ddm_refresh,
            "/ddm/login": self._handle_ddm_login,
            "/ddm/logout": self._handle_ddm_logout,
            "/ddm/enrollment": self._handle_ddm_enrollment,
            "/ddm/domains": self._handle_ddm_create_domain,
            "/ddm/context": self._handle_ddm_context,
            "/ddm/profile": self._handle_ddm_edit_profile,
            "/ddm/domains/update": self._handle_ddm_update_domain,
            "/device-lock-key": self._handle_device_lock_key,
            "/settings/monitoring": self._handle_monitoring_settings,
            "/diagnostics/reset": self._handle_reset_diagnostics,
            "/diagnostics/policy": self._handle_diagnostics_policy,
            "/event-journal/operations": self._handle_append_operation_event,
            "/host-audio/levels": self._handle_host_audio_levels,
            "/host-audio/meter": self._handle_host_audio_meter,
            "/host-audio/history": self._handle_host_audio_history,
            "/host-audio/jack/connect": self._handle_jack_connect,
            "/host-audio/jack/disconnect": self._handle_jack_disconnect,
            "/host-audio/pulse/volume": self._handle_pulse_volume,
            "/host-audio/pulse/mute": self._handle_pulse_mute,
            "/host-audio/pulse/default": self._handle_pulse_default,
            "/host-audio/pulse/move": self._handle_pulse_move,
            "/host-audio/cards": self._handle_link_card,
            "/shure/set": self._handle_shure_set,
            "/shutdown": self._handle_shutdown,
        }
        self.post_body_optional = {"/ddm/refresh", "/refresh", "/shutdown"}
        self.loopback_only_paths = {
            "/shutdown",
            "/event-journal/operations",
            "/host-audio/jack/connect",
            "/host-audio/jack/disconnect",
            "/host-audio/pulse/volume",
            "/host-audio/pulse/mute",
            "/host-audio/pulse/default",
            "/host-audio/pulse/move",
            "/host-audio/cards",
        }

    async def start(self):
        if self.tcp_server is not None:
            return
        self._stop_lock = asyncio.Lock()
        http_host = "127.0.0.1" if self.tls is not None else "0.0.0.0"
        self.tcp_server = await asyncio.start_server(self.handle_connection, http_host, self.port)
        logger.info(f"Daemon HTTP API listening on {http_host}:{self.port}")

        try:
            if self.tls is not None:
                context = build_ssl_context(self.tls)
                self._tls_context = context
                self._tls_coverage = identity_coverage(self.tls.certificate) if self.tls.generated else None
                self._tls_fingerprint = self._read_tls_fingerprint()
                self.tls_server = await asyncio.start_server(
                    self.handle_connection, "0.0.0.0", self.tls.port, ssl=context
                )
                logger.info(f"Daemon HTTPS API listening on port {self.tls.port}")
            self._register_events()
            await self._reconcile_bonjour(force=True)
            self._bonjour_monitor_task = asyncio.create_task(self._bonjour_monitor_loop())
        except BaseException:
            await self.stop()
            raise

    async def stop(self):
        stop_lock = self._stop_lock
        if stop_lock is None:
            stop_lock = asyncio.Lock()
            self._stop_lock = stop_lock

        async with stop_lock:
            await self.subscription_readback.stop()
            monitor_task = self._bonjour_monitor_task
            self._bonjour_monitor_task = None
            if monitor_task:
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    logger.debug("Bonjour monitor stopped")

            await self._close_bonjour()
            self._last_bonjour_probe_wall_time = None

            clients = list(self.sse_clients.values())
            if clients:
                await asyncio.gather(
                    *(self._close_sse_client(client, "daemon shutdown") for client in clients),
                    return_exceptions=True,
                )
            self.mcp_subscriptions.close_all()

            for attribute, label in (("tcp_server", "HTTP"), ("tls_server", "HTTPS")):
                server = getattr(self, attribute)
                setattr(self, attribute, None)
                if server:
                    server.close()
                    try:
                        await _bounded(server.wait_closed(), 5)
                    except asyncio.TimeoutError:
                        logger.warning(f"Daemon {label} API connections did not drain within 5s, abandoning them")

            self._unregister_events()

    def _register_events(self):
        if self._events_registered:
            return
        dispatcher = self.application.dispatcher
        dispatcher.on(EventType.DEVICE_DISCOVERED, self._on_device_event)
        dispatcher.on(EventType.DEVICE_UPDATED, self._on_device_event)
        dispatcher.on(EventType.DEVICE_TELEMETRY, self._on_device_telemetry)
        dispatcher.on(EventType.EXTERNAL_FLOW_CHANGED, self._on_external_flow_changed)
        dispatcher.on(EventType.DEVICE_REMOVED, self._on_device_removed)
        dispatcher.on(EventType.METER_VALUES, self._on_meter_values)
        dispatcher.on(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_event)
        dispatcher.on(EventType.SHURE_DEVICE_UPDATED, self._on_shure_event)
        dispatcher.on(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)
        dispatcher.on(EventType.SHURE_METER_VALUES, self._on_shure_meter)
        dispatcher.on(EventType.SHURE_SAMPLE, self._on_shure_sample)
        self._events_registered = True

    def _unregister_events(self):
        if not self._events_registered:
            return
        dispatcher = self.application.dispatcher
        dispatcher.off(EventType.DEVICE_DISCOVERED, self._on_device_event)
        dispatcher.off(EventType.DEVICE_UPDATED, self._on_device_event)
        dispatcher.off(EventType.DEVICE_TELEMETRY, self._on_device_telemetry)
        dispatcher.off(EventType.EXTERNAL_FLOW_CHANGED, self._on_external_flow_changed)
        dispatcher.off(EventType.DEVICE_REMOVED, self._on_device_removed)
        dispatcher.off(EventType.METER_VALUES, self._on_meter_values)
        dispatcher.off(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_event)
        dispatcher.off(EventType.SHURE_DEVICE_UPDATED, self._on_shure_event)
        dispatcher.off(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)
        dispatcher.off(EventType.SHURE_METER_VALUES, self._on_shure_meter)
        dispatcher.off(EventType.SHURE_SAMPLE, self._on_shure_sample)
        self._events_registered = False

    async def _on_device_event(self, event: DanteEvent):
        if not self.sse_clients:
            return
        if event.type == EventType.DEVICE_DISCOVERED:
            device_json = self._serialized_devices().get(event.server_name)
        else:
            device = self.application.devices.get(event.server_name)
            if device is None:
                return
            if device.online:
                self._dismissed_offline_inventory.discard(event.server_name)
            elif event.server_name in self._dismissed_offline_inventory:
                return
            device_json = self._serialized_device(event.server_name, device)
        if not device_json:
            return

        self._published_inventory[event.server_name] = _without_telemetry(device_json)
        await self._broadcast_sse(
            {
                "event": event.type.name.lower(),
                "server_name": event.server_name,
                "device": device_json,
            }
        )

    def _clients_want(self, attribute: str) -> bool:
        return any(client.view is None or getattr(client.view, attribute) for client in self.sse_clients.values())

    async def _on_device_telemetry(self, event: DanteEvent):
        if not self.sse_clients or event.server_name in self._dismissed_offline_inventory:
            return
        if not self._clients_want("telemetry") and not self._clients_want("notices"):
            return
        device = self.application.devices.get(event.server_name)
        if device is None:
            return
        fields = event.data.get("fields") or ()
        if any(client.view is None or client.view.telemetry for client in self.sse_clients.values()):
            telemetry = DanteDeviceSerializer.telemetry_to_json(device, fields)
            fields = telemetry
        else:
            telemetry = {}
        await self._broadcast_sse(
            {
                "event": "telemetry_updated",
                "server_name": event.server_name,
                "fields": sorted(fields),
                "telemetry": telemetry,
            }
        )

    async def _on_device_removed(self, event: DanteEvent):
        self._published_inventory.pop(event.server_name, None)
        await self._broadcast_sse(
            {
                "event": "device_removed",
                "server_name": event.server_name,
            }
        )

    async def _on_external_flow_changed(self, event: DanteEvent):
        await self._broadcast_sse({"event": "external_flow_changed", **event.data})

    async def _on_meter_values(self, event: DanteEvent):
        if self.sse_clients and not self._clients_want("meters"):
            return
        await self._broadcast_sse(
            {
                "event": "meter_values",
                "server_name": event.server_name,
                "tx": event.data.get("tx", {}),
                "rx": event.data.get("rx", {}),
                "metering_source": event.data.get("metering_source"),
                "wall_time": event.data.get("wall_time"),
                "source_ip": event.data.get("source_ip"),
                "source_port": event.data.get("source_port"),
                "tx_signal_presence": event.data.get("tx_signal_presence", {}),
                "rx_signal_presence": event.data.get("rx_signal_presence", {}),
            }
        )

    async def _on_shure_event(self, event: DanteEvent):
        if not self.shure:
            return
        device = self.shure.devices.get(event.device_name)
        if not device:
            return
        await self._broadcast_sse(
            {
                "event": event.type.name.lower(),
                "mac": event.device_name,
                "device": device.to_json(),
            }
        )

    def _on_host_audio_changed(self, component: str) -> None:
        if not self.sse_clients:
            return
        task = asyncio.get_running_loop().create_task(
            self._broadcast_sse({"event": "host_audio_changed", "component": component})
        )
        self._host_audio_broadcasts.add(task)
        task.add_done_callback(self._host_audio_broadcasts.discard)

    async def _on_shure_removed(self, event: DanteEvent):
        await self._broadcast_sse(
            {
                "event": "shure_device_removed",
                "mac": event.device_name,
            }
        )

    async def _on_shure_sample(self, event: DanteEvent):
        if self.sse_clients and not self._clients_want("meters"):
            return
        await self._broadcast_sse(
            {
                "event": "shure_meter_values",
                "mac": event.device_name,
                "channel": event.data.get("channel"),
                "values": event.data.get("values", {}),
            }
        )

    async def _on_shure_meter(self, event: DanteEvent):
        if event.data.get("sampled"):
            return
        if self.sse_clients and not self._clients_want("meters"):
            return
        await self._broadcast_sse(
            {
                "event": "shure_meter_values",
                "mac": event.device_name,
                "channel": event.data.get("channel"),
                "key": event.data.get("key"),
                "value": event.data.get("value"),
            }
        )

    def _serialized_device(self, server_name: str, device) -> dict | None:
        if self.managed_inventory is None or not self.managed_inventory.enabled:
            return DanteDeviceSerializer.to_json(device)
        if self.managed_controls.direct_devices().get(server_name) is not device:
            return self._serialized_devices().get(server_name)
        return self.managed_inventory.serialize_devices({server_name: device}).get(server_name)

    def _schedule_discovery_republish(self, delay: float) -> None:
        if self._discovery_republish is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._discovery_republish = loop.call_later(
            delay, lambda: loop.create_task(self.publish_inventory_changes(self._serialized_devices()))
        )

    def _serialized_devices(self, context_name: str | None = None) -> dict[str, dict]:
        scope = REQUEST_INVENTORY.get()
        if scope is not None and scope.get("reuse") and context_name is None:
            if "records" not in scope:
                scope["records"] = self._serialize_inventory(None)
            return scope["records"]
        return self._serialize_inventory(context_name)

    def _serialize_inventory(self, context_name: str | None) -> dict[str, dict]:
        if self.managed_inventory is not None and self.managed_inventory.enabled:
            records = self.managed_inventory.serialize_devices(self.managed_controls.direct_devices())
            records = self.managed_controls.reconcile(records)
            remaining = (
                UNENROLLED_DISCOVERY_GRACE_SECONDS - (time.monotonic() - self.discovery_started_monotonic)
                if self.discovery_started_monotonic is not None
                else 0.0
            )
            if remaining > 0:
                records = {key: record for key, record in records.items() if not _awaiting_direct_discovery(record)}
                self._schedule_discovery_republish(remaining)
        else:
            self.managed_controls.clear()
            records = {
                server_name: DanteDeviceSerializer.to_json(device)
                for server_name, device in self.application.devices.items()
            }
        for key, record in records.items():
            if record.get("online"):
                self._dismissed_offline_inventory.discard(key)
        records = {key: record for key, record in records.items() if key not in self._dismissed_offline_inventory}
        if context_name is not None:
            records = {
                server_name: record
                for server_name, record in records.items()
                if not record.get("ddm_device_id") or record.get("ddm_context") == context_name
            }
        return records

    def _snapshot_payload(self) -> dict:
        shure_state = {}
        if self.shure:
            shure_state = {mac: device.to_json() for mac, device in self.shure.devices.items()}
        metering_state = self.metering.get_cached_levels_by_server() if self.metering else {}
        return {
            "event": "snapshot",
            "api_version": API_VERSION,
            "devices": self._serialized_devices(),
            "external_flows": self.application.external_flows.to_dict(),
            "shure_devices": shure_state,
            "metering": metering_state,
            "metering_scale": metering_scale(),
            "managed": self._managed_payload(),
        }

    def _managed_payload(self) -> dict | None:
        if self.managed_inventory is None or not isinstance(
            getattr(self.managed_inventory, "configuration", None), DDMConfiguration
        ):
            return None
        return {
            "status": self.managed_inventory.status(),
            "domains": self.managed_inventory.domains(),
            "connections": self._connection_state(self.managed_inventory.configuration),
        }

    async def publish_inventory_snapshot(self) -> None:
        if not self.sse_clients:
            return
        payload = self._snapshot_payload()
        self._published_inventory = {
            server_name: _without_telemetry(record) for server_name, record in payload["devices"].items()
        }
        self._published_managed = payload["managed"]
        await self._broadcast_sse(payload)

    async def publish_inventory_changes(self, records: dict[str, dict]) -> None:
        if not self.sse_clients:
            return
        if not self._published_inventory:
            await self.publish_inventory_snapshot()
            return
        for server_name in sorted(self._published_inventory.keys() - records.keys()):
            del self._published_inventory[server_name]
            await self._broadcast_sse({"event": "device_removed", "server_name": server_name})
        for server_name, record in records.items():
            visible = _without_telemetry(record)
            previous = self._published_inventory.get(server_name)
            if previous == visible:
                continue
            self._published_inventory[server_name] = visible
            await self._broadcast_sse(
                {
                    "event": "device_discovered" if previous is None else "device_updated",
                    "server_name": server_name,
                    "device": record,
                },
                full_record_clients=previous is None or substantive_record(previous) != substantive_record(visible),
            )
        managed = self._managed_payload()
        if managed != self._published_managed:
            self._published_managed = managed
            await self._broadcast_sse({"event": "managed_status", "managed": managed})

    async def _broadcast_sse(self, data, *, full_record_clients: bool = True):
        shared_payload = None
        for client in tuple(self.sse_clients.values()):
            if client.view is not None:
                for event in client.view.events_for(data):
                    if event is data:
                        if shared_payload is None:
                            shared_payload = _encode_sse(data)
                        payload = shared_payload
                    else:
                        payload = _encode_sse(event)
                    if not self._enqueue_sse(client, event, payload):
                        break
                continue
            if not full_record_clients:
                continue
            if shared_payload is None:
                shared_payload = _encode_sse(data)
            self._enqueue_sse(client, data, shared_payload)

    def _enqueue_sse(self, client: _SseClient, data: dict, payload: bytes) -> bool:
        # Detailed samples contain complete vectors. Partial signal-presence
        # updates and control events must retain their original ordering.
        key = (
            data["server_name"]
            if data.get("event") == "meter_values" and data.get("metering_source") == "detailed"
            else None
        )
        try:
            if key is None:
                client.queue.put_nowait(payload)
                client.pending_meters.clear()
            elif key in client.pending_meters:
                client.pending_meters[key].payload = payload
            else:
                update = _MeterUpdate(key, payload)
                client.queue.put_nowait(update)
                client.pending_meters[key] = update
        except asyncio.QueueFull:
            self._drop_sse_client(client, "outbound event queue full")
            return False
        return True

    @staticmethod
    def _sse_payload(client: _SseClient, payload: bytes | _MeterUpdate) -> bytes:
        if isinstance(payload, _MeterUpdate):
            if client.pending_meters.get(payload.key) is payload:
                del client.pending_meters[payload.key]
            return payload.payload
        return payload

    async def _sse_sender(self, client: _SseClient):
        try:
            while True:
                chunks = [self._sse_payload(client, await client.queue.get())]
                while not client.queue.empty():
                    chunks.append(self._sse_payload(client, client.queue.get_nowait()))
                transport = getattr(client.writer, "transport", None)
                if transport is not None and transport.is_closing():
                    raise ConnectionResetError("SSE client transport is closing")
                client.writer.write(b"".join(chunks))
                if transport is None or transport.get_write_buffer_size():
                    await _bounded(
                        client.writer.drain(),
                        SSE_DRAIN_TIMEOUT_SECONDS,
                    )
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, OSError) as exception:
            logger.debug(f"SSE client writer stopped: {exception}")
        finally:
            self._drop_sse_client(client, "writer stopped", cancel_sender=False)

    def _drop_sse_client(self, client: _SseClient, reason: str, cancel_sender: bool = True):
        if self.sse_clients.get(client.writer) is client:
            self.sse_clients.pop(client.writer, None)
            logger.debug(f"SSE client disconnected: {reason}")
        client.closed.set()
        if (
            cancel_sender
            and client.sender_task
            and client.sender_task is not asyncio.current_task()
            and not client.sender_task.done()
        ):
            client.sender_task.cancel()
        try:
            client.writer.close()
        except (OSError, RuntimeError) as exception:
            logger.debug(f"SSE client close error: {exception}", exc_info=True)

    async def _close_sse_client(self, client: _SseClient, reason: str):
        self._drop_sse_client(client, reason)
        task = client.sender_task
        if task and task is not asyncio.current_task():
            done, _ = await asyncio.wait({task}, timeout=SSE_CLOSE_TIMEOUT_SECONDS)
            if not done:
                logger.debug("SSE sender task did not cancel within the close timeout")
        try:
            await _bounded(
                client.writer.wait_closed(),
                SSE_CLOSE_TIMEOUT_SECONDS,
            )
        except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, OSError) as exception:
            logger.debug(f"SSE client shutdown ended with {exception}")

    async def _bonjour_monitor_loop(self):
        self._last_bonjour_probe_wall_time = time.time()
        while True:
            try:
                await asyncio.sleep(BONJOUR_MONITOR_INTERVAL_SECONDS)
                current_wall_time = time.time()
                elapsed_wall_time = current_wall_time - self._last_bonjour_probe_wall_time
                self._last_bonjour_probe_wall_time = current_wall_time

                self._renew_tls_identity()
                await self._reconcile_bonjour(
                    woke_from_sleep=(
                        elapsed_wall_time > BONJOUR_MONITOR_INTERVAL_SECONDS * BONJOUR_SLEEP_GAP_MULTIPLIER
                    )
                )
            except asyncio.CancelledError:
                raise
            except (OSError, RuntimeError, ZeroconfError) as exception:
                logger.warning(f"Daemon Bonjour monitor error: {exception}")

    def _read_tls_fingerprint(self) -> str | None:
        if self.tls is None:
            return None
        try:
            return certificate_fingerprint(self.tls.certificate)
        except TLSConfigurationError as exception:
            logger.warning(f"Daemon TLS fingerprint unavailable for Bonjour: {exception}")
            return None

    def _renew_tls_identity(self):
        if self.tls is None or not self.tls.generated or self._tls_context is None:
            return
        if time.time() < self._tls_renewal_retry_wall_time or identity_is_current(self._tls_coverage):
            return

        try:
            identity = ensure_generated_identity(self.tls.certificate)
            self._tls_context.load_cert_chain(certfile=str(identity), keyfile=str(identity))
        except (OSError, TLSConfigurationError, ssl.SSLError) as exception:
            self._tls_renewal_retry_wall_time = time.time() + TLS_RENEWAL_RETRY_SECONDS
            logger.warning(f"Daemon TLS certificate renewal failed: {exception}")
            return

        self._tls_coverage = identity_coverage(identity)
        self._tls_fingerprint = self._read_tls_fingerprint()
        logger.info("Daemon TLS certificate renewed")

    async def _reconcile_bonjour(self, force=False, woke_from_sleep=False):
        current_addresses = self._get_advertisement_addresses()
        if not current_addresses:
            if self.zeroconf or self.service_info:
                logger.info("Daemon Bonjour advertisement removed because no non-loopback IPv4 addresses are available")
                await self._close_bonjour()
            return

        refresh_reason = None
        recreate_service = False

        if force:
            refresh_reason = "startup"
            recreate_service = True
        elif not self.zeroconf or not self.service_info:
            refresh_reason = "registration missing"
            recreate_service = True
        elif (
            self._tls_fingerprint and self.service_info.properties.get(b"tls_sha256") != self._tls_fingerprint.encode()
        ):
            refresh_reason = "TLS identity changed"
        elif current_addresses != self._bonjour_addresses:
            previous_addresses = ", ".join(self._bonjour_addresses) or "none"
            refresh_reason = f"address change: {previous_addresses} -> {', '.join(current_addresses)}"
            recreate_service = True
        elif woke_from_sleep:
            refresh_reason = "wake from sleep"
            recreate_service = True
        elif (
            self._bonjour_registered_monotonic is None
            or time.monotonic() - self._bonjour_registered_monotonic >= BONJOUR_REFRESH_INTERVAL_SECONDS
        ):
            refresh_reason = "periodic refresh"

        if not refresh_reason:
            return

        await self._publish_bonjour(
            current_addresses,
            reason=refresh_reason,
            recreate_service=recreate_service,
        )

    async def _publish_bonjour(self, addresses, reason, recreate_service):
        registered_name = self.service_info.name if self.service_info else None
        service_info = self._build_service_info(addresses, name=registered_name)

        if not recreate_service and self.zeroconf and self.service_info:
            try:
                await self.zeroconf.async_update_service(service_info)
                self.service_info = service_info
                self._bonjour_addresses = addresses
                self._bonjour_registered_monotonic = time.monotonic()
                logger.info(f"Daemon Bonjour advertisement refreshed ({reason}) at {', '.join(addresses)}:{self.port}")
                return
            except (OSError, RuntimeError, ZeroconfError) as exception:
                logger.warning(f"Daemon Bonjour update failed ({reason}); recreating advertisement: {exception}")

        await self._close_bonjour()

        zeroconf = AsyncZeroconf(
            interfaces=list(addresses),
            ip_version=IPVersion.V4Only,
        )
        try:
            await zeroconf.async_register_service(service_info, allow_name_change=True)
        except asyncio.CancelledError:
            await self._dispose_bonjour(zeroconf, service_info)
            raise
        except (OSError, RuntimeError, ZeroconfError) as exception:
            await self._dispose_bonjour(zeroconf, service_info)
            logger.warning(f"Daemon Bonjour advertisement failed ({reason}): {exception}")
            return

        self.zeroconf = zeroconf
        self.service_info = service_info
        self._bonjour_addresses = addresses
        self._bonjour_registered_monotonic = time.monotonic()
        logger.info(f"Daemon Bonjour advertisement refreshed ({reason}) at {', '.join(addresses)}:{self.port}")

    async def _dispose_bonjour(self, zeroconf, service_info):
        if service_info:
            try:
                await _bounded(zeroconf.async_unregister_service(service_info), 5)
            except (OSError, RuntimeError, ZeroconfError) as exception:
                logger.warning(f"Daemon Bonjour unregister failed: {exception}", exc_info=True)

        try:
            await _bounded(zeroconf.async_close(), 5)
        except (OSError, RuntimeError, ZeroconfError) as exception:
            logger.warning(f"Daemon Bonjour close failed: {exception}", exc_info=True)

    async def _close_bonjour(self):
        zeroconf = self.zeroconf
        service_info = self.service_info

        self.zeroconf = None
        self.service_info = None
        self._bonjour_addresses = ()
        self._bonjour_registered_monotonic = None

        if zeroconf:
            cleanup_task = asyncio.create_task(self._dispose_bonjour(zeroconf, service_info))
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                await cleanup_task
                raise

    def _build_service_info(self, addresses, name=None):
        hostname = socket.gethostname().removesuffix(".local")
        server_hostname = _bounded_service_label(hostname, SERVICE_LABEL_MAXIMUM_BYTES)
        properties = {"version": "1"}
        if version := self.server_info.get("version"):
            properties["server_version"] = version
        if revision := self.server_info.get("git_revision"):
            properties["git_revision"] = revision
        properties["mcp_path"] = MCP_PATH
        if self.tls is not None:
            properties["tls_port"] = str(self.tls.port)
            if self._tls_fingerprint:
                properties["tls_sha256"] = self._tls_fingerprint
        properties["scheme"] = "https" if self.tls is not None else "http"
        return ServiceInfo(
            DAEMON_SERVICE_TYPE,
            name or f"{_daemon_service_instance_label(hostname)}.{DAEMON_SERVICE_TYPE}",
            addresses=[socket.inet_aton(address) for address in addresses],
            port=self.tls.port if self.tls is not None else self.port,
            properties=properties,
            server=f"{server_hostname}.local.",
        )

    def _get_advertisement_addresses(self):
        return advertisement_addresses(app_settings.interface)

    async def handle_connection(self, reader, writer):
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not raw:
                return

            request = raw.decode().strip()
            parts = request.split(" ", 2)
            if len(parts) < 2:
                await self._send_json(writer, {"error": "bad request"}, 400)
                return

            method = parts[0]
            path = parts[1]

            headers = {}
            while True:
                header_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                if header_line in (b"\r\n", b"\n", b""):
                    break
                decoded = header_line.decode().strip()
                if ":" in decoded:
                    key, value = decoded.split(":", 1)
                    headers[key.strip().lower()] = value.strip()

            body = None
            if method == "POST":
                content_length = int(headers.get("content-length", "0"))
                if content_length > 0:
                    body = await asyncio.wait_for(reader.readexactly(content_length), timeout=5.0)

            await self._route(method, path, body, writer, reader, headers)

        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError) as exception:
            logger.debug(f"Daemon HTTP API peer disconnected: {exception}")
        except Exception:
            logger.warning("Daemon HTTP API connection error", exc_info=True)

    async def _route(self, method, path, body, writer, reader, headers=None):
        headers = dict(headers or {})
        headers[":scheme"] = "https" if writer.get_extra_info("ssl_object") is not None else "http"

        if method == "GET" and urlsplit(path).path == "/events" and not prefers_web_page(headers):
            await self._handle_sse(writer, reader, parse_qs(urlsplit(path).query))
            return

        try:
            if urlsplit(path).path == MCP_PATH:
                await self._handle_mcp(method, body, writer, headers, reader)
            elif self.is_oauth_path(urlsplit(path).path):
                await self._handle_oauth(method, path, body, writer, headers)
            else:
                await self._dispatch(method, path, body, writer, headers)
        except TimeoutError:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
        except (BrokenPipeError, ConnectionResetError) as exception:
            logger.debug(f"Daemon HTTP API client left during {method} {path}: {exception}")
        except Exception as exception:
            logger.exception(f"Daemon HTTP API error handling {method} {path}")
            await self._send_json(writer, {"error": str(exception)}, 500)

        try:
            writer.close()
            await writer.wait_closed()
        except (BrokenPipeError, ConnectionResetError, OSError) as exception:
            logger.debug(f"Daemon HTTP API writer close ended with {exception}")

    async def _dispatch(self, method, path, body, writer, headers=None):
        if (
            path.startswith("/presets/")
            or path
            in {
                "/ddm/login",
                "/ddm/logout",
                "/ddm/context",
                "/ddm/enrollment",
                "/ddm/domains",
                "/ddm/profile",
                "/ddm/domains/update",
                "/device-lock-key",
                "/settings/monitoring",
                "/host-audio/cards",
            }
        ) and headers:
            origin = headers.get("origin")
            if origin and urlsplit(origin).netloc != headers.get("host"):
                await self._send_json(writer, {"error": "Settings must be changed from this app's origin"}, 403)
                return
        if method == "GET":
            route, _, query_string = path.partition("?")
            if prefers_web_page(headers) and is_application_route(route):
                await self._handle_web_asset(writer, unquote(route))
                return
            query = parse_qs(query_string)
            context_name = next(iter(query.get("context", ())), None)
            if route == "/server-info":
                await self._send_json(writer, self.server_info)
            elif route == "/api":
                await self._send_json(writer, api_contract())
            elif route == "/settings":
                await self._handle_get_settings(writer)
            elif route == "/ddm/connections":
                await self._handle_get_connections(writer)
            elif route == "/shure/devices":
                await self._handle_get_shure_devices(writer)
            elif route.startswith("/shure/devices/"):
                await self._handle_get_shure_device(writer, route[len("/shure/devices/") :])
            elif route == "/host-audio":
                await self._handle_get_host_audio(writer, query)
            elif route == "/host-audio/trace":
                await self._handle_get_host_audio_trace(writer, query)
            elif route == "/devices":
                await self._handle_get_devices(writer, context_name)
            elif route == "/external-flows":
                await self._send_json(writer, self.application.external_flows.to_dict())
            elif route == "/external-flows/diagnostics":
                await self._send_json(writer, self.application.sap.diagnostics())
            elif route == "/ddm/devices":
                await self._handle_get_ddm_devices(writer, context_name)
            elif route == "/ddm/domains":
                await self._handle_get_ddm_domains(writer, context_name)
            elif route == "/ddm/status":
                await self._handle_get_ddm_status(writer)
            elif route.startswith("/devices/"):
                await self._handle_get_device(writer, unquote(route[len("/devices/") :]), context_name)
            elif route.startswith("/interfaces/"):
                await self._handle_get_interfaces(writer, unquote(route[len("/interfaces/") :]))
            elif route.startswith("/redundancy/"):
                await self._handle_get_redundancy(writer, unquote(route[len("/redundancy/") :]))
            elif route.startswith("/lock-status/"):
                await self._handle_get_lock_status(writer, unquote(route[len("/lock-status/") :]))
            elif route.startswith("/transmit-flows/"):
                await self._handle_get_transmit_flows(writer, unquote(route[len("/transmit-flows/") :]))
            elif route == "/metering/status":
                await self._handle_metering_status(writer)
            elif route == "/metering/cache":
                await self._handle_metering_cache(writer)
            elif route.startswith("/metering/snapshot/"):
                await self._handle_metering_snapshot(writer, unquote(route[len("/metering/snapshot/") :]))
            elif route == "/event-journal":
                await self._handle_get_event_journal(writer, query)
            elif route.startswith("/diagnostics/"):
                await self._handle_diagnostics(
                    writer,
                    unquote(route[len("/diagnostics/") :]),
                    include_clock=query.get("clock", [""])[-1] != "0",
                    receiver_history=query.get("receiver_history", [""])[-1] != "0",
                    history_points=query.get("history", [""])[-1] == "points",
                )
            elif route == "/issues":
                await self._handle_get_issues(writer, query)
            else:
                await self._handle_web_asset(writer, unquote(route))
            return

        if method == "DELETE":
            route, _, query = path.partition("?")
            if route == "/event-journal":
                await self._handle_clear_event_journal(writer)
            elif route == "/devices":
                await self._handle_forget_devices(writer, parse_qs(query))
            elif route.startswith("/devices/"):
                await self._handle_forget_device(writer, unquote(route[len("/devices/") :]))
            else:
                await self._send_json(writer, {"error": "not found"}, 404)
            return

        if method != "POST":
            await self._send_json(writer, {"error": "not found"}, 404)
            return

        handler = self.post_handlers.get(path)
        if not handler:
            await self._send_json(writer, {"error": "not found"}, 404)
            return

        if path in self.loopback_only_paths and not self._peer_is_loopback(writer):
            await self._send_json(writer, {"error": "forbidden"}, 403)
            return

        params = {}
        if body:
            try:
                params = json.loads(body)
            except json.JSONDecodeError as exception:
                await self._send_json(writer, {"error": f"invalid json: {exception}"}, 400)
                return
            if not isinstance(params, dict):
                await self._send_json(writer, {"error": "body must be a json object"}, 400)
                return
        elif path not in self.post_body_optional:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        await handler(writer, params)

    async def _handle_get_event_journal(self, writer, query: dict[str, list[str]]) -> None:
        def first(name: str) -> str | None:
            values = query.get(name)
            return values[0] if values else None

        limit_text = first("limit")
        try:
            limit = int(limit_text) if limit_text is not None else None
            payload = self.event_journal.export(
                device=first("device"),
                kind=first("kind"),
                severity=first("severity"),
                since=first("since"),
                limit=limit,
            )
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        await self._send_json(writer, payload)

    async def _handle_clear_event_journal(self, writer) -> None:
        cleared = self.event_journal.clear()
        await self._send_json(
            writer,
            {
                "cleared": cleared,
                "scope": "local_event_journal",
                "device_commands_sent": 0,
            },
        )

    async def _handle_append_operation_event(self, writer, params) -> None:
        snapshot = params.pop("device_snapshot", None)
        observe_device = params.pop("observe_device", False)
        if not isinstance(snapshot, dict):
            await self._send_json(writer, {"error": "device_snapshot must be an object"}, 400)
            return
        allowed = {
            "operation_id",
            "correlation_id",
            "parent_preset_run_id",
            "operation_name",
            "lifecycle_phase",
            "requested_values",
            "acknowledgement_result_code",
            "transport",
            "effective_values",
            "final_operation_state",
            "persistence_request_acknowledgement",
            "persistence_confirmation",
            "evidence",
            "observation_source",
            "interface_identity",
            "channel_identity",
            "flow_identity",
        }
        if set(params) - (allowed | {"kind", "severity", "derivation_status"}):
            await self._send_json(writer, {"error": "operation event contains unsupported fields"}, 400)
            return
        try:
            kind = MonitoringEventKind(params.pop("kind"))
            if kind not in {MonitoringEventKind.CONFIGURATION_OPERATION, MonitoringEventKind.PRESET_RUN}:
                raise ValueError("operation event kind must be configuration_operation or preset_run")
            event = self.event_journal.record_operation_transition(
                snapshot,
                kind=kind,
                severity=EventSeverity(params.pop("severity")),
                derivation_status=DerivationStatus(params.pop("derivation_status")),
                **params,
            )
            await self.publish_journal_event(event)
            observed_events = self.event_journal.observe_snapshot(snapshot) if observe_device is True else []
            for observed_event in observed_events:
                await self.publish_journal_event(observed_event)
        except (KeyError, TypeError, ValueError) as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        await self._send_json(
            writer,
            {"event": event.to_dict(), "observed_events": [item.to_dict() for item in observed_events]},
        )

    async def _handle_get_issues(self, writer, query: dict[str, list[str]]) -> None:
        def first(name: str) -> str | None:
            values = query.get(name)
            return values[0] if values else None

        limit_text = first("limit")
        try:
            limit = int(limit_text) if limit_text is not None else None
            payload = self.event_journal.export_issues(
                device=first("device"),
                kind=first("kind"),
                severity=first("severity"),
                state=first("state"),
                limit=limit,
            )
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        await self._send_json(writer, payload)

    async def publish_journal_event(self, event: MonitoringEvent) -> None:
        if not self.sse_clients:
            return
        await self._broadcast_sse({"event": "monitoring_event", "journal_event": event.to_dict()})

    async def _handle_sse(self, writer, reader, query=None):
        client = None
        try:
            view = SseDeviceView.from_query(query or {})
            snapshot = self._snapshot_payload()
            initial = _encode_sse(view.initial_snapshot(snapshot) if view is not None else snapshot)

            client = _SseClient(writer=writer, view=view)
            client.queue.put_nowait(initial)
            self.sse_clients[writer] = client

            response_header = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream\r\n"
                "Cache-Control: no-cache\r\n"
                "Connection: keep-alive\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "\r\n"
            ).encode()
            writer.write(response_header)
            await _bounded(writer.drain(), SSE_DRAIN_TIMEOUT_SECONDS)

            if client.closed.is_set():
                return
            client.sender_task = asyncio.create_task(self._sse_sender(client))

            while True:
                read_task = asyncio.create_task(reader.read(1))
                closed_task = asyncio.create_task(client.closed.wait())
                try:
                    done, _ = await asyncio.wait(
                        (read_task, closed_task),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    pending = [task for task in (read_task, closed_task) if not task.done()]
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.wait(pending, timeout=SSE_CLOSE_TIMEOUT_SECONDS)
                    for task in (read_task, closed_task):
                        if task.done() and not task.cancelled() and task.exception() is not None:
                            logger.debug(f"SSE connection ended with {task.exception()}")
                if closed_task in done or not read_task.result():
                    break
        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, OSError) as exception:
            logger.debug(f"SSE connection ended with {exception}")
        finally:
            if client is not None:
                await self._close_sse_client(client, "peer disconnected")
            else:
                try:
                    writer.close()
                    await _bounded(writer.wait_closed(), SSE_CLOSE_TIMEOUT_SECONDS)
                except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, OSError) as exception:
                    logger.debug(f"SSE writer close ended with {exception}")
