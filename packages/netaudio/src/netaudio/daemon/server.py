from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from collections.abc import Coroutine
from typing import Any, Awaitable, cast

from zeroconf import ServiceStateChange
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

from netaudio.daemon.metering import MeteringManager
from netaudio.daemon.relay import RelayServer
from netaudio.shure.manager import ShureManager
from netaudio.dante.services.heartbeat import DanteHeartbeatService
from netaudio.dante.application import DanteApplication
from netaudio.dante.const import (
    SERVICE_CMC,
    SERVICES,
)
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.latency import nanoseconds_to_milliseconds
from netaudio.dante.state import DanteStateService


class DaemonAlreadyRunningError(Exception):
    pass


try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None

try:
    from netaudio.daemon.dbus_service import DBusService as _DBusService
except ImportError:
    _DBusService = None

logger = logging.getLogger("netaudio")

ONLINE_REVALIDATE_IDLE_SECONDS = 45.0


def _probe_device(device_ip: str) -> bool:
    from netaudio import core

    try:
        with core.CoreClient(device_ip, timeout_ms=1000, attempts=2) as client:
            client.get_device_info()
        return True
    except core.NetaudioCoreError:
        return False


def _sd_notify(state):
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    import socket as _socket

    sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_DGRAM)
    try:
        if addr[0] == "@":
            addr = "\0" + addr[1:]
        sock.sendto(state.encode(), addr)
    finally:
        sock.close()


class NetaudioDaemon:
    def __init__(self, dissect=False, capture=False, relay_port=None):
        from netaudio import core

        core.require()

        self._capture = capture
        self._relay_port = relay_port
        self._packet_store = None
        self._session_id = None

        from netaudio.common.config_loader import load_capture_profile, resolve_db_from_config

        profile_cfg, _ = load_capture_profile(None, None)

        from netaudio.common.app_config import settings as app_settings

        lock_key_value = profile_cfg.get("device_lock_key")
        if lock_key_value:
            app_settings.device_lock_key = lock_key_value.encode("ascii")
        else:
            from netaudio.common.key_extract import extract_lock_key

            extracted_key = extract_lock_key()
            if extracted_key:
                app_settings.device_lock_key = extracted_key
                logger.info("Extracted device lock key from Dante Controller")

        if capture:
            from netaudio.dante.packet_store import PacketStore

            db_path = resolve_db_from_config(None, profile_cfg)
            self._packet_store = PacketStore(db_path=db_path)

            active_session = self._packet_store.get_latest_session(active_only=True)
            if active_session:
                self._session_id = active_session["id"]
                logger.info(f"Capture: recording to session #{self._session_id}")
            else:
                logger.info("Capture: enabled but no active session")

        self.application = DanteApplication(packet_store=self._packet_store, dissect=dissect)

        if self._packet_store and self._session_id:
            self.application.capture_session_id = self._session_id
            for service in [self.application.settings, self.application.cmc, self.application.notifications]:
                service.session_id = self._session_id

        self.state = DanteStateService(self.application)
        self.zeroconf = None
        self.browser = None
        self.running = False
        self._redis = None
        self._stop_event = asyncio.Event()
        self._start_lock = asyncio.Lock()
        self._startup_task: asyncio.Task | None = None
        self._startup_waiters = 0
        self._stop_lock = asyncio.Lock()
        self._stop_complete = False
        self.metering = MeteringManager(self.application)
        self.shure = ShureManager(self.application.dispatcher) if ShureManager else None
        self.relay = RelayServer(
            self.application,
            self.state,
            metering=self.metering,
            shure=self.shure,
            port=self._relay_port,
            on_shutdown=self.request_shutdown,
            mark_offline=self.mark_device_offline,
        )
        self.heartbeat: DanteHeartbeatService | None = None
        self._revalidate_task: asyncio.Task | None = None
        self._pending_offline_tasks: dict[str, asyncio.Task] = {}
        self._background_tasks: set[asyncio.Task] = set()
        self._offline_failures: dict[str, int] = {}
        self._offline_candidate_since: dict[str, float] = {}
        self._dbus = None
        self._event_listeners_registered = False

    def request_shutdown(self):
        self.running = False
        self._stop_event.set()

    def _spawn_background(self, coroutine: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task | None:
        if not self.running:
            coroutine.close()
            return None
        task = asyncio.create_task(coroutine, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_task_done)
        return task

    def _background_task_done(self, task: asyncio.Task) -> None:
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None:
            logger.error(
                f"Daemon background task {task.get_name()} failed",
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    @property
    def devices(self) -> dict:
        return self.application.devices

    async def _connect_redis(self):
        if aioredis is None:
            logger.info("redis.asyncio not available, running without Redis")
            return

        candidate = None
        try:
            redis_socket = os.environ.get("REDIS_SOCKET")
            redis_host = os.environ.get("REDIS_HOST") or "localhost"
            redis_port = int(os.environ.get("REDIS_PORT") or 6379)
            redis_db = int(os.environ.get("REDIS_DB") or 0)

            if redis_socket:
                candidate = aioredis.Redis(unix_socket_path=redis_socket, db=redis_db)
            else:
                candidate = aioredis.Redis(host=redis_host, port=redis_port, db=redis_db)

            await cast(Awaitable[Any], candidate.ping())
            try:
                await candidate.config_set("notify-keyspace-events", "Kgh$")
            except Exception as exception:
                logger.warning(
                    f"Could not set Redis keyspace notification config, relying on server config: {exception}"
                )
            self._redis = candidate
            logger.info("Connected to Redis")
        except Exception as exception:
            logger.info(f"Redis not available, continuing without it: {exception}")
            if candidate is not None:
                try:
                    await candidate.aclose()
                except Exception as close_exception:
                    logger.debug(f"Redis failed-connect cleanup error: {close_exception}")
            self._redis = None

    async def _publish_device_to_redis(self, device):
        if not self._redis:
            return

        key = f"netaudio:daemon:device:{device.server_name}"
        try:
            await cast(
                Awaitable[int],
                self._redis.hset(
                    key,
                    mapping={
                        "server_name": device.server_name or "",
                        "name": device.name or "",
                        "ipv4": str(device.ipv4) if device.ipv4 else "",
                        "model_id": device.model_id or "",
                        "bluetooth_device": device.bluetooth_device or "",
                        "online": "1" if device.online else "0",
                        "last_seen": str(device.last_seen) if device.last_seen else "",
                    },
                ),
            )
        except Exception as exception:
            logger.warning(f"Redis publish error for {device.server_name}: {exception}")

    async def _delete_device_from_redis(self, server_name):
        if not self._redis:
            return

        key = f"netaudio:daemon:device:{server_name}"
        try:
            await self._redis.delete(key)
        except Exception as exception:
            logger.warning(f"Redis delete error for {server_name}: {exception}")

    def _load_shure_correlations(self):
        try:
            from netaudio.common.config_loader import default_config_path

            path = default_config_path()
            if not path.exists():
                return {}
            if sys.version_info >= (3, 11):
                import tomllib
            else:
                import tomli as tomllib
            data = tomllib.loads(path.read_text())
            return data.get("shure", {}).get("correlations", {})
        except Exception:
            return {}

    @staticmethod
    def _normalize_mac(mac):
        return mac.lower().replace(":", "").replace("-", "")[:12]

    def _find_dante_for_shure(self, shure_device):
        try:
            from netaudio.shure.device import ShureDeviceType
        except ImportError:
            return None
        if shure_device.device_type != ShureDeviceType.ad4d:
            return None

        correlations = self._load_shure_correlations()
        dante_mac = correlations.get(shure_device.mac)
        if not dante_mac:
            return None

        normalized_target = self._normalize_mac(dante_mac)
        for device in self.application.devices.values():
            if device.mac_address and self._normalize_mac(device.mac_address) == normalized_target:
                return device

        return None

    def _dante_device_to_dict(self, device):
        result = {
            "server_name": device.server_name or "",
            "name": device.name or "",
            "ip": str(device.ipv4) if device.ipv4 else "",
            "mac": device.mac_address or "",
            "sample_rate": device.sample_rate,
            "supported_sample_rates": device.supported_sample_rates,
            "encoding": device.encoding,
            "supported_encodings": device.supported_encodings,
            "latency": device.latency,
            "active_latency": device.active_latency,
            "configured_latency": device.configured_latency,
            "default_latency": device.default_latency,
            "min_latency": device.min_latency,
            "max_latency": device.max_latency,
            "standard_latency_choices": device.standard_latency_choices,
        }

        if device.tx_channels:
            result["tx_channels"] = {
                str(num): {"name": ch.name or "", "friendly_name": ch.friendly_name or ""}
                for num, ch in sorted(device.tx_channels.items())
                if ch.name
            }

        if device.rx_channels:
            result["rx_channels"] = {
                str(num): {"name": ch.name or "", "friendly_name": ch.friendly_name or ""}
                for num, ch in sorted(device.rx_channels.items())
                if ch.name
            }

        if device.subscriptions:
            subs = []
            for sub in device.subscriptions:
                subs.append(
                    {
                        "rx_channel_name": sub.rx_channel_name or "",
                        "tx_device_name": sub.tx_device_name or "",
                        "tx_channel_name": sub.tx_channel_name or "",
                        "status": ", ".join(sub.status_text()) if sub.status_code is not None else "",
                    }
                )
            result["subscriptions"] = subs

        return result

    async def _publish_shure_to_redis(self, mac):
        if not self._redis or not self.shure:
            return

        device = self.shure.devices.get(mac)
        if not device:
            return

        data = device.to_json()

        dante = self._find_dante_for_shure(device)
        if dante:
            data["dante"] = self._dante_device_to_dict(dante)

        try:
            await self._redis.set(f"netaudio:shure:{mac}", json.dumps(data))
        except Exception as exception:
            logger.warning(f"Redis publish error for Shure {mac}: {exception}")

    async def _publish_shure_meters_to_redis(self, mac, data):
        if not self._redis:
            return

        try:
            await self._redis.set(f"netaudio:shure:meters:{mac}", json.dumps(data))
        except Exception as exception:
            logger.warning(f"Redis meter publish error for Shure {mac}: {exception}")

    async def _delete_shure_from_redis(self, mac):
        if not self._redis:
            return

        try:
            await self._redis.delete(f"netaudio:shure:{mac}", f"netaudio:shure:meters:{mac}")
        except Exception as exception:
            logger.warning(f"Redis delete error for Shure {mac}: {exception}")

    async def _on_shure_discovered(self, event: DanteEvent):
        logger.info(f"Shure device discovered: {event.device_name}")
        await self._publish_shure_to_redis(event.device_name)

    async def _on_shure_updated(self, event: DanteEvent):
        await self._publish_shure_to_redis(event.device_name)

    async def _on_shure_removed(self, event: DanteEvent):
        logger.info(f"Shure device removed: {event.device_name}")
        await self._delete_shure_from_redis(event.device_name)

    async def _on_shure_meters(self, event: DanteEvent):
        await self._publish_shure_meters_to_redis(event.device_name, event.data)

    def _register_event_listeners(self):
        if self._event_listeners_registered:
            return
        self.application.dispatcher.on(EventType.DEVICE_DISCOVERED, self._on_device_discovered)
        self.application.dispatcher.on(EventType.DEVICE_UPDATED, self._on_device_updated)
        self.application.dispatcher.on(EventType.DEVICE_REMOVED, self._on_device_removed)
        self.application.dispatcher.on(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_discovered)
        self.application.dispatcher.on(EventType.SHURE_DEVICE_UPDATED, self._on_shure_updated)
        self.application.dispatcher.on(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)
        self.application.dispatcher.on(EventType.SHURE_METER_VALUES, self._on_shure_meters)

        self.state.register()
        self._event_listeners_registered = True

    async def _republish_correlated_shure(self, dante_device):
        if not self.shure:
            return
        correlations = self._load_shure_correlations()
        dante_mac = dante_device.mac_address or ""
        if not dante_mac:
            return
        normalized_dante = self._normalize_mac(dante_mac)
        for shure_mac, corr_dante_mac in correlations.items():
            if self._normalize_mac(corr_dante_mac) == normalized_dante:
                await self._publish_shure_to_redis(shure_mac)

    async def _on_device_discovered(self, event: DanteEvent):
        device = self.devices.get(event.server_name)
        if device:
            device.update_last_seen()
            logger.info(f"Device discovered (event): {event.server_name}")
            if device.ipv4:
                await self.application.cmc.register_device(str(device.ipv4))
            await self._publish_device_to_redis(device)
            await self._republish_correlated_shure(device)

    async def _on_device_updated(self, event: DanteEvent):
        device = self.devices.get(event.server_name)
        if device:
            await self._publish_device_to_redis(device)
            await self._republish_correlated_shure(device)

    async def _on_device_removed(self, event: DanteEvent):
        logger.info(f"Device removed (event): {event.server_name}")
        if self.metering:
            self.metering.cleanup_device(event.server_name)
        device = self.devices.get(event.server_name)
        if device:
            await self._publish_device_to_redis(device)
            await self.state.refresh_affected_subscriptions(device)
        else:
            await self._delete_device_from_redis(event.server_name)

    async def start(self):
        async with self._start_lock:
            if self._startup_task is None:
                self._startup_task = asyncio.create_task(
                    self._initialize(),
                    name="netaudio-daemon-startup",
                )
            startup_task = self._startup_task
            self._startup_waiters += 1

        cancelled = False
        try:
            await asyncio.shield(startup_task)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            async with self._start_lock:
                self._startup_waiters -= 1
                cancel_startup = cancelled and self._startup_waiters == 0 and not startup_task.done()
            if cancel_startup:
                startup_task.cancel()
                await asyncio.gather(startup_task, return_exceptions=True)

        await self._stop_event.wait()

    async def _initialize(self):
        self._stop_complete = False
        self._stop_event.clear()
        self.running = True
        try:
            await self._start_once()
        except BaseException:
            await self.stop()
            raise

    async def _start_once(self):

        try:
            await self.relay.start()
        except OSError as error:
            raise DaemonAlreadyRunningError(
                f"Another daemon is already listening on port {self.relay.port}: {error}"
            ) from error

        logger.info("Daemon listening")

        _sd_notify("STATUS=Connecting to Redis...")
        await self._connect_redis()

        _sd_notify("STATUS=Starting application...")
        await self.application.startup()

        await self.metering.start()

        if self.shure:
            await self.shure.start()

        from netaudio.common.app_config import settings as app_settings

        self.heartbeat = DanteHeartbeatService(
            device_by_ip=self.application._device_by_ip,
            get_devices=lambda: self.application.devices,
            mark_offline=self.mark_device_offline,
            interface_ip=app_settings.interface_ip,
        )
        await self.heartbeat.start()

        self._register_event_listeners()

        from netaudio.common.app_config import settings as app_settings

        if _DBusService and app_settings.dbus_enabled:
            try:
                self._dbus = _DBusService(self)
                await self._dbus.start()
            except Exception as e:
                logger.info(f"D-Bus not available, continuing without it: {e}")
                self._dbus = None

        if self.shure:
            for mac in self.shure.devices:
                await self._publish_shure_to_redis(mac)

        _sd_notify("STATUS=Starting mDNS browser...")
        self.zeroconf = AsyncZeroconf()
        self.browser = AsyncServiceBrowser(
            self.zeroconf.zeroconf,
            SERVICES,
            handlers=[self.on_service_state_change],
        )

        logger.info("mDNS browser started, watching for devices...")

        self._revalidate_task = asyncio.create_task(self._revalidate_devices_loop())
        self._spawn_background(
            self._recover_known_devices(delay=3, offline_only=True),
            name="recover-known-devices",
        )

        _sd_notify("READY=1\nSTATUS=Discovering devices...")

    async def stop(self):
        startup_task = self._startup_task
        if startup_task is not None and startup_task is not asyncio.current_task() and not startup_task.done():
            startup_task.cancel()
            await asyncio.gather(startup_task, return_exceptions=True)
        async with self._stop_lock:
            if self._stop_complete:
                return
            await self._stop_once()
            self._stop_complete = True

    async def _stop_once(self):
        self.running = False
        self._stop_event.set()

        if self._revalidate_task:
            self._revalidate_task.cancel()
            await asyncio.gather(self._revalidate_task, return_exceptions=True)
            self._revalidate_task = None

        offline_tasks = list(self._pending_offline_tasks.values())
        for task in offline_tasks:
            task.cancel()
        self._pending_offline_tasks.clear()
        if offline_tasks:
            await asyncio.gather(*offline_tasks, return_exceptions=True)

        background_tasks = list(self._background_tasks)
        for task in background_tasks:
            task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        browser = self.browser
        self.browser = None
        if browser:
            try:
                await browser.async_cancel()
            except Exception as exception:
                logger.debug(f"mDNS browser close error: {exception}")

        zeroconf = self.zeroconf
        self.zeroconf = None
        if zeroconf:
            try:
                await zeroconf.async_close()
            except Exception as exception:
                logger.debug(f"Zeroconf close error: {exception}")

        if self._dbus:
            try:
                await self._dbus.stop()
            except Exception as exception:
                logger.debug(f"D-Bus stop error: {exception}")
            self._dbus = None

        if self.heartbeat:
            try:
                await self.heartbeat.stop()
            except Exception as exception:
                logger.debug(f"Heartbeat stop error: {exception}")
            self.heartbeat = None

        if self.shure:
            try:
                await self.shure.stop()
            except Exception as exception:
                logger.debug(f"Shure stop error: {exception}")

        if self.relay:
            try:
                await self.relay.stop()
            except Exception as exception:
                logger.debug(f"Relay stop error: {exception}")

        if self.metering:
            try:
                await self.metering.stop()
            except Exception as exception:
                logger.debug(f"Metering stop error: {exception}")

        if self._redis:
            try:
                await self._redis.aclose()
            except Exception as exception:
                logger.debug(f"Redis close error: {exception}")
            self._redis = None

        try:
            await self.application.shutdown()
        except Exception as exception:
            logger.debug(f"Application shutdown error: {exception}")

        if self._packet_store:
            try:
                self._packet_store.close()
            except Exception as exception:
                logger.debug(f"Packet store close error: {exception}")
            self._packet_store = None

    def clear_offline_candidate(self, server_name: str) -> None:
        self._offline_failures.pop(server_name, None)
        self._offline_candidate_since.pop(server_name, None)
        task = self._pending_offline_tasks.pop(server_name, None)
        if task:
            task.cancel()

    def mark_device_offline(self, server_name: str) -> None:
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        now = time.monotonic()
        self._offline_failures[server_name] = self._offline_failures.get(server_name, 0) + 1
        self._offline_candidate_since.setdefault(server_name, now)

        failures = self._offline_failures[server_name]
        elapsed = now - self._offline_candidate_since[server_name]
        if failures < 2 or elapsed < 5.0:
            logger.info(f"Device offline candidate {server_name}: failures={failures} elapsed={elapsed:.1f}s")
            return

        if server_name in self._pending_offline_tasks:
            return

        self._pending_offline_tasks[server_name] = asyncio.create_task(self._finalize_offline_device(server_name))

    def mark_device_offline_verified(self, server_name: str, reason: str) -> None:
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        now = time.monotonic()
        self._offline_failures[server_name] = max(self._offline_failures.get(server_name, 0), 2)
        self._offline_candidate_since[server_name] = min(
            self._offline_candidate_since.get(server_name, now),
            now - 5.0,
        )

        if server_name in self._pending_offline_tasks:
            return

        logger.info(f"Device offline candidate verified by {reason}: {server_name}")
        self._pending_offline_tasks[server_name] = asyncio.create_task(self._finalize_offline_device(server_name))

    async def _finalize_offline_device(self, server_name: str) -> None:
        try:
            device = self.devices.get(server_name)
            if device is None or not device.online:
                return

            failures = self._offline_failures.get(server_name, 0)
            since = self._offline_candidate_since.get(server_name)
            if failures < 2 or since is None or (time.monotonic() - since) < 5.0:
                return

            device_ip = str(device.ipv4) if device.ipv4 else None
            if device_ip:
                try:
                    reachable = await asyncio.wait_for(
                        asyncio.to_thread(_probe_device, device_ip),
                        timeout=5.0,
                    )
                    if reachable:
                        logger.info(f"Offline candidate cleared by Dante probe: {server_name}")
                        device.update_last_seen()
                        self.clear_offline_candidate(server_name)
                        return
                except (asyncio.TimeoutError, OSError):
                    pass

            logger.info(f"Device confirmed offline after consecutive failures: {server_name}")
            self.application.mark_device_offline(server_name)
            online = sum(1 for d in self.devices.values() if d.online)
            _sd_notify(f"STATUS={online} device(s) online")
            self.clear_offline_candidate(server_name)
            self._spawn_background(
                self._recheck_offline_device(server_name),
                name=f"recheck-offline:{server_name}",
            )
        finally:
            self._pending_offline_tasks.pop(server_name, None)

    async def _recheck_offline_device(self, server_name: str, delay: float = 10) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        device = self.devices.get(server_name)
        if device is None or device.online:
            return

        device_ip = str(device.ipv4) if device.ipv4 else None
        if not device_ip:
            return

        try:
            reachable = await asyncio.wait_for(
                asyncio.to_thread(_probe_device, device_ip),
                timeout=5.0,
            )
            if not reachable:
                raise OSError("No Dante response")
            logger.info(f"Device reachable after mDNS removal, re-registering: {server_name}")
            self.clear_offline_candidate(server_name)
            device.online = True
            device.update_last_seen()
            self.application.dispatcher.emit_nowait(
                DanteEvent(type=EventType.DEVICE_UPDATED, server_name=server_name, device_name=device.name)
            )
            self._spawn_background(
                self.state.fetch_device_controls(server_name),
                name=f"fetch-controls:{server_name}",
            )
        except (asyncio.TimeoutError, OSError):
            logger.debug(f"Device not reachable after recheck: {server_name}")

    async def _recover_known_devices(self, delay: float = 0, offline_only: bool = True) -> None:
        if delay > 0:
            await asyncio.sleep(delay)

        for server_name, device in list(self.devices.items()):
            if offline_only and device.online:
                continue
            if not device.ipv4:
                continue
            self._spawn_background(
                self._recheck_offline_device(server_name, delay=0),
                name=f"recover-offline:{server_name}",
            )

    async def _verify_quiet_online_device(self, server_name: str) -> None:
        device = self.devices.get(server_name)
        if device is None or not device.online:
            return

        if not device.ipv4:
            return

        last_seen = device.last_seen
        if last_seen is not None and (time.time() - last_seen) < ONLINE_REVALIDATE_IDLE_SECONDS:
            return

        device_ip = str(device.ipv4)
        try:
            reachable = await asyncio.wait_for(
                asyncio.to_thread(_probe_device, device_ip),
                timeout=5.0,
            )
            if reachable:
                self.clear_offline_candidate(server_name)
                device.update_last_seen()
                return
        except (asyncio.TimeoutError, OSError):
            pass

        logger.info(f"Online device failed active revalidation: {server_name}")
        self.mark_device_offline_verified(server_name, reason="active_revalidation")

    async def _verify_quiet_online_devices(self) -> None:
        for server_name, device in list(self.devices.items()):
            if not device.online:
                continue
            if not device.ipv4:
                continue
            last_seen = device.last_seen
            if last_seen is not None and (time.time() - last_seen) < ONLINE_REVALIDATE_IDLE_SECONDS:
                continue
            self._spawn_background(
                self._verify_quiet_online_device(server_name),
                name=f"verify-online:{server_name}",
            )

    async def _revalidate_devices_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(30)
                await self._recover_known_devices(offline_only=True)
                await self._verify_quiet_online_devices()
            except asyncio.CancelledError:
                break
            except Exception as exception:
                logger.debug(f"Revalidation loop error: {exception}")

    def on_service_state_change(self, zeroconf, service_type, name, state_change):
        if service_type == "_netaudio-chan._udp.local.":
            return

        logger.debug(f"mDNS event: {state_change.name} - {service_type} - {name}")

        self._spawn_background(
            self.handle_service_change(zeroconf, service_type, name, state_change),
            name=f"mdns-change:{name}",
        )

    async def handle_service_change(self, zeroconf, service_type, name, state_change):
        try:
            info = AsyncServiceInfo(service_type, name)

            if state_change == ServiceStateChange.Removed:
                for server_name in list(self.devices.keys()):
                    if name.startswith(server_name.replace(".local.", "")):
                        logger.info(f"Device offline candidate (mDNS removed): {server_name}")
                        self.mark_device_offline(server_name)

                return

            if not await info.async_request(zeroconf, 3000):
                return

            addresses = info.parsed_addresses()

            if not addresses:
                return

            server_name = None

            for record in zeroconf.cache.entries_with_name(name):
                if hasattr(record, "server"):
                    server_name = record.server
                    break

            if not server_name:
                return

            service_properties = {}

            for key, value in info.properties.items():
                key = key.decode("utf-8") if isinstance(key, bytes) else key

                if not key:
                    continue

                if isinstance(value, bytes):
                    value = value.decode("utf-8")

                service_properties[key] = value

            service_data = {
                "ipv4": addresses[0],
                "name": name,
                "port": info.port,
                "properties": service_properties,
                "server_name": server_name,
                "type": service_type,
            }

            self.clear_offline_candidate(server_name)

            existing = self.devices.get(server_name)
            was_offline = existing is not None and not existing.online
            is_new = existing is None

            if is_new or was_offline:
                new_device = DanteDevice(server_name=server_name)
                new_device.ipv4 = addresses[0]
                self.application.register_device(server_name, new_device)
                if is_new:
                    logger.info(f"Device discovered: {server_name}")
                else:
                    logger.info(f"Device back online: {server_name}")
                online = sum(1 for d in self.devices.values() if d.online)
                _sd_notify(f"STATUS={online} device(s) online")
                if addresses[0]:
                    await self.application.cmc.register_device(addresses[0])
                if was_offline and self.metering:
                    self.metering.reactivate_device(server_name)

            device = self.devices[server_name]
            device.update_last_seen()

            old_ip = str(device.ipv4) if device.ipv4 else None
            new_ip = addresses[0]

            device_changed = bool(old_ip and old_ip != new_ip)
            if device_changed:
                logger.info(f"Device {server_name} IP changed: {old_ip} -> {new_ip}")

            device.ipv4 = new_ip

            if not device.services:
                device.services = {}

            device.services[name] = service_data

            if "id" in service_properties and service_type == SERVICE_CMC:
                device.mac_address = service_properties["id"]

                if device.ipv4 and device.mac_address:
                    if not device.dante_model:
                        self.application._send_conmon_query_for_device(device, "make_model")

                    if not device.dante_model_id:
                        self.application._send_conmon_query_for_device(device, "dante_model")
                        self._spawn_background(
                            self.state.retry_conmon_query(server_name),
                            name=f"retry-conmon:{server_name}",
                        )

            if "model" in service_properties:
                device.model_id = service_properties["model"]

            if "mf" in service_properties:
                device.manufacturer_mdns = service_properties["mf"]
                if not device.manufacturer:
                    device.manufacturer = service_properties["mf"]

            if "server_vers" in service_properties and service_type == SERVICE_CMC:
                device.software_version = service_properties["server_vers"]

            if "router_vers" in service_properties:
                device.firmware_version = service_properties["router_vers"]

            if "rate" in service_properties:
                device.sample_rate = int(service_properties["rate"])

            if "latency_ns" in service_properties:
                device.latency = nanoseconds_to_milliseconds(service_properties["latency_ns"])

            await self._publish_device_to_redis(device)

            arc_port = self.application.get_arc_port(device)
            if arc_port:
                if not is_new:
                    new_name = await device.fetch_device_name()
                    if new_name and new_name != device.name:
                        logger.info(f"Device name changed for {server_name}: {device.name!r} -> {new_name!r}")
                        device.name = new_name
                        device_changed = True

                if not device.tx_channels and not device.rx_channels:
                    self._spawn_background(
                        self.state.fetch_device_controls(server_name, delay=2),
                        name=f"delayed-controls:{server_name}",
                    )

            if device_changed:
                self.application.dispatcher.emit_nowait(
                    DanteEvent(
                        type=EventType.DEVICE_UPDATED,
                        device_name=device.name,
                        server_name=server_name,
                    )
                )

        except Exception:
            logger.exception(f"Service change error for {name}")


async def run_daemon(dissect=False, capture=False, relay_port=None):
    import signal

    daemon = NetaudioDaemon(dissect=dissect, capture=capture, relay_port=relay_port)
    loop = asyncio.get_running_loop()

    def handle_signal():
        daemon.request_shutdown()

    installed_signals = []
    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, handle_signal)
                installed_signals.append(sig)
            except (NotImplementedError, RuntimeError) as exception:
                logger.debug(f"Could not install {sig.name} handler: {exception}")

    try:
        await daemon.start()
    except DaemonAlreadyRunningError as error:
        logger.error(str(error))
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            try:
                await asyncio.wait_for(daemon.stop(), timeout=20)
            except asyncio.TimeoutError:
                logger.warning("Daemon shutdown timed out after 20s, exiting anyway")
        finally:
            for sig in installed_signals:
                loop.remove_signal_handler(sig)
        logger.info("Daemon stopped")
