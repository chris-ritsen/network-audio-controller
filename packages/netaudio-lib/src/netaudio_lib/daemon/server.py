import asyncio
import json
import logging
import os
import pickle
import struct
import sys

from zeroconf import ServiceStateChange
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

from netaudio_lib.common.socket_path import cleanup_daemon_socket, start_daemon_server
from netaudio_lib.dante.application import DanteApplication
from netaudio_lib.dante.const import SERVICE_CMC, SERVICES
from netaudio_lib.dante.device import DanteDevice
from netaudio_lib.dante.events import DanteEvent, EventType

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None

logger = logging.getLogger("netaudio")

BLUETOOTH_MODEL_IDS = {"DIOBT"}


class NetaudioDaemon:
    def __init__(self):
        self.application = DanteApplication()
        self.server = None
        self.zeroconf = None
        self.browser = None
        self.running = False
        self._redis = None
        self._populating: set[str] = set()

    @property
    def devices(self) -> dict:
        return self.application.devices

    async def _connect_redis(self):
        if aioredis is None:
            logger.info("redis.asyncio not available, running without Redis")
            return

        try:
            redis_socket = os.environ.get("REDIS_SOCKET")
            redis_host = os.environ.get("REDIS_HOST") or "localhost"
            redis_port = int(os.environ.get("REDIS_PORT") or 6379)
            redis_db = int(os.environ.get("REDIS_DB") or 0)

            if redis_socket:
                self._redis = aioredis.Redis(unix_socket_path=redis_socket, db=redis_db)
            else:
                self._redis = aioredis.Redis(host=redis_host, port=redis_port, db=redis_db)

            await self._redis.ping()
            await self._redis.config_set("notify-keyspace-events", "Kgh$")
            logger.info("Connected to Redis")
        except Exception as exception:
            logger.info(f"Redis not available, continuing without it: {exception}")
            self._redis = None

    async def _publish_device_to_redis(self, device):
        if not self._redis:
            return

        key = f"netaudio:daemon:device:{device.server_name}"
        try:
            await self._redis.hset(key, mapping={
                "server_name": device.server_name or "",
                "name": device.name or "",
                "ipv4": str(device.ipv4) if device.ipv4 else "",
                "model_id": device.model_id or "",
                "bluetooth_device": device.bluetooth_device or "",
            })
        except Exception as exception:
            logger.debug(f"Redis publish error for {device.server_name}: {exception}")

    async def _delete_device_from_redis(self, server_name):
        if not self._redis:
            return

        key = f"netaudio:daemon:device:{server_name}"
        try:
            await self._redis.delete(key)
        except Exception as exception:
            logger.debug(f"Redis delete error for {server_name}: {exception}")

    def _register_event_listeners(self):
        self.application.dispatcher.on(
            EventType.DEVICE_DISCOVERED, self._on_device_discovered
        )
        self.application.dispatcher.on(
            EventType.DEVICE_UPDATED, self._on_device_updated
        )
        self.application.dispatcher.on(
            EventType.DEVICE_REMOVED, self._on_device_removed
        )
        self.application.dispatcher.on(
            EventType.CHANNEL_NAME_UPDATED, self._on_channel_name_updated
        )
        self.application.dispatcher.on(
            EventType.SUBSCRIPTION_CHANGED, self._on_subscription_changed
        )
        self.application.dispatcher.on(
            EventType.AES67_CHANGED, self._on_aes67_changed
        )
        self.application.dispatcher.on(
            EventType.SAMPLE_RATE_CHANGED, self._on_sample_rate_changed
        )

    async def _on_device_discovered(self, event: DanteEvent):
        device = self.devices.get(event.server_name)
        if device:
            logger.info(f"Device discovered (event): {event.server_name}")
            await self._publish_device_to_redis(device)

    async def _on_device_updated(self, event: DanteEvent):
        device = self.devices.get(event.server_name)
        if device:
            await self._publish_device_to_redis(device)

    async def _on_device_removed(self, event: DanteEvent):
        logger.info(f"Device removed (event): {event.server_name}")
        await self._delete_device_from_redis(event.server_name)

    async def _on_channel_name_updated(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device:
            return

        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        logger.info(f"Re-fetching channels for {server_name} (channel name changed)")
        try:
            device.tx_channels = await self.application.arc.get_tx_channels(device, arc_port)
            rx_channels, subscriptions = await self.application.arc.get_rx_channels(device, arc_port)
            device.rx_channels = rx_channels
            device.subscriptions = subscriptions
            await self._publish_device_to_redis(device)
        except Exception as exception:
            logger.debug(f"Error re-fetching channels for {server_name}: {exception}")

    async def _on_subscription_changed(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device:
            return

        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        logger.info(f"Re-fetching subscriptions for {server_name}")
        try:
            rx_channels, subscriptions = await self.application.arc.get_rx_channels(device, arc_port)
            device.rx_channels = rx_channels
            device.subscriptions = subscriptions
            await self._publish_device_to_redis(device)
        except Exception as exception:
            logger.debug(f"Error re-fetching subscriptions for {server_name}: {exception}")

    async def _on_aes67_changed(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device:
            return

        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        logger.info(f"Re-fetching AES67 status for {server_name}")
        try:
            aes67_status = await self.application.arc.get_aes67_config(
                str(device.ipv4), arc_port
            )
            if aes67_status is not None:
                device.aes67_enabled = aes67_status
            await self._publish_device_to_redis(device)
        except Exception as exception:
            logger.debug(f"Error re-fetching AES67 for {server_name}: {exception}")

    async def _on_sample_rate_changed(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if device:
            new_rate = event.data.get("sample_rate")
            if new_rate:
                device.sample_rate = new_rate
                logger.info(f"Sample rate changed for {server_name}: {new_rate}")
            await self._publish_device_to_redis(device)

    async def start(self):
        self.running = True
        await self._connect_redis()

        self.server = await start_daemon_server(self.handle_client)

        logger.info("Daemon listening")

        await self.application.startup()
        self._register_event_listeners()

        self.zeroconf = AsyncZeroconf()
        self.browser = AsyncServiceBrowser(
            self.zeroconf.zeroconf,
            SERVICES,
            handlers=[self.on_service_state_change],
        )

        logger.info("mDNS browser started, watching for devices...")

        await self.application.cmc.start_heartbeat(
            lambda: [str(d.ipv4) for d in self.devices.values() if d.ipv4]
        )

        asyncio.create_task(self.periodic_refresh())

        async with self.server:
            await self.server.serve_forever()

    async def periodic_refresh(self):
        from netaudio_lib.dante.browser import DanteBrowser

        while self.running:
            await asyncio.sleep(15)

            if not self.running:
                break

            try:
                browser = DanteBrowser(mdns_timeout=5)
                scanned_devices = await browser.get_devices()

                if scanned_devices is None:
                    scanned_devices = {}

                scanned_names = set(scanned_devices.keys())
                cached_names = set(self.devices.keys())

                for server_name in cached_names - scanned_names:
                    logger.info(f"Device no longer found, removing: {server_name}")
                    self.application.unregister_device(server_name)
                    await self._delete_device_from_redis(server_name)

                for server_name, device in scanned_devices.items():
                    if server_name not in self.devices:
                        logger.info(f"Device discovered (scan): {server_name}")
                        self.application.register_device(server_name, device)
                    else:
                        if device.ipv4:
                            self.devices[server_name].ipv4 = device.ipv4
                        if device.services:
                            self.devices[server_name].services = device.services

                logger.debug(f"Scan complete: {len(self.devices)} devices")

                for server_name, device in self.devices.items():
                    if not device.tx_channels and not device.rx_channels:
                        await self._fetch_device_controls(server_name)

                for device in self.devices.values():
                    await self._publish_device_to_redis(device)

            except Exception as exception:
                logger.debug(f"Periodic scan error: {exception}")

    async def stop(self):
        self.running = False

        if self._redis:
            try:
                await self._redis.aclose()
            except Exception:
                pass

        await self.application.shutdown()

        if self.server:
            self.server.close()
            try:
                await self.server.wait_closed()
            except Exception:
                pass

        try:
            if self.browser:
                await self.browser.async_cancel()
        except Exception:
            pass

        try:
            if self.zeroconf:
                await self.zeroconf.async_close()
        except Exception:
            pass

        cleanup_daemon_socket()

    def on_service_state_change(
        self, zeroconf, service_type, name, state_change
    ):
        if service_type == "_netaudio-chan._udp.local.":
            return

        logger.debug(f"mDNS event: {state_change.name} - {service_type} - {name}")

        asyncio.create_task(
            self.handle_service_change(zeroconf, service_type, name, state_change)
        )

    async def handle_service_change(self, zeroconf, service_type, name, state_change):
        try:
            info = AsyncServiceInfo(service_type, name)

            if state_change == ServiceStateChange.Removed:
                for server_name in list(self.devices.keys()):
                    if name.startswith(server_name.replace(".local.", "")):
                        logger.info(f"Device removed: {server_name}")
                        self.application.unregister_device(server_name)
                        await self._delete_device_from_redis(server_name)

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

            is_new = server_name not in self.devices

            if is_new:
                device = DanteDevice(server_name=server_name)
                self.application.register_device(server_name, device)
                logger.info(f"Device discovered: {server_name}")
                if addresses[0]:
                    await self.application.cmc.register_device(addresses[0])

            device = self.devices[server_name]

            old_ip = str(device.ipv4) if device.ipv4 else None
            new_ip = addresses[0]

            if old_ip and old_ip != new_ip:
                logger.info(f"Device {server_name} IP changed: {old_ip} -> {new_ip}")

            device.ipv4 = new_ip

            if not device.services:
                device.services = {}

            device.services[name] = service_data

            if "id" in service_properties and service_type == SERVICE_CMC:
                device.mac_address = service_properties["id"]

            if "model" in service_properties:
                device.model_id = service_properties["model"]

            if "rate" in service_properties:
                device.sample_rate = int(service_properties["rate"])

            if "latency_ns" in service_properties:
                device.latency = int(service_properties["latency_ns"])

            await self._publish_device_to_redis(device)

            arc_port = self.application.get_arc_port(device)
            if arc_port and not device.tx_channels and not device.rx_channels:
                asyncio.create_task(self._fetch_device_controls(server_name, delay=2))

        except Exception as exception:
            logger.debug(f"Service change error: {exception}")

    async def _fetch_device_controls(self, server_name: str, delay: float = 0) -> None:
        if server_name in self._populating:
            return

        device = self.devices.get(server_name)
        if not device:
            return

        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        self._populating.add(server_name)

        try:
            if delay > 0:
                await asyncio.sleep(delay)

            device_ip = str(device.ipv4)

            if not device.name:
                name = await self.application.arc.get_device_name(device_ip, arc_port)
                if name:
                    device.name = name

            if device.tx_count is None or device.rx_count is None:
                counts = await self.application.arc.get_channel_count(device_ip, arc_port)
                if counts:
                    device.tx_count = device.tx_count_raw = counts[0]
                    device.rx_count = device.rx_count_raw = counts[1]

            if device.aes67_enabled is None:
                aes67_status = await self.application.arc.get_aes67_config(device_ip, arc_port)
                if aes67_status is not None:
                    device.aes67_enabled = aes67_status

            if not device.tx_channels and device.tx_count:
                device.tx_channels = await self.application.arc.get_tx_channels(device, arc_port)

            if not device.rx_channels and device.rx_count:
                rx_channels, subscriptions = await self.application.arc.get_rx_channels(device, arc_port)
                device.rx_channels = rx_channels
                device.subscriptions = subscriptions

            if device.bluetooth_device is None and device.model_id in BLUETOOTH_MODEL_IDS:
                self.application.settings.request_bluetooth_status(device_ip)

            logger.info(f"Fetched controls for {server_name}")
            await self._publish_device_to_redis(device)
        except Exception as exception:
            logger.debug(f"Error fetching controls for {server_name}: {exception}")
        finally:
            self._populating.discard(server_name)

    async def handle_client(self, reader, writer):
        try:
            cmd = await reader.read(1)

            if cmd == b'\x01':
                length_data = await reader.readexactly(4)
                length = struct.unpack(">I", length_data)[0]
                server_name = (await reader.readexactly(length)).decode("utf-8")

                if server_name in self.devices:
                    logger.info(f"Device unresponsive, removing: {server_name}")
                    self.application.unregister_device(server_name)
                    await self._delete_device_from_redis(server_name)

                writer.close()
                await writer.wait_closed()
                return

            if cmd == b'\x02':
                devices_json = {}
                for server_name, device in self.devices.items():
                    devices_json[server_name] = {
                        "server_name": device.server_name,
                        "name": device.name,
                        "ipv4": str(device.ipv4) if device.ipv4 else None,
                        "model_id": device.model_id,
                        "bluetooth_device": device.bluetooth_device,
                    }
                data = json.dumps(devices_json).encode()
            else:
                devices_for_client = {}
                for server_name, device in self.devices.items():
                    client_device = DanteDevice(
                        server_name=device.server_name,
                        dump_payloads=False,
                        debug=False,
                    )
                    client_device._ipv4 = device._ipv4
                    client_device.name = device.name
                    client_device.mac_address = device.mac_address
                    client_device.model_id = device.model_id
                    client_device.sample_rate = device.sample_rate
                    client_device.latency = device.latency
                    client_device.services = device.services
                    client_device.manufacturer = device.manufacturer
                    client_device.software = device.software
                    client_device.bluetooth_device = device.bluetooth_device
                    client_device.tx_channels = device.tx_channels
                    client_device.rx_channels = device.rx_channels
                    client_device.subscriptions = device.subscriptions
                    client_device.tx_count = device.tx_count
                    client_device.rx_count = device.rx_count
                    client_device.tx_count_raw = device.tx_count_raw
                    client_device.rx_count_raw = device.rx_count_raw
                    client_device.aes67_enabled = device.aes67_enabled
                    client_device.error = str(device.error) if device.error else None
                    client_device.dante_model = device.dante_model
                    client_device.dante_model_id = device.dante_model_id
                    devices_for_client[server_name] = client_device
                data = pickle.dumps(devices_for_client)

            length = struct.pack(">I", len(data))
            writer.write(length + data)
            await writer.drain()
        except Exception as exception:
            logger.error(f"Client handler error: {exception}")
        finally:
            writer.close()
            await writer.wait_closed()


async def run_daemon():
    import signal

    daemon = NetaudioDaemon()
    loop = asyncio.get_running_loop()

    def handle_signal():
        daemon.running = False
        if daemon.server:
            daemon.server.close()

    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, handle_signal)

    try:
        await daemon.start()
    except asyncio.CancelledError:
        pass
    finally:
        await daemon.stop()
        logger.info("Daemon stopped")
