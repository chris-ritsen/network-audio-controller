import asyncio
import json
import logging
import socket

from zeroconf import ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

from netaudio_lib.dante.device_serializer import DanteDeviceSerializer
from netaudio_lib.dante.events import DanteEvent, EventType

logger = logging.getLogger("netaudio")

RELAY_SERVICE_TYPE = "_netaudio-relay._tcp.local."
DEFAULT_RELAY_PORT = 9000


class RelayServer:
    def __init__(self, daemon, port=None):
        self.daemon = daemon
        self.port = port or DEFAULT_RELAY_PORT
        self.tcp_server = None
        self.zeroconf = None
        self.service_info = None
        self.sse_clients: list[asyncio.StreamWriter] = []

    async def start(self):
        self.tcp_server = await asyncio.start_server(
            self.handle_connection, "0.0.0.0", self.port
        )
        logger.info(f"Relay server listening on port {self.port}")

        self._register_events()
        await self._register_bonjour()

    async def stop(self):
        if self.zeroconf and self.service_info:
            await self.zeroconf.async_unregister_service(self.service_info)
            await self.zeroconf.async_close()

        for writer in self.sse_clients:
            try:
                writer.close()
            except Exception:
                pass
        self.sse_clients.clear()

        if self.tcp_server:
            self.tcp_server.close()
            await self.tcp_server.wait_closed()

    def _register_events(self):
        dispatcher = self.daemon.application.dispatcher
        dispatcher.on(EventType.DEVICE_DISCOVERED, self._on_device_event)
        dispatcher.on(EventType.DEVICE_UPDATED, self._on_device_event)
        dispatcher.on(EventType.DEVICE_REMOVED, self._on_device_removed)
        dispatcher.on(EventType.NOTIFICATION_RECEIVED, self._on_notification)
        dispatcher.on(EventType.METER_VALUES, self._on_meter_values)

    async def _on_device_event(self, event: DanteEvent):
        device = self.daemon.devices.get(event.server_name)
        if not device:
            return

        device_json = DanteDeviceSerializer.to_json(device)
        device_json["online"] = device.online
        device_json["tx_count"] = device.tx_count
        device_json["rx_count"] = device.rx_count

        await self._broadcast_sse({
            "event": event.type.name.lower(),
            "server_name": event.server_name,
            "device": device_json,
        })

    async def _on_device_removed(self, event: DanteEvent):
        await self._broadcast_sse({
            "event": "device_removed",
            "server_name": event.server_name,
        })

    async def _on_meter_values(self, event: DanteEvent):
        await self._broadcast_sse({
            "event": "meter_values",
            "server_name": event.server_name,
            "tx": event.data.get("tx", {}),
            "rx": event.data.get("rx", {}),
        })

    async def _on_notification(self, event: DanteEvent):
        device = self.daemon.devices.get(event.server_name)
        if not device:
            return

        device_json = DanteDeviceSerializer.to_json(device)
        device_json["online"] = device.online
        device_json["tx_count"] = device.tx_count
        device_json["rx_count"] = device.rx_count

        await self._broadcast_sse({
            "event": "device_updated",
            "server_name": event.server_name,
            "device": device_json,
        })

    async def _broadcast_sse(self, data):
        payload = f"data: {json.dumps(data, default=str)}\n\n".encode()
        dead_clients = []
        for writer in self.sse_clients:
            try:
                writer.write(payload)
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError, OSError):
                dead_clients.append(writer)
        for writer in dead_clients:
            self.sse_clients.remove(writer)

    async def _register_bonjour(self):
        hostname = socket.gethostname()
        local_ip = self._get_local_ip()

        self.service_info = ServiceInfo(
            RELAY_SERVICE_TYPE,
            f"netaudio-relay ({hostname}).{RELAY_SERVICE_TYPE}",
            addresses=[socket.inet_aton(local_ip)],
            port=self.port,
            properties={"version": "1"},
        )

        self.zeroconf = AsyncZeroconf()
        await self.zeroconf.async_register_service(self.service_info)
        logger.info(f"Relay advertised via Bonjour at {local_ip}:{self.port}")

    def _get_local_ip(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect(("10.255.255.255", 1))
                return sock.getsockname()[0]
        except Exception:
            return "127.0.0.1"

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

            await self._route(method, path, body, writer, reader)

        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as exception:
            logger.debug(f"Relay connection error: {exception}")

    async def _route(self, method, path, body, writer, reader):
        if method == "GET" and path == "/events":
            await self._handle_sse(writer, reader)
            return
        if method == "GET" and path == "/devices":
            await self._handle_get_devices(writer)
        elif method == "GET" and path.startswith("/devices/"):
            server_name = path[len("/devices/"):]
            await self._handle_get_device(writer, server_name)
        elif method == "POST" and path == "/subscribe":
            await self._handle_subscribe(writer, body)
        elif method == "POST" and path == "/unsubscribe":
            await self._handle_unsubscribe(writer, body)
        elif method == "POST" and path == "/identify":
            await self._handle_identify(writer, body)
        elif method == "POST" and path == "/rename-device":
            await self._handle_rename_device(writer, body)
        elif method == "POST" and path == "/rename-channel":
            await self._handle_rename_channel(writer, body)
        elif method == "POST" and path == "/set-latency":
            await self._handle_set_latency(writer, body)
        elif method == "POST" and path == "/lock":
            await self._handle_lock(writer, body)
        elif method == "POST" and path == "/unlock":
            await self._handle_unlock(writer, body)
        elif method == "POST" and path == "/refresh":
            await self._handle_refresh(writer, body)
        elif method == "POST" and path == "/metering/start":
            await self._handle_metering_start(writer, body)
        elif method == "POST" and path == "/metering/stop":
            await self._handle_metering_stop(writer, body)
        else:
            await self._send_json(writer, {"error": "not found"}, 404)

        try:
            writer.close()
            await writer.wait_closed()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    async def _handle_sse(self, writer, reader):
        response_header = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/event-stream\r\n"
            "Cache-Control: no-cache\r\n"
            "Connection: keep-alive\r\n"
            "Access-Control-Allow-Origin: *\r\n"
            "\r\n"
        ).encode()
        writer.write(response_header)
        await writer.drain()

        full_state = {}
        for server_name, device in self.daemon.devices.items():
            device_json = DanteDeviceSerializer.to_json(device)
            device_json["online"] = device.online
            device_json["tx_count"] = device.tx_count
            device_json["rx_count"] = device.rx_count
            full_state[server_name] = device_json

        initial = f"data: {json.dumps({'event': 'snapshot', 'devices': full_state}, default=str)}\n\n".encode()
        writer.write(initial)
        await writer.drain()

        self.sse_clients.append(writer)

        try:
            while True:
                data = await reader.read(1)
                if not data:
                    break
                await asyncio.sleep(0.1)
        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            if writer in self.sse_clients:
                self.sse_clients.remove(writer)

    async def _handle_get_devices(self, writer):
        devices_json = {}
        for server_name, device in self.daemon.devices.items():
            devices_json[server_name] = DanteDeviceSerializer.to_json(device)
            devices_json[server_name]["online"] = device.online
            devices_json[server_name]["tx_count"] = device.tx_count
            devices_json[server_name]["rx_count"] = device.rx_count
        await self._send_json(writer, devices_json)

    async def _handle_get_device(self, writer, server_name):
        device = self.daemon.devices.get(server_name)
        if not device:
            for name, candidate in self.daemon.devices.items():
                if candidate.name and candidate.name.lower() == server_name.lower():
                    device = candidate
                    break

        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        device_json = DanteDeviceSerializer.to_json(device)
        device_json["online"] = device.online
        device_json["tx_count"] = device.tx_count
        device_json["rx_count"] = device.rx_count
        await self._send_json(writer, device_json)

    async def _handle_subscribe(self, writer, body):
        if not body:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        params = json.loads(body)
        rx_device_name = params.get("rx_device")
        rx_channel_number = params.get("rx_channel")
        tx_channel_name = params.get("tx_channel")
        tx_device_name = params.get("tx_device")

        device = self._find_device(rx_device_name)
        if not device:
            await self._send_json(writer, {"error": "rx device not found"}, 404)
            return

        await self._broadcast_sse({
            "event": "subscription_pending",
            "action": "add",
            "rx_device": rx_device_name,
            "rx_channel": rx_channel_number,
            "tx_channel": tx_channel_name,
            "tx_device": tx_device_name,
        })

        try:
            command_args = device.commands.command_add_subscription(
                rx_channel_number, tx_channel_name, tx_device_name
            )
            await device.dante_command(
                *command_args, logical_command_name="add_subscription"
            )
            await self._send_json(writer, {"success": True})
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _handle_unsubscribe(self, writer, body):
        if not body:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        params = json.loads(body)
        rx_device_name = params.get("rx_device")
        rx_channel_number = params.get("rx_channel")

        device = self._find_device(rx_device_name)
        if not device:
            await self._send_json(writer, {"error": "rx device not found"}, 404)
            return

        rx_channel = device.rx_channels.get(rx_channel_number)
        if not rx_channel:
            await self._send_json(writer, {"error": "rx channel not found"}, 404)
            return

        try:
            await device.operations.remove_subscription(rx_channel)
            await self._send_json(writer, {"success": True})
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _handle_identify(self, writer, body):
        if not body:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        params = json.loads(body)
        device_name = params.get("device")

        device = self._find_device(device_name)
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        try:
            await device.operations.identify()
            await self._broadcast_sse({
                "event": "identify_started",
                "server_name": device.server_name,
                "duration": 6,
            })
            await self._send_json(writer, {"success": True})
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _handle_rename_device(self, writer, body):
        if not body:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        params = json.loads(body)
        device_name = params.get("device")
        new_name = params.get("name")

        device = self._find_device(device_name)
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        try:
            await device.operations.set_name(new_name)
            await self._send_json(writer, {"success": True})
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _handle_rename_channel(self, writer, body):
        if not body:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        params = json.loads(body)
        device_name = params.get("device")
        channel_type = params.get("channel_type")
        channel_number = params.get("channel_number")
        new_name = params.get("name")

        device = self._find_device(device_name)
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        try:
            await device.operations.set_channel_name(channel_type, channel_number, new_name)
            await self._send_json(writer, {"success": True})
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _handle_set_latency(self, writer, body):
        if not body:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        params = json.loads(body)
        device_name = params.get("device")
        latency = params.get("latency")

        device = self._find_device(device_name)
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        try:
            await device.operations.set_latency(latency)
            await self._send_json(writer, {"success": True})
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _handle_lock(self, writer, body):
        if not body:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        params = json.loads(body)
        device_name = params.get("device")
        pin = params.get("pin")

        device = self._find_device(device_name)
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        lock_key = self._get_lock_key()
        if not lock_key:
            await self._send_json(writer, {"error": "device_lock_key not configured"}, 503)
            return

        from netaudio_lib.dante.device_operations import validate_pin
        error = validate_pin(pin or "")
        if error:
            await self._send_json(writer, {"error": error}, 400)
            return

        try:
            result = await device.operations.lock_device(pin, lock_key)
            if result.get("success"):
                device.is_locked = result.get("lock_state") == 1
                await self._broadcast_device_updated(device)
            await self._send_json(writer, result)
        except TimeoutError:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _handle_unlock(self, writer, body):
        if not body:
            await self._send_json(writer, {"error": "missing body"}, 400)
            return

        params = json.loads(body)
        device_name = params.get("device")
        pin = params.get("pin")

        device = self._find_device(device_name)
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        lock_key = self._get_lock_key()
        if not lock_key:
            await self._send_json(writer, {"error": "device_lock_key not configured"}, 503)
            return

        from netaudio_lib.dante.device_operations import validate_pin
        error = validate_pin(pin or "")
        if error:
            await self._send_json(writer, {"error": error}, 400)
            return

        try:
            result = await device.operations.unlock_device(pin, lock_key)
            if result.get("success"):
                device.is_locked = result.get("lock_state") == 1
                await self._broadcast_device_updated(device)
            await self._send_json(writer, result)
        except TimeoutError:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _broadcast_device_updated(self, device):
        device_json = DanteDeviceSerializer.to_json(device)
        device_json["online"] = device.online
        device_json["tx_count"] = device.tx_count
        device_json["rx_count"] = device.rx_count
        await self._broadcast_sse({
            "event": "device_updated",
            "server_name": device.server_name,
            "device": device_json,
        })

    def _get_lock_key(self):
        from netaudio_lib.common.app_config import settings as app_settings
        if app_settings.device_lock_key:
            return app_settings.device_lock_key
        from netaudio_lib.common.key_extract import extract_lock_key
        key = extract_lock_key()
        if key:
            app_settings.device_lock_key = key
            logger.info("Extracted device lock key from Dante Controller")
        return key

    async def _handle_refresh(self, writer, body):
        try:
            device_name = None
            if body:
                params = json.loads(body)
                device_name = params.get("device")

            if device_name:
                device = self._find_device(device_name)
                if not device:
                    await self._send_json(writer, {"error": "device not found"}, 404)
                    return
                await self.daemon.refresh_device(device.server_name)
            else:
                await self.daemon.refresh_all_devices()

            await self._send_json(writer, {"success": True})
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _handle_metering_start(self, writer, body):
        try:
            if not body:
                await self._send_json(writer, {"error": "device required"}, 400)
                return
            params = json.loads(body)
            device_name = params.get("device")
            device = self._find_device(device_name)
            if not device:
                await self._send_json(writer, {"error": "device not found"}, 404)
                return
            client_id = params.get("client_id", "relay_http")
            if self.daemon.metering:
                self.daemon.metering.add_persistent(device.server_name, client_id)
            await self._send_json(writer, {"success": True})
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    async def _handle_metering_stop(self, writer, body):
        try:
            if not body:
                await self._send_json(writer, {"error": "device required"}, 400)
                return
            params = json.loads(body)
            device_name = params.get("device")
            device = self._find_device(device_name)
            if not device:
                await self._send_json(writer, {"error": "device not found"}, 404)
                return
            client_id = params.get("client_id", "relay_http")
            if self.daemon.metering:
                self.daemon.metering.remove_persistent(device.server_name, client_id)
            await self._send_json(writer, {"success": True})
        except Exception as exception:
            await self._send_json(writer, {"error": str(exception)}, 500)

    def _find_device(self, name):
        if not name:
            return None
        device = self.daemon.devices.get(name)
        if device:
            return device
        for server_name, candidate in self.daemon.devices.items():
            if candidate.name and candidate.name.lower() == name.lower():
                return candidate
        return None

    async def _send_json(self, writer, data, status=200):
        body = json.dumps(data, default=str).encode()
        status_text = "OK" if status == 200 else "Error"
        response = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Access-Control-Allow-Origin: *\r\n"
            f"\r\n"
        ).encode() + body
        writer.write(response)
        await writer.drain()
