import asyncio
import json
import logging
import socket
from urllib.parse import unquote

from zeroconf import ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

from netaudio.dante.device_operations import validate_pin
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.events import DanteEvent, EventType

logger = logging.getLogger("netaudio")

RELAY_SERVICE_TYPE = "_netaudio-relay._tcp.local."
DEFAULT_RELAY_PORT = 9000

STATUS_TEXT = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    500: "Internal Server Error",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}


class RelayServer:
    def __init__(self, application, state, metering=None, shure=None, port=None, on_shutdown=None):
        self.application = application
        self.state = state
        self.metering = metering
        self.shure = shure
        self.on_shutdown = on_shutdown
        self.port = port or DEFAULT_RELAY_PORT
        self.tcp_server = None
        self.zeroconf = None
        self.service_info = None
        self.sse_clients: list[asyncio.StreamWriter] = []
        self.post_handlers = {
            "/subscribe": self._handle_subscribe,
            "/unsubscribe": self._handle_unsubscribe,
            "/identify": self._handle_identify,
            "/rename-device": self._handle_rename_device,
            "/rename-channel": self._handle_rename_channel,
            "/set-latency": self._handle_set_latency,
            "/lock": self._handle_lock,
            "/unlock": self._handle_unlock,
            "/refresh": self._handle_refresh,
            "/set-sample-rate": self._handle_set_sample_rate,
            "/set-encoding": self._handle_set_encoding,
            "/set-gain": self._handle_set_gain,
            "/set-aes67": self._handle_set_aes67,
            "/set-preferred-leader": self._handle_set_preferred_leader,
            "/reboot": self._handle_reboot,
            "/interface": self._handle_set_interface,
            "/metering/start": self._handle_metering_start,
            "/metering/stop": self._handle_metering_stop,
            "/report-unresponsive": self._handle_report_unresponsive,
            "/shutdown": self._handle_shutdown,
        }
        self.post_body_optional = {"/refresh", "/shutdown"}
        self.loopback_only_paths = {"/shutdown"}

    async def start(self):
        self.tcp_server = await asyncio.start_server(
            self.handle_connection, "0.0.0.0", self.port
        )
        logger.info(f"Relay server listening on port {self.port}")

        self._register_events()
        try:
            await self._register_bonjour()
        except Exception as exception:
            logger.warning(
                f"Relay Bonjour registration failed, continuing without it: {type(exception).__name__}: {exception}"
            )
            self.service_info = None

    async def stop(self):
        if self.zeroconf and self.service_info:
            try:
                await asyncio.wait_for(self.zeroconf.async_unregister_service(self.service_info), timeout=5)
                await asyncio.wait_for(self.zeroconf.async_close(), timeout=5)
            except Exception as exception:
                logger.warning(f"Relay Bonjour unregister failed: {type(exception).__name__}: {exception}")

        for writer in self.sse_clients:
            try:
                writer.close()
            except Exception as exception:
                logger.debug(f"SSE client close error: {exception}")
        self.sse_clients.clear()

        if self.tcp_server:
            self.tcp_server.close()
            try:
                await asyncio.wait_for(self.tcp_server.wait_closed(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("Relay connections did not drain within 5s, abandoning them")

    def _register_events(self):
        dispatcher = self.application.dispatcher
        dispatcher.on(EventType.DEVICE_DISCOVERED, self._on_device_event)
        dispatcher.on(EventType.DEVICE_UPDATED, self._on_device_event)
        dispatcher.on(EventType.DEVICE_REMOVED, self._on_device_removed)
        dispatcher.on(EventType.METER_VALUES, self._on_meter_values)
        dispatcher.on(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_event)
        dispatcher.on(EventType.SHURE_DEVICE_UPDATED, self._on_shure_event)
        dispatcher.on(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)
        dispatcher.on(EventType.SHURE_METER_VALUES, self._on_shure_meter)

    async def _on_device_event(self, event: DanteEvent):
        device = self.application.devices.get(event.server_name)
        if not device:
            return

        device_json = DanteDeviceSerializer.to_json(device)

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

    async def _on_shure_event(self, event: DanteEvent):
        if not self.shure:
            return
        device = self.shure.devices.get(event.device_name)
        if not device:
            return
        await self._broadcast_sse({
            "event": event.type.name.lower(),
            "mac": event.device_name,
            "device": device.to_json(),
        })

    async def _on_shure_removed(self, event: DanteEvent):
        await self._broadcast_sse({
            "event": "shure_device_removed",
            "mac": event.device_name,
        })

    async def _on_shure_meter(self, event: DanteEvent):
        await self._broadcast_sse({
            "event": "shure_meter_values",
            "mac": event.device_name,
            "channel": event.data.get("channel"),
            "key": event.data.get("key"),
            "value": event.data.get("value"),
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

        try:
            await self._dispatch(method, path, body, writer)
        except TimeoutError:
            await self._send_json(writer, {"error": "device did not respond"}, 504)
        except Exception as exception:
            logger.exception(f"Relay error handling {method} {path}")
            await self._send_json(writer, {"error": str(exception)}, 500)

        try:
            writer.close()
            await writer.wait_closed()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    async def _dispatch(self, method, path, body, writer):
        if method == "GET":
            if path == "/shure/devices":
                await self._handle_get_shure_devices(writer)
            elif path.startswith("/shure/devices/"):
                await self._handle_get_shure_device(writer, path[len("/shure/devices/"):])
            elif path == "/devices":
                await self._handle_get_devices(writer)
            elif path.startswith("/devices/"):
                await self._handle_get_device(writer, unquote(path[len("/devices/"):]))
            elif path == "/metering/status":
                await self._handle_metering_status(writer)
            elif path.startswith("/metering/snapshot/"):
                await self._handle_metering_snapshot(writer, unquote(path[len("/metering/snapshot/"):]))
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
        for server_name, device in self.application.devices.items():
            full_state[server_name] = DanteDeviceSerializer.to_json(device)

        shure_state = {}
        if self.shure:
            for mac, device in self.shure.devices.items():
                shure_state[mac] = device.to_json()

        initial = f"data: {json.dumps({'event': 'snapshot', 'devices': full_state, 'shure_devices': shure_state}, default=str)}\n\n".encode()
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

    async def _handle_get_shure_devices(self, writer):
        if not self.shure:
            await self._send_json(writer, {})
            return
        result = {}
        for mac, device in self.shure.devices.items():
            result[mac] = device.to_json()
        await self._send_json(writer, result)

    async def _handle_get_shure_device(self, writer, mac):
        if not self.shure:
            await self._send_json(writer, {"error": "shure not available"}, 404)
            return
        device = self.shure.devices.get(mac)
        if not device:
            for m, d in self.shure.devices.items():
                if d.name and d.name.lower() == mac.lower():
                    device = d
                    break
                if d.ip == mac:
                    device = d
                    break
        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return
        await self._send_json(writer, device.to_json())

    async def _handle_get_devices(self, writer):
        devices_json = {
            server_name: DanteDeviceSerializer.to_json(device)
            for server_name, device in self.application.devices.items()
        }
        await self._send_json(writer, devices_json)

    async def _handle_get_device(self, writer, server_name):
        device = self.application.devices.get(server_name)
        if not device:
            for name, candidate in self.application.devices.items():
                if candidate.name and candidate.name.lower() == server_name.lower():
                    device = candidate
                    break

        if not device:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return

        await self._send_json(writer, DanteDeviceSerializer.to_json(device))

    async def _handle_subscribe(self, writer, params):
        rx_device_name = params.get("rx_device")
        device = await self._require_device(writer, rx_device_name, "rx device not found")
        if not device:
            return

        subscriptions = params.get("subscriptions")
        if subscriptions is not None:
            try:
                records = [
                    (entry["rx_channel"], entry["tx_channel"], entry["tx_device"])
                    for entry in subscriptions
                ]
            except (KeyError, TypeError) as exception:
                await self._send_json(
                    writer, {"error": f"invalid subscription entry: {exception}"}, 400
                )
                return
            if not records:
                await self._send_json(writer, {"error": "subscriptions list is empty"}, 400)
                return

            for rx_channel_number, tx_channel_name, tx_device_name in records:
                await self._broadcast_sse({
                    "event": "subscription_pending",
                    "action": "add",
                    "rx_device": rx_device_name,
                    "rx_channel": rx_channel_number,
                    "tx_channel": tx_channel_name,
                    "tx_device": tx_device_name,
                })

            await device.operations.add_subscriptions_by_name(records)
            await self._send_json(writer, {"success": True, "count": len(records)})
            return

        rx_channel_number = params.get("rx_channel")
        tx_channel_name = params.get("tx_channel")
        tx_device_name = params.get("tx_device")
        if rx_channel_number is None or not tx_channel_name or not tx_device_name:
            await self._send_json(
                writer, {"error": "rx_channel, tx_channel, tx_device required"}, 400
            )
            return

        await self._broadcast_sse({
            "event": "subscription_pending",
            "action": "add",
            "rx_device": rx_device_name,
            "rx_channel": rx_channel_number,
            "tx_channel": tx_channel_name,
            "tx_device": tx_device_name,
        })

        await device.operations.add_subscription_by_name(
            rx_channel_number, tx_channel_name, tx_device_name
        )
        await self._send_json(writer, {"success": True})

    async def _handle_unsubscribe(self, writer, params):
        device = await self._require_device(writer, params.get("rx_device"), "rx device not found")
        if not device:
            return

        rx_channel_numbers = params.get("rx_channels")
        if rx_channel_numbers:
            rx_channels = []
            for number in rx_channel_numbers:
                channel = device.rx_channels.get(number)
                if not channel:
                    await self._send_json(writer, {"error": f"rx channel {number} not found"}, 404)
                    return
                rx_channels.append(channel)

            await device.operations.remove_subscriptions(rx_channels)
            await self._send_json(writer, {"success": True, "count": len(rx_channels)})
            return

        rx_channel = device.rx_channels.get(params.get("rx_channel"))
        if not rx_channel:
            await self._send_json(writer, {"error": "rx channel not found"}, 404)
            return

        await device.operations.remove_subscription(rx_channel)
        await self._send_json(writer, {"success": True})

    async def _handle_identify(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        await device.operations.identify()
        await self._broadcast_sse({
            "event": "identify_started",
            "server_name": device.server_name,
            "duration": 6,
        })
        await self._send_json(writer, {"success": True})

    async def _handle_rename_device(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        await device.operations.set_name(params.get("name"))
        await self._send_json(writer, {"success": True})

    async def _handle_rename_channel(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        await device.operations.set_channel_name(
            params.get("channel_type"), params.get("channel_number"), params.get("name")
        )
        await self._send_json(writer, {"success": True})

    async def _handle_set_latency(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        await device.operations.set_latency(params.get("latency"))
        await self._send_json(writer, {"success": True})

    async def _handle_lock(self, writer, params):
        await self._handle_lock_operation(writer, params, locking=True)

    async def _handle_unlock(self, writer, params):
        await self._handle_lock_operation(writer, params, locking=False)

    async def _handle_lock_operation(self, writer, params, locking):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        lock_key = self._get_lock_key()
        if not lock_key:
            await self._send_json(writer, {"error": "device_lock_key not configured"}, 503)
            return

        pin = params.get("pin")
        error = validate_pin(pin or "")
        if error:
            await self._send_json(writer, {"error": error}, 400)
            return

        if locking:
            result = await device.operations.lock_device(pin, lock_key)
        else:
            result = await device.operations.unlock_device(pin, lock_key)

        await self._send_json(writer, result)

    def _get_lock_key(self):
        from netaudio.common.app_config import settings as app_settings
        if app_settings.device_lock_key:
            return app_settings.device_lock_key
        from netaudio.common.key_extract import extract_lock_key
        key = extract_lock_key()
        if key:
            app_settings.device_lock_key = key
            logger.info("Extracted device lock key from Dante Controller")
        return key

    async def _handle_refresh(self, writer, params):
        device_name = params.get("device")
        if device_name:
            device = await self._require_device(writer, device_name)
            if not device:
                return
            await self.state.refresh_device(device.server_name)
        else:
            await self.state.refresh_all_devices()
        await self._send_json(writer, {"success": True})

    @staticmethod
    def _peer_is_loopback(writer):
        peername = writer.get_extra_info("peername")
        if not peername:
            return False
        return peername[0] in ("127.0.0.1", "::1", "::ffff:127.0.0.1")

    async def _handle_shutdown(self, writer, params):
        await self._send_json(writer, {"success": True})
        if self.on_shutdown is not None:
            self.on_shutdown()

    async def _handle_report_unresponsive(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        if device.online:
            logger.info(f"Device reported unresponsive, marking offline: {device.server_name}")
            self.application.mark_device_offline(device.server_name)
        await self._send_json(writer, {"success": True})

    async def _handle_metering_status(self, writer):
        if not self.metering:
            await self._send_json(writer, {})
            return
        await self._send_json(writer, self.metering.get_status())

    async def _handle_metering_snapshot(self, writer, name):
        device = self._find_device(name)
        if not device or not device.ipv4:
            await self._send_json(writer, {"error": "device not found"}, 404)
            return
        if not self.metering:
            await self._send_json(writer, {"error": "metering not available"}, 503)
            return

        levels = await self.metering.snapshot(device.server_name, timeout=3.0)
        if levels is None:
            await self._send_json(writer, {"error": "no metering data"}, 504)
            return

        tx_names = {}
        if device.tx_channels:
            for channel in device.tx_channels.values():
                tx_names[channel.number] = channel.friendly_name or channel.name
        rx_names = {}
        if device.rx_channels:
            for channel in device.rx_channels.values():
                rx_names[channel.number] = channel.friendly_name or channel.name

        response = {
            "tx": {},
            "rx": {},
            "wall_time": levels.get("wall_time"),
            "source_ip": levels.get("source_ip"),
        }
        for channel_number, level in levels.get("tx", {}).items():
            response["tx"][channel_number] = {
                "name": tx_names.get(channel_number, ""),
                "level": level,
            }
        for channel_number, level in levels.get("rx", {}).items():
            response["rx"][channel_number] = {
                "name": rx_names.get(channel_number, ""),
                "level": level,
            }
        await self._send_json(writer, response)

    async def _handle_metering_start(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        client_id = params.get("client_id", "relay_http")
        if self.metering:
            self.metering.add_persistent(device.server_name, client_id)
        await self._send_json(writer, {"success": True})

    async def _handle_metering_stop(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        client_id = params.get("client_id", "relay_http")
        if self.metering:
            self.metering.remove_persistent(device.server_name, client_id)
        await self._send_json(writer, {"success": True})

    async def _handle_set_sample_rate(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        await device.operations.set_sample_rate(params.get("sample_rate"))
        await self._send_json(writer, {"success": True})

    async def _handle_set_encoding(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        await device.operations.set_encoding(params.get("encoding"))
        await self._send_json(writer, {"success": True})

    async def _handle_set_gain(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        await device.operations.set_gain_level(
            params.get("channel_number"), params.get("gain_level"), params.get("device_type", "")
        )
        await self._send_json(writer, {"success": True})

    async def _handle_set_preferred_leader(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        await self.application.settings.set_preferred_leader(
            str(device.ipv4), params.get("preferred")
        )
        await self._send_json(writer, {"success": True})

    async def _handle_set_aes67(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        await device.operations.enable_aes67(params.get("enabled"))
        await self._send_json(writer, {"success": True})

    async def _handle_reboot(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return
        await device.operations.reboot()
        await self._send_json(writer, {"success": True})

    async def _handle_set_interface(self, writer, params):
        device = await self._require_device(writer, params.get("device"))
        if not device:
            return

        mode = params.get("mode", "").lower()
        if mode not in ("dhcp", "static"):
            await self._send_json(writer, {"error": "mode must be 'dhcp' or 'static'"}, 400)
            return

        device_ip = str(device.ipv4)
        if mode == "dhcp":
            result = await self.application.set_interface_dhcp(device_ip)
        else:
            ip_address = params.get("ip")
            netmask = params.get("netmask")
            if not all([ip_address, netmask]):
                await self._send_json(writer, {"error": "static mode requires ip, netmask"}, 400)
                return
            result = await self.application.set_interface_static(
                device_ip, ip_address, netmask, params.get("dns") or "", params.get("gateway") or ""
            )
        await self._send_json(writer, {"success": True, "reboot_required": True, "interfaces": result})

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
            f"Access-Control-Allow-Origin: *\r\n"
            f"\r\n"
        ).encode() + body
        writer.write(response)
        await writer.drain()
