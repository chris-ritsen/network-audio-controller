from __future__ import annotations

import asyncio
import logging
import re
import time

from netaudio.asynchronous_primitives import DeferredAsyncioEvent
from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio.shure.device import (
    BatteryType,
    ShureChannel,
    ShureDeviceInfo,
    ShureP10TChannel,
    ShureTransmitter,
    parse_ad4d,
    parse_p10t,
)
from netaudio.shure.discovery import get_shure_neighbor_entries

logger = logging.getLogger("netaudio")

SHURE_CONTROL_PORT = 2202
METER_RATE_MS = 100
NEIGHBOR_SCAN_INTERVAL = 30
RECONNECT_DELAY = 5

AD4D_DEVICE_KEYS = [
    "DEVICE_ID",
    "MODEL",
    "FW_VER",
    "RF_BAND",
    "TRANSMISSION_MODE",
    "QUADVERSITY_MODE",
    "ENCRYPTION_MODE",
]

AD4D_CHANNEL_KEYS = [
    "CHAN_NAME",
    "AUDIO_GAIN",
    "AUDIO_MUTE",
    "FREQUENCY",
    "GROUP_CHANNEL",
    "FD_MODE",
    "ENCRYPTION_STATUS",
    "INTERFERENCE_STATUS",
    "AUDIO_LEVEL_PEAK",
    "AUDIO_LEVEL_RMS",
    "CHAN_QUALITY",
    "ANTENNA_STATUS",
    "TX_BATT_MINS",
    "TX_BATT_TYPE",
    "TX_BATT_CHARGE_PERCENT",
    "TX_BATT_BARS",
    "TX_BATT_CYCLE_COUNT",
    "TX_BATT_TEMP_F",
    "TX_MODEL",
    "TX_DEVICE_ID",
    "TX_POWER_LEVEL",
    "TX_MUTE_MODE_STATUS",
]

P10T_DEVICE_KEYS = ["DEVICE_NAME"]

P10T_CHANNEL_KEYS = [
    "CHAN_NAME",
    "AUDIO_IN_LVL",
    "GROUP_CHAN",
    "FREQUENCY",
    "RF_TX_LVL",
    "RF_MUTE",
    "AUDIO_TX_MODE",
    "AUDIO_IN_LINE_LVL",
]

MODEL_PREFIXES = {
    "AD4D": "ad4d",
    "AD4Q": "ad4d",
    "P10T": "p10t",
}


def _safe_int(value, default=None):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _normalize_mac(mac):
    normalized_address = re.sub(r"[:\-.]", "", mac).lower()[:12]
    return ":".join(normalized_address[index : index + 2] for index in range(0, len(normalized_address), 2))


def _detect_protocol(model_name):
    upper_model_name = model_name.upper()
    for prefix, protocol in MODEL_PREFIXES.items():
        if upper_model_name.startswith(prefix):
            return protocol
    return None


_ad4d_rep_re = re.compile(r"<\s*REP\s+(?:(\d)\s+)?([A-Z0-9_]+)\s+\{?([^>}]*?)\}?\s*>")
_p10t_report_re = re.compile(r"<\s*REPORT\s+(?:(\d)\s+)?([A-Z0-9_]+)\s+([^>]*?)\s*>")


class ShureConnection:
    def __init__(self, ip: str, mac: str, manager: ShureManager):
        self.ip = ip
        self.mac = mac
        self.manager = manager
        self.protocol: str | None = None
        self.model: str | None = None
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._read_task: asyncio.Task[None] | None = None
        self._running = False

    async def connect(self):
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(self.ip, SHURE_CONTROL_PORT),
            timeout=2.0,
        )
        self._running = True

        await self._send("GET MODEL")
        await self._send("GET DEVICE_ID")
        await self._send("GET DEVICE_NAME")

        probe_data = await self._read_until_quiet(0.3)

        rep_match = re.search(r"REP MODEL\s+\{?([^}>]+)", probe_data)
        report_match = re.search(r"REPORT DEVICE_NAME\s+([^>]+?)\s*>", probe_data)

        if rep_match:
            self.model = rep_match.group(1).strip()
            self.protocol = _detect_protocol(self.model) or "ad4d"
        elif report_match:
            self.model = report_match.group(1).strip()
            self.protocol = _detect_protocol(self.model) or "p10t"
        else:
            raise ConnectionError(f"Could not detect protocol for {self.ip}")

        await self._populate()

        self._read_task = asyncio.create_task(self._read_loop())

    async def _populate(self):
        if self.protocol == "ad4d":
            for key in AD4D_DEVICE_KEYS:
                await self._send(f"GET {key}")
            for channel in ("1", "2", "3", "4"):
                for key in AD4D_CHANNEL_KEYS:
                    await self._send(f"GET {channel} {key}")
                await self._send(f"SET {channel} METER_RATE {METER_RATE_MS}")
        else:
            for key in P10T_DEVICE_KEYS:
                await self._send(f"GET {key}")
            for channel in ("1", "2"):
                for key in P10T_CHANNEL_KEYS:
                    await self._send(f"GET {channel} {key}")
                await self._send(f"SET {channel} METER_RATE {METER_RATE_MS}")

    async def _send(self, command):
        writer = self.writer
        if writer is None or writer.is_closing():
            raise ConnectionError(f"Shure connection to {self.ip} is not writable")
        writer.write(f"< {command} >\r\n".encode())
        await writer.drain()

    async def _read_until_quiet(self, timeout):
        reader = self.reader
        if reader is None:
            raise ConnectionError(f"Shure connection to {self.ip} has no reader")
        chunks = []
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            try:
                chunk = await asyncio.wait_for(reader.read(8192), timeout=0.1)
                if not chunk:
                    break
                chunks.append(chunk.decode("utf-8", errors="replace"))
            except asyncio.TimeoutError:
                if chunks:
                    break
        return "".join(chunks)

    async def _read_loop(self):
        reader = self.reader
        if reader is None:
            raise ConnectionError(f"Shure connection to {self.ip} has no reader")
        buffer = ""
        try:
            while self._running:
                response_data = await reader.read(8192)
                if not response_data:
                    break
                buffer += response_data.decode("utf-8", errors="replace")

                while ">" in buffer:
                    message_end = buffer.index(">") + 1
                    message = buffer[:message_end]
                    buffer = buffer[message_end:]
                    self._handle_message(message)
        except asyncio.CancelledError:
            logger.debug(f"Shure read loop cancelled: {self.ip}")
            raise
        except (ConnectionError, OSError) as exception:
            logger.debug(f"Shure read loop ended for {self.ip}: {exception}")
        finally:
            reconnect = self._running
            logger.info(f"Shure connection lost: {self.ip}")
            self._running = False
            writer = self.writer
            if reconnect and writer is not None and not writer.is_closing():
                try:
                    writer.close()
                    await writer.wait_closed()
                except (ConnectionError, OSError) as exception:
                    logger.debug(f"Failed to close lost Shure connection to {self.ip}: {exception}")
                self.reader = None
                self.writer = None
            self.manager._on_connection_lost(self, reconnect=reconnect)

    def _handle_message(self, message):
        if self.protocol == "ad4d":
            match = _ad4d_rep_re.search(message)
            if not match:
                return
            channel_text, key, value = match.groups()
            channel = int(channel_text) if channel_text else None
            self.manager._on_shure_report(self.ip, self.mac, self.protocol, channel, key.strip(), value.strip())
        else:
            match = _p10t_report_re.search(message)
            if not match:
                return
            channel_text, key, value = match.groups()
            channel = int(channel_text) if channel_text else None
            self.manager._on_shure_report(self.ip, self.mac, self.protocol, channel, key.strip(), value.strip())

    async def close(self):
        self._running = False
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                logger.debug(f"Shure read task cancelled: {self.ip}")
            except Exception:
                logger.exception(f"Shure read task failed while closing {self.ip}")
        writer = self.writer
        if writer and not writer.is_closing():
            if self.protocol == "ad4d":
                channels = ("1", "2", "3", "4")
            elif self.protocol == "p10t":
                channels = ("1", "2")
            else:
                channels = ()
            for channel in channels:
                try:
                    await self._send(f"SET {channel} METER_RATE 0")
                except Exception:
                    logger.exception(f"Failed to disable Shure metering for channel {channel} on {self.ip}")
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                logger.exception(f"Failed to close Shure connection to {self.ip}")
        self.reader = None
        self.writer = None


class ShureManager:
    def __init__(self, dispatcher: DanteEventDispatcher):
        self.dispatcher = dispatcher
        self.devices: dict[str, ShureDeviceInfo] = {}
        self._connections: dict[str, ShureConnection] = {}
        self._raw_reports: dict[str, dict] = {}
        self._scan_task: asyncio.Task[None] | None = None
        self._connect_tasks: dict[str, asyncio.Task[None]] = {}
        self._reconnect_tasks: dict[str, asyncio.Task[None]] = {}
        self._stop_event = DeferredAsyncioEvent()
        self._running = False

    async def start(self):
        if self._running:
            return
        self._stop_event.clear()
        self._running = True
        self._scan_task = asyncio.create_task(self._scan_loop(), name="shure-neighbor-scan")
        logger.info("ShureManager started")

    async def stop(self):
        self._running = False
        self._stop_event.set()

        scan_tasks = [self._scan_task] if self._scan_task is not None else []
        await self._cancel_tasks(scan_tasks, "neighbor scan")
        self._scan_task = None

        connect_tasks = list(self._connect_tasks.values())
        await self._cancel_tasks(connect_tasks, "connect")
        self._connect_tasks.clear()

        reconnect_tasks = list(self._reconnect_tasks.values())
        await self._cancel_tasks(reconnect_tasks, "reconnect")
        self._reconnect_tasks.clear()

        connections = list(self._connections.values())
        if connections:
            close_results = await asyncio.gather(
                *(connection.close() for connection in connections),
                return_exceptions=True,
            )
            for close_result in close_results:
                if isinstance(close_result, BaseException):
                    logger.error(
                        "Shure connection cleanup failed",
                        exc_info=(
                            type(close_result),
                            close_result,
                            close_result.__traceback__,
                        ),
                    )
        self._connections.clear()
        self.devices.clear()
        self._raw_reports.clear()

        logger.info("ShureManager stopped")

    async def _cancel_tasks(self, tasks, operation):
        for task in tasks:
            task.cancel()
        if not tasks:
            return
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        for task_result in task_results:
            if isinstance(task_result, BaseException) and not isinstance(task_result, asyncio.CancelledError):
                logger.error(
                    f"Shure {operation} task failed during cleanup",
                    exc_info=(
                        type(task_result),
                        task_result,
                        task_result.__traceback__,
                    ),
                )

    async def send_command(self, mac, command):
        normalized_mac_address = _normalize_mac(mac)
        for connection in self._connections.values():
            if _normalize_mac(connection.mac) == normalized_mac_address:
                await connection._send(command)
                return True
        return False

    async def _scan_loop(self):
        try:
            while self._running:
                try:
                    await self._scan_arp()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Shure neighbor scan failed")
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=NEIGHBOR_SCAN_INTERVAL,
                    )
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            logger.debug("Shure neighbor scan loop cancelled")
            raise

    async def _scan_arp(self):
        event_loop = asyncio.get_running_loop()
        entries = await event_loop.run_in_executor(None, get_shure_neighbor_entries)

        known_ip_addresses = set(self._connections)
        found_ip_addresses = {ip_address for ip_address, _ in entries}

        for ip_address in known_ip_addresses - found_ip_addresses:
            connection = self._connections.get(ip_address)
            if connection:
                logger.info(f"Shure device gone from neighbor table: {ip_address}")
                await connection.close()
                self._connections.pop(ip_address, None)
                self._mark_device_offline(connection.mac)

        for ip_address, mac_address in entries:
            if ip_address not in self._connections and ip_address not in self._reconnect_tasks:
                self._schedule_connect(ip_address, mac_address)

    def _schedule_connect(self, ip_address: str, mac_address: str) -> None:
        existing = self._connect_tasks.get(ip_address)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(
            self._connect_device(ip_address, mac_address),
            name=f"shure-connect:{ip_address}",
        )
        self._connect_tasks[ip_address] = task
        task.add_done_callback(
            lambda completed_task, task_ip_address=ip_address: self._connect_done(
                task_ip_address,
                completed_task,
            )
        )

    def _connect_done(self, ip_address: str, task: asyncio.Task[None]) -> None:
        if self._connect_tasks.get(ip_address) is task:
            self._connect_tasks.pop(ip_address, None)
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None:
            logger.error(
                f"Shure connection task failed for {ip_address}",
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    async def _connect_device(self, ip_address, mac_address):
        connection = None
        try:
            connection = ShureConnection(ip_address, mac_address, self)
            await connection.connect()
            self._connections[ip_address] = connection
            normalized_mac_address = _normalize_mac(mac_address)

            raw_reports = self._raw_reports.get(normalized_mac_address, {})
            if connection.protocol == "ad4d":
                device = parse_ad4d(
                    raw_reports,
                    ip_address,
                    normalized_mac_address,
                )
            else:
                device = parse_p10t(
                    raw_reports,
                    ip_address,
                    normalized_mac_address,
                )

            device.mark_seen()
            self.devices[normalized_mac_address] = device
            logger.info(f"Shure device connected: {ip_address} ({connection.model})")

            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.SHURE_DEVICE_DISCOVERED,
                    device_name=normalized_mac_address,
                    data={
                        "ip": ip_address,
                        "model": connection.model,
                        "protocol": connection.protocol,
                    },
                )
            )
        except asyncio.CancelledError:
            if connection is not None:
                await connection.close()
            raise
        except (OSError, TimeoutError) as exception:
            if connection is not None:
                await connection.close()
            logger.debug(f"Failed to connect Shure device {ip_address}: {exception}")
            self._schedule_reconnect(ip_address, mac_address)

    def _schedule_reconnect(self, ip_address, mac_address):
        if not self._running or ip_address in self._reconnect_tasks:
            return

        async def _reconnect():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=RECONNECT_DELAY,
                )
            except asyncio.TimeoutError:
                self._reconnect_tasks.pop(ip_address, None)
                if self._running and ip_address not in self._connections:
                    self._schedule_connect(ip_address, mac_address)

        task = asyncio.create_task(
            _reconnect(),
            name=f"shure-reconnect:{ip_address}",
        )
        self._reconnect_tasks[ip_address] = task
        task.add_done_callback(
            lambda completed_task, task_ip_address=ip_address: self._reconnect_done(
                task_ip_address,
                completed_task,
            )
        )

    def _reconnect_done(self, ip_address, task):
        if self._reconnect_tasks.get(ip_address) is task:
            self._reconnect_tasks.pop(ip_address, None)
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None:
            logger.error(
                f"Shure reconnection task failed for {ip_address}",
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    def _mark_device_offline(self, mac_address: str) -> None:
        normalized_mac_address = _normalize_mac(mac_address)
        device = self.devices.get(normalized_mac_address)
        if device is None or not device.mark_offline():
            return
        self.dispatcher.emit_nowait(
            DanteEvent(
                type=EventType.SHURE_DEVICE_UPDATED,
                device_name=normalized_mac_address,
                data={"online": False, "last_seen": device.last_seen},
            )
        )

    def _on_connection_lost(self, connection, *, reconnect):
        if self._connections.get(connection.ip) is connection:
            self._connections.pop(connection.ip, None)
        self._mark_device_offline(connection.mac)
        if self._running and reconnect and connection.ip not in self._connections:
            self._schedule_reconnect(connection.ip, connection.mac)

    def _on_shure_report(
        self,
        ip_address,
        mac_address,
        protocol,
        channel,
        key,
        value,
    ):
        normalized_mac_address = _normalize_mac(mac_address)
        raw_reports = self._raw_reports.setdefault(normalized_mac_address, {})

        if channel is not None:
            raw_reports.setdefault(channel, {})[key] = value
        else:
            raw_reports[key] = value

        device = self.devices.get(normalized_mac_address)
        if not device:
            return

        device.last_seen = time.time()
        changed = False
        if not device.online:
            device.online = True
            changed = True

        if channel is None:
            if protocol == "ad4d":
                if key == "DEVICE_ID":
                    if device.name != value.strip():
                        device.name = value.strip()
                        changed = True
                elif key == "MODEL":
                    if device.model != value.strip():
                        device.model = value.strip()
                        changed = True
                elif key == "FW_VER":
                    if device.firmware_version != value.strip():
                        device.firmware_version = value.strip()
                        changed = True
                elif key == "RF_BAND":
                    device.rf_band = value.strip()
                elif key == "TRANSMISSION_MODE":
                    device.transmission_mode = value.strip()
                elif key == "QUADVERSITY_MODE":
                    device.quadversity_mode = value.strip()
                elif key == "ENCRYPTION_MODE":
                    device.encryption_mode = value.strip()
            else:
                if key == "DEVICE_NAME":
                    if device.name != value.strip():
                        device.name = value.strip()
                        changed = True
        else:
            if protocol == "ad4d":
                changed = self._update_ad4d_channel(device, channel, key, value)
            else:
                changed = self._update_p10t_channel(device, channel, key, value)

        if changed:
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.SHURE_DEVICE_UPDATED,
                    device_name=normalized_mac_address,
                    data={
                        "ip": ip_address,
                        "channel": channel,
                        "key": key,
                        "value": value,
                    },
                )
            )

        if key in ("AUDIO_LEVEL_PEAK", "AUDIO_LEVEL_RMS", "AUDIO_IN_LVL_L", "AUDIO_IN_LVL_R"):
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.SHURE_METER_VALUES,
                    device_name=normalized_mac_address,
                    data={
                        "ip": ip_address,
                        "channel": channel,
                        "key": key,
                        "value": value,
                    },
                )
            )

    def _update_ad4d_channel(self, device, channel_number, key, value):
        channel = device.channels.get(channel_number)
        if not channel:
            channel = ShureChannel(
                number=channel_number,
                transmitter=ShureTransmitter(),
            )
            device.channels[channel_number] = channel

        if not channel.transmitter:
            channel.transmitter = ShureTransmitter()

        changed = False

        if key == "CHAN_NAME":
            if channel.name != value:
                channel.name = value
                changed = True
        elif key == "FREQUENCY":
            channel.frequency = _safe_int(value)
        elif key == "AUDIO_GAIN":
            channel.audio_gain = _safe_int(value)
        elif key == "AUDIO_MUTE":
            channel.audio_mute = value == "ON"
        elif key == "AUDIO_LEVEL_PEAK":
            channel.audio_level_peak = _safe_int(value)
        elif key == "AUDIO_LEVEL_RMS":
            channel.audio_level_rms = _safe_int(value)
        elif key == "CHAN_QUALITY":
            channel.signal_quality = _safe_int(value)
        elif key == "ANTENNA_STATUS":
            previous_antenna_status = channel.antenna_status
            channel.antenna_status = value
            if previous_antenna_status != value:
                changed = True
        elif key == "ENCRYPTION_STATUS":
            channel.encryption_status = value
        elif key == "INTERFERENCE_STATUS":
            channel.interference_status = value
        elif key == "FD_MODE":
            channel.fd_mode = value == "ON"
        elif key == "GROUP_CHANNEL":
            channel.group_channel = value
        elif key == "TX_MODEL":
            previous_model = channel.transmitter.model
            channel.transmitter.model = value
            if previous_model != value:
                changed = True
        elif key == "TX_DEVICE_ID":
            channel.transmitter.device_id = value or None
        elif key == "TX_BATT_MINS":
            channel.transmitter.battery_minutes = _safe_int(value)
            changed = True
        elif key == "TX_BATT_TYPE":
            if value in BatteryType._value2member_map_:
                channel.transmitter.battery_type = BatteryType(value)
        elif key == "TX_BATT_CHARGE_PERCENT":
            channel.transmitter.battery_charge_percent = _safe_int(value)
            changed = True
        elif key == "TX_BATT_BARS":
            channel.transmitter.battery_bars = _safe_int(value)
        elif key == "TX_BATT_CYCLE_COUNT":
            channel.transmitter.battery_cycle_count = _safe_int(value)
        elif key == "TX_BATT_TEMP_F":
            channel.transmitter.battery_temp_f = _safe_int(value)
        elif key == "TX_POWER_LEVEL":
            channel.transmitter.power_level = _safe_int(value)
        elif key == "TX_MUTE_MODE_STATUS":
            previous_mute_status = channel.transmitter.mute_status
            channel.transmitter.mute_status = value
            if previous_mute_status != value:
                changed = True

        return changed

    def _update_p10t_channel(self, device, channel_number, key, value):
        channel = device.channels.get(channel_number)
        if not channel:
            channel = ShureP10TChannel(number=channel_number)
            device.channels[channel_number] = channel

        changed = False

        if key == "CHAN_NAME":
            if channel.name != value:
                channel.name = value
                changed = True
        elif key == "FREQUENCY":
            channel.frequency = _safe_int(value)
        elif key == "AUDIO_IN_LVL":
            channel.audio_in_level = _safe_int(value)
        elif key == "AUDIO_IN_LVL_L":
            channel.audio_in_level_l = _safe_int(value)
        elif key == "AUDIO_IN_LVL_R":
            channel.audio_in_level_r = _safe_int(value)
        elif key == "RF_TX_LVL":
            channel.rf_tx_level = _safe_int(value)
        elif key == "RF_MUTE":
            channel.rf_mute = value == "1"
        elif key == "AUDIO_TX_MODE":
            channel.audio_tx_mode = _safe_int(value)
        elif key == "AUDIO_IN_LINE_LVL":
            channel.audio_in_line_level = _safe_int(value)
        elif key == "GROUP_CHAN":
            channel.group_channel = value

        return changed
