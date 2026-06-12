from __future__ import annotations

import asyncio
import logging
import re
import subprocess

from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio.shure.device import (
    BatteryType,
    ShureChannel,
    ShureDeviceInfo,
    ShureDeviceType,
    ShureP10TChannel,
    ShureTransmitter,
    parse_ad4d,
    parse_p10t,
)

logger = logging.getLogger("netaudio")

SHURE_CONTROL_PORT = 2202
SHURE_OUI = "00:0e:dd"
METER_RATE_MS = 100
ARP_SCAN_INTERVAL = 30
RECONNECT_DELAY = 5

AD4D_DEVICE_KEYS = [
    "DEVICE_ID", "MODEL", "FW_VER", "RF_BAND",
    "TRANSMISSION_MODE", "QUADVERSITY_MODE", "ENCRYPTION_MODE",
]

AD4D_CHANNEL_KEYS = [
    "CHAN_NAME", "AUDIO_GAIN", "AUDIO_MUTE", "FREQUENCY", "GROUP_CHANNEL",
    "FD_MODE", "ENCRYPTION_STATUS", "INTERFERENCE_STATUS",
    "AUDIO_LEVEL_PEAK", "AUDIO_LEVEL_RMS", "CHAN_QUALITY",
    "ANTENNA_STATUS", "TX_BATT_MINS", "TX_BATT_TYPE", "TX_BATT_CHARGE_PERCENT",
    "TX_BATT_BARS", "TX_BATT_CYCLE_COUNT", "TX_BATT_TEMP_F",
    "TX_MODEL", "TX_DEVICE_ID", "TX_POWER_LEVEL", "TX_MUTE_MODE_STATUS",
]

P10T_DEVICE_KEYS = ["DEVICE_NAME"]

P10T_CHANNEL_KEYS = [
    "CHAN_NAME", "AUDIO_IN_LVL", "GROUP_CHAN", "FREQUENCY",
    "RF_TX_LVL", "RF_MUTE", "AUDIO_TX_MODE", "AUDIO_IN_LINE_LVL",
]

MODEL_PREFIXES = {
    "AD4D": "ad4d",
    "AD4Q": "ad4d",
    "P10T": "p10t",
}

_safe_int_sentinel = object()


def _safe_int(value, default=None):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _normalize_mac(mac):
    raw = re.sub(r"[:\-.]", "", mac).lower()[:12]
    return ":".join(raw[i:i + 2] for i in range(0, len(raw), 2))


def _detect_protocol(model_str):
    upper = model_str.upper()
    for prefix, proto in MODEL_PREFIXES.items():
        if upper.startswith(prefix):
            return proto
    return None


_ad4d_rep_re = re.compile(r"<\s*REP\s+(?:(\d)\s+)?([A-Z0-9_]+)\s+\{?([^>}]*?)\}?\s*>")
_p10t_report_re = re.compile(r"<\s*REPORT\s+(?:(\d)\s+)?([A-Z0-9_]+)\s+([^>]*?)\s*>")


class ShureConnection:
    def __init__(self, ip, mac, manager):
        self.ip = ip
        self.mac = mac
        self.manager = manager
        self.protocol = None
        self.model = None
        self.reader = None
        self.writer = None
        self._read_task = None
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
        report_match = re.search(r"REPORT DEVICE_NAME\s+(\S+)", probe_data)

        if rep_match:
            self.model = rep_match.group(1).strip()
            self.protocol = _detect_protocol(self.model) or "ad4d"
        elif report_match:
            self.model = report_match.group(1).strip()
            self.protocol = _detect_protocol(self.model) or "p10t"
        else:
            raise ConnectionError(f"Could not detect protocol for {self.ip}")

        await self._populate(probe_data)

        self._read_task = asyncio.create_task(self._read_loop())

    async def _populate(self, initial_data=""):
        if self.protocol == "ad4d":
            for key in AD4D_DEVICE_KEYS:
                await self._send(f"GET {key}")
            for ch in ("1", "2", "3", "4"):
                for key in AD4D_CHANNEL_KEYS:
                    await self._send(f"GET {ch} {key}")
                await self._send(f"SET {ch} METER_RATE {METER_RATE_MS}")
        else:
            for key in P10T_DEVICE_KEYS:
                await self._send(f"GET {key}")
            for ch in ("1", "2"):
                for key in P10T_CHANNEL_KEYS:
                    await self._send(f"GET {ch} {key}")
                await self._send(f"SET {ch} METER_RATE {METER_RATE_MS}")

    async def _send(self, command):
        if self.writer and not self.writer.is_closing():
            self.writer.write(f"< {command} >\r\n".encode())
            await self.writer.drain()

    async def _read_until_quiet(self, timeout):
        chunks = []
        end = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < end:
            try:
                chunk = await asyncio.wait_for(self.reader.read(8192), timeout=0.1)
                if not chunk:
                    break
                chunks.append(chunk.decode("utf-8", errors="ignore"))
            except asyncio.TimeoutError:
                if chunks:
                    break
        return "".join(chunks)

    async def _read_loop(self):
        buffer = ""
        try:
            while self._running:
                data = await self.reader.read(8192)
                if not data:
                    break
                buffer += data.decode("utf-8", errors="ignore")

                while ">" in buffer:
                    end = buffer.index(">") + 1
                    msg = buffer[:end]
                    buffer = buffer[end:]
                    self._handle_message(msg)
        except (ConnectionError, OSError, asyncio.CancelledError):
            pass
        finally:
            logger.info(f"Shure connection lost: {self.ip}")
            self._running = False
            self.manager._on_connection_lost(self)

    def _handle_message(self, msg):
        if self.protocol == "ad4d":
            m = _ad4d_rep_re.search(msg)
            if not m:
                return
            ch_str, key, value = m.groups()
            channel = int(ch_str) if ch_str else None
            self.manager._on_shure_report(self.ip, self.mac, self.protocol, channel, key.strip(), value.strip())
        else:
            m = _p10t_report_re.search(msg)
            if not m:
                return
            ch_str, key, value = m.groups()
            channel = int(ch_str) if ch_str else None
            self.manager._on_shure_report(self.ip, self.mac, self.protocol, channel, key.strip(), value.strip())

    async def close(self):
        self._running = False
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except (asyncio.CancelledError, Exception):
                pass
        if self.writer and not self.writer.is_closing():
            if self.protocol == "ad4d":
                channels = ("1", "2", "3", "4")
            else:
                channels = ("1", "2")
            for ch in channels:
                try:
                    await self._send(f"SET {ch} METER_RATE 0")
                except Exception:
                    pass
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass


class ShureManager:
    def __init__(self, dispatcher: DanteEventDispatcher):
        self.dispatcher = dispatcher
        self.devices: dict[str, ShureDeviceInfo] = {}
        self._connections: dict[str, ShureConnection] = {}
        self._raw: dict[str, dict] = {}
        self._scan_task = None
        self._reconnect_tasks: dict[str, asyncio.Task] = {}
        self._running = False

    async def start(self):
        self._running = True
        self._scan_task = asyncio.create_task(self._scan_loop())
        logger.info("ShureManager started")

    async def stop(self):
        self._running = False

        if self._scan_task:
            self._scan_task.cancel()
            try:
                await self._scan_task
            except asyncio.CancelledError:
                pass

        for task in self._reconnect_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._reconnect_tasks.clear()

        for conn in list(self._connections.values()):
            await conn.close()
        self._connections.clear()

        logger.info("ShureManager stopped")

    async def send_command(self, mac, command):
        norm_mac = _normalize_mac(mac)
        for conn in self._connections.values():
            if _normalize_mac(conn.mac) == norm_mac:
                await conn._send(command)
                return True
        return False

    async def _scan_loop(self):
        try:
            while self._running:
                await self._scan_arp()
                await asyncio.sleep(ARP_SCAN_INTERVAL)
        except asyncio.CancelledError:
            pass

    async def _scan_arp(self):
        loop = asyncio.get_event_loop()
        entries = await loop.run_in_executor(None, _get_shure_arp_entries)

        known_ips = set(self._connections.keys())
        found_ips = {ip for ip, mac in entries}

        for ip in known_ips - found_ips:
            conn = self._connections.get(ip)
            if conn:
                logger.info(f"Shure device gone from ARP: {ip}")
                await conn.close()
                self._connections.pop(ip, None)
                mac = conn.mac
                if mac in self.devices:
                    del self.devices[mac]
                    del self._raw[mac]
                    self.dispatcher.emit_nowait(DanteEvent(
                        type=EventType.SHURE_DEVICE_REMOVED,
                        device_name=mac,
                    ))

        for ip, mac in entries:
            if ip not in self._connections and ip not in self._reconnect_tasks:
                asyncio.create_task(self._connect_device(ip, mac))

    async def _connect_device(self, ip, mac):
        try:
            conn = ShureConnection(ip, mac, self)
            await conn.connect()
            self._connections[ip] = conn
            norm_mac = _normalize_mac(mac)

            raw = self._raw.get(norm_mac, {})
            if conn.protocol == "ad4d":
                device = parse_ad4d(raw, ip, norm_mac)
            else:
                device = parse_p10t(raw, ip, norm_mac)

            self.devices[norm_mac] = device
            logger.info(f"Shure device connected: {ip} ({conn.model})")

            self.dispatcher.emit_nowait(DanteEvent(
                type=EventType.SHURE_DEVICE_DISCOVERED,
                device_name=norm_mac,
                data={"ip": ip, "model": conn.model, "protocol": conn.protocol},
            ))
        except Exception as exc:
            logger.debug(f"Failed to connect Shure device {ip}: {exc}")
            self._schedule_reconnect(ip, mac)

    def _schedule_reconnect(self, ip, mac):
        if ip in self._reconnect_tasks:
            return

        async def _reconnect():
            await asyncio.sleep(RECONNECT_DELAY)
            self._reconnect_tasks.pop(ip, None)
            if self._running and ip not in self._connections:
                await self._connect_device(ip, mac)

        self._reconnect_tasks[ip] = asyncio.create_task(_reconnect())

    def _on_connection_lost(self, conn):
        self._connections.pop(conn.ip, None)
        if self._running:
            self._schedule_reconnect(conn.ip, conn.mac)

    def _on_shure_report(self, ip, mac, protocol, channel, key, value):
        norm_mac = _normalize_mac(mac)
        raw = self._raw.setdefault(norm_mac, {})

        if channel is not None:
            raw.setdefault(channel, {})[key] = value
        else:
            raw[key] = value

        device = self.devices.get(norm_mac)
        if not device:
            return

        changed = False

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
            self.dispatcher.emit_nowait(DanteEvent(
                type=EventType.SHURE_DEVICE_UPDATED,
                device_name=norm_mac,
                data={"ip": ip, "channel": channel, "key": key, "value": value},
            ))

        if key in ("AUDIO_LEVEL_PEAK", "AUDIO_LEVEL_RMS", "AUDIO_IN_LVL_L", "AUDIO_IN_LVL_R"):
            self.dispatcher.emit_nowait(DanteEvent(
                type=EventType.SHURE_METER_VALUES,
                device_name=norm_mac,
                data={"ip": ip, "channel": channel, "key": key, "value": value},
            ))

    def _update_ad4d_channel(self, device, ch_num, key, value):
        ch = device.channels.get(ch_num)
        if not ch:
            ch = ShureChannel(number=ch_num, transmitter=ShureTransmitter())
            device.channels[ch_num] = ch

        if not ch.transmitter:
            ch.transmitter = ShureTransmitter()

        changed = False

        if key == "CHAN_NAME":
            if ch.name != value:
                ch.name = value
                changed = True
        elif key == "FREQUENCY":
            ch.frequency = _safe_int(value)
        elif key == "AUDIO_GAIN":
            ch.audio_gain = _safe_int(value)
        elif key == "AUDIO_MUTE":
            ch.audio_mute = value == "ON"
        elif key == "AUDIO_LEVEL_PEAK":
            ch.audio_level_peak = _safe_int(value)
        elif key == "AUDIO_LEVEL_RMS":
            ch.audio_level_rms = _safe_int(value)
        elif key == "CHAN_QUALITY":
            ch.signal_quality = _safe_int(value)
        elif key == "ANTENNA_STATUS":
            old = ch.antenna_status
            ch.antenna_status = value
            if old != value:
                changed = True
        elif key == "ENCRYPTION_STATUS":
            ch.encryption_status = value
        elif key == "INTERFERENCE_STATUS":
            ch.interference_status = value
        elif key == "FD_MODE":
            ch.fd_mode = value == "ON"
        elif key == "GROUP_CHANNEL":
            ch.group_channel = value
        elif key == "TX_MODEL":
            old = ch.transmitter.model
            ch.transmitter.model = value
            if old != value:
                changed = True
        elif key == "TX_DEVICE_ID":
            ch.transmitter.device_id = value or None
        elif key == "TX_BATT_MINS":
            ch.transmitter.battery_minutes = _safe_int(value)
            changed = True
        elif key == "TX_BATT_TYPE":
            if value in BatteryType._value2member_map_:
                ch.transmitter.battery_type = BatteryType(value)
        elif key == "TX_BATT_CHARGE_PERCENT":
            ch.transmitter.battery_charge_percent = _safe_int(value)
            changed = True
        elif key == "TX_BATT_BARS":
            ch.transmitter.battery_bars = _safe_int(value)
        elif key == "TX_BATT_CYCLE_COUNT":
            ch.transmitter.battery_cycle_count = _safe_int(value)
        elif key == "TX_BATT_TEMP_F":
            ch.transmitter.battery_temp_f = _safe_int(value)
        elif key == "TX_POWER_LEVEL":
            ch.transmitter.power_level = _safe_int(value)
        elif key == "TX_MUTE_MODE_STATUS":
            old = ch.transmitter.mute_status
            ch.transmitter.mute_status = value
            if old != value:
                changed = True

        return changed

    def _update_p10t_channel(self, device, ch_num, key, value):
        ch = device.channels.get(ch_num)
        if not ch:
            ch = ShureP10TChannel(number=ch_num)
            device.channels[ch_num] = ch

        changed = False

        if key == "CHAN_NAME":
            if ch.name != value:
                ch.name = value
                changed = True
        elif key == "FREQUENCY":
            ch.frequency = _safe_int(value)
        elif key == "AUDIO_IN_LVL":
            ch.audio_in_level = _safe_int(value)
        elif key == "AUDIO_IN_LVL_L":
            ch.audio_in_level_l = _safe_int(value)
        elif key == "AUDIO_IN_LVL_R":
            ch.audio_in_level_r = _safe_int(value)
        elif key == "RF_TX_LVL":
            ch.rf_tx_level = _safe_int(value)
        elif key == "RF_MUTE":
            ch.rf_mute = value == "1"
        elif key == "AUDIO_TX_MODE":
            ch.audio_tx_mode = _safe_int(value)
        elif key == "AUDIO_IN_LINE_LVL":
            ch.audio_in_line_level = _safe_int(value)
        elif key == "GROUP_CHAN":
            ch.group_channel = value

        return changed


def _get_shure_arp_entries():
    result = subprocess.run(["ip", "neigh", "show"], capture_output=True, text=True)
    entries = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[3] == "lladdr":
            ip = parts[0]
            mac = parts[4].lower()
            if mac.startswith(SHURE_OUI):
                entries.append((ip, mac))
    return entries
