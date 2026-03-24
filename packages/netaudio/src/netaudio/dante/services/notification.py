import asyncio
import logging
import struct

from netaudio.dante.const import (
    DEVICE_INFO_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
)
from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio.dante.service import DanteMulticastService

logger = logging.getLogger("netaudio")

NOTIFICATION_TOPOLOGY_CHANGE = 16
NOTIFICATION_INTERFACE_STATUS = 17
NOTIFICATION_CLOCKING_STATUS = 32
NOTIFICATION_VERSIONS_STATUS = 96
NOTIFICATION_CLEAR_CONFIG_STATUS = 120
NOTIFICATION_SAMPLE_RATE_STATUS = 128
NOTIFICATION_ENCODING_STATUS = 130
NOTIFICATION_DEVICE_REBOOT = 146
NOTIFICATION_MANF_VERSIONS_STATUS = 192
NOTIFICATION_ROUTING_READY = 256
NOTIFICATION_TX_CHANNEL_CHANGE = 257
NOTIFICATION_RX_CHANNEL_CHANGE = 258
NOTIFICATION_TX_LABEL_CHANGE = 259
NOTIFICATION_TX_FLOW_CHANGE = 260
NOTIFICATION_RX_FLOW_CHANGE = 261
NOTIFICATION_PROPERTY_CHANGE = 262
NOTIFICATION_LATENCY_CHANGE = 262
NOTIFICATION_ROUTING_DEVICE_CHANGE = 288
NOTIFICATION_SETTINGS_CHANGE = 4110
NOTIFICATION_AES67_STATUS = 4103

NOTIFICATION_NAMES = {
    NOTIFICATION_TOPOLOGY_CHANGE: "Topology Change",
    NOTIFICATION_INTERFACE_STATUS: "Interface Status",
    NOTIFICATION_CLOCKING_STATUS: "Clocking Status",
    NOTIFICATION_VERSIONS_STATUS: "Versions Status",
    NOTIFICATION_CLEAR_CONFIG_STATUS: "Clear Config Status",
    NOTIFICATION_SAMPLE_RATE_STATUS: "Sample Rate Status",
    NOTIFICATION_ENCODING_STATUS: "Encoding Status",
    NOTIFICATION_DEVICE_REBOOT: "Device Reboot",
    NOTIFICATION_MANF_VERSIONS_STATUS: "Manufacturer Versions Status",
    NOTIFICATION_ROUTING_READY: "Routing Ready",
    NOTIFICATION_TX_CHANNEL_CHANGE: "TX Channel Change",
    NOTIFICATION_RX_CHANNEL_CHANGE: "RX Channel Change",
    NOTIFICATION_TX_LABEL_CHANGE: "TX Label Change",
    NOTIFICATION_TX_FLOW_CHANGE: "TX Flow Change",
    NOTIFICATION_RX_FLOW_CHANGE: "RX Flow Change",
    NOTIFICATION_PROPERTY_CHANGE: "Property Change",
    NOTIFICATION_ROUTING_DEVICE_CHANGE: "Routing Device Change",
    NOTIFICATION_SETTINGS_CHANGE: "Settings Change",
    NOTIFICATION_AES67_STATUS: "AES67 Status",
}

CONMON_OPCODE_MAKE_MODEL_RESPONSE = 0x00C0
CONMON_OPCODE_DANTE_MODEL_RESPONSE = 0x0060
CONMON_MANUFACTURER_OFFSET = 0x4C
CONMON_MANUFACTURER_END = 0xCC
CONMON_PRODUCT_NAME_OFFSET = 0xCC
CONMON_PRODUCT_NAME_END = 0x14C
CONMON_PRODUCT_VERSION_OFFSET = 0x14C
CONMON_PRODUCT_VERSION_END = 0x150
CONMON_BOARD_CODENAME_OFFSET = 0x2C
CONMON_BOARD_CODENAME_END = 0x58
CONMON_BOARD_NAME_OFFSET = 0x58
CONMON_BOARD_NAME_END = 0x98
PROTOCOL_SETTINGS = 0xFFFF
PROTOCOL_CONTROL = 0x27FF


class DanteNotificationService(DanteMulticastService):
    def __init__(self, dispatcher: DanteEventDispatcher, device_lookup=None, packet_store=None, interface_ip: str | None = None, dissect: bool = False):
        super().__init__(
            multicast_group=MULTICAST_GROUP_CONTROL_MONITORING,
            multicast_port=DEVICE_INFO_PORT,
            packet_store=packet_store,
            interface_ip=interface_ip,
            dissect=dissect,
        )
        self._dispatcher = dispatcher
        self._device_lookup = device_lookup
        self._pending_conmon: dict[str, dict] = {}
        self._conmon_waiters: dict[str, asyncio.Event] = {}
        self._conmon_received: dict[str, set[int]] = {}
        self._conmon_expected_count: dict[str, int] = {}

    def set_device_lookup(self, lookup_func):
        self._device_lookup = lookup_func

    def register_conmon_waiter(self, device_ip: str, expected_count: int = 2) -> asyncio.Event:
        event = asyncio.Event()
        self._conmon_waiters[device_ip] = event
        self._conmon_received[device_ip] = set()
        self._conmon_expected_count[device_ip] = expected_count
        return event

    def unregister_conmon_waiter(self, device_ip: str) -> None:
        self._conmon_waiters.pop(device_ip, None)
        self._conmon_received.pop(device_ip, None)
        self._conmon_expected_count.pop(device_ip, None)

    def _notify_conmon_waiter(self, source_ip: str, opcode: int) -> None:
        if source_ip not in self._conmon_waiters:
            return

        self._conmon_received[source_ip].add(opcode)
        expected = self._conmon_expected_count.get(source_ip, 2)

        if len(self._conmon_received[source_ip]) >= expected:
            self._conmon_waiters[source_ip].set()

    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        if len(data) < 4:
            return

        source_ip = addr[0]

        if self._dissect:
            try:
                from netaudio.common.app_config import settings as app_settings
                from netaudio.dante.packet_dissector import dissect_and_render, format_dissect_label
                color = not app_settings.no_color
                label = format_dissect_label("multicast", f"{source_ip}:{addr[1]}", color=color)
                rendered = dissect_and_render(data, indent="  ", color=color)
                logger.debug(f"Dissect [{label}] {len(data)}B:\n{rendered}")
            except Exception as exception:
                logger.debug(f"Dissect error: {exception}")

        if self._packet_store:
            device = self._lookup_device(source_ip)
            try:
                self._packet_store.store_packet(
                    payload=data,
                    source_type="multicast",
                    src_ip=source_ip,
                    src_port=addr[1],
                    device_name=device.name if device else None,
                    device_ip=source_ip,
                    multicast_group=self._multicast_group,
                    multicast_port=self._multicast_port,
                    session_id=self._session_id,
                )
            except Exception as exception:
                logger.debug(f"PacketStore error (notification): {exception}")

        protocol_id = struct.unpack(">H", data[0:2])[0]

        if protocol_id == PROTOCOL_SETTINGS:
            if self._handle_conmon_response(data, source_ip):
                return
            self._handle_settings_notification(data, source_ip)
            return

        device = self._lookup_device(source_ip)
        device_name = device.name if device else ""
        server_name = device.server_name if device else ""

        if len(data) < 28:
            logger.debug(
                f"Short multicast packet from {source_ip} ({device_name}), "
                f"{len(data)} bytes, protocol=0x{protocol_id:04X}, hex={data.hex()}"
            )
            return

        notification_id = struct.unpack(">H", data[26:28])[0]
        notification_name = NOTIFICATION_NAMES.get(notification_id, f"Unknown(0x{notification_id:04X})")

        logger.debug(f"Notification from {source_ip} ({device_name}): {notification_name} (id={notification_id})")

        self._dispatcher.emit_nowait(
            DanteEvent(
                type=EventType.NOTIFICATION_RECEIVED,
                device_name=device_name,
                server_name=server_name,
                data={
                    "notification_id": notification_id,
                    "notification_name": notification_name,
                    "source_ip": source_ip,
                    "raw": data,
                },
            )
        )

    def _handle_settings_notification(self, data: bytes, source_ip: str) -> None:
        device = self._lookup_device(source_ip)
        device_name = device.name if device else ""
        server_name = device.server_name if device else ""

        if len(data) >= 28:
            notification_id = struct.unpack(">H", data[26:28])[0]
        else:
            notification_id = None

        logger.debug(
            f"Settings notification from {source_ip} ({device_name}), "
            f"{len(data)} bytes, notification_id={notification_id}, hex={data.hex()}"
        )

        if notification_id is not None:
            notification_name = NOTIFICATION_NAMES.get(notification_id, f"Unknown(0x{notification_id:04X})")
            self._dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.NOTIFICATION_RECEIVED,
                    device_name=device_name,
                    server_name=server_name,
                    data={
                        "notification_id": notification_id,
                        "notification_name": notification_name,
                        "source_ip": source_ip,
                        "raw": data,
                    },
                )
            )

    def _handle_conmon_response(self, data: bytes, source_ip: str) -> bool:
        opcode = self._extract_conmon_opcode(data)

        if opcode is None:
            return False

        if opcode == CONMON_OPCODE_MAKE_MODEL_RESPONSE:
            self._handle_make_model_response(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_DANTE_MODEL_RESPONSE:
            self._handle_dante_model_response(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True

        return False

    def _handle_make_model_response(self, data: bytes, source_ip: str) -> None:
        product_name, product_version, manufacturer = self.parse_make_model_response(data)
        logger.debug(
            f"Conmon make_model from {source_ip} ({len(data)}B): "
            f"name={product_name!r} version={product_version!r} manufacturer={manufacturer!r}"
        )

        parsed = {}

        if product_name:
            parsed["dante_model"] = product_name

        if product_version:
            parsed["product_version"] = product_version

        if manufacturer:
            parsed["manufacturer"] = manufacturer

        if not parsed:
            return

        device = self._lookup_device(source_ip)

        if device is None:
            self._cache_pending(source_ip, parsed)
            return

        self._apply_conmon_data(device, parsed)

    def _handle_dante_model_response(self, data: bytes, source_ip: str) -> None:
        board_codename, board_name = self.parse_dante_model_response(data)
        logger.debug(
            f"Conmon dante_model from {source_ip} ({len(data)}B): codename={board_codename!r} board_name={board_name!r}"
        )

        parsed = {}

        if board_codename:
            parsed["dante_model_id"] = board_codename

        if board_name:
            parsed["board_name"] = board_name

        if not parsed:
            return

        device = self._lookup_device(source_ip)

        if device is None:
            self._cache_pending(source_ip, parsed)
            return

        self._apply_conmon_data(device, parsed)

    def _cache_pending(self, source_ip: str, parsed: dict) -> None:
        if source_ip not in self._pending_conmon:
            self._pending_conmon[source_ip] = {}
        self._pending_conmon[source_ip].update(parsed)

    @staticmethod
    def _apply_conmon_data(device, parsed: dict) -> None:
        for field, value in parsed.items():
            if field == "manufacturer":
                setattr(device, field, value)
            elif not getattr(device, field, None):
                setattr(device, field, value)

    def apply_pending_for_device(self, device) -> None:
        if not device.ipv4:
            return

        ip_str = str(device.ipv4)
        pending = self._pending_conmon.pop(ip_str, None)

        if pending:
            self._apply_conmon_data(device, pending)
            logger.debug(f"Applied pending conmon data for {ip_str}: {list(pending.keys())}")

    @staticmethod
    def _extract_conmon_opcode(data: bytes) -> int | None:
        if len(data) < 0x20:
            return None

        try:
            magic_pos = data.find(b"Audinate", 4)

            if magic_pos < 0:
                return None

            opcode_pos = magic_pos + 10

            if opcode_pos + 2 > len(data):
                return None

            return struct.unpack(">H", data[opcode_pos : opcode_pos + 2])[0]
        except Exception:
            return None

    @staticmethod
    def _extract_null_terminated_string(data: bytes, start: int, end: int) -> str:
        if len(data) < end:
            return ""

        try:
            raw = data[start:end]
            null_pos = raw.find(b"\x00")

            if null_pos >= 0:
                raw = raw[:null_pos]

            first_printable = 0
            while first_printable < len(raw) and raw[first_printable] < 0x20:
                first_printable += 1

            raw = raw[first_printable:]

            if not raw:
                return ""

            text = raw.decode("utf-8", errors="replace").strip()

            if text and all(c.isprintable() or c == " " for c in text):
                return text
        except Exception:
            pass

        return ""

    @staticmethod
    def parse_make_model_response(data: bytes) -> tuple[str, str, str]:
        product_name = DanteNotificationService._extract_null_terminated_string(
            data, CONMON_PRODUCT_NAME_OFFSET, CONMON_PRODUCT_NAME_END
        )
        manufacturer = DanteNotificationService._extract_null_terminated_string(
            data, CONMON_MANUFACTURER_OFFSET, CONMON_MANUFACTURER_END
        )
        product_version = ""

        try:
            if len(data) >= CONMON_PRODUCT_VERSION_END:
                version_bytes = data[CONMON_PRODUCT_VERSION_OFFSET:CONMON_PRODUCT_VERSION_END]
                major, minor, patch, build = struct.unpack("BBBB", version_bytes)

                if major or minor or patch or build:
                    product_version = f"{major}.{minor}.{build}"
        except Exception:
            pass

        return product_name, product_version, manufacturer

    @staticmethod
    def parse_dante_model_response(data: bytes) -> tuple[str, str]:
        board_codename = DanteNotificationService._extract_null_terminated_string(
            data, CONMON_BOARD_CODENAME_OFFSET, CONMON_BOARD_CODENAME_END
        )
        board_name = DanteNotificationService._extract_null_terminated_string(
            data, CONMON_BOARD_NAME_OFFSET, CONMON_BOARD_NAME_END
        )

        return board_codename, board_name

    def _lookup_device(self, ip_str: str):
        if self._device_lookup:
            return self._device_lookup(ip_str)
        return None
