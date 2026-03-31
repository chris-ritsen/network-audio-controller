import asyncio
import logging
import socket
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

CONMON_OPCODE_INTERFACE_STATUS = 0x0011
CONMON_OPCODE_MAKE_MODEL_RESPONSE = 0x00C0
CONMON_OPCODE_DANTE_MODEL_RESPONSE = 0x0060
CONMON_OPCODE_AES67_CURRENT_NEW = 0x1007
CONMON_AES67_CURRENT_NEW_OFFSET = 0x21
CONMON_OPCODE_PTP_CLOCK_STATUS = 0x0020
CONMON_PREFERRED_LEADER_OFFSET = 0x26
CONMON_PTP_V1_ROLE_OFFSET = 0x48
PTP_V1_ROLE_MASTER = 0x0006
PTP_V1_ROLE_SLAVE = 0x0009

PTP_V1_ROLE_MAP = {
    PTP_V1_ROLE_MASTER: "Leader",
    PTP_V1_ROLE_SLAVE: "Follower",
}
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

INTERFACE_MODE_DYNAMIC = 0x0001
INTERFACE_MODE_STATIC = 0x0003

INTERFACE_MODE_NAMES = {
    INTERFACE_MODE_DYNAMIC: "dynamic",
    INTERFACE_MODE_STATIC: "static",
}

AES67_CURRENT_NEW_MAP = {
    0x00: (False, False),
    0x01: (True, False),
    0x02: (False, True),
    0x03: (True, True),
}


def parse_aes67_current_new_byte(state_byte: int) -> tuple[bool | None, bool | None]:
    result = AES67_CURRENT_NEW_MAP.get(state_byte)
    if result is not None:
        return result
    return (None, None)


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
        self._aes67_waiters: dict[str, asyncio.Event] = {}
        self._aes67_results: dict[str, tuple[bool | None, bool | None]] = {}
        self._preferred_leader_waiters: dict[str, asyncio.Event] = {}
        self._preferred_leader_results: dict[str, bool | None] = {}
        self._interface_waiters: dict[str, asyncio.Event] = {}
        self._interface_results: dict[str, list[dict]] = {}

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

    def register_aes67_waiter(self, device_ip: str) -> asyncio.Event:
        event = asyncio.Event()
        self._aes67_waiters[device_ip] = event
        self._aes67_results.pop(device_ip, None)
        return event

    def unregister_aes67_waiter(self, device_ip: str) -> None:
        self._aes67_waiters.pop(device_ip, None)

    def get_aes67_result(self, device_ip: str) -> tuple[bool | None, bool | None] | None:
        return self._aes67_results.pop(device_ip, None)

    def _notify_aes67_waiter(self, source_ip: str, current: bool | None, configured: bool | None) -> None:
        if source_ip in self._aes67_waiters:
            self._aes67_results[source_ip] = (current, configured)
            self._aes67_waiters[source_ip].set()

    def register_preferred_leader_waiter(self, device_ip: str) -> asyncio.Event:
        event = asyncio.Event()
        self._preferred_leader_waiters[device_ip] = event
        self._preferred_leader_results.pop(device_ip, None)
        return event

    def unregister_preferred_leader_waiter(self, device_ip: str) -> None:
        self._preferred_leader_waiters.pop(device_ip, None)

    def get_preferred_leader_result(self, device_ip: str) -> bool | None:
        return self._preferred_leader_results.pop(device_ip, None)

    def _notify_preferred_leader_waiter(self, source_ip: str, preferred_leader: bool | None) -> None:
        if source_ip in self._preferred_leader_waiters:
            self._preferred_leader_results[source_ip] = preferred_leader
            self._preferred_leader_waiters[source_ip].set()

    def register_interface_waiter(self, device_ip: str) -> asyncio.Event:
        event = asyncio.Event()
        self._interface_waiters[device_ip] = event
        self._interface_results.pop(device_ip, None)
        return event

    def unregister_interface_waiter(self, device_ip: str) -> None:
        self._interface_waiters.pop(device_ip, None)

    def get_interface_result(self, device_ip: str) -> list[dict] | None:
        return self._interface_results.pop(device_ip, None)

    def _notify_interface_waiter(self, source_ip: str, interfaces: list[dict]) -> None:
        if source_ip in self._interface_waiters:
            self._interface_results[source_ip] = interfaces
            self._interface_waiters[source_ip].set()

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

        if opcode == CONMON_OPCODE_INTERFACE_STATUS:
            self._handle_interface_status(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_MAKE_MODEL_RESPONSE:
            self._handle_make_model_response(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_DANTE_MODEL_RESPONSE:
            self._handle_dante_model_response(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_AES67_CURRENT_NEW:
            self._handle_aes67_current_new(data, source_ip)
            self._notify_conmon_waiter(source_ip, opcode)
            return True
        elif opcode == CONMON_OPCODE_PTP_CLOCK_STATUS:
            self._handle_ptp_clock_status(data, source_ip)
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

    def _handle_aes67_current_new(self, data: bytes, source_ip: str) -> None:
        if len(data) <= CONMON_AES67_CURRENT_NEW_OFFSET:
            return

        state_byte = data[CONMON_AES67_CURRENT_NEW_OFFSET]
        aes67_current, aes67_configured = parse_aes67_current_new_byte(state_byte)

        logger.debug(
            f"Conmon aes67_current_new from {source_ip} ({len(data)}B): "
            f"byte=0x{state_byte:02X} current={aes67_current} configured={aes67_configured}"
        )

        device = self._lookup_device(source_ip)

        if device is None:
            parsed = {}
            if aes67_current is not None:
                parsed["aes67_current"] = aes67_current
            if aes67_configured is not None:
                parsed["aes67_configured"] = aes67_configured
            if parsed:
                self._cache_pending(source_ip, parsed)
        else:
            if aes67_current is not None:
                device.aes67_current = aes67_current
            if aes67_configured is not None:
                device.aes67_configured = aes67_configured

        self._notify_aes67_waiter(source_ip, aes67_current, aes67_configured)

    def _handle_ptp_clock_status(self, data: bytes, source_ip: str) -> None:
        if len(data) <= CONMON_PREFERRED_LEADER_OFFSET:
            return

        preferred_leader_byte = data[CONMON_PREFERRED_LEADER_OFFSET]
        preferred_leader = preferred_leader_byte == 0x01

        ptp_v1_role = None
        if len(data) >= CONMON_PTP_V1_ROLE_OFFSET + 2:
            role_value = struct.unpack(">H", data[CONMON_PTP_V1_ROLE_OFFSET:CONMON_PTP_V1_ROLE_OFFSET + 2])[0]
            ptp_v1_role = PTP_V1_ROLE_MAP.get(role_value)

        logger.debug(
            f"Conmon ptp_clock_status from {source_ip} ({len(data)}B): "
            f"preferred_leader=0x{preferred_leader_byte:02X} ({preferred_leader}) "
            f"ptp_v1_role={ptp_v1_role}"
        )

        device = self._lookup_device(source_ip)

        parsed = {"preferred_leader": preferred_leader}
        if ptp_v1_role is not None:
            parsed["ptp_v1_role"] = ptp_v1_role

        if device is None:
            self._cache_pending(source_ip, parsed)
        else:
            device.preferred_leader = preferred_leader
            if ptp_v1_role is not None:
                device.ptp_v1_role = ptp_v1_role

        self._notify_preferred_leader_waiter(source_ip, preferred_leader)

    def _handle_interface_status(self, data: bytes, source_ip: str) -> None:
        if len(data) < 0x40:
            return

        interface_count = struct.unpack(">H", data[0x20:0x22])[0]
        interfaces = []

        offset = 0x28
        for _ in range(interface_count):
            if offset + 20 > len(data):
                break

            mode_value = struct.unpack(">H", data[offset:offset + 2])[0]
            mode = INTERFACE_MODE_NAMES.get(mode_value, f"unknown(0x{mode_value:04X})")
            mac_bytes = data[offset + 2:offset + 8]
            mac_address = ":".join(f"{byte:02X}" for byte in mac_bytes)
            ip_address = socket.inet_ntoa(data[offset + 8:offset + 12])
            netmask = socket.inet_ntoa(data[offset + 12:offset + 16])

            interface_info = {
                "mode": mode,
                "mac_address": mac_address,
                "ip_address": ip_address,
                "netmask": netmask,
            }

            if mode == "dynamic":
                gateway = socket.inet_ntoa(data[offset + 16:offset + 20])
                dns_server = socket.inet_ntoa(data[offset + 20:offset + 24])
                interface_info["gateway"] = gateway
                interface_info["dns_server"] = dns_server
                offset += 24
            elif mode == "static":
                dns_server = socket.inet_ntoa(data[offset + 16:offset + 20])
                gateway = socket.inet_ntoa(data[offset + 20:offset + 24])
                interface_info["dns_server"] = dns_server
                interface_info["gateway"] = gateway
                offset += 24
            else:
                offset += 20

            interfaces.append(interface_info)

        reboot_required = False
        pending_config = None
        if len(data) > 0x49:
            reboot_flag = struct.unpack(">H", data[0x48:0x4a])[0]
            reboot_required = reboot_flag != 0

            if reboot_flag == 0x0004:
                pending_config = {"mode": "dynamic"}
            elif reboot_flag == 0x0006 and len(data) >= 0x5c:
                pending_ip = socket.inet_ntoa(data[0x4c:0x50])
                pending_mask = socket.inet_ntoa(data[0x50:0x54])
                pending_dns = socket.inet_ntoa(data[0x54:0x58])
                pending_gw = socket.inet_ntoa(data[0x58:0x5c])
                pending_config = {
                    "mode": "static",
                    "ip_address": pending_ip,
                    "netmask": pending_mask,
                    "dns_server": pending_dns,
                    "gateway": pending_gw,
                }

        logger.debug(
            f"Conmon interface_status from {source_ip} ({len(data)}B): "
            f"interface_count={interface_count} reboot_required={reboot_required} "
            f"pending_config={pending_config} interfaces={interfaces}"
        )

        device = self._lookup_device(source_ip)

        if device is None:
            self._cache_pending(source_ip, {
                "interfaces": interfaces,
                "interface_reboot_required": reboot_required,
                "interface_pending_config": pending_config,
            })
        else:
            device.interfaces = interfaces
            device.interface_reboot_required = reboot_required
            device.interface_pending_config = pending_config

        self._notify_interface_waiter(source_ip, interfaces)

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
