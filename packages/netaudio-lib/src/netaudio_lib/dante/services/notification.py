import logging
import struct

from netaudio_lib.dante.const import (
    DEVICE_INFO_PORT,
    MULTICAST_GROUP_CONTROL_MONITORING,
)
from netaudio_lib.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio_lib.dante.service import DanteMulticastService

logger = logging.getLogger("netaudio")

NOTIFICATION_TOPOLOGY_CHANGE = 16
NOTIFICATION_INTERFACE_STATUS = 17
NOTIFICATION_SAMPLE_RATE_STATUS = 128
NOTIFICATION_ENCODING_STATUS = 130
NOTIFICATION_DEVICE_REBOOT = 146
NOTIFICATION_TX_CHANNEL_CHANGE = 257
NOTIFICATION_RX_CHANNEL_CHANGE = 258
NOTIFICATION_TX_LABEL_CHANGE = 259
NOTIFICATION_TX_FLOW_CHANGE = 260
NOTIFICATION_RX_FLOW_CHANGE = 261
NOTIFICATION_PROPERTY_CHANGE = 262
NOTIFICATION_LATENCY_CHANGE = 262
NOTIFICATION_SETTINGS_CHANGE = 4110
NOTIFICATION_AES67_STATUS = 4103

NOTIFICATION_NAMES = {
    NOTIFICATION_TOPOLOGY_CHANGE: "Topology Change",
    NOTIFICATION_INTERFACE_STATUS: "Interface Status",
    NOTIFICATION_SAMPLE_RATE_STATUS: "Sample Rate Status",
    NOTIFICATION_ENCODING_STATUS: "Encoding Status",
    NOTIFICATION_DEVICE_REBOOT: "Device Reboot",
    NOTIFICATION_TX_CHANNEL_CHANGE: "TX Channel Change",
    NOTIFICATION_RX_CHANNEL_CHANGE: "RX Channel Change",
    NOTIFICATION_TX_LABEL_CHANGE: "TX Label Change",
    NOTIFICATION_TX_FLOW_CHANGE: "TX Flow Change",
    NOTIFICATION_RX_FLOW_CHANGE: "RX Flow Change",
    NOTIFICATION_PROPERTY_CHANGE: "Property Change",
    NOTIFICATION_SETTINGS_CHANGE: "Settings Change",
    NOTIFICATION_AES67_STATUS: "AES67 Status",
}

class DanteNotificationService(DanteMulticastService):
    def __init__(self, dispatcher: DanteEventDispatcher, device_lookup=None, packet_store=None):
        super().__init__(
            multicast_group=MULTICAST_GROUP_CONTROL_MONITORING,
            multicast_port=DEVICE_INFO_PORT,
            packet_store=packet_store,
        )
        self._dispatcher = dispatcher
        self._device_lookup = device_lookup

    def set_device_lookup(self, lookup_func):
        self._device_lookup = lookup_func

    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        if len(data) < 28:
            return

        source_ip = addr[0]

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
                )
            except Exception as exception:
                logger.debug(f"PacketStore error (notification): {exception}")

        notification_id = struct.unpack(">H", data[26:28])[0]
        notification_name = NOTIFICATION_NAMES.get(notification_id, f"Unknown(0x{notification_id:04X})")

        device = self._lookup_device(source_ip)
        device_name = device.name if device else ""
        server_name = device.server_name if device else ""

        logger.debug(
            f"Notification from {source_ip} ({device_name}): {notification_name} "
            f"(id={notification_id})"
        )

        self._dispatcher.emit_nowait(DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            device_name=device_name,
            server_name=server_name,
            data={
                "notification_id": notification_id,
                "notification_name": notification_name,
                "source_ip": source_ip,
                "raw": data,
            },
        ))

    def _lookup_device(self, ip_str: str):
        if self._device_lookup:
            return self._device_lookup(ip_str)
        return None
