import asyncio
import logging
import struct
import time

from netaudio.dante.const import (
    DEVICE_HEARTBEAT_PORT,
    HEARTBEAT_LOCK_UNRELIABLE_MODEL_IDS,
    MULTICAST_GROUP_HEARTBEAT,
)
from netaudio.dante.service import DanteMulticastService

logger = logging.getLogger("netaudio")

OFFLINE_THRESHOLD_SECONDS = 15.0
SWEEP_INTERVAL_SECONDS = 5.0

HEARTBEAT_HEADER_SIZE = 0x20
SUBBLOCK_HEADER_SIZE = 4
SUBBLOCK_LOCK_STATUS = 0x8002
LOCK_STATE_OFFSET = 16
LOCK_STATE_LOCKED = 0x0001
LOCK_STATE_UNLOCKED = 0x0002


def _parse_lock_state(data: bytes) -> bool | None:
    offset = HEARTBEAT_HEADER_SIZE
    while offset + SUBBLOCK_HEADER_SIZE <= len(data):
        block_size = struct.unpack_from(">H", data, offset)[0]
        if block_size < SUBBLOCK_HEADER_SIZE or offset + block_size > len(data):
            break
        sub_opcode = struct.unpack_from(">H", data, offset + 2)[0]
        if sub_opcode == SUBBLOCK_LOCK_STATUS:
            if offset + LOCK_STATE_OFFSET + 2 <= len(data):
                lock_value = struct.unpack_from(">H", data, offset + LOCK_STATE_OFFSET)[0]
                if lock_value == LOCK_STATE_LOCKED:
                    return True
                if lock_value == LOCK_STATE_UNLOCKED:
                    return False
            return None
        offset += block_size
    return None


class DanteHeartbeatService(DanteMulticastService):
    def __init__(self, device_by_ip=None, get_devices=None, mark_offline=None, interface_ip=None):
        super().__init__(
            multicast_group=MULTICAST_GROUP_HEARTBEAT,
            multicast_port=DEVICE_HEARTBEAT_PORT,
            interface_ip=interface_ip,
        )
        self._device_by_ip = device_by_ip
        self._get_devices = get_devices
        self._mark_offline = mark_offline
        self._sweep_task = None

    async def start(self) -> None:
        await super().start()
        self._sweep_task = asyncio.create_task(self._sweep_loop())

    async def stop(self) -> None:
        if self._sweep_task:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass
            self._sweep_task = None
        await super().stop()

    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        source_ip = addr[0]
        if not self._device_by_ip:
            return

        device = self._device_by_ip(source_ip)
        if device:
            device.update_last_seen()
            if getattr(device, "model_id", None) in HEARTBEAT_LOCK_UNRELIABLE_MODEL_IDS:
                return
            lock_state = _parse_lock_state(data)
            if lock_state is not None:
                device.is_locked = lock_state
            elif device.is_locked is None:
                device.is_locked = False

    async def _sweep_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(SWEEP_INTERVAL_SECONDS)
                self._check_stale_devices()
            except asyncio.CancelledError:
                break
            except Exception as exception:
                logger.debug(f"Heartbeat sweep error: {exception}")

    def _check_stale_devices(self) -> None:
        if not self._get_devices or not self._mark_offline:
            return

        now = time.time()
        devices = self._get_devices()

        for server_name, device in list(devices.items()):
            if not device.online:
                continue
            if device.last_seen is None:
                continue
            age = now - device.last_seen
            if age > OFFLINE_THRESHOLD_SECONDS:
                logger.info(f"Device offline (no heartbeat for {age:.1f}s): {server_name}")
                self._mark_offline(server_name)
