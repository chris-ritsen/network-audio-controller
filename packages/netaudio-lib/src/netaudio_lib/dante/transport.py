import asyncio
import logging
import struct
from typing import Callable

logger = logging.getLogger("netaudio")


class DanteUnicastProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport: asyncio.DatagramTransport | None = None
        self._pending: dict[tuple[str, int], asyncio.Future] = {}

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        remote_ip = addr[0]
        transaction_id = self._extract_transaction_id(data)

        if transaction_id is not None:
            key = (remote_ip, transaction_id)
            future = self._pending.get(key)
            if future is not None and not future.done():
                future.set_result(data)
                return

        logger.debug(
            f"Unmatched unicast packet from {remote_ip}, "
            f"transaction_id={transaction_id}, length={len(data)}"
        )

    def error_received(self, exc: Exception) -> None:
        logger.debug(f"Unicast protocol error: {exc}")

    def connection_lost(self, exc: Exception | None) -> None:
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()

    async def send_and_expect(
        self,
        data: bytes,
        remote_addr: tuple[str, int],
        transaction_id: int,
        timeout: float = 0.5,
    ) -> bytes | None:
        if self.transport is None:
            return None

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        key = (remote_addr[0], transaction_id)
        self._pending[key] = future

        try:
            self.transport.sendto(data, remote_addr)
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            logger.debug(
                f"Timeout waiting for response from {remote_addr[0]}:{remote_addr[1]}, "
                f"transaction_id=0x{transaction_id:04X}"
            )
            return None
        except Exception as exception:
            logger.debug(f"Error in send_and_expect: {exception}")
            return None
        finally:
            self._pending.pop(key, None)

    def send_fire_and_forget(self, data: bytes, remote_addr: tuple[str, int]) -> None:
        if self.transport is not None:
            self.transport.sendto(data, remote_addr)

    @staticmethod
    def _extract_transaction_id(data: bytes) -> int | None:
        if len(data) < 6:
            return None
        return struct.unpack(">H", data[4:6])[0]

    def close(self) -> None:
        if self.transport is not None:
            self.transport.close()
            self.transport = None


class DanteMulticastProtocol(asyncio.DatagramProtocol):
    def __init__(self, callback: Callable[[bytes, tuple[str, int]], None]):
        self.transport: asyncio.DatagramTransport | None = None
        self._callback = callback

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        try:
            self._callback(data, addr)
        except Exception:
            logger.exception("Error in multicast callback")

    def error_received(self, exc: Exception) -> None:
        logger.debug(f"Multicast protocol error: {exc}")

    def connection_lost(self, exc: Exception | None) -> None:
        pass

    def close(self) -> None:
        if self.transport is not None:
            self.transport.close()
            self.transport = None
