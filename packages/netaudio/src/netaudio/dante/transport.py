from __future__ import annotations

import asyncio
import logging
from typing import Callable

logger = logging.getLogger("netaudio")


class DanteMulticastProtocol(asyncio.DatagramProtocol):
    def __init__(self, callback: Callable[[bytes, tuple[str, int]], None]):
        self._callback = callback

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        logger.log(5, f"Multicast datagram from {addr[0]}:{addr[1]}, {len(data)} bytes")
        try:
            self._callback(data, addr)
        except Exception:
            logger.exception("Error in multicast callback")

    def error_received(self, exc: Exception) -> None:
        logger.debug(f"Multicast protocol error: {exc}")
