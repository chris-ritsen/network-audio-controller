from __future__ import annotations

import logging
import os
import socket

logger = logging.getLogger("netaudio")

_unusable_socket: str | None = None


def notify_systemd(state):
    global _unusable_socket
    configured = os.environ.get("NOTIFY_SOCKET")
    if not configured or configured == _unusable_socket:
        return

    address = "\0" + configured[1:] if configured[0] == "@" else configured
    notification_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        notification_socket.sendto(state.encode(), address)
    except OSError as error:
        _unusable_socket = configured
        logger.warning(f"Cannot send service notifications to {configured}; continuing without them: {error}")
    finally:
        notification_socket.close()
