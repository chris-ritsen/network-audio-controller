from __future__ import annotations

import os
import socket


def notify_systemd(state):
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return

    notification_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        if address[0] == "@":
            address = "\0" + address[1:]
        notification_socket.sendto(state.encode(), address)
    finally:
        notification_socket.close()
