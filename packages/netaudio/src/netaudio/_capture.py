import logging
import sqlite3
import struct
from collections import OrderedDict

from netaudio.core.binding import NetaudioCoreError
from netaudio.dante.packet_store import PacketRecord

logger = logging.getLogger("netaudio")


class PacketDissector:
    def __init__(self):
        self.requests = OrderedDict()

    def __call__(self, payload, device_ip, port, direction):
        from netaudio.dante.dissection.header import parse_packet_header

        request = None
        header = parse_packet_header(payload)

        if header is not None and header["transaction_id"] is not None:
            key = (device_ip, port, header["protocol_id"], header["opcode"], header["transaction_id"])

            if direction == "request":
                self.requests[key] = payload
                self.requests.move_to_end(key)

                if len(self.requests) > 256:
                    self.requests.popitem(last=False)
            elif direction == "response":
                request = self.requests.pop(key, None)

        _dissect(payload, device_ip, port, direction, request=request)


def open_capture_session():
    from netaudio.cli import state

    if not state.capture:
        return None, None

    from netaudio.common.config_loader import load_capture_profile, resolve_db_from_config
    from netaudio.dante.packet_store import PacketStore

    profile_cfg, _ = load_capture_profile(None, None)
    db_path = resolve_db_from_config(None, profile_cfg)
    store = PacketStore(db_path=db_path)
    active = store.get_latest_session(active_only=True)
    return store, (active["id"] if active else None)


def _record(store, session_id, payload, device_ip, port, direction, source_type):
    try:
        store.store_packet(
            PacketRecord(
                payload=payload,
                source_type=source_type,
                device_ip=device_ip,
                dst_ip=device_ip if direction == "request" else None,
                dst_port=port if direction == "request" else None,
                src_ip=device_ip if direction == "response" else None,
                src_port=port if direction == "response" else None,
                direction=direction,
                session_id=session_id,
            )
        )
    except (OSError, ValueError, sqlite3.Error) as exception:
        logger.warning(f"PacketStore error ({direction}): {exception}", exc_info=True)


def _dissect(payload, device_ip, port, direction, request=None):
    try:
        from netaudio.common.app_config import settings
        from netaudio.dante.dissection.rendering import dissect_and_render, format_dissect_label

        color = not settings.no_color
        label = format_dissect_label(direction, f"{device_ip}:{port}", color=color)
        rendered = dissect_and_render(payload, indent="  ", color=color, direction=direction, request=request)
        logger.info(f"Dissect [{label}] {len(payload)}B:\n{rendered}")
    except (LookupError, ValueError, NetaudioCoreError, struct.error) as exception:
        logger.warning(f"Dissect error: {exception}", exc_info=True)
