import asyncio
import logging

from netaudio import core

logger = logging.getLogger("netaudio")


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


class CaptureObserver:
    def __init__(self, store, session_id, dissect):
        self.store = store
        self.session_id = session_id
        self.dissect = dissect
        self.buffer = []

    def __call__(self, packet, response, device_ip, port):
        self.buffer.append((packet, device_ip, port, "request", "netaudio_request"))
        if self.dissect:
            _dissect(packet, device_ip, port, "request")
        if response is not None:
            self.buffer.append((response, device_ip, port, "response", "netaudio_response"))
            if self.dissect:
                _dissect(response, device_ip, port, "response")

    def flush(self):
        if not self.store:
            self.buffer.clear()
            return
        for payload, device_ip, port, direction, source_type in self.buffer:
            _record(self.store, self.session_id, payload, device_ip, port, direction, source_type)
        self.buffer.clear()


def make_observer(store, session_id, dissect):
    return CaptureObserver(store, session_id, dissect)


def _record(store, session_id, payload, device_ip, port, direction, source_type):
    try:
        store.store_packet(
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
    except Exception as exception:
        logger.debug(f"PacketStore error ({direction}): {exception}")


def _dissect(payload, device_ip, port, direction):
    try:
        from netaudio.common.app_config import settings
        from netaudio.dante.packet_dissector import dissect_and_render, format_dissect_label

        color = not settings.no_color
        label = format_dissect_label(direction, f"{device_ip}:{port}", color=color)
        rendered = dissect_and_render(payload, indent="  ", color=color)
        logger.info(f"Dissect [{label}] {len(payload)}B:\n{rendered}")
    except Exception as exception:
        logger.debug(f"Dissect error: {exception}")


def _query(client, spec, port, parse_kind=None, starting_channel=None):
    packet = core.build_command(spec)
    response = client.request(packet, port)
    if parse_kind is None:
        return response
    if not response:
        return None
    if starting_channel is not None:
        return core.parse_page(parse_kind, response, starting_channel)
    return core.parse_response(parse_kind, response)


def fetch_device_name(client, arc_port):
    return _query(client, {"command": "device_name"}, arc_port, "device_name")


def fetch_rx_records(client, arc_port):
    rx = []
    page = 0
    while True:
        records = _query(client, {"command": "receivers", "page": page}, arc_port, "rx", page * 16 + 1) or []
        rx.extend(records)
        if len(records) < 16:
            break
        page += 1
    return rx


def fetch_tx_records(client, arc_port, channel_count):
    friendly = {}
    if channel_count > 0:
        friendly_records = (
            _query(
                client,
                {"command": "transmitter_names", "channel_count": channel_count},
                arc_port,
                "tx_friendly",
                1,
            )
            or []
        )
        for number, friendly_name in friendly_records:
            if friendly_name:
                friendly[number] = friendly_name

    tx = []
    page = 0
    while True:
        records = (
            _query(
                client,
                {"command": "transmitters", "page": page},
                arc_port,
                "tx_info",
                page * 32 + 1,
            )
            or []
        )
        for record in records:
            record["friendly_name"] = friendly.get(record["number"])
        tx.extend(records)
        if len(records) < 32:
            break
        page += 1
    return tx


def _fetch_instrumented(client, arc_port, include_channels=True):
    name = fetch_device_name(client, arc_port)
    counts = _query(client, {"command": "channel_count"}, arc_port, "channel_count")
    if counts is None:
        counts = {"tx_count": 0, "rx_count": 0, "locked": None}

    rx = fetch_rx_records(client, arc_port) if include_channels else []
    tx = fetch_tx_records(client, arc_port, counts["tx_count"]) if include_channels else []
    channel_audio_metadata = client.get_channel_audio_metadata(counts["tx_count"], counts["rx_count"])

    settings_data = _query(client, {"command": "device_settings"}, arc_port, "device_settings")
    latency_config_response = _query(client, {"command": "query_latency_config"}, arc_port)
    aes67 = None
    aes67_multicast_prefix = None
    if latency_config_response:
        aes67 = core.parse_response("aes67_configured", latency_config_response)
        latency_settings = core.parse_response("device_settings", latency_config_response)
        if isinstance(latency_settings, dict):
            aes67_multicast_prefix = latency_settings.get("aes67_multicast_prefix")

    return {
        "name": name,
        "counts": (counts["tx_count"], counts["rx_count"], counts["locked"]),
        "rx": rx,
        "tx": tx,
        "channel_audio_metadata": channel_audio_metadata,
        "settings": settings_data,
        "aes67": aes67,
        "aes67_multicast_prefix": aes67_multicast_prefix,
    }


async def populate_instrumented(device, observer):
    arc_port = device._arc_port()
    client = core.CoreClient(str(device.ipv4), arc_port=arc_port)
    client.observer = observer
    mac = core.host_mac()
    if mac:
        client.set_host_mac(mac)
    try:
        data = await asyncio.to_thread(_fetch_instrumented, client, arc_port)
        device.apply_controls(device.controls_data_from_core(data))
    finally:
        client.close()
