from __future__ import annotations

import datetime

from netaudio.dante.packet_store import PacketStore


def _default_session_name() -> str:
    return datetime.datetime.now().strftime("session_%Y%m%d_%H%M%S")


def open_packet_store(
    config: str | None = None,
    profile: str | None = None,
    db: str | None = None,
) -> PacketStore:
    from netaudio.common.config_loader import load_capture_profile, resolve_db_from_config

    profile_config, _ = load_capture_profile(config, profile)
    db_path = resolve_db_from_config(db, profile_config)
    return PacketStore(db_path=db_path)
