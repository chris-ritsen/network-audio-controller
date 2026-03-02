import os
import sys
from pathlib import Path


def get_runtime_dir() -> Path:
    if sys.platform == "win32":
        return Path(os.environ.get("TEMP", os.environ.get("TMP", "C:\\Temp")))

    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg:
        return Path(xdg)

    tmpdir = os.environ.get("TMPDIR")
    if tmpdir:
        return Path(tmpdir)

    return Path("/tmp")


def get_socket_dir() -> Path:
    from netaudio_lib.common.app_config import settings as app_settings

    if app_settings.socket_path:
        return Path(app_settings.socket_path).parent

    runtime_dir = get_runtime_dir()
    socket_dir = runtime_dir / "netaudio"
    return socket_dir


def get_socket_path() -> Path:
    from netaudio_lib.common.app_config import settings as app_settings

    if app_settings.socket_path:
        return Path(app_settings.socket_path)

    return get_socket_dir() / "netaudio.sock"


def ensure_socket_dir() -> Path:
    socket_dir = get_socket_dir()
    socket_dir.mkdir(parents=True, exist_ok=True)
    return socket_dir
