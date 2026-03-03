import asyncio
import os
import sys
from pathlib import Path

DAEMON_TCP_HOST = "127.0.0.1"
DAEMON_TCP_PORT = 19199


def is_windows():
    return sys.platform == "win32"


def get_runtime_dir() -> Path:
    if is_windows():
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


async def start_daemon_server(handle_client):
    if is_windows():
        return await asyncio.start_server(handle_client, DAEMON_TCP_HOST, DAEMON_TCP_PORT)

    socket_path = get_socket_path()
    if socket_path.exists():
        socket_path.unlink()
    ensure_socket_dir()
    server = await asyncio.start_unix_server(handle_client, path=str(socket_path))
    os.chmod(socket_path, 0o600)
    return server


async def open_daemon_connection():
    if is_windows():
        return await asyncio.open_connection(DAEMON_TCP_HOST, DAEMON_TCP_PORT)

    return await asyncio.open_unix_connection(str(get_socket_path()))


def daemon_is_accessible() -> bool:
    if is_windows():
        import socket
        try:
            with socket.create_connection((DAEMON_TCP_HOST, DAEMON_TCP_PORT), timeout=0.5):
                return True
        except (ConnectionRefusedError, OSError):
            return False

    return get_socket_path().exists()


def cleanup_daemon_socket():
    if is_windows():
        return

    socket_path = get_socket_path()
    if socket_path.exists():
        try:
            socket_path.unlink()
        except Exception:
            pass
