import asyncio
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger("netaudio")

NAMED_PIPE_ADDRESS = r"\\.\pipe\netaudio"


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
    from netaudio.common.app_config import settings as app_settings

    if app_settings.socket_path:
        return Path(app_settings.socket_path).parent

    runtime_dir = get_runtime_dir()
    socket_dir = runtime_dir / "netaudio"
    return socket_dir


def get_socket_path() -> Path:
    from netaudio.common.app_config import settings as app_settings

    if app_settings.socket_path:
        return Path(app_settings.socket_path)

    return get_socket_dir() / "netaudio.sock"


def ensure_socket_dir() -> Path:
    socket_dir = get_socket_dir()
    socket_dir.mkdir(parents=True, exist_ok=True)
    return socket_dir


class DaemonAlreadyRunningError(Exception):
    pass


async def _check_existing_daemon(socket_path: Path) -> bool:
    if not socket_path.exists():
        return False

    try:
        reader, writer = await asyncio.open_unix_connection(str(socket_path))
        writer.close()
        await writer.wait_closed()
        return True
    except (ConnectionRefusedError, FileNotFoundError, OSError):
        return False


async def start_daemon_server(handle_client):
    if is_windows():
        loop = asyncio.get_running_loop()

        def protocol_factory():
            reader = asyncio.StreamReader()
            return asyncio.StreamReaderProtocol(reader, handle_client)

        pipe_servers = await loop.start_serving_pipe(protocol_factory, NAMED_PIPE_ADDRESS)
        return pipe_servers[0]

    socket_path = get_socket_path()

    if await _check_existing_daemon(socket_path):
        raise DaemonAlreadyRunningError(f"Another daemon is already listening on {socket_path}")

    if socket_path.exists():
        socket_path.unlink()

    ensure_socket_dir()
    server = await asyncio.start_unix_server(handle_client, path=str(socket_path))
    os.chmod(socket_path, 0o600)
    return server


async def open_daemon_connection():
    if is_windows():
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        transport, _ = await loop.create_pipe_connection(lambda: protocol, NAMED_PIPE_ADDRESS)
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        return reader, writer

    return await asyncio.open_unix_connection(str(get_socket_path()))


def daemon_is_accessible() -> bool:
    if is_windows():
        return os.path.exists(NAMED_PIPE_ADDRESS)

    return get_socket_path().exists()


def cleanup_daemon_socket():
    if is_windows():
        return

    socket_path = get_socket_path()
    if socket_path.exists():
        try:
            socket_path.unlink()
        except Exception as exception:
            logger.debug(f"Failed to remove socket file {socket_path}: {exception}")
