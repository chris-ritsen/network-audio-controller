from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

SYSTEMD_UNIT_NAME = "netaudio.service"
LAUNCHD_LABEL = "com.netaudio.daemon"
MANAGED_MARKER = "X-NetaudioManaged"


def platform_name() -> str:
    if sys.platform == "darwin":
        return "launchd"
    if sys.platform.startswith("linux"):
        return "systemd"
    return "unsupported"


def executable_path() -> str:
    found = shutil.which("netaudio")
    if found:
        return str(Path(found).resolve())
    return str(Path(sys.argv[0]).resolve())


def systemd_unit_path() -> Path:
    base = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return base / "systemd" / "user" / SYSTEMD_UNIT_NAME


def launchd_plist_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"


def service_file_path() -> Path:
    if platform_name() == "launchd":
        return launchd_plist_path()
    return systemd_unit_path()


def spawn_log_path() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Logs" / "netaudio" / "daemon.log"
    base = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state")
    return base / "netaudio" / "daemon.log"


def generate_systemd_unit(executable: str) -> str:
    return (
        "[Unit]\n"
        "Description=netaudio daemon - network audio device discovery and control\n"
        f"{MANAGED_MARKER}=true\n"
        "After=network.target\n"
        "\n"
        "[Service]\n"
        "Type=notify\n"
        f"ExecStart={executable} daemon run\n"
        "Restart=on-failure\n"
        "RestartSec=5\n"
        "\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    )


def generate_launchd_plist(executable: str) -> str:
    log_path = str(spawn_log_path())
    payload = {
        "Label": LAUNCHD_LABEL,
        "ProgramArguments": [executable, "daemon", "run"],
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "StandardOutPath": log_path,
        "StandardErrorPath": log_path,
    }
    return plistlib.dumps(payload, sort_keys=True).decode("utf-8")


def generate_service_file() -> str:
    if platform_name() == "launchd":
        return generate_launchd_plist(executable_path())
    return generate_systemd_unit(executable_path())


def is_installed() -> bool:
    return service_file_path().exists()


def is_managed_by_netaudio() -> bool:
    path = service_file_path()
    if not path.exists():
        return False
    if platform_name() == "launchd":
        return True
    return MANAGED_MARKER in path.read_text()


def running_under_systemd() -> bool:
    return bool(os.environ.get("NOTIFY_SOCKET"))


async def _systemd_manager():
    from dbus_fast import BusType
    from dbus_fast.aio import MessageBus

    bus = await MessageBus(bus_type=BusType.SESSION).connect()
    introspection = await bus.introspect("org.freedesktop.systemd1", "/org/freedesktop/systemd1")
    proxy = bus.get_proxy_object("org.freedesktop.systemd1", "/org/freedesktop/systemd1", introspection)
    return bus, proxy.get_interface("org.freedesktop.systemd1.Manager")


async def systemd_daemon_reload() -> None:
    bus, manager = await _systemd_manager()
    try:
        await manager.call_reload()
    finally:
        bus.disconnect()


async def systemd_enable(start: bool) -> None:
    bus, manager = await _systemd_manager()
    try:
        await manager.call_reload()
        await manager.call_enable_unit_files([SYSTEMD_UNIT_NAME], False, True)
        if start:
            await manager.call_start_unit(SYSTEMD_UNIT_NAME, "replace")
    finally:
        bus.disconnect()


async def systemd_disable_and_stop() -> None:
    bus, manager = await _systemd_manager()
    try:
        if await _systemd_unit_active(manager):
            await manager.call_stop_unit(SYSTEMD_UNIT_NAME, "replace")
        await manager.call_disable_unit_files([SYSTEMD_UNIT_NAME], False)
        await manager.call_reload()
    finally:
        bus.disconnect()


async def _systemd_unit_active(manager) -> bool:
    units = await manager.call_list_units_by_names([SYSTEMD_UNIT_NAME])
    for unit in units:
        if unit[3] == "active" or unit[3] == "activating":
            return True
    return False


async def systemd_unit_active() -> bool:
    bus, manager = await _systemd_manager()
    try:
        return await _systemd_unit_active(manager)
    finally:
        bus.disconnect()


async def systemd_start() -> None:
    bus, manager = await _systemd_manager()
    try:
        await manager.call_start_unit(SYSTEMD_UNIT_NAME, "replace")
    finally:
        bus.disconnect()


async def systemd_stop() -> None:
    bus, manager = await _systemd_manager()
    try:
        await manager.call_stop_unit(SYSTEMD_UNIT_NAME, "replace")
    finally:
        bus.disconnect()


async def systemd_restart() -> None:
    bus, manager = await _systemd_manager()
    try:
        await manager.call_restart_unit(SYSTEMD_UNIT_NAME, "replace")
    finally:
        bus.disconnect()


def _launchd_domain() -> str:
    return f"gui/{os.getuid()}"


def launchd_bootstrap() -> subprocess.CompletedProcess:
    return subprocess.run(
        ["launchctl", "bootstrap", _launchd_domain(), str(launchd_plist_path())],
        capture_output=True,
        text=True,
    )


def launchd_bootout() -> subprocess.CompletedProcess:
    return subprocess.run(
        ["launchctl", "bootout", f"{_launchd_domain()}/{LAUNCHD_LABEL}"],
        capture_output=True,
        text=True,
    )


def launchd_kickstart() -> subprocess.CompletedProcess:
    return subprocess.run(
        ["launchctl", "kickstart", "-k", f"{_launchd_domain()}/{LAUNCHD_LABEL}"],
        capture_output=True,
        text=True,
    )


def launchd_loaded() -> bool:
    result = subprocess.run(
        ["launchctl", "print", f"{_launchd_domain()}/{LAUNCHD_LABEL}"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def spawn_detached(relay_port: int | None) -> Path:
    log_path = spawn_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    command = [executable_path(), "daemon", "run"]
    if relay_port:
        command.extend(["--relay-port", str(relay_port)])
    with open(log_path, "ab") as log_file:
        subprocess.Popen(
            command,
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    return log_path
