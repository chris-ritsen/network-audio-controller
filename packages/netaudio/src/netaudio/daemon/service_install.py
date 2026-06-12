from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

SYSTEMD_UNIT_NAME = "netaudio.service"
LAUNCHD_LABEL = "com.netaudio.daemon"
WINDOWS_TASK_NAME = "netaudio-daemon"
MANAGED_MARKER = "X-NetaudioManaged"


def platform_name() -> str:
    if sys.platform == "darwin":
        return "launchd"
    if sys.platform.startswith("linux"):
        return "systemd"
    if sys.platform == "win32":
        return "taskscheduler"
    return "unsupported"


def executable_path() -> str:
    found = shutil.which("netaudio")
    if found:
        return str(Path(found).absolute())
    return str(Path(sys.argv[0]).absolute())


def systemd_unit_path() -> Path:
    base = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return base / "systemd" / "user" / SYSTEMD_UNIT_NAME


def launchd_plist_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"


def service_file_path() -> Path:
    if platform_name() == "launchd":
        return launchd_plist_path()
    return systemd_unit_path()


def service_location() -> str:
    if platform_name() == "taskscheduler":
        return f"Task Scheduler task {WINDOWS_TASK_NAME}"
    return str(service_file_path())


def spawn_log_path() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Logs" / "netaudio" / "daemon.log"
    if sys.platform == "win32":
        base = Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
        return base / "netaudio" / "daemon.log"
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
    if platform_name() == "taskscheduler":
        return windows_task_installed()
    return service_file_path().exists()


def is_managed_by_netaudio() -> bool:
    if platform_name() == "taskscheduler":
        return windows_task_managed()
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


TASK_TRIGGER_LOGON = 9
TASK_ACTION_EXEC = 0
TASK_CREATE_OR_UPDATE = 6
TASK_LOGON_INTERACTIVE_TOKEN = 3


def _task_scheduler():
    import win32com.client

    scheduler = win32com.client.Dispatch("Schedule.Service")
    scheduler.Connect()
    return scheduler


def _windows_get_task(scheduler):
    import pywintypes

    try:
        return scheduler.GetFolder("\\").GetTask(WINDOWS_TASK_NAME)
    except pywintypes.com_error:
        return None


def _windows_build_definition(scheduler):
    definition = scheduler.NewTask(0)
    definition.RegistrationInfo.Description = (
        f"netaudio daemon - network audio device discovery and control [{MANAGED_MARKER}]"
    )
    definition.Triggers.Create(TASK_TRIGGER_LOGON)
    action = definition.Actions.Create(TASK_ACTION_EXEC)
    action.Path = executable_path()
    action.Arguments = "daemon start"
    definition.Settings.DisallowStartIfOnBatteries = False
    definition.Settings.StopIfGoingOnBatteries = False
    definition.Settings.StartWhenAvailable = True
    definition.Settings.ExecutionTimeLimit = "PT5M"
    return definition


def windows_task_xml() -> str:
    return _windows_build_definition(_task_scheduler()).XmlText


def windows_task_installed() -> bool:
    return _windows_get_task(_task_scheduler()) is not None


def windows_task_managed() -> bool:
    task = _windows_get_task(_task_scheduler())
    if task is None:
        return False
    return MANAGED_MARKER in (task.Definition.RegistrationInfo.Description or "")


def windows_task_enabled() -> bool:
    task = _windows_get_task(_task_scheduler())
    return task is not None and bool(task.Enabled)


def windows_task_register() -> None:
    scheduler = _task_scheduler()
    definition = _windows_build_definition(scheduler)
    scheduler.GetFolder("\\").RegisterTaskDefinition(
        WINDOWS_TASK_NAME, definition, TASK_CREATE_OR_UPDATE, None, None, TASK_LOGON_INTERACTIVE_TOKEN
    )


def windows_task_delete() -> None:
    _task_scheduler().GetFolder("\\").DeleteTask(WINDOWS_TASK_NAME, 0)


def windows_task_run() -> None:
    task = _windows_get_task(_task_scheduler())
    if task is None:
        raise RuntimeError(f"Task Scheduler task {WINDOWS_TASK_NAME} is not installed")
    task.Run("")


def spawn_detached(relay_port: int | None) -> Path:
    log_path = spawn_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    command = [executable_path(), "daemon", "run"]
    if relay_port:
        command.extend(["--relay-port", str(relay_port)])
    options = {}
    if sys.platform == "win32":
        options["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        options["start_new_session"] = True
    with open(log_path, "ab") as log_file:
        subprocess.Popen(
            command,
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.DEVNULL,
            **options,
        )
    return log_path
