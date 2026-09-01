from __future__ import annotations

import asyncio
import logging
import socket
import time
from typing import Optional

logger = logging.getLogger("netaudio")

import typer

from netaudio.daemon.client import get_device_summaries_from_daemon, shutdown_daemon
from netaudio.daemon import service_install

from netaudio.icons import icon

app = typer.Typer(help="Manage the netaudio daemon.", no_args_is_help=True)


def _port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _effective_daemon_port(daemon_port):
    from netaudio.common.app_config import settings as app_settings

    return daemon_port or app_settings.daemon_port


def _pin_client_port(effective_port):
    from netaudio.common.app_config import settings as app_settings

    app_settings.daemon_port = effective_port


def _wait_for_shutdown(daemon_port, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _port_in_use(daemon_port):
            return True
        time.sleep(0.25)
    return False


def _wait_for_startup(daemon_port, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _port_in_use(daemon_port):
            return True
        time.sleep(0.25)
    return False


def _run_foreground(daemon_port):
    from netaudio.common.app_config import settings as app_settings
    from netaudio.cli import state
    from netaudio.daemon.server import run_daemon

    effective_port = _effective_daemon_port(daemon_port)
    app_settings.daemon_port = effective_port
    asyncio.run(run_daemon(dissect=state.dissect, capture=state.capture, daemon_port=effective_port))


def _uses_configured_port(effective_port) -> bool:
    return effective_port == _effective_daemon_port(None)


def _service_active() -> bool:
    platform = service_install.platform_name()
    if platform == "systemd":
        try:
            return asyncio.run(service_install.systemd_unit_active())
        except Exception as exception:
            logger.debug(f"systemd state query failed: {exception}")
            return False
    if platform == "launchd":
        return service_install.launchd_loaded()
    if platform == "taskscheduler":
        return service_install.windows_task_enabled()
    return False


@app.command()
def run(
    daemon_port: Optional[int] = typer.Option(
        None, "--port", help="Daemon HTTP API port.", envvar="NETAUDIO_DAEMON_PORT"
    ),
):
    """Run the daemon in the foreground (Ctrl-C to stop)."""
    _run_foreground(daemon_port)


@app.command()
def start(
    daemon_port: Optional[int] = typer.Option(
        None, "--port", help="Daemon HTTP API port.", envvar="NETAUDIO_DAEMON_PORT"
    ),
):
    """Start the daemon (via the boot service if installed, otherwise in the background)."""
    if service_install.running_under_systemd():
        _run_foreground(daemon_port)
        return

    effective_port = _effective_daemon_port(daemon_port)
    on_configured_port = _uses_configured_port(effective_port)
    _pin_client_port(effective_port)
    if _port_in_use(effective_port):
        typer.echo(f"{icon('online')}Daemon is already running.")
        return

    platform = service_install.platform_name()
    use_service = service_install.is_installed() and on_configured_port
    if use_service and platform == "systemd":
        asyncio.run(service_install.systemd_start())
        if not _wait_for_startup(effective_port):
            typer.echo("Daemon service started but the daemon port never opened.", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"{icon('online')}Daemon started (systemd).")
        return

    if use_service and platform == "launchd":
        if service_install.launchd_loaded():
            service_install.launchd_kickstart()
        else:
            service_install.launchd_bootstrap()
        if not _wait_for_startup(effective_port):
            typer.echo("Daemon service started but the daemon port never opened.", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"{icon('online')}Daemon started (launchd).")
        return

    if use_service and platform == "taskscheduler":
        service_install.windows_task_run()
        if not _wait_for_startup(effective_port):
            typer.echo("Daemon task started but the daemon port never opened.", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"{icon('online')}Daemon started (Task Scheduler).")
        return

    log_path = service_install.spawn_detached(daemon_port)
    if not _wait_for_startup(effective_port):
        typer.echo(f"Daemon did not come up. Check logs: {log_path}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"{icon('online')}Daemon started in the background. Logs: {log_path}")


@app.command()
def stop(
    daemon_port: Optional[int] = typer.Option(
        None, "--port", help="Daemon HTTP API port.", envvar="NETAUDIO_DAEMON_PORT"
    ),
):
    """Stop the daemon."""
    effective_port = _effective_daemon_port(daemon_port)
    on_configured_port = _uses_configured_port(effective_port)
    _pin_client_port(effective_port)

    if not _port_in_use(effective_port):
        typer.echo(f"{icon('offline')}Daemon is not running.")
        return

    if service_install.platform_name() == "systemd" and on_configured_port and _service_active():
        asyncio.run(service_install.systemd_stop())
        if not _wait_for_shutdown(effective_port):
            typer.echo("Timed out waiting for daemon to stop.", err=True)
            raise typer.Exit(code=1)
        return

    if not asyncio.run(shutdown_daemon()):
        typer.echo("Error stopping daemon: daemon did not acknowledge shutdown.", err=True)
        raise typer.Exit(code=1)
    if not _wait_for_shutdown(effective_port):
        typer.echo("Timed out waiting for daemon to stop.", err=True)
        raise typer.Exit(code=1)


@app.command()
def restart(
    daemon_port: Optional[int] = typer.Option(
        None, "--port", help="Daemon HTTP API port.", envvar="NETAUDIO_DAEMON_PORT"
    ),
):
    """Restart the daemon."""
    effective_port = _effective_daemon_port(daemon_port)
    on_configured_port = _uses_configured_port(effective_port)
    _pin_client_port(effective_port)

    if service_install.platform_name() == "systemd" and on_configured_port and _service_active():
        asyncio.run(service_install.systemd_restart())
        if not _wait_for_startup(effective_port):
            typer.echo("Daemon service restarted but the daemon port never opened.", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"{icon('online')}Daemon restarted (systemd).")
        return

    if _port_in_use(effective_port):
        asyncio.run(shutdown_daemon())
        if not _wait_for_shutdown(effective_port):
            typer.echo("Timed out waiting for daemon to stop.", err=True)
            raise typer.Exit(code=1)

    start(daemon_port=daemon_port)


@app.command()
def status(
    daemon_port: Optional[int] = typer.Option(
        None, "--port", help="Daemon HTTP API port.", envvar="NETAUDIO_DAEMON_PORT"
    ),
):
    """Show daemon status."""
    effective_port = _effective_daemon_port(daemon_port)
    _pin_client_port(effective_port)

    platform = service_install.platform_name()
    if service_install.is_installed():
        managed = "netaudio-managed" if service_install.is_managed_by_netaudio() else "user-managed"
        active = "active" if _service_active() else "inactive"
        typer.echo(f"Boot service: installed ({platform}, {managed}, {active}) at {service_install.service_location()}")
    else:
        typer.echo("Boot service: not installed (install with: netaudio daemon install)")

    if not _port_in_use(effective_port):
        typer.echo(f"{icon('offline')}Daemon is not running.")
        raise typer.Exit(code=1)

    devices = asyncio.run(get_device_summaries_from_daemon())
    if devices is None:
        typer.echo("Daemon port is open but the daemon is not responding.")
        raise typer.Exit(code=1)

    typer.echo(f"{icon('online')}Daemon is running. {len(devices)} device(s) cached.")


@app.command()
def install(
    start_service: bool = typer.Option(True, "--start/--no-start", help="Start the service after installing."),
    force: bool = typer.Option(False, "--force", help="Overwrite an existing service file."),
    print_only: bool = typer.Option(False, "--print", help="Print the generated service file without writing it."),
):
    """Install the daemon as a boot service (systemd on Linux, launchd on macOS, Task Scheduler on Windows)."""
    import sys

    platform = service_install.platform_name()
    if platform == "unsupported":
        typer.echo(f"No supported service manager on this platform ({sys.platform}).", err=True)
        raise typer.Exit(code=1)

    if platform == "taskscheduler":
        if print_only:
            typer.echo(service_install.windows_task_xml())
            return
        if service_install.windows_task_installed() and not force:
            if service_install.windows_task_managed():
                typer.echo(f"Task {service_install.WINDOWS_TASK_NAME} already registered. Use --force to rewrite it.")
            else:
                typer.echo(
                    f"A task named {service_install.WINDOWS_TASK_NAME} already exists and was not created by netaudio. Use --force to overwrite it.",
                    err=True,
                )
                raise typer.Exit(code=1)
            return
        service_install.windows_task_register()
        typer.echo(f"Registered Task Scheduler task {service_install.WINDOWS_TASK_NAME} (runs at logon).")
        if start_service:
            start(daemon_port=None)
        return

    content = service_install.generate_service_file()
    path = service_install.service_file_path()

    if print_only:
        typer.echo(content)
        return

    if path.exists() and not force:
        if service_install.is_managed_by_netaudio():
            typer.echo(f"Service already installed at {path}. Use --force to rewrite it.")
        else:
            typer.echo(
                f"A service file already exists at {path} and was not created by netaudio. Use --force to overwrite it.",
                err=True,
            )
            raise typer.Exit(code=1)
        return

    launchd_was_loaded = platform == "launchd" and service_install.launchd_loaded()

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    typer.echo(f"Wrote {path}")

    if platform == "systemd":
        asyncio.run(service_install.systemd_enable(start_service))
        typer.echo(f"Enabled {service_install.SYSTEMD_UNIT_NAME}" + (" and started it." if start_service else "."))
        return

    if launchd_was_loaded:
        result = service_install.launchd_bootout()
        if result.returncode != 0:
            typer.echo(f"launchctl bootout failed: {result.stderr.strip()}", err=True)
            raise typer.Exit(code=1)

    if not start_service:
        typer.echo(f"Installed {service_install.LAUNCHD_LABEL} without loading it.")
        return

    result = service_install.launchd_bootstrap()
    if result.returncode != 0:
        typer.echo(f"launchctl bootstrap failed: {result.stderr.strip()}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"Loaded {service_install.LAUNCHD_LABEL} into launchd.")


@app.command()
def uninstall(
    force: bool = typer.Option(False, "--force", help="Remove the service file even if netaudio did not create it."),
):
    """Remove the boot service."""
    platform = service_install.platform_name()

    if platform == "taskscheduler":
        if not service_install.windows_task_installed():
            typer.echo("No boot service installed.")
            return
        if not service_install.windows_task_managed() and not force:
            typer.echo(
                f"Task {service_install.WINDOWS_TASK_NAME} was not created by netaudio. Use --force to remove it anyway.",
                err=True,
            )
            raise typer.Exit(code=1)
        service_install.windows_task_delete()
        typer.echo(f"Removed Task Scheduler task {service_install.WINDOWS_TASK_NAME}")
        return

    path = service_install.service_file_path()

    if not path.exists():
        typer.echo("No boot service installed.")
        return

    if not service_install.is_managed_by_netaudio() and not force:
        typer.echo(f"{path} was not created by netaudio. Use --force to remove it anyway.", err=True)
        raise typer.Exit(code=1)

    if platform == "systemd":
        asyncio.run(service_install.systemd_disable_and_stop())
    elif platform == "launchd" and service_install.launchd_loaded():
        service_install.launchd_bootout()

    path.unlink()
    typer.echo(f"Removed {path}")


@app.command()
def logs(
    follow: bool = typer.Option(False, "--follow", "-f", help="Follow new log output."),
    lines: int = typer.Option(50, "--lines", "-n", help="Number of recent lines to show."),
):
    """Show daemon logs."""
    import subprocess

    if service_install.platform_name() == "systemd" and service_install.is_installed():
        command = ["journalctl", "--user", "-u", service_install.SYSTEMD_UNIT_NAME, "-n", str(lines), "--no-pager"]
        if follow:
            command.append("-f")
        raise typer.Exit(code=subprocess.run(command).returncode)

    log_path = service_install.spawn_log_path()
    if not log_path.exists():
        typer.echo(f"No log file at {log_path}.", err=True)
        raise typer.Exit(code=1)

    content = log_path.read_text().splitlines()
    for line in content[-lines:]:
        typer.echo(line)

    if follow:
        with open(log_path, "r") as log_file:
            log_file.seek(0, 2)
            try:
                while True:
                    line = log_file.readline()
                    if line:
                        typer.echo(line.rstrip("\n"))
                    else:
                        time.sleep(0.25)
            except KeyboardInterrupt:
                raise typer.Exit(code=0)
