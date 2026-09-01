from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager
from typing import Any, NoReturn, Optional

import typer

from netaudio._common_cli import HELP_CONTEXT_SETTINGS

from netaudio.commands.virtual_process import (
    ProcessRecord,
    VirtualLifecycleError,
    _ownership_state,
    _pid_exists,
    _process_exit_waiter,
    _request_process_stop,
)

logger = logging.getLogger("netaudio")

app = typer.Typer(help="Virtual Dante device.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)


def _default_runtime_directory() -> str:
    if os.name == "nt":
        base_directory = os.environ.get("LOCALAPPDATA") or os.path.join(os.path.expanduser("~"), "AppData", "Local")
        return os.path.join(base_directory, "netaudio", "Runtime")
    if sys.platform == "darwin":
        return os.path.join(os.path.expanduser("~"), "Library", "Caches", "netaudio", "runtime")
    if os.environ.get("XDG_RUNTIME_DIR"):
        return os.path.join(os.environ["XDG_RUNTIME_DIR"], "netaudio")
    owner_identifier = str(os.getuid()) if hasattr(os, "getuid") else os.environ.get("USER", "unknown")
    return os.path.join(tempfile.gettempdir(), f"netaudio-{owner_identifier}")


RUNTIME_DIRECTORY = _default_runtime_directory()
PIDFILE = os.path.join(RUNTIME_DIRECTORY, "virtual.pid")
LOGFILE = os.path.join(RUNTIME_DIRECTORY, "virtual.log")
START_LOCKFILE = os.path.join(RUNTIME_DIRECTORY, "virtual-start.lock")
START_TIMEOUT_SECONDS = 10.0
STOP_TIMEOUT_SECONDS = 5.0
KILL_TIMEOUT_SECONDS = 1.0
MAX_READINESS_MESSAGE_BYTES = 65_536
READINESS_HOST = "127.0.0.1"


def _ensure_runtime_directory() -> None:
    try:
        os.makedirs(RUNTIME_DIRECTORY, mode=0o700, exist_ok=True)
        if not os.path.isdir(RUNTIME_DIRECTORY):
            raise VirtualLifecycleError(f"runtime path {RUNTIME_DIRECTORY} is not a directory")
        if os.name != "nt" and hasattr(os, "getuid"):
            status = os.stat(RUNTIME_DIRECTORY)
            if status.st_uid != os.getuid():
                raise VirtualLifecycleError(f"runtime directory {RUNTIME_DIRECTORY} is owned by another user")
            if status.st_mode & 0o077:
                os.chmod(RUNTIME_DIRECTORY, 0o700)
    except VirtualLifecycleError:
        raise
    except OSError as exception:
        raise VirtualLifecycleError(f"cannot prepare runtime directory {RUNTIME_DIRECTORY}: {exception}") from exception


def _remove_file_if_present(path: str) -> bool:
    try:
        os.unlink(path)
        return True
    except FileNotFoundError:
        return False


def _atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    _ensure_runtime_directory()
    temporary_path = f"{path}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with open(temporary_path, "w", encoding="utf-8") as output:
            json.dump(payload, output)
            output.write("\n")
        try:
            os.chmod(temporary_path, 0o600)
        except OSError as exception:
            logger.warning("Could not restrict permissions on %s: %s", temporary_path, exception)
        os.replace(temporary_path, path)
    finally:
        _remove_file_if_present(temporary_path)


def _read_process_record() -> Optional[ProcessRecord]:
    try:
        with open(PIDFILE, encoding="utf-8") as source:
            raw_record = source.read().strip()
    except FileNotFoundError:
        return None
    except OSError as exception:
        raise VirtualLifecycleError(f"cannot read process record {PIDFILE}: {exception}") from exception

    if not raw_record:
        raise VirtualLifecycleError(f"process record {PIDFILE} is empty")

    try:
        legacy_pid = int(raw_record)
    except ValueError:
        legacy_pid = None
    if legacy_pid is not None:
        if legacy_pid <= 0:
            raise VirtualLifecycleError(f"process record {PIDFILE} contains an invalid PID")
        return ProcessRecord(pid=legacy_pid, token=None)

    try:
        process_data = json.loads(raw_record)
        pid = int(process_data["pid"])
        token = process_data["token"]
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exception:
        raise VirtualLifecycleError(f"process record {PIDFILE} is invalid") from exception

    if pid <= 0 or not isinstance(token, str) or not token:
        raise VirtualLifecycleError(f"process record {PIDFILE} is invalid")

    name = process_data.get("name")
    started_at = process_data.get("started_at")
    return ProcessRecord(
        pid=pid,
        token=token,
        name=name if isinstance(name, str) else None,
        started_at=float(started_at) if isinstance(started_at, (int, float)) else None,
    )


def _write_process_record(record: ProcessRecord) -> None:
    if not record.token:
        raise VirtualLifecycleError("refusing to write a process record without an ownership token")
    _atomic_write_json(
        PIDFILE,
        {
            "version": 1,
            "pid": record.pid,
            "token": record.token,
            "name": record.name,
            "started_at": record.started_at,
        },
    )


def _remove_process_record(record: Optional[ProcessRecord] = None) -> bool:
    if record is not None:
        try:
            current_record = _read_process_record()
        except VirtualLifecycleError as exception:
            logger.warning("Could not verify the process record before removal: %s", exception)
            return False
        if current_record is None or current_record.pid != record.pid or current_record.token != record.token:
            return False

    return _remove_file_if_present(PIDFILE)


def _existing_managed_process() -> Optional[ProcessRecord]:
    record = _read_process_record()
    if record is None:
        return None

    state = _ownership_state(record)
    if state == "owned":
        return record
    if state in ("dead", "different"):
        _remove_process_record(record)
        return None
    raise VirtualLifecycleError(
        f"cannot verify ownership of PID {record.pid}; refusing to replace or signal it. "
        f"Remove {PIDFILE} manually after checking the process."
    )


def _read_json_file(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as source:
            payload = json.load(source)
    except (OSError, json.JSONDecodeError) as exception:
        raise VirtualLifecycleError(f"cannot read startup claim {path}: {exception}") from exception
    if not isinstance(payload, dict):
        raise VirtualLifecycleError(f"startup claim {path} is invalid")
    return payload


def _remove_startup_claim(token: str) -> bool:
    try:
        current_claim = _read_json_file(START_LOCKFILE)
    except VirtualLifecycleError as exception:
        logger.warning("Could not verify the startup claim before removal: %s", exception)
        return False
    if current_claim.get("token") != token:
        return False
    return _remove_file_if_present(START_LOCKFILE)


@contextmanager
def _startup_claim():
    _ensure_runtime_directory()
    token = uuid.uuid4().hex
    payload = json.dumps({"pid": os.getpid(), "token": token, "created_at": time.time()}) + "\n"

    for attempt in range(2):
        try:
            descriptor = os.open(START_LOCKFILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            existing = _read_json_file(START_LOCKFILE)
            try:
                owner_pid = int(existing["pid"])
                owner_token = existing["token"]
            except (KeyError, TypeError, ValueError) as exception:
                raise VirtualLifecycleError(
                    f"startup claim {START_LOCKFILE} is invalid; remove it manually after checking for a running start"
                ) from exception
            if not isinstance(owner_token, str) or not owner_token:
                raise VirtualLifecycleError(f"startup claim {START_LOCKFILE} is invalid")
            if _pid_exists(owner_pid):
                raise VirtualLifecycleError(f"another virtual-device start is already in progress (PID {owner_pid})")
            if not _remove_startup_claim(owner_token):
                raise VirtualLifecycleError("a stale startup claim changed while it was being inspected")
            if attempt == 0:
                continue
            raise VirtualLifecycleError("could not acquire the virtual-device startup claim")
        else:
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                    output.write(payload)
                break
            except Exception:
                _remove_startup_claim(token)
                raise
    else:
        raise VirtualLifecycleError("could not acquire the virtual-device startup claim")

    try:
        yield
    finally:
        if not _remove_startup_claim(token):
            logger.warning("Virtual-device startup claim changed before it could be released")


def _create_readiness_listener() -> socket.socket:
    readiness_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        readiness_listener.bind((READINESS_HOST, 0))
        readiness_listener.listen()
    except OSError:
        readiness_listener.close()
        raise
    return readiness_listener


def _send_readiness_message(readiness_port: int, message: dict[str, Any]) -> None:
    encoded_message = f"{json.dumps(message)}\n".encode("utf-8")
    if len(encoded_message) > MAX_READINESS_MESSAGE_BYTES:
        raise VirtualLifecycleError("startup response exceeded the size limit")
    with socket.create_connection((READINESS_HOST, readiness_port), timeout=START_TIMEOUT_SECONDS) as connection:
        connection.sendall(encoded_message)


def _read_readiness_message(connection: socket.socket, timeout: float) -> dict[str, Any]:
    try:
        connection.settimeout(timeout)
        with connection.makefile("rb") as source:
            encoded_message = source.readline(MAX_READINESS_MESSAGE_BYTES + 1)
    except (OSError, TimeoutError) as exception:
        raise VirtualLifecycleError(f"could not read child-process startup response: {exception}") from exception
    if not encoded_message or len(encoded_message) > MAX_READINESS_MESSAGE_BYTES or not encoded_message.endswith(b"\n"):
        raise VirtualLifecycleError("invalid startup response from child process")
    try:
        message = json.loads(encoded_message)
    except (UnicodeDecodeError, json.JSONDecodeError) as exception:
        raise VirtualLifecycleError(f"invalid startup response from child process: {exception}") from exception
    if not isinstance(message, dict):
        raise VirtualLifecycleError("invalid startup response from child process")
    return message


def _notify_when_process_exits(process: subprocess.Popen[Any], readiness_port: int, token: str) -> None:
    return_code = process.wait()
    try:
        _send_readiness_message(
            readiness_port,
            {
                "status": "exited",
                "token": token,
                "pid": process.pid,
                "return_code": return_code,
            },
        )
    except (OSError, VirtualLifecycleError) as exception:
        logger.debug("Could not report child-process exit to startup waiter: %s", exception)


def _wait_for_startup(process: subprocess.Popen[Any], readiness_listener: socket.socket, token: str) -> dict[str, Any]:
    deadline = time.monotonic() + START_TIMEOUT_SECONDS
    readiness_port = int(readiness_listener.getsockname()[1])
    exit_notifier = threading.Thread(
        target=_notify_when_process_exits,
        args=(process, readiness_port, token),
        daemon=True,
        name=f"netaudio-virtual-start-{process.pid}",
    )
    exit_notifier.start()

    while True:
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            raise VirtualLifecycleError(f"startup timed out after {START_TIMEOUT_SECONDS:g} seconds")
        readiness_listener.settimeout(remaining_seconds)
        try:
            connection, _peer_address = readiness_listener.accept()
        except TimeoutError as exception:
            raise VirtualLifecycleError(f"startup timed out after {START_TIMEOUT_SECONDS:g} seconds") from exception
        except OSError as exception:
            raise VirtualLifecycleError(f"could not receive child-process startup response: {exception}") from exception

        with connection:
            read_timeout = deadline - time.monotonic()
            if read_timeout <= 0:
                raise VirtualLifecycleError(f"startup timed out after {START_TIMEOUT_SECONDS:g} seconds")
            message = _read_readiness_message(connection, read_timeout)
        if message.get("token") != token or message.get("pid") != process.pid:
            logger.warning("Ignored startup response that did not match the spawned process")
            continue
        if message.get("status") == "ready":
            return message
        if message.get("status") == "error":
            detail = message.get("error") or "unknown startup error"
            raise VirtualLifecycleError(str(detail))
        if message.get("status") == "exited":
            return_code = message.get("return_code")
            raise VirtualLifecycleError(f"child process exited during startup (status {return_code})")
        raise VirtualLifecycleError("startup response had an unknown status")


def _terminate_spawned_process(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=KILL_TIMEOUT_SECONDS)
        return
    except (OSError, subprocess.TimeoutExpired) as exception:
        logger.debug("Graceful child-process termination failed: %s", exception)
    try:
        process.kill()
        process.wait(timeout=KILL_TIMEOUT_SECONDS)
    except (OSError, subprocess.TimeoutExpired) as exception:
        logger.warning("Could not terminate failed virtual-device child PID %s: %s", process.pid, exception)


def _validate_configuration(tx: int, rx: int, sample_rate: int) -> None:
    if tx < 0:
        raise typer.BadParameter("--tx cannot be negative")
    if rx < 0:
        raise typer.BadParameter("--rx cannot be negative")
    if sample_rate <= 0:
        raise typer.BadParameter("--sample-rate must be positive")


def _start_background_claimed(
    *,
    name: str,
    model: str,
    tx: int,
    rx: int,
    sample_rate: int,
    interface: Optional[str],
) -> None:
    existing = _existing_managed_process()
    if existing is not None:
        raise VirtualLifecycleError(f"already running (PID {existing.pid})")

    token = uuid.uuid4().hex
    command = [
        sys.executable,
        "-m",
        "netaudio",
        "virtual",
        "start",
        "--foreground",
        "--ownership-token",
        token,
        "--name",
        name,
        "--model",
        model,
        "--tx",
        str(tx),
        "--rx",
        str(rx),
        "--sample-rate",
        str(sample_rate),
    ]
    if interface:
        command.extend(("--interface", interface))

    popen_options: dict[str, Any] = {}
    if os.name == "nt":
        popen_options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_options["start_new_session"] = True

    process: Optional[subprocess.Popen[Any]] = None
    with _create_readiness_listener() as readiness_listener:
        readiness_port = int(readiness_listener.getsockname()[1])
        command.extend(("--readiness-port", str(readiness_port)))
        try:
            with open(LOGFILE, "w", encoding="utf-8") as log_file:
                process = subprocess.Popen(command, stdout=log_file, stderr=log_file, **popen_options)
            message = _wait_for_startup(process, readiness_listener, token)
        except Exception:
            if process is not None:
                _terminate_spawned_process(process)
            raise

    typer.echo(f"Started virtual device '{name}' (PID {process.pid})")
    typer.echo(f"  MAC: {message.get('mac', 'unknown')}")
    typer.echo(f"  Ports: {message.get('ports', [])}")
    typer.echo(f"  Log: {LOGFILE}")


def _start_background(
    *,
    name: str,
    model: str,
    tx: int,
    rx: int,
    sample_rate: int,
    interface: Optional[str],
) -> None:
    with _startup_claim():
        _start_background_claimed(
            name=name,
            model=model,
            tx=tx,
            rx=rx,
            sample_rate=sample_rate,
            interface=interface,
        )


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    event_loop = asyncio.get_running_loop()

    def request_stop(*_ignored_arguments: object) -> None:
        event_loop.call_soon_threadsafe(stop_event.set)

    handled_signals = [signal.SIGINT, signal.SIGTERM]
    if os.name == "nt":
        handled_signals.append(signal.SIGBREAK)
    for handled_signal in handled_signals:
        try:
            event_loop.add_signal_handler(handled_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(handled_signal, request_stop)
            except (OSError, ValueError):
                logger.debug("Signal %s is not available on this platform", handled_signal)


async def _run_virtual_device(
    *,
    name: str,
    model: str,
    tx: int,
    rx: int,
    sample_rate: int,
    interface: Optional[str],
    ownership_token: Optional[str],
    readiness_port: Optional[int],
) -> None:
    from netaudio.dante.virtual_device import VirtualDevice, VirtualDeviceConfig

    tx_channels = [f"Ch {index + 1}" for index in range(tx)]
    rx_channels = [f"Ch {index + 1}" for index in range(rx)]
    device_configuration = VirtualDeviceConfig(
        name=name,
        model=model,
        tx_channels=tx_channels,
        rx_channels=rx_channels,
        sample_rate=sample_rate,
        interface_ip=interface,
    )
    device = VirtualDevice(device_configuration)
    record: Optional[ProcessRecord] = None
    readiness_reported = False

    try:
        await device.start()
        ports = [transport.get_extra_info("sockname")[1] for transport in device._transports]

        if ownership_token is not None:
            record = ProcessRecord(
                pid=os.getpid(),
                token=ownership_token,
                name=name,
                started_at=time.time(),
            )
            _write_process_record(record)

        if readiness_port is not None:
            await asyncio.to_thread(
                _send_readiness_message,
                readiness_port,
                {
                    "status": "ready",
                    "token": ownership_token,
                    "pid": os.getpid(),
                    "mac": device.mac,
                    "ports": ports,
                },
            )
            readiness_reported = True

        typer.echo(f"Virtual device '{name}' running ({tx} TX, {rx} RX, {sample_rate}Hz)")
        typer.echo(f"  MAC: {device.mac}")
        typer.echo(f"  Ports: {ports}")
        if ownership_token is None:
            typer.echo("  Foreground session; press Ctrl+C to stop")

        stop_event = asyncio.Event()
        _install_signal_handlers(stop_event)
        await stop_event.wait()
    except Exception as exception:
        if readiness_port is not None and not readiness_reported:
            try:
                await asyncio.to_thread(
                    _send_readiness_message,
                    readiness_port,
                    {
                        "status": "error",
                        "token": ownership_token,
                        "pid": os.getpid(),
                        "error": f"{type(exception).__name__}: {exception}",
                    },
                )
            except (OSError, VirtualLifecycleError):
                logger.exception("Could not report virtual-device startup failure")
        raise
    finally:
        try:
            await device.stop()
        except Exception:
            logger.exception("Virtual device cleanup failed")
        if record is not None:
            _remove_process_record(record)


def _start_virtual(
    *,
    name: str,
    model: str,
    tx: int,
    rx: int,
    sample_rate: int,
    interface: Optional[str],
    foreground: bool,
    ownership_token: Optional[str] = None,
    readiness_port: Optional[int] = None,
) -> None:
    _validate_configuration(tx, rx, sample_rate)
    if (ownership_token is None) != (readiness_port is None):
        raise typer.BadParameter("internal startup options must be provided together")

    if not foreground:
        _start_background(
            name=name,
            model=model,
            tx=tx,
            rx=rx,
            sample_rate=sample_rate,
            interface=interface,
        )
        return

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("netaudio").setLevel(logging.DEBUG)
    logging.getLogger("zeroconf").setLevel(logging.WARNING)
    asyncio.run(
        _run_virtual_device(
            name=name,
            model=model,
            tx=tx,
            rx=rx,
            sample_rate=sample_rate,
            interface=interface,
            ownership_token=ownership_token,
            readiness_port=readiness_port,
        )
    )


def _report_lifecycle_error(exception: VirtualLifecycleError) -> NoReturn:
    typer.echo(f"Error: {exception}", err=True)
    raise typer.Exit(code=1)


@app.command("start", help="Start the virtual Dante device.")
def virtual_start(
    name: str = typer.Option("netaudio-virtual", "--name", "-n", help="Device name to advertise."),
    model: str = typer.Option("netaudio", "--model", help="Model name to advertise."),
    tx: int = typer.Option(2, "--tx", help="Number of transmitter channels."),
    rx: int = typer.Option(2, "--rx", help="Number of receiver channels."),
    sample_rate: int = typer.Option(48000, "--sample-rate", "-r", help="Sample rate in Hz."),
    interface: Optional[str] = typer.Option(None, "--interface", "-i", help="Network interface to bind."),
    foreground: bool = typer.Option(False, "--foreground", "-f", help="Run in the foreground instead of detaching."),
    ownership_token: Optional[str] = typer.Option(
        None, "--ownership-token", hidden=True, help="Ownership token handed to the detached process."
    ),
    readiness_port: Optional[int] = typer.Option(
        None, "--readiness-port", hidden=True, help="Local port the detached process reports readiness on."
    ),
) -> None:
    try:
        _start_virtual(
            name=name,
            model=model,
            tx=tx,
            rx=rx,
            sample_rate=sample_rate,
            interface=interface,
            foreground=foreground,
            ownership_token=ownership_token,
            readiness_port=readiness_port,
        )
    except VirtualLifecycleError as exception:
        _report_lifecycle_error(exception)
    except KeyboardInterrupt:
        return
    except Exception as exception:
        typer.echo(f"Error: virtual device failed: {exception}", err=True)
        raise typer.Exit(code=1) from exception


def _require_owned_process(record: ProcessRecord, missing_ok: bool) -> bool:
    state = _ownership_state(record)
    if state == "owned":
        return True
    if state == "dead":
        _remove_process_record(record)
        if missing_ok:
            return False
        raise VirtualLifecycleError("not running (removed stale process record)")
    if state == "different":
        _remove_process_record(record)
        raise VirtualLifecycleError(
            f"recorded PID {record.pid} belongs to another process; removed the stale record without signalling it"
        )
    raise VirtualLifecycleError(
        f"cannot verify ownership of PID {record.pid}; refusing to signal it. "
        f"Remove {PIDFILE} manually after checking the process."
    )


def _stop_virtual(*, missing_ok: bool = False) -> bool:
    record = _read_process_record()
    if record is None:
        if missing_ok:
            return False
        raise VirtualLifecycleError("not running")

    if not _require_owned_process(record, missing_ok):
        return False

    with _process_exit_waiter(record.pid) as wait_for_exit:
        if not _require_owned_process(record, missing_ok):
            return False
        try:
            _request_process_stop(record.pid)
        except ProcessLookupError:
            _remove_process_record(record)
            if missing_ok:
                return False
            raise VirtualLifecycleError("not running")
        except (OSError, PermissionError) as exception:
            raise VirtualLifecycleError(f"could not stop PID {record.pid}: {exception}") from exception

        process_exited = wait_for_exit(STOP_TIMEOUT_SECONDS)
        state = "dead" if process_exited else _ownership_state(record)
        if not process_exited and state == "owned":
            force_signal = signal.SIGTERM if os.name == "nt" else signal.SIGKILL
            try:
                os.kill(record.pid, force_signal)
            except ProcessLookupError:
                process_exited = True
                state = "dead"
            except (OSError, PermissionError) as exception:
                raise VirtualLifecycleError(f"PID {record.pid} did not stop: {exception}") from exception
            else:
                process_exited = wait_for_exit(KILL_TIMEOUT_SECONDS)
                state = "dead" if process_exited else _ownership_state(record)

    if state == "unknown" and not process_exited:
        raise VirtualLifecycleError(f"stop was requested for PID {record.pid}, but its exit could not be verified")
    if state == "owned":
        raise VirtualLifecycleError(f"PID {record.pid} did not stop within the timeout")

    _remove_process_record(record)
    typer.echo(f"Stopped virtual device (PID {record.pid})")
    return True


@app.command("stop", help="Stop the virtual Dante device.")
def virtual_stop() -> None:
    try:
        _stop_virtual()
    except VirtualLifecycleError as exception:
        _report_lifecycle_error(exception)


@app.command("restart", help="Restart the virtual Dante device.")
def virtual_restart(
    name: str = typer.Option("netaudio-virtual", "--name", "-n", help="Device name to advertise."),
    model: str = typer.Option("netaudio", "--model", help="Model name to advertise."),
    tx: int = typer.Option(2, "--tx", help="Number of transmitter channels."),
    rx: int = typer.Option(2, "--rx", help="Number of receiver channels."),
    sample_rate: int = typer.Option(48000, "--sample-rate", "-r", help="Sample rate in Hz."),
    interface: Optional[str] = typer.Option(None, "--interface", "-i", help="Network interface to bind."),
) -> None:
    try:
        _stop_virtual(missing_ok=True)
        _start_virtual(
            name=name,
            model=model,
            tx=tx,
            rx=rx,
            sample_rate=sample_rate,
            interface=interface,
            foreground=False,
        )
    except VirtualLifecycleError as exception:
        _report_lifecycle_error(exception)


@app.command("status", help="Show whether the virtual Dante device is running.")
def virtual_status() -> None:
    try:
        record = _read_process_record()
    except VirtualLifecycleError as exception:
        _report_lifecycle_error(exception)

    if record is None:
        typer.echo("Not running")
        raise typer.Exit(code=1)

    state = _ownership_state(record)
    if state == "owned":
        label = f" '{record.name}'" if record.name else ""
        typer.echo(f"Running{label} (PID {record.pid})")
        if os.path.exists(LOGFILE):
            typer.echo(f"Log: {LOGFILE}")
        return
    if state == "dead":
        _remove_process_record(record)
        typer.echo("Not running (removed stale process record)")
    elif state == "different":
        _remove_process_record(record)
        typer.echo(f"Not running (PID {record.pid} belongs to another process; removed stale record)")
    else:
        typer.echo(
            f"State unknown: cannot verify ownership of PID {record.pid}; refusing to manage it",
            err=True,
        )
    raise typer.Exit(code=1)


def _print_last_lines(path: str, lines: int) -> None:
    if lines == 0:
        return
    with open(path, encoding="utf-8", errors="replace") as source:
        recent_lines = deque(source, maxlen=lines)
    for line in recent_lines:
        typer.echo(line, nl=False)


def _follow_log_file(lines: int) -> None:
    environment = os.environ.copy()
    if os.name == "nt":
        environment["NETAUDIO_LOG_FILE"] = LOGFILE
        command = [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            f"Get-Content -LiteralPath $env:NETAUDIO_LOG_FILE -Wait -Tail {lines}",
        ]
    else:
        command = ["tail", "-f", "-n", str(lines), LOGFILE]
    try:
        os.execvpe(command[0], command, environment)
    except OSError as exception:
        raise VirtualLifecycleError(f"could not follow log file {LOGFILE}: {exception}") from exception


@app.command("logs", help="Show the virtual Dante device log.")
def virtual_logs(
    follow: bool = typer.Option(False, "--follow", "-f", help="Follow the log as it grows."),
    lines: int = typer.Option(50, "--lines", "-n", help="Number of trailing lines to show."),
) -> None:
    if lines < 0:
        raise typer.BadParameter("--lines cannot be negative")
    if not os.path.exists(LOGFILE):
        typer.echo("No log file")
        raise typer.Exit(1)

    if not follow:
        _print_last_lines(LOGFILE, lines)
        return

    try:
        _follow_log_file(lines)
    except KeyboardInterrupt:
        return
    except VirtualLifecycleError as exception:
        _report_lifecycle_error(exception)
