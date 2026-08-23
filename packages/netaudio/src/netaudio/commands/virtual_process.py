from __future__ import annotations

import logging
import os
import select
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional, cast


logger = logging.getLogger("netaudio")

PROCESS_EXIT_POLL_INTERVAL_SECONDS = 0.05


class VirtualLifecycleError(RuntimeError):
    pass


@dataclass(frozen=True)
class ProcessRecord:
    pid: int
    token: Optional[str]
    name: Optional[str] = None
    started_at: Optional[float] = None


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _windows_pid_exists(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exception:
        logger.debug("Could not probe PID %s: %s", pid, exception)
        return False
    return True


def _windows_process_api() -> tuple[Any, Any]:
    import ctypes
    from ctypes import wintypes

    windows_kernel = cast(Any, ctypes).WinDLL("kernel32", use_last_error=True)
    windows_kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    windows_kernel.OpenProcess.restype = wintypes.HANDLE
    windows_kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
    windows_kernel.CloseHandle.restype = wintypes.BOOL
    windows_kernel.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    windows_kernel.WaitForSingleObject.restype = wintypes.DWORD
    return ctypes, windows_kernel


def _windows_pid_exists(pid: int) -> bool:
    try:
        ctypes, windows_kernel = _windows_process_api()
        process_query_limited_information = 0x1000
        process_handle = windows_kernel.OpenProcess(process_query_limited_information, False, pid)
        if process_handle:
            windows_kernel.CloseHandle(process_handle)
            return True
        return ctypes.get_last_error() == 5
    except (AttributeError, OSError) as exception:
        logger.debug("Could not inspect Windows PID %s: %s", pid, exception)
        return False


def _process_command(pid: int) -> Optional[str]:
    process_command_line_path = f"/proc/{pid}/cmdline"
    if os.path.exists(process_command_line_path):
        try:
            with open(process_command_line_path, "rb") as source:
                return source.read().replace(b"\0", b" ").decode(errors="replace").strip()
        except OSError as exception:
            logger.debug("Could not read command line for PID %s: %s", pid, exception)
            return None

    try:
        if os.name == "nt":
            command = [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                (
                    f'$process = Get-CimInstance Win32_Process -Filter "ProcessId = {pid}"; '
                    "if ($process) { $process.CommandLine }"
                ),
            ]
        else:
            command = ["ps", "-ww", "-p", str(pid), "-o", "command="]
        result = subprocess.run(
            command,
            capture_output=True,
            check=False,
            text=True,
            timeout=1.0,
        )
    except (OSError, subprocess.SubprocessError) as exception:
        logger.debug("Could not inspect command line for PID %s: %s", pid, exception)
        return None

    if result.returncode != 0 or not result.stdout.strip():
        return None
    return result.stdout.strip()


def _ownership_state(record: ProcessRecord) -> str:
    if not _pid_exists(record.pid):
        return "dead"
    if not record.token:
        return "unknown"

    command = _process_command(record.pid)
    if command is None:
        return "unknown"
    if record.token in command and "netaudio" in command and "virtual" in command:
        return "owned"
    return "different"


def _process_already_exited(_timeout: float) -> bool:
    return True


@contextmanager
def _windows_process_exit_waiter(pid: int) -> Iterator[Callable[[float], bool]]:
    ctypes, windows_kernel = _windows_process_api()
    synchronize_access = 0x00100000
    process_handle = windows_kernel.OpenProcess(synchronize_access, False, pid)
    if not process_handle:
        error_code = ctypes.get_last_error()
        if error_code == 87:
            yield _process_already_exited
            return
        raise VirtualLifecycleError(f"could not open PID {pid} for an exit wait (Windows error {error_code})")

    def wait_for_exit(timeout: float) -> bool:
        wait_result = windows_kernel.WaitForSingleObject(process_handle, max(0, int(timeout * 1000)))
        if wait_result == 0:
            return True
        if wait_result == 0x00000102:
            return False
        error_code = ctypes.get_last_error()
        raise VirtualLifecycleError(f"could not wait for PID {pid} to exit (Windows error {error_code})")

    try:
        yield wait_for_exit
    finally:
        if not windows_kernel.CloseHandle(process_handle):
            logger.warning("Could not close Windows process handle for PID %s", pid)


@contextmanager
def _linux_process_exit_waiter(pid: int) -> Iterator[Callable[[float], bool]]:
    try:
        process_descriptor = cast(Any, os).pidfd_open(pid)
    except ProcessLookupError:
        yield _process_already_exited
        return
    except OSError as exception:
        raise VirtualLifecycleError(f"could not open PID {pid} for an exit wait: {exception}") from exception

    def wait_for_exit(timeout: float) -> bool:
        try:
            readable_descriptors, _writable_descriptors, _exceptional_descriptors = select.select(
                [process_descriptor], [], [], timeout
            )
        except OSError as exception:
            raise VirtualLifecycleError(f"could not wait for PID {pid} to exit: {exception}") from exception
        return bool(readable_descriptors)

    try:
        yield wait_for_exit
    finally:
        try:
            os.close(process_descriptor)
        except OSError as exception:
            logger.warning("Could not close process descriptor for PID %s: %s", pid, exception)


@contextmanager
def _kqueue_process_exit_waiter(pid: int) -> Iterator[Callable[[float], bool]]:
    event_queue = select.kqueue()
    process_event = select.kevent(
        pid,
        filter=select.KQ_FILTER_PROC,
        flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE,
        fflags=select.KQ_NOTE_EXIT,
    )
    try:
        event_queue.control([process_event], 0, 0)
    except ProcessLookupError:
        event_queue.close()
        yield _process_already_exited
        return
    except OSError as exception:
        event_queue.close()
        raise VirtualLifecycleError(f"could not register an exit wait for PID {pid}: {exception}") from exception

    def wait_for_exit(timeout: float) -> bool:
        try:
            return bool(event_queue.control(None, 1, timeout))
        except OSError as exception:
            raise VirtualLifecycleError(f"could not wait for PID {pid} to exit: {exception}") from exception

    try:
        yield wait_for_exit
    finally:
        event_queue.close()


@contextmanager
def _fallback_process_exit_waiter(pid: int) -> Iterator[Callable[[float], bool]]:
    process_exit_poll = threading.Event()

    def wait_for_exit(timeout: float) -> bool:
        deadline = time.monotonic() + max(0.0, timeout)
        while _pid_exists(pid):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            process_exit_poll.wait(min(PROCESS_EXIT_POLL_INTERVAL_SECONDS, remaining))
        return True

    yield wait_for_exit


@contextmanager
def _process_exit_waiter(pid: int) -> Iterator[Callable[[float], bool]]:
    if os.name == "nt":
        with _windows_process_exit_waiter(pid) as wait_for_exit:
            yield wait_for_exit
        return
    if sys.platform.startswith("linux") and callable(getattr(os, "pidfd_open", None)):
        with _linux_process_exit_waiter(pid) as wait_for_exit:
            yield wait_for_exit
        return
    if hasattr(select, "kqueue"):
        with _kqueue_process_exit_waiter(pid) as wait_for_exit:
            yield wait_for_exit
        return
    with _fallback_process_exit_waiter(pid) as wait_for_exit:
        yield wait_for_exit


def _request_process_stop(pid: int) -> None:
    if os.name == "nt":
        try:
            os.kill(pid, signal.CTRL_BREAK_EVENT)
            return
        except OSError as exception:
            logger.debug("Could not send CTRL_BREAK_EVENT to PID %s: %s", pid, exception)
    os.kill(pid, signal.SIGTERM)
