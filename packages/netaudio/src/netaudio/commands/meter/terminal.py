from __future__ import annotations

import asyncio
import contextlib
import os
import re
import select
import shutil
import signal
import sys
import time
from collections import deque
from typing import Any, TextIO

_MOUSE_REPORTING_ENABLE = "\x1b[?1000h\x1b[?1006h"
_MOUSE_REPORTING_DISABLE = "\x1b[?1006l\x1b[?1000l"
_X10_MOUSE_PREFIX = b"\x1b[M"
_SGR_MOUSE_MAX_BYTES = 64


_ESCAPE_KEYS = {
    "\x1b[A": "up",
    "\x1b[B": "down",
    "\x1b[C": "right",
    "\x1b[D": "left",
    "\x1bOM": "terminal_enter",
    "\x1b[13u": "terminal_enter",
    "\x1b[13;1u": "terminal_enter",
    "\x1b[27;1;13~": "terminal_enter",
    "\x1b[5~": "page_up",
    "\x1b[6~": "page_down",
    "\x1b[H": "home",
    "\x1b[1~": "home",
    "\x1b[F": "end",
    "\x1b[4~": "end",
    "\x1bv": "page_up",
    "\x1b<": "home",
    "\x1b>": "end",
}
_CHAR_KEYS = {
    "q": "quit",
    "Q": "quit",
    "j": "down",
    "k": "up",
    "n": "next_search",
    "N": "previous_search",
    "G": "end",
    "f": "open_filter",
    "F": "open_filter",
    "/": "open_search",
    "\r": "accept",
    "\n": "accept",
    "\b": "clear_filter",
    "\x7f": "clear_filter",
    "\x02": "page_up",
    "\x04": "half_page_down",
    "\x06": "page_down",
    "\x0e": "down",
    "\x10": "up",
    "\x15": "half_page_up",
    "\x16": "page_down",
    "\x1b": "cancel",
    "\x03": "quit",
}
_SGR_MOUSE_PATTERN = re.compile(r"^\x1b\[<(\d+);(\d+);(\d+)([Mm])")


class KeyDecoder:
    def __init__(self):
        self.buffer = ""
        self.byte_buffer = b""
        self.pending_g = False
        self.pending_z = False
        self.discarding_sgr_mouse = False

    def _decode_character(self, character: str, *, text_mode: bool) -> str | None:
        if text_mode:
            self.pending_g = False
            self.pending_z = False
            if character == "\x1b":
                return "cancel"
            if character in ("\r", "\n"):
                return "accept"
            if character in ("\b", "\x7f"):
                return "backspace"
            if character == "\x03":
                return "quit"
            if character.isprintable():
                return f"text:{character}"
            return None

        if self.pending_z:
            self.pending_z = False
            action = {
                "z": "align_center",
                "m": "align_center",
                ".": "align_center",
                "t": "align_top",
                "b": "align_bottom",
                "\r": "align_top",
                "\n": "align_top",
                "-": "align_bottom",
            }.get(character)
            if action is not None:
                return action
        if character == "z":
            self.pending_g = False
            self.pending_z = True
            return None
        if character == "g":
            if self.pending_g:
                self.pending_g = False
                return "home"
            self.pending_g = True
            return None
        self.pending_g = False
        self.pending_z = False
        return _CHAR_KEYS.get(character)

    def feed(self, text: str, *, text_mode: bool = False) -> list[str]:
        self.buffer += text
        result: list[str] = []
        sequences = sorted(_ESCAPE_KEYS, key=len, reverse=True)
        effective_text_mode = text_mode
        while self.buffer:
            if self.discarding_sgr_mouse:
                terminators = [index for marker in ("M", "m") if (index := self.buffer.find(marker)) >= 0]
                if not terminators:
                    self.buffer = ""
                    break
                self.buffer = self.buffer[min(terminators) + 1 :]
                self.discarding_sgr_mouse = False
                continue
            if self.buffer[0] != "\x1b":
                character, self.buffer = self.buffer[0], self.buffer[1:]
                key = self._decode_character(character, text_mode=effective_text_mode)
                if key:
                    result.append(key)
                    if not effective_text_mode and key in ("open_filter", "open_search"):
                        effective_text_mode = True
                    elif effective_text_mode and key == "accept":
                        effective_text_mode = False
                continue

            if self.buffer.startswith("\x1b[<"):
                terminators = [
                    index for marker in ("M", "m") if (index := self.buffer.find(marker, len("\x1b[<"))) >= 0
                ]
                if terminators:
                    candidate_end = min(terminators) + 1
                    candidate = self.buffer[:candidate_end]
                    self.pending_g = False
                    self.pending_z = False
                    self.buffer = self.buffer[candidate_end:]
                    mouse_match = (
                        _SGR_MOUSE_PATTERN.fullmatch(candidate) if candidate_end <= _SGR_MOUSE_MAX_BYTES else None
                    )
                    if mouse_match is not None:
                        button = int(mouse_match.group(1))
                        button_without_modifiers = button & ~(4 | 8 | 16)
                        if mouse_match.group(4) == "M":
                            if button_without_modifiers == 64:
                                result.append("wheel_up")
                            elif button_without_modifiers == 65:
                                result.append("wheel_down")
                    continue
                if len(self.buffer) > _SGR_MOUSE_MAX_BYTES:
                    self.buffer = ""
                    self.pending_g = False
                    self.pending_z = False
                    self.discarding_sgr_mouse = True
                break

            matched = next((sequence for sequence in sequences if self.buffer.startswith(sequence)), None)
            if matched:
                action = _ESCAPE_KEYS[matched]
                if action == "terminal_enter":
                    key = self._decode_character("\r", text_mode=effective_text_mode)
                else:
                    self.pending_g = False
                    self.pending_z = False
                    key = action
                if key:
                    result.append(key)
                    if effective_text_mode and key == "accept":
                        effective_text_mode = False
                self.buffer = self.buffer[len(matched) :]
                continue
            if any(sequence.startswith(self.buffer) for sequence in sequences):
                break
            self.pending_g = False
            self.pending_z = False
            if len(self.buffer) > 1 and self.buffer[1] not in ("[", "O"):
                result.append("cancel")
                effective_text_mode = False
            self.buffer = self.buffer[1:]
        return result

    def feed_bytes(self, data: bytes, *, text_mode: bool = False) -> list[str]:
        """Decode terminal bytes, canonicalizing older X10 mouse packets first."""
        self.byte_buffer += data
        canonical_chunks = []
        cursor = 0
        while cursor < len(self.byte_buffer):
            escape_index = self.byte_buffer.find(b"\x1b", cursor)
            if escape_index < 0:
                canonical_chunks.append(self.byte_buffer[cursor:])
                cursor = len(self.byte_buffer)
                break
            canonical_chunks.append(self.byte_buffer[cursor:escape_index])
            remaining = self.byte_buffer[escape_index:]
            if len(remaining) < len(_X10_MOUSE_PREFIX) and _X10_MOUSE_PREFIX.startswith(remaining):
                cursor = escape_index
                break
            if remaining.startswith(_X10_MOUSE_PREFIX):
                if len(remaining) < 6:
                    cursor = escape_index
                    break
                button = max(0, remaining[3] - 32)
                canonical_chunks.append(f"\x1b[<{button};0;0M".encode())
                cursor = escape_index + 6
                continue
            canonical_chunks.append(bytes([0x1B]))
            cursor = escape_index + 1
        self.byte_buffer = self.byte_buffer[cursor:]
        if not canonical_chunks:
            return []
        return self.feed(b"".join(canonical_chunks).decode(errors="ignore"), text_mode=text_mode)

    def flush_bytes(self, *, text_mode: bool = False) -> list[str]:
        """Resolve or discard an incomplete raw terminal packet after a read timeout."""
        if not self.byte_buffer:
            return []
        pending, self.byte_buffer = self.byte_buffer, b""
        if pending.startswith(_X10_MOUSE_PREFIX):
            return []
        return self.feed(pending.decode(errors="ignore"), text_mode=text_mode)

    def flush(self) -> list[str]:
        """Resolve a lone Escape after the escape-sequence read window."""
        self.discarding_sgr_mouse = False
        if self.buffer == "\x1b":
            self.buffer = ""
            self.pending_g = False
            self.pending_z = False
            return ["cancel"]
        self.buffer = ""
        return []


class MeterTerminal:
    """Small cross-platform terminal adapter; no UI framework or global handlers."""

    def __init__(self, stdin: TextIO | None = None, stdout: TextIO | None = None):
        self.stdin = stdin or sys.stdin
        self.stdout = stdout or sys.stdout
        self._old_termios = None
        self._fd: int | None = None
        self._entered = False
        self._windows_output_handle = None
        self._windows_output_mode = None
        self._old_signal_handlers: dict[int, Any] = {}
        self._decoder = KeyDecoder()
        self._pending_keys: deque[str] = deque()

    @property
    def is_interactive(self) -> bool:
        return bool(self.stdin.isatty() and self.stdout.isatty())

    @property
    def size(self) -> os.terminal_size:
        with contextlib.suppress(OSError, ValueError, AttributeError):
            return os.get_terminal_size(self.stdout.fileno())
        return shutil.get_terminal_size((100, 24))

    def __enter__(self):
        if not self.is_interactive:
            raise RuntimeError("interactive meter requires a TTY")
        try:
            if os.name == "nt":
                self._enable_windows_vt_output()
            else:
                import termios
                import tty

                fd = self.stdin.fileno()
                self._fd = fd
                self._old_termios = termios.tcgetattr(fd)
                tty.setcbreak(fd)
            self._entered = True
            self._install_signal_handlers()
            mouse_enable = "" if os.name == "nt" else _MOUSE_REPORTING_ENABLE
            self.stdout.write(f"\x1b[?1049h\x1b[?25l{mouse_enable}\x1b[2J\x1b[H")
            self.stdout.flush()
            return self
        except BaseException:
            try:
                if self._entered:
                    with contextlib.suppress(OSError):
                        mouse_disable = "" if os.name == "nt" else _MOUSE_REPORTING_DISABLE
                        self.stdout.write(f"{mouse_disable}\x1b[0m\x1b[?25h\x1b[?1049l")
                        self.stdout.flush()
            finally:
                self._restore_terminal()
                self._restore_signal_handlers()
                self._entered = False
            raise

    def __exit__(self, _exception_type, _exception, _traceback):
        if not self._entered:
            return
        try:
            with contextlib.suppress(OSError):
                mouse_disable = "" if os.name == "nt" else _MOUSE_REPORTING_DISABLE
                self.stdout.write(f"{mouse_disable}\x1b[0m\x1b[?25h\x1b[?1049l")
                self.stdout.flush()
        finally:
            try:
                self._restore_terminal()
            finally:
                self._restore_signal_handlers()
                self._entered = False

    @staticmethod
    def _exit_for_signal(signum, _frame) -> None:
        raise SystemExit(128 + signum)

    def _install_signal_handlers(self) -> None:
        for signal_name in ("SIGTERM", "SIGHUP", "SIGTSTP"):
            signum = getattr(signal, signal_name, None)
            if signum is None:
                continue
            try:
                previous = signal.getsignal(signum)
                if previous == signal.SIG_IGN:
                    continue
                handler = self._suspend_for_signal if signal_name == "SIGTSTP" else self._exit_for_signal
                signal.signal(signum, handler)
            except (OSError, ValueError):
                continue
            self._old_signal_handlers[signum] = previous

    def _suspend_for_signal(self, signum, _frame) -> None:
        previous = self._old_signal_handlers.get(signum, signal.SIG_DFL)
        self.__exit__(None, None, None)
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)
        signal.signal(signum, previous)
        self.__enter__()

    def _restore_signal_handlers(self) -> None:
        for signum, previous in self._old_signal_handlers.items():
            with contextlib.suppress(OSError, ValueError):
                signal.signal(signum, previous)
        self._old_signal_handlers.clear()

    def _enable_windows_vt_output(self) -> None:
        import ctypes
        import msvcrt

        handle = getattr(msvcrt, "get_osfhandle")(self.stdout.fileno())
        mode = ctypes.c_ulong()
        kernel32 = getattr(ctypes, "windll").kernel32
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            raise OSError("could not read Windows console mode")
        if not kernel32.SetConsoleMode(handle, mode.value | 0x0004):
            raise OSError("could not enable Windows terminal output")
        self._windows_output_handle = handle
        self._windows_output_mode = mode.value

    def _restore_terminal(self) -> None:
        if self._old_termios is not None and self._fd is not None:
            import termios

            with contextlib.suppress(OSError, termios.error):
                termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_termios)
            self._old_termios = None
            self._fd = None
        if self._windows_output_handle is not None and self._windows_output_mode is not None:
            import ctypes

            with contextlib.suppress(OSError):
                getattr(ctypes, "windll").kernel32.SetConsoleMode(
                    self._windows_output_handle,
                    self._windows_output_mode,
                )
            self._windows_output_handle = None
            self._windows_output_mode = None

    def draw(self, frame: str) -> None:
        self.stdout.write("\x1b[H" + frame + "\x1b[J")
        self.stdout.flush()

    async def read_key(self, timeout: float = 0.1, *, text_mode: bool = False) -> str | None:
        return await asyncio.to_thread(self._read_key_blocking, timeout, text_mode)

    def _read_key_blocking(self, timeout: float, text_mode: bool = False) -> str | None:
        if self._pending_keys:
            return self._pending_keys.popleft()
        if os.name == "nt":
            return self._read_windows_key(timeout, text_mode=text_mode)
        if self._fd is None:
            return None
        readable, _, _ = select.select([self._fd], [], [], timeout)
        if not readable:
            self._pending_keys.extend(self._decoder.flush_bytes(text_mode=text_mode))
            self._pending_keys.extend(self._decoder.flush())
            return self._pending_keys.popleft() if self._pending_keys else None
        raw = os.read(self._fd, 32)
        if not raw:
            return "quit"
        self._pending_keys.extend(self._decoder.feed_bytes(raw, text_mode=text_mode))
        return self._pending_keys.popleft() if self._pending_keys else None

    def _read_windows_key(self, timeout: float, *, text_mode: bool = False) -> str | None:
        import msvcrt

        kbhit = getattr(msvcrt, "kbhit")
        getwch = getattr(msvcrt, "getwch")
        deadline = time.monotonic() + timeout
        while not kbhit():
            if time.monotonic() >= deadline:
                return None
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
        character = getwch()
        if character in ("\x00", "\xe0"):
            special = getwch()
            return {
                "H": "up",
                "P": "down",
                "K": "left",
                "M": "right",
                "I": "page_up",
                "Q": "page_down",
                "G": "home",
                "O": "end",
            }.get(special)
        return self._decoder._decode_character(character, text_mode=text_mode)
