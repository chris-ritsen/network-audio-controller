from __future__ import annotations

import asyncio
import os
from collections import deque
from unittest.mock import AsyncMock

import pytest

from netaudio.commands.meter_tui import (
    MOUSE_WHEEL_ROWS,
    MeterTerminal,
    MeterViewOptions,
    MeterViewport,
    run_meter_tui,
    stop_metering_attempts,
)
from tests.test_meter_tui import _channel, _device, _sample


@pytest.mark.asyncio
async def test_tui_does_not_stop_auto_detailed_device_if_terminal_never_opens():
    class BrokenTerminal:
        def __enter__(self):
            raise OSError("terminal unavailable")

        def __exit__(self, _exception_type, _exception, _traceback):
            pass

    start_metering = AsyncMock(return_value=True)
    stop_metering = AsyncMock(return_value=True)

    with pytest.raises(OSError, match="terminal unavailable"):
        await run_meter_tui(
            {"lx.local.": _device("lx.local.", "lx-dante", model_id="LX-DANTE")},
            MeterViewOptions(no_color=True),
            terminal=BrokenTerminal(),
            stream_factory=_one_sample_stream("lx.local."),
            start_metering=start_metering,
            stop_metering=stop_metering,
        )

    start_metering.assert_not_awaited()
    stop_metering.assert_not_awaited()


class FakeTerminal:
    def __init__(self, keys: list[str | None], *, columns: int = 100, lines: int = 12):
        self._keys = deque(keys)
        self._size = os.terminal_size((columns, lines))
        self.entered = False
        self.exited = False
        self.frames: list[str] = []

    @property
    def size(self):
        return self._size

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, _exception_type, _exception, _traceback):
        self.exited = True

    def draw(self, frame: str) -> None:
        self.frames.append(frame)

    async def read_key(self, _timeout: float = 0.1, *, text_mode: bool = False) -> str | None:
        await asyncio.sleep(0)
        key = self._keys.popleft() if self._keys else "quit"
        if text_mode and key and len(key) == 1 and key.isprintable():
            return f"text:{key}"
        return key


class RecordingTerminalOutput:
    def __init__(self, file_descriptor: int):
        self.file_descriptor = file_descriptor
        self.writes: list[str] = []

    @staticmethod
    def isatty():
        return True

    def fileno(self):
        return self.file_descriptor

    def write(self, value: str):
        self.writes.append(value)
        return len(value)

    @staticmethod
    def flush():
        pass


def _stable_terminal_attributes(attributes, pending_input_flag: int):
    stable_attributes = list(attributes)
    stable_attributes[3] &= ~pending_input_flag
    return stable_attributes


@pytest.mark.asyncio
async def test_mouse_wheel_moves_through_meter_rows(monkeypatch):
    moves: list[int] = []
    original_move = MeterViewport.move

    def recording_move(viewport, amount, page_size):
        moves.append(amount)
        original_move(viewport, amount, page_size)

    monkeypatch.setattr(MeterViewport, "move", recording_move)
    server_name = "input.local."
    terminal = FakeTerminal(["wheel_down", "wheel_up", "quit"])

    await run_meter_tui(
        {
            server_name: _device(
                server_name,
                "Input",
                tx={number: _channel(number, f"Channel {number}") for number in range(1, 9)},
            )
        },
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream(server_name),
        start_metering=AsyncMock(return_value=True),
        stop_metering=AsyncMock(return_value=True),
    )

    assert moves == [MOUSE_WHEEL_ROWS, -MOUSE_WHEEL_ROWS]


def _one_sample_stream(server_name: str):
    async def stream():
        yield {
            "event": "meter_values",
            "server_name": server_name,
            **_sample(tx={1: 0x7B}),
        }
        await asyncio.Event().wait()

    return stream


@pytest.mark.asyncio
async def test_passive_tui_uses_async_events_and_never_starts_or_stops_detailed_metering():
    server_name = "input.local."
    devices = {
        server_name: _device(
            server_name,
            "Input",
            tx={1: _channel(1, "shelford")},
            model_id="DIOUSB",
        ),
    }
    terminal = FakeTerminal(["quit"])
    start_calls = []
    stop_calls = []

    async def start_metering(*args):
        start_calls.append(args)
        return True

    async def stop_metering(*args):
        stop_calls.append(args)
        return True

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream(server_name),
        start_metering=start_metering,
        stop_metering=stop_metering,
    )

    assert start_calls == []
    assert stop_calls == []
    assert terminal.entered is True
    assert terminal.exited is True
    assert terminal.frames
    assert "PASSIVE" in terminal.frames[-1]
    assert "7B" in terminal.frames[-1]


@pytest.mark.asyncio
async def test_default_tui_starts_and_stops_detailed_metering_for_lx_dante_and_a32_only():
    devices = {
        "lx.local.": _device(
            "lx.local.",
            "lx-dante",
            tx={1: _channel(1, "mic")},
            model_id="LX-DANTE",
        ),
        "avio.local.": _device(
            "avio.local.",
            "avio-input",
            tx={1: _channel(1, "input")},
            model_id="DAI2",
        ),
        "a32.local.": _device(
            "a32.local.",
            "a32",
            tx={1: _channel(1, "input")},
            model_id="_0000000000000001",
            dante_model="A32 Dante AD/DA Converter",
        ),
    }
    terminal = FakeTerminal(["quit"])
    start_metering = AsyncMock(return_value=True)
    stop_metering = AsyncMock(return_value=True)

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream("avio.local."),
        start_metering=start_metering,
        stop_metering=stop_metering,
    )

    assert start_metering.await_count == 2
    assert stop_metering.await_count == 2
    start_calls = {call.args[0]: call.args[1] for call in start_metering.await_args_list}
    stop_calls = {call.args[0]: call.args[1] for call in stop_metering.await_args_list}
    assert set(start_calls) == set(stop_calls) == {"a32.local.", "lx.local."}
    assert start_calls == stop_calls
    assert "MIXED/" in terminal.frames[-1]


@pytest.mark.asyncio
async def test_lx_only_tui_is_labeled_detailed_not_mixed():
    server_name = "lx.local."
    terminal = FakeTerminal(["quit"])

    await run_meter_tui(
        {
            server_name: _device(
                server_name,
                "lx-dante",
                tx={1: _channel(1, "mic")},
                model_id="LX-DANTE",
            )
        },
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream(server_name),
        start_metering=AsyncMock(return_value=True),
        stop_metering=AsyncMock(return_value=True),
    )

    assert "DETAILED/" in terminal.frames[-1]
    assert "MIXED" not in terminal.frames[-1]


@pytest.mark.asyncio
async def test_tui_filter_form_changes_device_and_direction_without_restarting():
    devices = {
        "zulu.local.": _device(
            "zulu.local.",
            "Zulu",
            tx={1: _channel(1, "tx")},
            rx={1: _channel(1, "rx")},
        ),
        "alpha.local.": _device(
            "alpha.local.",
            "Alpha",
            tx={1: _channel(1, "tx")},
            rx={1: _channel(1, "rx")},
        ),
    }
    terminal = FakeTerminal(["open_filter", "a", "l", "p", "h", "a", "down", "right", "accept", "quit"])

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream("alpha.local."),
        start_metering=AsyncMock(return_value=True),
        stop_metering=AsyncMock(return_value=True),
    )

    assert "devices~alpha · TX" in terminal.frames[-1]
    assert "Zulu" not in terminal.frames[-1]
    assert " RX " not in terminal.frames[-1]


@pytest.mark.asyncio
async def test_filter_enter_pins_highlighted_device_when_query_matches_multiple_devices():
    devices = {
        "avio-input.local.": _device(
            "avio-input.local.",
            "AVIO Input",
            tx={1: _channel(1, "input")},
        ),
        "avio-output.local.": _device(
            "avio-output.local.",
            "AVIO Output",
            tx={1: _channel(1, "output")},
        ),
        "a32.local.": _device(
            "a32.local.",
            "A32",
            tx={1: _channel(1, "other")},
        ),
    }
    terminal = FakeTerminal(["open_filter", "a", "v", "i", "o", "wheel_down", "accept", "quit"])

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True, show_rx=False),
        terminal=terminal,
        stream_factory=_one_sample_stream("avio-input.local."),
        start_metering=AsyncMock(return_value=True),
        stop_metering=AsyncMock(return_value=True),
    )

    assert "device=AVIO Output · TX" in terminal.frames[-1]
    assert "AVIO Input" not in terminal.frames[-1]
    assert "A32" not in terminal.frames[-1]


@pytest.mark.asyncio
async def test_blank_filter_enter_keeps_all_devices_visible():
    devices = {
        "alpha.local.": _device("alpha.local.", "Alpha", tx={1: _channel(1, "alpha")}),
        "zulu.local.": _device("zulu.local.", "Zulu", tx={1: _channel(1, "zulu")}),
    }
    terminal = FakeTerminal(["open_filter", "accept", "quit"])

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True, show_rx=False),
        terminal=terminal,
        stream_factory=_one_sample_stream("alpha.local."),
        start_metering=AsyncMock(return_value=True),
        stop_metering=AsyncMock(return_value=True),
    )

    assert "all devices · TX" in terminal.frames[-1]
    assert "Alpha" in terminal.frames[-1]
    assert "Zulu" in terminal.frames[-1]


@pytest.mark.asyncio
async def test_typed_filter_previews_in_meter_list_and_escape_restores_previous_state():
    devices = {
        "alpha.local.": _device("alpha.local.", "Alpha", tx={1: _channel(1, "alpha")}),
        "zulu.local.": _device("zulu.local.", "Zulu", tx={1: _channel(1, "zulu")}),
    }
    terminal = FakeTerminal(["open_filter", "z", "u", "l", "u", "cancel", "quit"])

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream("alpha.local."),
        start_metering=AsyncMock(return_value=True),
        stop_metering=AsyncMock(return_value=True),
    )

    preview_frames = [frame for frame in terminal.frames if "devices~zulu" in frame]
    assert preview_frames
    assert all("Alpha" not in frame for frame in preview_frames)
    assert "all devices · TX+RX" in terminal.frames[-1]
    assert "Alpha" in terminal.frames[-1]
    assert "Zulu" in terminal.frames[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize("clear_key", ["clear_filter", "cancel"])
async def test_enter_pins_selected_device_and_backspace_or_escape_undoes_it(clear_key):
    devices = {
        "alpha.local.": _device(
            "alpha.local.",
            "Alpha",
            tx={1: _channel(1, "alpha-tx")},
            rx={1: _channel(1, "alpha-rx")},
        ),
        "zulu.local.": _device(
            "zulu.local.",
            "Zulu",
            tx={1: _channel(1, "zulu-tx")},
            rx={1: _channel(1, "zulu-rx")},
        ),
    }
    terminal = FakeTerminal(["end", "accept", clear_key, "quit"])

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream("alpha.local."),
        start_metering=AsyncMock(return_value=True),
        stop_metering=AsyncMock(return_value=True),
    )

    pinned_frames = [frame for frame in terminal.frames if "device=Zulu" in frame]
    assert pinned_frames
    assert all("Alpha" not in frame for frame in pinned_frames)
    assert "all devices · TX+RX" in terminal.frames[-1]
    assert "Alpha" in terminal.frames[-1]
    assert "Zulu" in terminal.frames[-1]


@pytest.mark.asyncio
async def test_slash_search_selects_channel_keeps_rows_and_n_and_navigate_matches():
    devices = {
        "alpha.local.": _device("alpha.local.", "Alpha", tx={1: _channel(1, "Other")}),
        "zulu.local.": _device("zulu.local.", "Zulu", tx={1: _channel(1, "Shelford")}),
    }
    terminal = FakeTerminal(
        ["open_search", "s", "h", "e", "l", "f", "o", "r", "d", "accept", "next_search", "previous_search", "quit"]
    )

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream("alpha.local."),
        start_metering=AsyncMock(return_value=True),
        stop_metering=AsyncMock(return_value=True),
    )

    assert "/shelford" in terminal.frames[-1]
    assert "Alpha" in terminal.frames[-1]
    assert ">Zulu" in terminal.frames[-1]


@pytest.mark.asyncio
async def test_escape_from_search_prompt_restores_previous_search():
    device = _device(
        "device.local.",
        "Device",
        tx={1: _channel(1, "alpha"), 2: _channel(2, "shelford")},
    )
    terminal = FakeTerminal(
        [
            "open_search",
            "a",
            "l",
            "p",
            "h",
            "a",
            "accept",
            "open_search",
            "s",
            "h",
            "e",
            "l",
            "f",
            "o",
            "r",
            "d",
            "cancel",
            "quit",
        ]
    )

    await run_meter_tui(
        {"device.local.": device},
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream("device.local."),
        start_metering=AsyncMock(return_value=True),
        stop_metering=AsyncMock(return_value=True),
    )

    assert "/alpha" in terminal.frames[-1]
    assert "/shelford" not in terminal.frames[-1]


@pytest.mark.asyncio
async def test_passive_tui_falls_back_to_cache_without_starting_or_stopping():
    server_name = "input.local."
    devices = {
        server_name: _device(server_name, "Input", tx={1: _channel(1, "shelford")}),
    }
    terminal = FakeTerminal(["quit"])
    start_calls = []
    stop_calls = []

    async def empty_stream():
        if False:
            yield {}

    async def cache_reader():
        return {server_name: _sample(tx={1: 0x7B})}

    async def start_metering(*args):
        start_calls.append(args)
        return True

    async def stop_metering(*args):
        stop_calls.append(args)
        return True

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=empty_stream,
        cache_reader=cache_reader,
        start_metering=start_metering,
        stop_metering=stop_metering,
    )

    assert start_calls == []
    assert stop_calls == []
    assert "cache fallback" in terminal.frames[-1]
    assert "0x7B" in terminal.frames[-1]


@pytest.mark.asyncio
async def test_detailed_tui_stops_every_attempted_start_even_when_an_acknowledgement_is_lost():
    devices = {
        "first.local.": _device("first.local.", "First", tx={1: _channel(1, "one")}),
        "second.local.": _device("second.local.", "Second", tx={1: _channel(1, "two")}),
        "third.local.": _device("third.local.", "Third", tx={1: _channel(1, "three")}),
    }
    terminal = FakeTerminal(["quit"])
    start_calls = []
    stop_calls = []

    async def start_metering(server_name, client_id):
        start_calls.append((server_name, client_id))
        return server_name != "second.local."

    async def stop_metering(server_name, client_id):
        stop_calls.append((server_name, client_id))
        return True

    await run_meter_tui(
        devices,
        MeterViewOptions(detailed=True, no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream("first.local."),
        start_metering=start_metering,
        stop_metering=stop_metering,
    )

    assert [server_name for server_name, _ in start_calls] == [
        "first.local.",
        "second.local.",
        "third.local.",
    ]
    client_ids = {client_id for _, client_id in start_calls}
    assert len(client_ids) == 1
    client_id = client_ids.pop()
    assert client_id.startswith("meter_tui:")
    assert stop_calls == [
        ("third.local.", client_id),
        ("second.local.", client_id),
        ("first.local.", client_id),
    ]
    assert terminal.exited is True


def _silent_stream():
    async def stream():
        await asyncio.Event().wait()
        yield {}

    return stream


@pytest.mark.asyncio
async def test_passive_tui_escalates_to_detailed_for_devices_with_no_samples(monkeypatch):
    import netaudio.commands.meter_runtime as meter_runtime_module

    monkeypatch.setattr(meter_runtime_module, "DETAILED_ESCALATION_SECONDS", 0.0)
    server_name = "silent.local."
    devices = {
        server_name: _device(
            server_name,
            "Silent",
            tx={1: _channel(1, "mic")},
            model_id="UNKNOWN-MODEL",
        ),
    }
    terminal = FakeTerminal(["down", "down", "quit"])
    start_calls = []
    stop_calls = []

    async def start_metering(*args):
        start_calls.append(args)
        return True

    async def stop_metering(*args):
        stop_calls.append(args)
        return True

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_silent_stream(),
        start_metering=start_metering,
        stop_metering=stop_metering,
    )

    assert [call[0] for call in start_calls] == [server_name]
    assert [call[0] for call in stop_calls] == [server_name]
    assert "MIXED" in terminal.frames[-1]


@pytest.mark.asyncio
async def test_passive_tui_does_not_escalate_for_devices_with_fresh_passive_samples(monkeypatch):
    import netaudio.commands.meter_runtime as meter_runtime_module

    monkeypatch.setattr(meter_runtime_module, "DETAILED_ESCALATION_SECONDS", 0.0)
    server_name = "input.local."
    devices = {
        server_name: _device(
            server_name,
            "Input",
            tx={1: _channel(1, "shelford")},
            model_id="DIOUSB",
        ),
    }
    terminal = FakeTerminal(["down", "down", "quit"])
    start_calls = []

    async def start_metering(*args):
        start_calls.append(args)
        return True

    async def stop_metering(*args):
        return True

    await run_meter_tui(
        devices,
        MeterViewOptions(no_color=True),
        terminal=terminal,
        stream_factory=_one_sample_stream(server_name),
        start_metering=start_metering,
        stop_metering=stop_metering,
    )

    assert start_calls == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX EOF handling")
def test_posix_eof_decodes_as_quit(monkeypatch):
    terminal = MeterTerminal()
    terminal._fd = 123
    monkeypatch.setattr("netaudio.commands.meter_terminal.select.select", lambda *_args: ([123], [], []))
    monkeypatch.setattr("netaudio.commands.meter_terminal.os.read", lambda *_args: b"")

    assert terminal._read_key_blocking(0.1) == "quit"


@pytest.mark.skipif(os.name == "nt", reason="POSIX terminal attributes")
def test_terminal_attributes_are_restored_if_enter_output_fails():
    import pty
    import termios

    master_fd, slave_fd = pty.openpty()

    class Input:
        @staticmethod
        def isatty():
            return True

        @staticmethod
        def fileno():
            return slave_fd

    class BrokenOutput:
        @staticmethod
        def isatty():
            return True

        @staticmethod
        def fileno():
            return slave_fd

        @staticmethod
        def write(_text):
            raise OSError("terminal disappeared")

        @staticmethod
        def flush():
            pass

    before = termios.tcgetattr(slave_fd)
    try:
        with pytest.raises(OSError, match="terminal disappeared"):
            MeterTerminal(Input(), BrokenOutput()).__enter__()
        assert _stable_terminal_attributes(
            termios.tcgetattr(slave_fd),
            getattr(termios, "PENDIN", 0),
        ) == _stable_terminal_attributes(before, getattr(termios, "PENDIN", 0))
    finally:
        os.close(master_fd)
        os.close(slave_fd)


@pytest.mark.skipif(os.name == "nt", reason="POSIX terminal attributes")
def test_terminal_restores_attributes_and_signal_handlers_on_normal_exit():
    import pty
    import signal
    import termios

    master_fd, slave_fd = pty.openpty()
    input_stream = os.fdopen(os.dup(slave_fd), "r")
    output_stream = RecordingTerminalOutput(slave_fd)
    before_attributes = termios.tcgetattr(slave_fd)
    watched_signals = [
        signum
        for signal_name in ("SIGTERM", "SIGHUP", "SIGTSTP")
        if (signum := getattr(signal, signal_name, None)) is not None
    ]
    before_handlers = {signum: signal.getsignal(signum) for signum in watched_signals}
    try:
        terminal = MeterTerminal(input_stream, output_stream)
        with terminal:
            assert terminal._entered is True
        assert terminal._entered is False
        assert _stable_terminal_attributes(
            termios.tcgetattr(slave_fd),
            getattr(termios, "PENDIN", 0),
        ) == _stable_terminal_attributes(before_attributes, getattr(termios, "PENDIN", 0))
        assert {signum: signal.getsignal(signum) for signum in watched_signals} == before_handlers
    finally:
        input_stream.close()
        os.close(master_fd)
        os.close(slave_fd)


@pytest.mark.skipif(os.name == "nt", reason="xterm mouse reporting")
def test_terminal_enables_and_disables_mouse_reporting():
    import pty

    master_fd, slave_fd = pty.openpty()
    input_stream = os.fdopen(os.dup(slave_fd), "r")

    output_stream = RecordingTerminalOutput(slave_fd)
    try:
        with MeterTerminal(input_stream, output_stream):
            pass
        assert "\x1b[?1000h\x1b[?1006h" in output_stream.writes[0]
        assert "\x1b[?1006l\x1b[?1000l" in output_stream.writes[-1]
    finally:
        input_stream.close()
        os.close(master_fd)
        os.close(slave_fd)


@pytest.mark.asyncio
async def test_stop_metering_attempts_retries_only_unacknowledged_devices():
    calls: list[str] = []

    async def stop_metering(server_name, _client_id):
        calls.append(server_name)
        return server_name == "first.local." or calls.count(server_name) > 1

    failed = await stop_metering_attempts(
        ["first.local.", "second.local."],
        "client",
        stop_metering,
    )

    assert failed == []
    assert calls == ["first.local.", "second.local.", "second.local."]
