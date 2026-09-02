from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable

from netaudio.commands.meter_models import (
    DETAILED_ESCALATION_SECONDS,
    MOUSE_WHEEL_ROWS,
    SEARCH_QUERY_LIMIT,
    MeterFilterDialog,
    MeterRowKey,
    MeterViewModel,
    MeterViewport,
    automatic_detailed_metering_targets,
)
from netaudio.commands.meter_rendering import meter_page_size, render_meter_filter_prompt, render_meter_frame
from netaudio.commands.meter_terminal import MeterTerminal


logger = logging.getLogger("netaudio")


async def _consume_events(
    model: MeterViewModel,
    stream_factory: Callable[[], AsyncIterator[dict]],
    cache_reader: Callable[[], Awaitable[dict[str, dict] | None]],
) -> None:
    delay = 0.25
    while True:
        try:
            model.connection_status = "connecting"
            async for event in stream_factory():
                model.connection_status = "connected"
                delay = 0.25
                model.apply_event(event)
            model.connection_status = "disconnected"
        except asyncio.CancelledError:
            raise
        except (OSError, RuntimeError, TimeoutError, ValueError):
            model.connection_status = "reconnecting"
        try:
            cache = await cache_reader()
        except asyncio.CancelledError:
            raise
        except (OSError, RuntimeError, TimeoutError, ValueError):
            cache = None
        if isinstance(cache, dict):
            for server_name, sample in cache.items():
                model.apply_sample(server_name, sample)
            model.connection_status = "cache fallback"
        await asyncio.sleep(delay)
        delay = min(5.0, delay * 2)


async def stop_metering_attempts(
    server_names: list[str],
    client_id: str,
    stop_metering: Callable[[str, str], Awaitable[bool]],
    *,
    attempts: int = 3,
) -> list[str]:
    """Retry idempotent stop requests and return devices that still failed."""
    pending = list(server_names)
    for attempt in range(max(1, attempts)):
        results = await asyncio.gather(
            *(stop_metering(server_name, client_id) for server_name in pending),
            return_exceptions=True,
        )
        pending = [server_name for server_name, result in zip(pending, results) if result is not True]
        if not pending:
            break
        if attempt + 1 < attempts:
            await asyncio.sleep(0.1 * (2**attempt))
    return pending


StartMetering = Callable[[str, str], Awaitable[bool]]
StopMetering = Callable[[str, str], Awaitable[bool]]


async def _request_metering_starts(
    server_names: tuple[str, ...],
    client_id: str,
    start_metering: StartMetering,
) -> list[object]:
    return await asyncio.gather(
        *(start_metering(server_name, client_id) for server_name in server_names),
        return_exceptions=True,
    )


def _meter_display_mode(detailed: bool, attempted_devices: list[str], device_count: int) -> str:
    if detailed or (attempted_devices and len(attempted_devices) == device_count):
        return "detailed"
    if attempted_devices:
        return "mixed"
    return "passive"


@dataclass
class _MeteringState:
    devices: dict
    model: MeterViewModel
    start_metering: StartMetering
    client_id: str
    attempted_devices: list[str]
    display_mode: str
    escalation_done: bool
    escalation_deadline: float
    issued_devices: list[str] = field(default_factory=list)
    start_task: asyncio.Task[list[object]] | None = None
    detailed_status: str = "starting"
    accepted_starts: int = 0

    @classmethod
    def create(
        cls,
        devices: dict,
        model: MeterViewModel,
        start_metering: StartMetering,
        client_id: str,
        detailed: bool,
    ) -> _MeteringState:
        attempted_devices = list(devices) if detailed else automatic_detailed_metering_targets(devices)
        return cls(
            devices=devices,
            model=model,
            start_metering=start_metering,
            client_id=client_id,
            attempted_devices=attempted_devices,
            display_mode=_meter_display_mode(detailed, attempted_devices, len(devices)),
            escalation_done=bool(detailed),
            escalation_deadline=time.monotonic() + DETAILED_ESCALATION_SECONDS,
        )

    def issue_initial_starts(self) -> None:
        # Once these requests are issued, balance every one with a stop even
        # if the local daemon response is lost.
        self.issued_devices = list(self.attempted_devices)
        self._schedule_starts(self.attempted_devices)

    def _schedule_starts(self, server_names: list[str]) -> None:
        self.start_task = asyncio.create_task(
            _request_metering_starts(
                tuple(server_names),
                self.client_id,
                self.start_metering,
            )
        )

    def collect_completed_starts(self) -> None:
        task = self.start_task
        if task is None or not task.done():
            return
        results = task.result()
        self.accepted_starts += sum(result is True for result in results)
        self.detailed_status = f"{self.accepted_starts}/{len(self.issued_devices)} requested"
        self.start_task = None

    def _escalation_is_due(self, now: float) -> bool:
        return not self.escalation_done and self.start_task is None and now >= self.escalation_deadline

    def _silent_devices(self) -> list[str]:
        return sorted(
            server_name
            for server_name, device in self.devices.items()
            if server_name not in self.issued_devices
            and getattr(device, "online", True)
            and getattr(device, "ipv4", None)
            and server_name not in self.model.samples
        )

    def maybe_escalate(self, now: float) -> bool:
        if not self._escalation_is_due(now):
            return False
        self.escalation_done = True
        silent_devices = self._silent_devices()
        if not silent_devices:
            return False
        self.attempted_devices.extend(silent_devices)
        self.issued_devices.extend(silent_devices)
        if self.display_mode == "passive":
            self.display_mode = "mixed"
        self._schedule_starts(silent_devices)
        return True

    async def cancel_pending_starts(self) -> None:
        task = self.start_task
        if task is None:
            return
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@dataclass
class _MeterInteraction:
    model: MeterViewModel
    viewport: MeterViewport
    filter_dialog: MeterFilterDialog | None = None
    search_buffer: str | None = None
    search_original: str = ""
    search_original_key: MeterRowKey | None = None
    last_frame: str | None = None

    @property
    def text_mode(self) -> bool:
        return self.search_buffer is not None or (
            self.filter_dialog is not None and self.filter_dialog.selected_field == 0
        )

    def invalidate(self) -> None:
        self.last_frame = None

    def _handle_wheel(self, key: str | None, page_size: int) -> bool:
        movements = {"wheel_up": -MOUSE_WHEEL_ROWS, "wheel_down": MOUSE_WHEEL_ROWS}
        if key not in movements:
            return False
        self.viewport.move(movements[key], page_size)
        self.invalidate()
        return True

    def _cancel_filter(self, dialog: MeterFilterDialog) -> None:
        dialog.cancel(self.model)
        self.filter_dialog = None

    def _accept_filter(self, dialog: MeterFilterDialog) -> None:
        pin_filtered_device = dialog.selected_field == 0 and bool(dialog.device_query)
        dialog.apply(self.model)
        self.filter_dialog = None
        if not pin_filtered_device:
            return
        selected_key = self.viewport.selected_key
        if selected_key is not None:
            self.model.set_device_filter(selected_key.server_name)

    def _edit_filter_query(self, dialog: MeterFilterDialog, key: str | None) -> bool:
        if dialog.selected_field != 0:
            return False
        if key == "backspace":
            dialog.backspace()
        elif key and key.startswith("text:"):
            dialog.append_text(key[5:])
        else:
            return False
        dialog.apply(self.model)
        return True

    def _handle_filter_key(self, key: str | None) -> None:
        dialog = self.filter_dialog
        if dialog is None:
            return
        if key in ("cancel", "open_filter"):
            self._cancel_filter(dialog)
        elif key == "accept":
            self._accept_filter(dialog)
        elif key in ("up", "down"):
            dialog.move_field(-1 if key == "up" else 1)
        elif key in ("left", "right"):
            dialog.change_value(-1 if key == "left" else 1)
            dialog.apply(self.model)
        elif not self._edit_filter_query(dialog, key):
            return
        self.invalidate()

    def _refresh_search_preview(self, page_size: int) -> None:
        query = self.search_buffer or ""
        self.model.set_search_query(query)
        self.viewport.move_to_match(
            lambda row: self.model.row_matches_search(row, query),
            direction=1,
            page_size=page_size,
            include_current=True,
        )

    def _edit_search_query(self, key: str | None, page_size: int) -> bool:
        current = self.search_buffer or ""
        if key == "backspace":
            self.search_buffer = current[:-1]
        elif key and key.startswith("text:"):
            self.search_buffer = (current + key[5:])[:SEARCH_QUERY_LIMIT]
        else:
            return False
        self._refresh_search_preview(page_size)
        return True

    def _handle_search_key(self, key: str | None, page_size: int) -> None:
        if key == "cancel":
            self.model.set_search_query(self.search_original)
            self.search_buffer = None
            self.viewport.select_key(self.search_original_key, page_size)
        elif key == "accept":
            self.model.set_search_query(self.search_buffer or "")
            self.search_buffer = None
        elif not self._edit_search_query(key, page_size):
            return
        self.invalidate()

    def _open_search(self) -> None:
        self.search_original = self.model.search_query
        self.search_original_key = self.viewport.selected_key
        self.search_buffer = ""
        self.model.set_search_query("")
        self.invalidate()

    def _pin_selected_device(self) -> None:
        selected_key = self.viewport.selected_key
        if selected_key is None:
            return
        self.model.set_device_filter(selected_key.server_name)
        self.invalidate()

    def _move_search_result(self, key: str | None, page_size: int) -> bool:
        if key not in ("next_search", "previous_search") or not self.model.search_query:
            return False
        self.viewport.move_to_match(
            lambda row: self.model.row_matches_search(row, self.model.search_query),
            direction=1 if key == "next_search" else -1,
            page_size=page_size,
        )
        self.invalidate()
        return True

    def _navigate(self, key: str | None, page_size: int) -> bool:
        row_movements = {
            "up": -1,
            "down": 1,
            "page_up": -max(1, page_size),
            "page_down": max(1, page_size),
            "half_page_up": -max(1, page_size // 2),
            "half_page_down": max(1, page_size // 2),
        }
        alignments = {"align_top": "top", "align_center": "center", "align_bottom": "bottom"}
        if key in row_movements:
            self.viewport.move(row_movements[key], page_size)
        elif key in alignments:
            self.viewport.align_selected(alignments[key], page_size)
        elif key == "home":
            self.viewport.home(page_size)
        elif key == "end":
            self.viewport.end(page_size)
        else:
            return False
        self.invalidate()
        return True

    def _handle_normal_key(self, key: str | None, page_size: int) -> None:
        if key == "open_filter":
            self.filter_dialog = MeterFilterDialog(self.model)
            self.invalidate()
        elif key == "open_search":
            self._open_search()
        elif key == "accept":
            self._pin_selected_device()
        elif key in ("cancel", "clear_filter"):
            if self.model.undo_content_filter():
                self.invalidate()
        elif not self._move_search_result(key, page_size):
            self._navigate(key, page_size)

    def handle_key(self, key: str | None, page_size: int) -> bool:
        if key == "quit":
            return True
        if self._handle_wheel(key, page_size):
            return False
        if self.filter_dialog is not None:
            self._handle_filter_key(key)
        elif self.search_buffer is not None:
            self._handle_search_key(key, page_size)
        else:
            self._handle_normal_key(key, page_size)
        return False


def _draw_meter_frame(
    terminal: MeterTerminal,
    metering: _MeteringState,
    interaction: _MeterInteraction,
    *,
    no_color: bool,
) -> int:
    terminal_size = terminal.size
    page_size = meter_page_size(terminal_size.lines)
    interaction.viewport.replace_rows(interaction.model.rows(), page_size)
    frame = render_meter_frame(
        interaction.viewport,
        width=terminal_size.columns,
        height=terminal_size.lines,
        mode=metering.display_mode,
        connection_status=interaction.model.connection_status,
        no_color=no_color,
        detailed_status=metering.detailed_status if metering.attempted_devices else None,
        filter_status=interaction.model.filter_status,
        search_prompt=interaction.search_buffer,
        filter_prompt=(
            render_meter_filter_prompt(interaction.filter_dialog) if interaction.filter_dialog is not None else None
        ),
    )
    if frame != interaction.last_frame:
        terminal.draw(frame)
        interaction.last_frame = frame
    return page_size


@dataclass
class _MeterRuntime:
    terminal: MeterTerminal
    metering: _MeteringState
    interaction: _MeterInteraction
    no_color: bool

    async def _read_and_dispatch(self, page_size: int) -> bool:
        try:
            key = await self.terminal.read_key(0.1, text_mode=self.interaction.text_mode)
        except KeyboardInterrupt:
            return True
        return self.interaction.handle_key(key, page_size)

    async def run(self) -> None:
        with self.terminal:
            # Let an immediately available SSE snapshot land before the first draw.
            await asyncio.sleep(0)
            if self.metering.attempted_devices:
                _draw_meter_frame(
                    self.terminal,
                    self.metering,
                    self.interaction,
                    no_color=self.no_color,
                )
                self.metering.issue_initial_starts()
                # Start the local daemon requests, but never make the UI wait
                # for their response before accepting input.
                await asyncio.sleep(0)

            # The original loop always refreshed once after the optional
            # pre-request frame.
            self.interaction.invalidate()
            while True:
                self.metering.collect_completed_starts()
                if self.metering.maybe_escalate(time.monotonic()):
                    self.interaction.invalidate()
                page_size = _draw_meter_frame(
                    self.terminal,
                    self.metering,
                    self.interaction,
                    no_color=self.no_color,
                )
                if await self._read_and_dispatch(page_size):
                    return


def _meter_dependencies(
    stream_factory: Callable[[], AsyncIterator[dict]] | None,
    cache_reader: Callable[[], Awaitable[dict[str, dict] | None]] | None,
    start_metering: StartMetering | None,
    stop_metering: StopMetering | None,
) -> tuple[
    Callable[[], AsyncIterator[dict]],
    Callable[[], Awaitable[dict[str, dict] | None]],
    StartMetering,
    StopMetering,
]:
    from netaudio.daemon.client import (
        meter_cache_from_daemon,
        meter_start_on_daemon,
        meter_stop_on_daemon,
        stream_daemon_events,
    )

    return (
        stream_factory or stream_daemon_events,
        cache_reader or meter_cache_from_daemon,
        start_metering or meter_start_on_daemon,
        stop_metering or meter_stop_on_daemon,
    )


async def _cleanup_meter_runtime(
    consumer: asyncio.Task[None],
    metering: _MeteringState,
    stop_metering: StopMetering,
) -> None:
    consumer.cancel()
    await asyncio.gather(consumer, return_exceptions=True)
    await metering.cancel_pending_starts()
    failed_stops = await stop_metering_attempts(
        list(reversed(metering.issued_devices)),
        metering.client_id,
        stop_metering,
    )
    if failed_stops:
        logger.warning(
            "Detailed metering cleanup failed for %s (client %s)",
            ", ".join(failed_stops),
            metering.client_id,
        )


@dataclass(frozen=True)
class MeterViewOptions:
    channel_patterns: list[str] | None = None
    detailed: bool = False
    no_color: bool = False
    show_rx: bool = True
    show_tx: bool = True


async def run_meter_tui(
    devices: dict,
    options: MeterViewOptions,
    *,
    terminal: MeterTerminal | None = None,
    stream_factory: Callable[[], AsyncIterator[dict]] | None = None,
    cache_reader: Callable[[], Awaitable[dict[str, dict] | None]] | None = None,
    start_metering: Callable[[str, str], Awaitable[bool]] | None = None,
    stop_metering: Callable[[str, str], Awaitable[bool]] | None = None,
) -> None:
    stream_factory, cache_reader, start_metering, stop_metering = _meter_dependencies(
        stream_factory,
        cache_reader,
        start_metering,
        stop_metering,
    )
    terminal = terminal or MeterTerminal()
    model = MeterViewModel(
        devices,
        show_tx=options.show_tx,
        show_rx=options.show_rx,
        channel_patterns=options.channel_patterns,
    )
    viewport = MeterViewport()
    consumer = asyncio.create_task(_consume_events(model, stream_factory, cache_reader))
    client_id = f"meter_tui:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    metering = _MeteringState.create(devices, model, start_metering, client_id, options.detailed)
    interaction = _MeterInteraction(model, viewport)
    runtime = _MeterRuntime(terminal, metering, interaction, options.no_color)

    try:
        await runtime.run()
    finally:
        await _cleanup_meter_runtime(consumer, metering, stop_metering)
