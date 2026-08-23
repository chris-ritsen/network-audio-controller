from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
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
        except Exception:
            model.connection_status = "reconnecting"
        try:
            cache = await cache_reader()
        except asyncio.CancelledError:
            raise
        except Exception:
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


async def run_meter_tui(
    devices: dict,
    *,
    show_tx: bool,
    show_rx: bool,
    channel_patterns: list[str] | None,
    detailed: bool,
    no_color: bool,
    terminal: MeterTerminal | None = None,
    stream_factory: Callable[[], AsyncIterator[dict]] | None = None,
    cache_reader: Callable[[], Awaitable[dict[str, dict] | None]] | None = None,
    start_metering: Callable[[str, str], Awaitable[bool]] | None = None,
    stop_metering: Callable[[str, str], Awaitable[bool]] | None = None,
) -> None:
    from netaudio.daemon.client import (
        meter_cache_from_daemon,
        meter_start_on_daemon,
        meter_stop_on_daemon,
        stream_daemon_events,
    )

    terminal = terminal or MeterTerminal()
    stream_factory = stream_factory or stream_daemon_events
    cache_reader = cache_reader or meter_cache_from_daemon
    start_metering = start_metering or meter_start_on_daemon
    stop_metering = stop_metering or meter_stop_on_daemon

    model = MeterViewModel(
        devices,
        show_tx=show_tx,
        show_rx=show_rx,
        channel_patterns=channel_patterns,
    )
    viewport = MeterViewport()
    consumer = asyncio.create_task(_consume_events(model, stream_factory, cache_reader))
    client_id = f"meter_tui:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    attempted_devices = list(devices) if detailed else automatic_detailed_metering_targets(devices)
    if detailed or (attempted_devices and len(attempted_devices) == len(devices)):
        display_mode = "detailed"
    elif attempted_devices:
        display_mode = "mixed"
    else:
        display_mode = "passive"
    issued_devices: list[str] = []
    start_task: asyncio.Task[list[object]] | None = None
    detailed_status = "starting"
    accepted_starts = 0
    escalation_done = bool(detailed)
    escalation_deadline = time.monotonic() + DETAILED_ESCALATION_SECONDS
    filter_dialog: MeterFilterDialog | None = None
    search_buffer: str | None = None
    search_original = ""
    search_original_key: MeterRowKey | None = None

    try:
        with terminal:
            # Let an immediately available SSE snapshot land before the first draw.
            await asyncio.sleep(0)
            if attempted_devices:
                initial_rows = model.rows()
                viewport.replace_rows(initial_rows, meter_page_size(terminal.size.lines))
                terminal.draw(
                    render_meter_frame(
                        viewport,
                        width=terminal.size.columns,
                        height=terminal.size.lines,
                        mode=display_mode,
                        connection_status=model.connection_status,
                        no_color=no_color,
                        detailed_status=detailed_status,
                        filter_status=model.filter_status,
                    )
                )
                # Once these requests are issued, balance every one with a
                # stop even if the local relay response is lost.
                issued_devices = list(attempted_devices)

                async def request_starts() -> list[object]:
                    return await asyncio.gather(
                        *(start_metering(server_name, client_id) for server_name in attempted_devices),
                        return_exceptions=True,
                    )

                start_task = asyncio.create_task(request_starts())
                # Start the local relay requests, but never make the UI wait
                # for their response before accepting input.
                await asyncio.sleep(0)

            last_frame = None
            while True:
                if start_task is not None and start_task.done():
                    results = start_task.result()
                    accepted_starts += sum(result is True for result in results)
                    detailed_status = f"{accepted_starts}/{len(issued_devices)} requested"
                    start_task = None
                if not escalation_done and start_task is None and time.monotonic() >= escalation_deadline:
                    escalation_done = True
                    silent_devices = sorted(
                        server_name
                        for server_name, device in devices.items()
                        if server_name not in issued_devices
                        and getattr(device, "online", True)
                        and getattr(device, "ipv4", None)
                        and server_name not in model.samples
                    )
                    if silent_devices:
                        attempted_devices = list(attempted_devices) + silent_devices
                        issued_devices.extend(silent_devices)
                        if display_mode == "passive":
                            display_mode = "mixed"

                        async def request_escalation_starts(targets=tuple(silent_devices)) -> list[object]:
                            return await asyncio.gather(
                                *(start_metering(server_name, client_id) for server_name in targets),
                                return_exceptions=True,
                            )

                        start_task = asyncio.create_task(request_escalation_starts())
                        last_frame = None
                terminal_size = terminal.size
                page_size = meter_page_size(terminal_size.lines)
                viewport.replace_rows(model.rows(), page_size)
                frame = render_meter_frame(
                    viewport,
                    width=terminal_size.columns,
                    height=terminal_size.lines,
                    mode=display_mode,
                    connection_status=model.connection_status,
                    no_color=no_color,
                    detailed_status=detailed_status if attempted_devices else None,
                    filter_status=model.filter_status,
                    search_prompt=search_buffer,
                    filter_prompt=render_meter_filter_prompt(filter_dialog) if filter_dialog is not None else None,
                )
                if frame != last_frame:
                    terminal.draw(frame)
                    last_frame = frame

                try:
                    text_mode = search_buffer is not None or (
                        filter_dialog is not None and filter_dialog.selected_field == 0
                    )
                    key = await terminal.read_key(0.1, text_mode=text_mode)
                except KeyboardInterrupt:
                    break
                if key == "quit":
                    break
                if key == "wheel_up":
                    viewport.move(-MOUSE_WHEEL_ROWS, page_size)
                    last_frame = None
                    continue
                if key == "wheel_down":
                    viewport.move(MOUSE_WHEEL_ROWS, page_size)
                    last_frame = None
                    continue
                if filter_dialog is not None:
                    if key == "cancel" or key == "open_filter":
                        filter_dialog.cancel(model)
                        filter_dialog = None
                        last_frame = None
                    elif key == "accept":
                        pin_filtered_device = filter_dialog.selected_field == 0 and bool(filter_dialog.device_query)
                        filter_dialog.apply(model)
                        filter_dialog = None
                        if pin_filtered_device:
                            selected_key = viewport.selected_key
                            if selected_key is not None:
                                model.set_device_filter(selected_key.server_name)
                        last_frame = None
                    elif key == "up":
                        filter_dialog.move_field(-1)
                        last_frame = None
                    elif key == "down":
                        filter_dialog.move_field(1)
                        last_frame = None
                    elif key == "left":
                        filter_dialog.change_value(-1)
                        filter_dialog.apply(model)
                        last_frame = None
                    elif key == "right":
                        filter_dialog.change_value(1)
                        filter_dialog.apply(model)
                        last_frame = None
                    elif key == "backspace" and filter_dialog.selected_field == 0:
                        filter_dialog.backspace()
                        filter_dialog.apply(model)
                        last_frame = None
                    elif key and key.startswith("text:") and filter_dialog.selected_field == 0:
                        filter_dialog.append_text(key[5:])
                        filter_dialog.apply(model)
                        last_frame = None
                    continue
                if search_buffer is not None:
                    if key == "cancel":
                        model.set_search_query(search_original)
                        search_buffer = None
                        viewport.select_key(search_original_key, page_size)
                        last_frame = None
                    elif key == "accept":
                        model.set_search_query(search_buffer)
                        search_buffer = None
                        last_frame = None
                    elif key == "backspace":
                        search_buffer = search_buffer[:-1]
                        model.set_search_query(search_buffer)
                        viewport.move_to_match(
                            lambda row: model.row_matches_search(row, search_buffer or ""),
                            direction=1,
                            page_size=page_size,
                            include_current=True,
                        )
                        last_frame = None
                    elif key and key.startswith("text:"):
                        search_buffer = (search_buffer + key[5:])[:SEARCH_QUERY_LIMIT]
                        model.set_search_query(search_buffer)
                        viewport.move_to_match(
                            lambda row: model.row_matches_search(row, search_buffer or ""),
                            direction=1,
                            page_size=page_size,
                            include_current=True,
                        )
                        last_frame = None
                    continue
                if key == "open_filter":
                    filter_dialog = MeterFilterDialog(model)
                    last_frame = None
                    continue
                if key == "open_search":
                    search_original = model.search_query
                    search_original_key = viewport.selected_key
                    search_buffer = ""
                    model.set_search_query("")
                    last_frame = None
                    continue
                if key == "accept":
                    selected_key = viewport.selected_key
                    if selected_key is not None:
                        model.set_device_filter(selected_key.server_name)
                        last_frame = None
                    continue
                if key in ("cancel", "clear_filter"):
                    if model.undo_content_filter():
                        last_frame = None
                    continue
                if key == "next_search" and model.search_query:
                    viewport.move_to_match(
                        lambda row: model.row_matches_search(row, model.search_query),
                        direction=1,
                        page_size=page_size,
                    )
                    last_frame = None
                    continue
                if key == "previous_search" and model.search_query:
                    viewport.move_to_match(
                        lambda row: model.row_matches_search(row, model.search_query),
                        direction=-1,
                        page_size=page_size,
                    )
                    last_frame = None
                    continue
                if key == "up":
                    viewport.move(-1, page_size)
                    last_frame = None
                elif key == "down":
                    viewport.move(1, page_size)
                    last_frame = None
                elif key == "page_up":
                    viewport.move(-max(1, page_size), page_size)
                    last_frame = None
                elif key == "page_down":
                    viewport.move(max(1, page_size), page_size)
                    last_frame = None
                elif key == "half_page_up":
                    viewport.move(-max(1, page_size // 2), page_size)
                    last_frame = None
                elif key == "half_page_down":
                    viewport.move(max(1, page_size // 2), page_size)
                    last_frame = None
                elif key == "align_top":
                    viewport.align_selected("top", page_size)
                    last_frame = None
                elif key == "align_center":
                    viewport.align_selected("center", page_size)
                    last_frame = None
                elif key == "align_bottom":
                    viewport.align_selected("bottom", page_size)
                    last_frame = None
                elif key == "home":
                    viewport.home(page_size)
                    last_frame = None
                elif key == "end":
                    viewport.end(page_size)
                    last_frame = None
    finally:
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        if start_task is not None:
            if not start_task.done():
                start_task.cancel()
            await asyncio.gather(start_task, return_exceptions=True)
        failed_stops = await stop_metering_attempts(
            list(reversed(issued_devices)),
            client_id,
            stop_metering,
        )
        if failed_stops:
            logger.warning(
                "Detailed metering cleanup failed for %s (client %s)",
                ", ".join(failed_stops),
                client_id,
            )
