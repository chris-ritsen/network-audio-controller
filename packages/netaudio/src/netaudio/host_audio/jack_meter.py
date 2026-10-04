from __future__ import annotations

import asyncio
import logging
import math
import multiprocessing
import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from operator import mul

logger = logging.getLogger("netaudio")

METER_CLIENT_NAME = "netaudio-meter"
SETTLE_SECONDS = 0.1
DEFAULT_WINDOW_SECONDS = 0.5
MAXIMUM_WINDOW_SECONDS = 5.0
IDLE_CLOSE_SECONDS = 60.0


def _decibels(value: float) -> float | None:
    if value <= 0:
        return None
    return round(20 * math.log10(value), 1)


class _MeterClient:
    def __init__(self, jack) -> None:
        self.stopped = False
        self.active: tuple = ()
        self.client = jack.Client(
            METER_CLIENT_NAME, no_start_server=True, servername=os.environ.get("JACK_DEFAULT_SERVER") or None
        )
        self.client.set_process_callback(self._process)
        self.client.set_shutdown_callback(self._shutdown)
        self.client.activate()

    def _process(self, frames: int) -> None:
        for port, total in self.active:
            samples = memoryview(port.get_buffer()).cast("f")
            peak = max(max(samples), -min(samples))
            if peak > total[0]:
                total[0] = peak
            total[1] += math.fsum(map(mul, samples, samples))
            total[2] += len(samples)

    def _shutdown(self, status, reason) -> None:
        self.stopped = True

    def period_seconds(self) -> float:
        return self.client.blocksize / self.client.samplerate


_meter_client: _MeterClient | None = None
_meter_lock = threading.Lock()
_idle_timer: threading.Timer | None = None


def _close_idle_client() -> None:
    global _meter_client
    import jack

    with _meter_lock:
        meter = _meter_client
        _meter_client = None
        if meter is not None:
            try:
                meter.client.close()
            except jack.JackError as exception:
                logger.debug("Closing the idle JACK meter client: %s", exception)


def _client(jack) -> _MeterClient:
    global _meter_client
    if _meter_client is not None and _meter_client.stopped:
        try:
            _meter_client.client.close()
        except jack.JackError as exception:
            logger.debug("Closing the stopped JACK meter client: %s", exception)
        _meter_client = None
    if _meter_client is None:
        _meter_client = _MeterClient(jack)
    return _meter_client


def measure(targets: dict[str, list[str]], seconds: float) -> dict[str, dict]:
    global _idle_timer
    import jack

    with _meter_lock:
        if _idle_timer is not None:
            _idle_timer.cancel()
            _idle_timer = None
        try:
            return _measure(jack, targets, seconds)
        except jack.JackError as exception:
            return {target: {"error": f"cannot meter JACK ports: {exception}"} for target in targets}
        finally:
            _idle_timer = threading.Timer(IDLE_CLOSE_SECONDS, _close_idle_client)
            _idle_timer.daemon = True
            _idle_timer.start()


def _measure(jack, targets: dict[str, list[str]], seconds: float) -> dict[str, dict]:
    meter = _client(jack)
    inputs = {}
    totals: dict[str, list[float]] = {}
    try:
        for index, target in enumerate(targets):
            inputs[target] = meter.client.inports.register(f"meter_{index + 1}")
            totals[target] = [0.0, 0.0, 0.0]
        unreachable = {}
        for target, sources in targets.items():
            for source in sources:
                try:
                    meter.client.connect(source, inputs[target])
                except jack.JackError as exception:
                    unreachable[target] = str(exception)
        time.sleep(SETTLE_SECONDS)
        meter.active = tuple((inputs[target], totals[target]) for target in targets)
        time.sleep(seconds)
        meter.active = ()
        time.sleep(meter.period_seconds() * 2)
    finally:
        meter.active = ()
        for port in inputs.values():
            try:
                port.unregister()
            except jack.JackError as exception:
                logger.debug("Removing JACK meter port %s: %s", port.name, exception)
    results = {}
    for target, (peak, energy, count) in totals.items():
        if target in unreachable:
            results[target] = {"error": unreachable[target]}
            continue
        if not targets[target]:
            results[target] = {"connected": False}
            continue
        rms = math.sqrt(energy / count) if count else 0.0
        results[target] = {
            "peak_dbfs": _decibels(peak),
            "rms_dbfs": _decibels(rms),
            "frames": int(count),
        }
    return results


class JackMeter:
    def __init__(self):
        self._pool: ProcessPoolExecutor | None = None

    def _executor(self) -> ProcessPoolExecutor:
        if self._pool is None:
            self._pool = ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"))
        return self._pool

    async def measure(self, targets: dict[str, list[str]], seconds: float = DEFAULT_WINDOW_SECONDS) -> dict[str, dict]:
        if not targets:
            return {}
        window = min(max(seconds, 0.05), MAXIMUM_WINDOW_SECONDS)
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(self._executor(), measure, targets, window)
        except BrokenProcessPool:
            logger.warning("The JACK meter process exited; starting a new one")
            self._pool = None
            return await loop.run_in_executor(self._executor(), measure, targets, window)

    def stop(self) -> None:
        pool = self._pool
        self._pool = None
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
