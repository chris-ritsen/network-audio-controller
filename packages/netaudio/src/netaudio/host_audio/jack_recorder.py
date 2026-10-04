from __future__ import annotations

import asyncio
import logging
import math
import multiprocessing
import os
import threading
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("netaudio")

RECORDER_CLIENT_NAME = "netaudio-meter-history"
IGNORED_CLIENT_PREFIX = "netaudio-meter"
RESTART_SECONDS = 5.0


def _decibels(value: float) -> float | None:
    if value <= 0:
        return None
    return round(20 * math.log10(value), 1)


class _Bank:
    def __init__(self, numpy, ports: tuple, frames: int) -> None:
        self.ports = ports
        self.block = numpy.zeros((len(ports), frames), numpy.float32)
        self.magnitudes = numpy.zeros((len(ports), frames), numpy.float32)
        self.peaks = numpy.zeros(len(ports), numpy.float64)
        self.energy = numpy.zeros(len(ports), numpy.float64)
        self.frames = 0


class _Recorder:
    def __init__(self, connection) -> None:
        import jack
        import numpy

        self.jack = jack
        self.numpy = numpy
        self.connection = connection
        self.stopped = threading.Event()
        self.changed = threading.Event()
        self.client = jack.Client(
            RECORDER_CLIENT_NAME, no_start_server=True, servername=os.environ.get("JACK_DEFAULT_SERVER") or None
        )
        self.inputs: dict[str, Any] = {}
        self.sources: dict[str, str] = {}
        self.counter = 0
        self.bank = _Bank(numpy, (), self.client.blocksize)
        self.client.set_process_callback(self._process)
        self.client.set_shutdown_callback(self._shutdown)
        self.client.set_port_registration_callback(self._graph_changed, only_available=False)
        self.client.set_port_connect_callback(self._graph_changed, only_available=False)
        self.client.set_port_rename_callback(self._graph_changed, only_available=False)
        self.client.set_blocksize_callback(self._blocksize_changed)
        self.client.activate()

    def _process(self, frames: int) -> None:
        bank = self.bank
        if not bank.ports or bank.block.shape[1] != frames:
            return
        block = bank.block
        for index, port in enumerate(bank.ports):
            block[index] = port.get_array()
        numpy = self.numpy
        numpy.abs(block, out=bank.magnitudes)
        numpy.maximum(bank.peaks, bank.magnitudes.max(axis=1), out=bank.peaks)
        bank.energy += numpy.einsum("ij,ij->i", block, block)
        bank.frames += frames

    def _shutdown(self, status, reason) -> None:
        self.stopped.set()
        self.changed.set()

    def _graph_changed(self, *arguments) -> None:
        self.changed.set()

    def _blocksize_changed(self, blocksize: int) -> None:
        self.changed.set()

    def _period(self) -> float:
        return self.client.blocksize / self.client.samplerate

    def _wanted(self) -> list[str]:
        wanted = []
        for port in self.client.get_ports(is_audio=True, is_output=True):
            if port.name.startswith(IGNORED_CLIENT_PREFIX):
                continue
            if any(
                not connected.name.startswith(IGNORED_CLIENT_PREFIX)
                for connected in self.client.get_all_connections(port)
            ):
                wanted.append(port.name)
        return wanted

    def _update_ports(self) -> None:
        jack = self.jack
        wanted = self._wanted()
        removed = [name for name in self.inputs if name not in wanted]
        for name in wanted:
            if name in self.inputs:
                continue
            self.counter += 1
            try:
                port: Any = self.client.inports.register(f"in_{self.counter}")
            except jack.JackError as exception:
                logger.debug("The level recorder could not add a port for %s: %s", name, exception)
                continue
            try:
                self.client.connect(name, port)
            except jack.JackError as exception:
                logger.debug("The level recorder could not follow %s: %s", name, exception)
                port.unregister()
                continue
            self.inputs[name] = port
            self.sources[port.name] = name
        old = self.bank
        names = [name for name in wanted if name in self.inputs]
        bank = _Bank(self.numpy, tuple(self.inputs[name] for name in names), self.client.blocksize)
        previous = {self.sources[port.name]: index for index, port in enumerate(old.ports)}
        for index, name in enumerate(names):
            if name in previous and old.block.shape[1] == bank.block.shape[1]:
                bank.peaks[index] = old.peaks[previous[name]]
                bank.energy[index] = old.energy[previous[name]]
        bank.frames = old.frames
        self.bank = bank
        if removed:
            time.sleep(self._period() * 2)
            for name in removed:
                port = self.inputs.pop(name)
                self.sources.pop(port.name, None)
                port.unregister()

    def _flush(self, second: int) -> None:
        bank = self.bank
        self.bank = _Bank(self.numpy, bank.ports, bank.block.shape[1])
        time.sleep(self._period())
        if not bank.frames:
            return
        levels = {}
        for index, port in enumerate(bank.ports):
            rms = math.sqrt(bank.energy[index] / bank.frames)
            levels[self.sources[port.name]] = [_decibels(float(bank.peaks[index])), _decibels(rms)]
        self.connection.send({"second": second, "levels": levels})

    def run(self) -> None:
        self._update_ports()
        next_second = math.floor(time.time()) + 1
        while not self.stopped.is_set():
            if self.changed.wait(max(next_second - time.time(), 0)):
                self.changed.clear()
                if not self.stopped.is_set():
                    self._update_ports()
                continue
            now = time.time()
            if now - next_second > 1:
                next_second = math.floor(now) + 1
                continue
            self._flush(next_second - 1)
            next_second += 1
        try:
            self.client.deactivate()
            self.client.close()
        except self.jack.JackError as exception:
            logger.debug("Closing the level recorder: %s", exception)


def run_recorder(connection) -> None:
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["OMP_NUM_THREADS"] = "1"
    try:
        import jack
    except ImportError as exception:
        connection.send({"error": f"JACK-Client is not installed: {exception}"})
        return
    try:
        recorder = _Recorder(connection)
    except (ImportError, jack.JackError, OSError) as exception:
        connection.send({"error": str(exception)})
        return
    try:
        recorder.run()
    except BrokenPipeError:
        recorder.client.close()


class JackRecorder:
    def __init__(self, on_levels: Callable[[int, dict], None], available: Callable[[], bool]) -> None:
        self.on_levels = on_levels
        self.available = available
        self.reason: str | None = "not started"
        self.ports = 0
        self.started_at: float | None = None
        self._process = None
        self._connection = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._restart: asyncio.TimerHandle | None = None
        self._running = False
        self._connection_key: object = None
        self._failed_key: object = None

    @property
    def recording(self) -> bool:
        return self._process is not None and self.reason is None

    def start(self, connection_key: object = None) -> None:
        self._running = True
        self._loop = asyncio.get_running_loop()
        if connection_key is not None:
            self._connection_key = connection_key
        if self._process is not None or not self.available():
            return
        if self._failed_key is not None and self._failed_key == self._connection_key:
            return
        context = multiprocessing.get_context("spawn")
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(target=run_recorder, args=(sender,), name=RECORDER_CLIENT_NAME, daemon=True)
        process.start()
        sender.close()
        self._process, self._connection = process, receiver
        self.reason = None
        self.started_at = time.time()
        self._loop.add_reader(receiver.fileno(), self._readable)
        logger.info("Host audio: recording JACK levels")

    def _readable(self) -> None:
        connection = self._connection
        if connection is None:
            return
        try:
            message = connection.recv()
        except (EOFError, OSError):
            self._ended("the level recorder stopped")
            return
        if "error" in message:
            logger.warning("Host audio: cannot record JACK levels: %s", message["error"])
            self._failed_key = self._connection_key
            self._ended(message["error"], restart=False)
            return
        self.ports = len(message["levels"])
        self.on_levels(message["second"], message["levels"])

    def _ended(self, reason: str, restart: bool = True) -> None:
        if self._connection is not None and self._loop is not None:
            self._loop.remove_reader(self._connection.fileno())
            self._connection.close()
        if self._process is not None:
            self._process.join(0)
        self._connection = None
        self._process = None
        self.reason = reason
        self.ports = 0
        if restart and self._running and self._loop is not None and self._restart is None:
            self._restart = self._loop.call_later(RESTART_SECONDS, self._restart_now)

    def _restart_now(self) -> None:
        self._restart = None
        if self._running:
            self.start()

    def stop(self) -> None:
        self._running = False
        if self._restart is not None:
            self._restart.cancel()
            self._restart = None
        process = self._process
        if self._connection is not None and self._loop is not None:
            self._loop.remove_reader(self._connection.fileno())
            self._connection.close()
        self._connection = None
        self._process = None
        if process is not None:
            process.terminate()
            process.join(1)
        self.reason = "stopped"
