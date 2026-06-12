from __future__ import annotations

import asyncio
import logging
import os
import signal
import subprocess
import sys

import typer

logger = logging.getLogger("netaudio")

app = typer.Typer(help="Virtual Dante device.", no_args_is_help=True)

PIDFILE = "/tmp/netaudio-virtual.pid"
LOGFILE = "/tmp/netaudio-virtual.log"


def _read_pid() -> int | None:
    try:
        pid = int(open(PIDFILE).read().strip())
        os.kill(pid, 0)
        return pid
    except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
        return None


def _write_pid():
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))


def _remove_pid():
    try:
        os.unlink(PIDFILE)
    except FileNotFoundError:
        pass


@app.command("start")
def virtual_start(
    name: str = typer.Option("netaudio-virtual", "--name", "-n"),
    model: str = typer.Option("netaudio", "--model"),
    tx: int = typer.Option(2, "--tx"),
    rx: int = typer.Option(2, "--rx"),
    sample_rate: int = typer.Option(48000, "--sample-rate", "-r"),
    interface: str = typer.Option(None, "--interface", "-i"),
    foreground: bool = typer.Option(False, "--foreground", "-f"),
):
    pid = _read_pid()
    if pid:
        typer.echo(f"Already running (PID {pid})")
        raise typer.Exit(1)

    if not foreground:
        cmd = [
            sys.executable, "-m", "netaudio", "virtual", "start",
            "--foreground",
            "--name", name,
            "--model", model,
            "--tx", str(tx),
            "--rx", str(rx),
            "--sample-rate", str(sample_rate),
        ]
        if interface:
            cmd += ["--interface", interface]

        log = open(LOGFILE, "w")
        proc = subprocess.Popen(
            cmd, stdout=log, stderr=log,
            start_new_session=True,
        )
        typer.echo(f"Started virtual device '{name}' (PID {proc.pid})")
        typer.echo(f"  Log: {LOGFILE}")
        return

    from netaudio.dante.virtual_device import VirtualDevice, VirtualDeviceConfig

    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s", force=True)
    logging.getLogger("netaudio").setLevel(logging.DEBUG)
    logging.getLogger("zeroconf").setLevel(logging.WARNING)

    tx_channels = [f"Ch {i+1}" for i in range(tx)]
    rx_channels = [f"Ch {i+1}" for i in range(rx)]

    config = VirtualDeviceConfig(
        name=name,
        model=model,
        tx_channels=tx_channels,
        rx_channels=rx_channels,
        sample_rate=sample_rate,
        interface_ip=interface,
    )

    async def _run():
        _write_pid()
        device = VirtualDevice(config)
        try:
            await device.start()
            ports = [t.get_extra_info("sockname")[1] for t in device._transports]
            typer.echo(f"Virtual device '{name}' running ({tx} TX, {rx} RX, {sample_rate}Hz)")
            typer.echo(f"  MAC: {device.mac}")
            typer.echo(f"  Ports: {ports}")

            stop_event = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop_event.set)

            await stop_event.wait()
        finally:
            await device.stop()
            _remove_pid()

    asyncio.run(_run())


@app.command("stop")
def virtual_stop():
    pid = _read_pid()
    if not pid:
        typer.echo("Not running")
        raise typer.Exit(1)

    os.kill(pid, signal.SIGTERM)
    typer.echo(f"Stopped (PID {pid})")
    _remove_pid()


@app.command("restart")
def virtual_restart(
    name: str = typer.Option("netaudio-virtual", "--name", "-n"),
    model: str = typer.Option("netaudio", "--model"),
    tx: int = typer.Option(2, "--tx"),
    rx: int = typer.Option(2, "--rx"),
    sample_rate: int = typer.Option(48000, "--sample-rate", "-r"),
    interface: str = typer.Option(None, "--interface", "-i"),
):
    pid = _read_pid()
    if pid:
        os.kill(pid, signal.SIGTERM)
        _remove_pid()
        import time
        time.sleep(1)

    virtual_start(
        name=name, model=model, tx=tx, rx=rx,
        sample_rate=sample_rate, interface=interface, foreground=False,
    )


@app.command("status")
def virtual_status():
    pid = _read_pid()
    if pid:
        typer.echo(f"Running (PID {pid})")
        if os.path.exists(LOGFILE):
            typer.echo(f"Log: {LOGFILE}")
    else:
        typer.echo("Not running")


@app.command("logs")
def virtual_logs(
    follow: bool = typer.Option(False, "--follow", "-f"),
    lines: int = typer.Option(50, "--lines", "-n"),
):
    if not os.path.exists(LOGFILE):
        typer.echo("No log file")
        raise typer.Exit(1)

    if follow:
        os.execvp("tail", ["tail", "-f", "-n", str(lines), LOGFILE])
    else:
        os.execvp("tail", ["tail", "-n", str(lines), LOGFILE])
