from __future__ import annotations

import sys
from pathlib import Path
from typing import NoReturn

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.commands.endpoint_worker import execute as _execute

app = typer.Typer(
    help="Serve or receive native network audio, inspect properties and decode audio packets.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)


def _format_arguments(sample_rate: int, sample_bits: int, slots: int) -> list[str]:
    return ["--sample-rate", str(sample_rate), "--sample-bits", str(sample_bits), "--slots", str(slots)]


@app.command(help="List compiled audio backends or the devices and formats of an explicit backend.")
def devices(
    backend: str | None = typer.Option(None, help="Audio backend such as jack, alsa, pulseaudio, pipewire, wasapi or coreaudio."),
) -> NoReturn:
    arguments = ["audio-devices"]

    if backend is not None:
        arguments.extend(["--backend", backend])

    _execute(arguments)


@app.command(help="Run a foreground virtual audio endpoint with native audio devices or explicit WAVE adapters on Linux.")
def serve(
    configuration: Path = typer.Option(
        ...,
        exists=True,
        dir_okay=False,
        help="JSON with endpoint identity, interface, clock, state_directory and audio. Set audio.type=native for live input/output with an explicit backend and device configuration, or audio.type=wave with input and optional output for files. Native JACK exposes unconnected send_1/send_2 and receive_1/receive_2 ports.",
    ),
    duration: int | None = typer.Option(None, min=1, help="Stop after this many seconds; otherwise run until interrupted."),
) -> NoReturn:
    if sys.platform != "linux":
        typer.echo("Error: the virtual endpoint currently requires Linux.", err=True)
        raise typer.Exit(code=1)

    arguments = ["serve", "--configuration", str(configuration)]

    if duration is not None:
        arguments.extend(["--duration", str(duration)])

    _execute(arguments)


@app.command(
    help="Allocate one admitted unicast source flow, record native-v2 PCM and attempt one owned STOP. Changes source flow state."
)
def receive_unicast(
    session: Path = typer.Option(
        ...,
        exists=True,
        dir_okay=False,
        help="Required JSON fields: profile (native_v2_reference1102), control_endpoint, source_address, interface_address, receiver_hostname, receiver_flow_name, source_channel_identifiers, source_slot_capacity, sample_rate, sample_bits, requested_frames_per_packet, maximum_media_datagram_bytes, reply_profile (reference_handle or detailed_interface_outcomes).",
    ),
    duration: int = typer.Option(..., min=1, help="Finite recording duration in seconds after allocation."),
    output: Path = typer.Option(..., help="New directory for PCM, raw media packets, control exchanges and manifest."),
    timeout: int = typer.Option(2, min=1, help="Deadline in seconds for each single control attempt; no retries."),
) -> NoReturn:
    _execute(
        [
            "receive-unicast",
            "--session",
            str(session),
            "--duration",
            str(duration),
            "--output",
            str(output),
            "--timeout",
            str(timeout),
        ]
    )


@app.command(
    help="Read current audio packetization, latency and named clock-domain properties without changing devices."
)
def inspect(
    peer: str = typer.Option(..., help="Current device ARC address and port, ADDRESS:PORT."),
    interface_address: str = typer.Option(..., help="IPv4 address of the local interface."),
    timeout: int = typer.Option(2, min=1, help="Deadline for each read-only request in seconds."),
) -> NoReturn:
    _execute(["inspect", "--peer", peer, "--interface-address", interface_address, "--timeout", str(timeout)])


@app.command(help="Record an existing native-v2 multicast flow into timestamped packets and PCM WAVE segments.")
def receive(
    destination: str = typer.Option(..., help="Existing multicast group and UDP port, ADDRESS:PORT."),
    source: str = typer.Option(..., help="Established sender address and media port, ADDRESS:PORT."),
    interface_address: str = typer.Option(..., help="IPv4 address of the local receiving interface."),
    sample_rate: int = typer.Option(..., min=1, help="Configured sample rate in hertz."),
    sample_bits: int = typer.Option(..., help="Configured signed integer PCM width: 16, 24 or 32."),
    slots: int = typer.Option(..., min=1, help="Configured number of ordered network channel slots."),
    duration: int = typer.Option(..., min=1, help="Finite recording duration in seconds."),
    output: Path = typer.Option(..., help="New directory for packet records, PCM WAVE segments and manifest."),
) -> NoReturn:
    _execute(
        [
            "receive",
            "--destination",
            destination,
            "--source",
            source,
            "--interface-address",
            interface_address,
            *_format_arguments(sample_rate, sample_bits, slots),
            "--duration",
            str(duration),
            "--output",
            str(output),
        ]
    )


@app.command(help="Decode one native-v2 packet using its established sample rate, width and slot count.")
def decode(
    input: Path = typer.Option(..., exists=True, dir_okay=False, help="One complete native-v2 datagram."),
    sample_rate: int = typer.Option(..., min=1, help="Configured sample rate in hertz."),
    sample_bits: int = typer.Option(..., help="Configured signed integer PCM width: 16, 24 or 32."),
    slots: int = typer.Option(..., min=1, help="Configured number of ordered network channel slots."),
) -> NoReturn:
    _execute(["decode", "--input", str(input), *_format_arguments(sample_rate, sample_bits, slots)])
