from __future__ import annotations

import os
from pathlib import Path
from typing import NoReturn
from uuid import uuid4

import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.commands.endpoint_worker import execute

app = typer.Typer(
    help="Share live video between computers, display it, or publish a virtual camera.",
    no_args_is_help=True,
    context_settings=HELP_CONTEXT_SETTINGS,
)

DEFAULT_DURATION_SECONDS = 28_800


def _output_directory(output: Path | None) -> Path:
    if output is not None:
        return output

    state = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    directory = state / "netaudio" / "video"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / str(uuid4())


@app.command(help="Receive video from a NetAudio computer for display or use as an application camera.")
def listen(
    bind: str = typer.Option(..., help="This computer's IPv4 address and port, for example 192.168.1.20:47770."),
    from_address: str = typer.Option(..., "--from", help="IPv4 address of the one permitted sender."),
    display: bool = typer.Option(False, help="Open a video window. Press F for fullscreen, Escape to close."),
    fullscreen: bool = typer.Option(False, help="Open the video viewer fullscreen."),
    camera: Path | None = typer.Option(
        None, help="Existing V4L2 loopback output, for example /dev/video20. Select this camera in OBS."
    ),
    output: Path | None = typer.Option(
        None,
        help="New directory for session diagnostics; defaults to the user state directory. No frame files are recorded.",
    ),
    duration: int = typer.Option(
        DEFAULT_DURATION_SECONDS,
        min=1,
        max=DEFAULT_DURATION_SECONDS,
        help="Maximum session seconds. Either peer can stop with Ctrl-C.",
    ),
) -> NoReturn:
    if not (display or fullscreen or camera):
        raise typer.BadParameter("Select --display, --fullscreen or --camera.")

    arguments = [
        "video-listen",
        "--bind",
        bind,
        "--from",
        from_address,
        "--output",
        str(_output_directory(output)),
        "--duration",
        str(duration),
    ]

    if display:
        arguments.append("--display")

    if fullscreen:
        arguments.append("--fullscreen")

    if camera is not None:
        arguments.extend(["--camera", str(camera)])

    execute(arguments)


@app.command(
    help="Send live desktop, window, camera or video-file pictures to another NetAudio computer. Video only, 1080p30."
)
def share(
    receiver: str = typer.Option(..., help="Receiver IPv4 address and port, for example 192.168.1.20:47770."),
    desktop: bool = typer.Option(False, help="Capture the X11 desktop."),
    window: int | None = typer.Option(None, min=1, help="Capture this X11 window identifier with --desktop."),
    file: Path | None = typer.Option(
        None, exists=True, dir_okay=False, help="Play an ordinary video file as the source. Its audio is not played."
    ),
    camera: Path | None = typer.Option(None, help="Read a V4L2 camera, including an OBS virtual camera."),
    output: Path | None = typer.Option(
        None, help="New session diagnostics directory; defaults to the user state directory."
    ),
    duration: int = typer.Option(
        DEFAULT_DURATION_SECONDS,
        min=1,
        max=DEFAULT_DURATION_SECONDS,
        help="Maximum session seconds. Ctrl-C closes the stream.",
    ),
) -> NoReturn:
    if sum([desktop, file is not None, camera is not None]) != 1:
        raise typer.BadParameter("Select exactly one of --desktop, --file or --camera.")

    if window is not None and not desktop:
        raise typer.BadParameter("--window requires --desktop.")

    arguments = [
        "video-share",
        "--receiver",
        receiver,
        "--output",
        str(_output_directory(output)),
        "--duration",
        str(duration),
    ]

    if desktop:
        arguments.append("--desktop")

    if window is not None:
        arguments.extend(["--window", str(window)])

    if file is not None:
        arguments.extend(["--file", str(file)])

    if camera is not None:
        arguments.extend(["--camera", str(camera)])

    execute(arguments)


@app.command(help="Print the receiver session JSON schema, including required fields and their types.")
def session_schema() -> NoReturn:
    execute(["video-session-schema"])


@app.command(help="Receive an existing configured H.264 stream into NV12 frames and an identity/timing manifest.")
def receive(
    configuration: Path = typer.Option(
        ...,
        exists=True,
        dir_okay=False,
        help="Current session JSON: authority and value sources, interface/address, exact source/destination, payload_type, synchronization_source, width/height, cadence, colorimetry, profile/level, SPS/PPS hex, finite limits and playout policy. Requires a video-enabled build and GStreamer. Does not discover, allocate or display video.",
    ),
    output: Path = typer.Option(
        ..., help="New output directory for NV12 frame files, frame identities and the final manifest."
    ),
    duration: int = typer.Option(
        ..., min=1, help="Maximum recording duration in seconds; authority expiry or interruption stops earlier."
    ),
) -> NoReturn:
    execute(
        ["video-receive", "--configuration", str(configuration), "--output", str(output), "--duration", str(duration)]
    )


@app.command(help="Print the H.264 transmitter session JSON schema.")
def transmit_schema() -> NoReturn:
    execute(["video-transmit-schema"])


@app.command(help="Encode a finite 1080p30 BGRA file and transmit H.264 RTP to one configured receiver.")
def transmit(
    configuration: Path = typer.Option(
        ...,
        exists=True,
        dir_okay=False,
        help="Current source session JSON; see transmit-schema for required authority, endpoint, encoder, RTP and limit fields.",
    ),
    input: Path = typer.Option(
        ...,
        exists=True,
        dir_okay=False,
        help="Finite file of tightly packed, top-down 1920x1080 BGRA frames. Alpha is discarded.",
    ),
    output: Path = typer.Option(
        ..., help="New directory for actual encoder parameters, frame timing and the final manifest."
    ),
    duration: int = typer.Option(
        ..., min=1, help="Maximum run duration in seconds; input completion or authority expiry stops earlier."
    ),
    prepare_only: bool = typer.Option(
        False, help="Encode the first frame and report actual parameters without opening a media socket."
    ),
) -> NoReturn:
    arguments = [
        "video-transmit",
        "--configuration",
        str(configuration),
        "--input",
        str(input),
        "--output",
        str(output),
        "--duration",
        str(duration),
    ]

    if prepare_only:
        arguments.append("--prepare-only")

    execute(arguments)
