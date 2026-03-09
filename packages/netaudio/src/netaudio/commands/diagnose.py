from __future__ import annotations

import asyncio
from typing import Optional

import typer

from netaudio.icons import icon


app = typer.Typer(help="Device diagnostics.", no_args_is_help=True)


DIAGNOSE_QUERIES = [
    ("device_info", "command_device_info", {}),
    ("device_name", "command_device_name", {}),
    ("channel_count", "command_channel_count", {}),
    ("device_settings", "command_device_settings", {}),
    ("tx_channels_p0", "command_transmitters", {"page": 0}),
    ("tx_channels_p1", "command_transmitters", {"page": 1}),
    ("tx_channels_p2", "command_transmitters", {"page": 2}),
    ("tx_channels_p3", "command_transmitters", {"page": 3}),
    ("tx_channel_names_p0", "command_transmitters", {"page": 0, "friendly_names": True}),
    ("tx_channel_names_p1", "command_transmitters", {"page": 1, "friendly_names": True}),
    ("tx_channel_names_p2", "command_transmitters", {"page": 2, "friendly_names": True}),
    ("tx_channel_names_p3", "command_transmitters", {"page": 3, "friendly_names": True}),
    ("rx_channels_p0", "command_receivers", {"page": 0}),
    ("rx_channels_p1", "command_receivers", {"page": 1}),
    ("rx_channels_p2", "command_receivers", {"page": 2}),
    ("rx_channels_p3", "command_receivers", {"page": 3}),
]


@app.command("run")
def diagnose_run(
    device_ip: str = typer.Option(..., "--device-ip", "-d", help="Target device IP address."),
    timeout: float = typer.Option(2.0, "--timeout", help="Response timeout per query."),
    session_name: Optional[str] = typer.Option(None, "--session-name", help="Override session name."),
    output_dir: Optional[str] = typer.Option(None, "--output-dir", help="Output directory for bundle."),
    db: Optional[str] = typer.Option(None, "--db", help="SQLite database path."),
    config: Optional[str] = typer.Option(None, "--config", help="Capture config TOML path."),
    profile: Optional[str] = typer.Option(None, "--profile", help="Capture config profile name."),
):
    asyncio.run(_run_diagnose(
        device_ip=device_ip,
        timeout=timeout,
        session_name=session_name or f"diagnose_{device_ip.replace('.', '_')}",
        output_dir=output_dir,
        config=config,
        profile=profile,
        db_override=db,
    ))


async def _run_diagnose(
    device_ip: str,
    timeout: float,
    session_name: str,
    output_dir: str | None,
    config: str | None,
    profile: str | None,
    db_override: str | None,
):
    from netaudio_lib.dante.device_commands import DanteDeviceCommands
    from netaudio_lib.dante.protocol_verifier import ProtocolVerifier

    commands = DanteDeviceCommands()

    async with ProtocolVerifier(
        device_ip=device_ip,
        session_name=session_name,
        config=config,
        profile=profile,
        db=db_override,
        output_dir=output_dir,
        category="diagnostic",
    ) as verifier:

        verifier.marker(
            "diagnose_started",
            marker_type="system",
            note=f"Device diagnostic: {device_ip}",
        )

        print(f"{icon('diagnostic')}Diagnosing {device_ip}")
        print()

        success = 0
        failed = 0

        for label, method_name, kwargs in DIAGNOSE_QUERIES:
            method = getattr(commands, method_name)
            command_tuple = method(**kwargs)

            response = await verifier.send_command(
                command_tuple, timeout=timeout, label=label,
            )

            if response is None:
                print(f"  {icon('timeout')}[{label:30s}]  TIMEOUT")
                failed += 1
            else:
                opcode_hex = ""
                if len(response) >= 8:
                    import struct
                    opcode = struct.unpack(">H", response[6:8])[0]
                    opcode_hex = f"0x{opcode:04X}"
                print(f"  {icon('success')}[{label:30s}]  {len(response):>5d}B  {opcode_hex}")
                success += 1

        print()
        print(f"{icon('diagnostic')}Diagnostic complete: {success} responses, {failed} timeouts")

        verifier.marker(
            "diagnose_finished",
            marker_type="system",
            note=f"Diagnostic complete: {success} responses, {failed} timeouts",
            data={"success": success, "failed": failed},
        )
