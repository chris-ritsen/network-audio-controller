from __future__ import annotations

import asyncio

import typer

from netaudio._common import _command_context, CapabilityProbeTimeout
from netaudio._common_output import output_table
from netaudio._common_selection import filter_devices, sort_devices


def network_status(
    timeout: float = typer.Option(
        2.0,
        "--timeout",
        min=0.1,
        help="Per-probe network-status response timeout in seconds.",
    ),
):
    """Probe Controller-compatible link and switch-configuration status."""

    async def run():
        async with _command_context() as (devices, send):
            filtered = filter_devices(devices)
            if not filtered:
                typer.echo("Error: no devices matched.", err=True)
                raise typer.Exit(code=1)

            async def probe(server_name, device):
                if device.ipv4 is None:
                    return server_name, device, None, None

                async def capture(operation):
                    try:
                        return await operation(device.ipv4, timeout=timeout)
                    except CapabilityProbeTimeout:
                        return None

                link_status, switch_configuration = await asyncio.gather(
                    capture(send.probe_link_status),
                    capture(send.probe_switch_configuration),
                )
                return server_name, device, link_status, switch_configuration

            results = await asyncio.gather(
                *(probe(server_name, device) for server_name, device in sort_devices(filtered))
            )
            headers = [
                "Name",
                "IP Address",
                "Record",
                "Label",
                "Link",
                "Speed",
                "Status Word",
                "Size",
                "Pointer",
                "Prefix Words",
                "Trailing Bytes",
                "Switch Mode",
                "Switch Mode Codes",
                "Available Switch Modes",
                "Raw Record",
            ]
            rows = []
            json_data = {}
            for server_name, device, link_status, switch_configuration in results:
                device_name = device.name or server_name
                address = str(device.ipv4) if device.ipv4 is not None else None
                json_data[server_name] = {
                    "name": device.name,
                    "server_name": server_name,
                    "ipv4": address,
                    "manufacturer": device.manufacturer,
                    "dante_model": device.dante_model,
                    "available": link_status is not None or switch_configuration is not None,
                    "link_status_available": link_status is not None,
                    "link_status": link_status.to_dict() if link_status is not None else None,
                    "switch_configuration_available": switch_configuration is not None,
                    "switch_configuration": switch_configuration,
                }

                switch_mode = ""
                switch_mode_codes = ""
                available_switch_modes = ""
                if switch_configuration is not None:
                    choices = {choice["code"]: choice["label"] for choice in switch_configuration["choices"]}
                    mode_codes = switch_configuration["mode_codes_at_record_offsets_20_and_22"]
                    selected_labels = [choices.get(code, f"unknown 0x{code:04X}") for code in mode_codes]
                    switch_mode = selected_labels[0] if len(set(mode_codes)) == 1 else " / ".join(selected_labels)
                    switch_mode_codes = " ".join(f"0x{code:04X}" for code in mode_codes)
                    available_switch_modes = ", ".join(f"0x{code:04X} {label}" for code, label in choices.items())

                if link_status is None:
                    rows.append(
                        [
                            device_name,
                            address or "",
                            "",
                            "",
                            "no response",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            switch_mode,
                            switch_mode_codes,
                            available_switch_modes,
                            "",
                        ]
                    )
                    continue
                for record_index, record in enumerate(link_status.records):
                    rows.append(
                        [
                            device_name,
                            address or "",
                            str(record.record_index),
                            record.label or "",
                            "up" if record.link_up else "down",
                            f"{record.link_speed_megabits_per_second} Mbps",
                            f"0x{record.raw_link_status_word:08X}",
                            str(record.record_size_bytes),
                            f"0x{record.record_pointer:04X}",
                            " ".join(f"0x{word:08X}" for word in record.unmapped_prefix_words),
                            record.unmapped_trailing_hexadecimal,
                            switch_mode if record_index == 0 else "",
                            switch_mode_codes if record_index == 0 else "",
                            available_switch_modes if record_index == 0 else "",
                            record.raw_record_hexadecimal,
                        ]
                    )

            output_table(headers, rows, json_data=json_data)

    asyncio.run(run())
