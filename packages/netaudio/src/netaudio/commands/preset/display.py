from typing import Any

import typer


def show_preset_dry_run(preset_devices: dict[str, dict[str, Any]]) -> None:
    for device_name, config in preset_devices.items():
        typer.echo(f"\n{device_name}:")
        if "preferred_leader" in config:
            typer.echo(f"  preferred leader: {'on' if config['preferred_leader'] else 'off'}")
        if "sample_rate" in config:
            typer.echo(f"  sample rate: {config['sample_rate']}")
        if "encoding" in config:
            typer.echo(f"  encoding: {config['encoding']}")
        if "latency" in config:
            typer.echo(f"  latency: {config['latency']:g} ms")
        if "interface_mode" in config:
            mode = config["interface_mode"]
            if mode == "static":
                typer.echo(
                    f"  interface: static {config.get('ip_address', '')} mask={config.get('netmask', '')} "
                    f"gw={config.get('gateway', '')} dns={config.get('dns_server', '')}"
                )
            else:
                typer.echo(f"  interface: {mode}")
        if "additional_interfaces" in config:
            count = config["additional_interfaces"]
            typer.echo(f"  additional interfaces: {count} (unsupported for load)")
        if "transmitter_channel_names" in config:
            for channel_number, channel_name in sorted(config["transmitter_channel_names"].items()):
                typer.echo(f"  tx {channel_number}: {channel_name}")
        if "rx_subscriptions" in config:
            for channel_number, subscription in sorted(config["rx_subscriptions"].items()):
                if subscription is None:
                    typer.echo(f"  rx {channel_number}: unsubscribed")
                    continue
                transmitter_device = subscription["tx_device"]
                if transmitter_device == ".":
                    transmitter_device = device_name
                typer.echo(f"  rx {channel_number}: {subscription['tx_channel']}@{transmitter_device}")
