from __future__ import annotations

import asyncio
import json
import platform
import shutil
import subprocess
from fnmatch import fnmatch
from typing import Any, Optional

import typer

from netaudio._common_cli import HELP_CONTEXT_SETTINGS

from netaudio import __version__

app = typer.Typer(
    help="Report issues with diagnostic context.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)

REPO = "chris-ritsen/network-audio-controller"

PRIVACY_LEVELS = ["minimal", "network", "full"]

PRIVACY_DESCRIPTIONS = {
    "minimal": "Version, platform, device count, models, firmware",
    "network": "Adds IP addresses, MAC addresses, interface config",
    "full": "Adds device names, channel names, subscriptions, routing",
}


def _normalize_report_filter_patterns(device_filter: str) -> list[str]:
    patterns = []
    for term in device_filter.split(","):
        pattern = term.strip().lower()
        if not pattern:
            continue
        if not any(char in pattern for char in "*?["):
            pattern = f"*{pattern}*"
        patterns.append(pattern)
    return patterns


def _device_matches_report_filter(device: dict, device_filter: str) -> bool:
    patterns = _normalize_report_filter_patterns(device_filter)
    if not patterns:
        return True

    candidate_values = [
        device.get("name", ""),
        device.get("server_name", ""),
        device.get("ipv4", ""),
        device.get("mac_address", ""),
        device.get("dante_model", ""),
        device.get("model", ""),
        device.get("manufacturer", ""),
    ]
    mac_address = str(device.get("mac_address", ""))
    candidate_values.append(mac_address.replace(":", "").replace("-", "").replace(".", ""))

    searchable = [str(value).lower() for value in candidate_values if value]
    return any(fnmatch(value, pattern) for pattern in patterns for value in searchable)


def _filter_report_diagnostics(diagnostics: dict, device_filter: str) -> dict:
    devices = diagnostics.get("devices", [])
    filtered_devices = [device for device in devices if _device_matches_report_filter(device, device_filter)]

    filtered = dict(diagnostics)
    filtered["devices"] = filtered_devices
    filtered["device_count"] = len(filtered_devices)
    return filtered


def _gh_available() -> bool:
    return shutil.which("gh") is not None


def _gh_authenticated() -> bool:
    result = subprocess.run(
        ["gh", "auth", "status"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


async def _collect_diagnostics() -> dict:
    from netaudio.dante.application import DanteApplication
    from netaudio.common.app_config import settings
    from netaudio.daemon.client import get_devices_from_daemon
    from netaudio.dante.device_serializer import DanteDeviceSerializer

    diagnostics: dict[str, Any] = {
        "version": __version__,
        "python": platform.python_version(),
        "platform": f"{platform.system()} {platform.release()} {platform.machine()}",
    }

    devices = await get_devices_from_daemon()
    source = "daemon"

    if devices is None:
        source = "direct"
        application = DanteApplication()
        await application.startup()
        try:
            devices = await application.discover_and_populate(timeout=settings.mdns_timeout)
        finally:
            await application.shutdown()

    from netaudio._common_selection import filter_devices

    devices = filter_devices(devices or {})

    diagnostics["source"] = source
    diagnostics["device_count"] = len(devices) if devices else 0

    device_details = []
    if devices:
        serializer = DanteDeviceSerializer()
        for server_name, device in sorted(devices.items()):
            device_json = serializer.to_json(device)
            device_details.append(device_json)

    diagnostics["devices"] = device_details
    return diagnostics


def _filter_device(device: dict, level: str) -> dict:
    if level == "minimal":
        filtered: dict[str, Any] = {}
        dante_model = device.get("dante_model", "")
        model = device.get("model", "")
        filtered["dante_model"] = dante_model or model
        filtered["dante_model_id"] = device.get("dante_model_id", "")
        filtered["firmware_version"] = device.get("firmware_version", "")
        filtered["software_version"] = device.get("software_version", "")
        filtered["sample_rate_hz"] = device.get("sample_rate_hz", "")
        filtered["encoding"] = device.get("encoding")
        filtered["aes67_current"] = device.get("aes67_current")
        filtered["aes67_configured"] = device.get("aes67_configured")
        filtered["aes67_multicast_prefix"] = device.get("aes67_multicast_prefix")
        filtered["sample_rate_pullup_raw_value"] = device.get("sample_rate_pullup_raw_value")
        filtered["requested_sample_rate_pullup_raw_value"] = device.get("requested_sample_rate_pullup_raw_value")
        filtered["supported_sample_rate_pullup_raw_values"] = device.get("supported_sample_rate_pullup_raw_values")
        filtered["preferred_leader"] = device.get("preferred_leader")
        filtered["clock_source_code"] = device.get("clock_source_code")
        filtered["clock_subdomain"] = device.get("clock_subdomain")
        filtered["is_locked"] = device.get("is_locked")
        return filtered

    if level == "network":
        filtered = _filter_device(device, "minimal")
        filtered["ipv4"] = device.get("ipv4", "")
        filtered["mac_address"] = device.get("mac_address", "")
        filtered["manufacturer"] = device.get("manufacturer", "")
        filtered["link_speed_mbps"] = device.get("link_speed_mbps")
        interfaces = device.get("interfaces")
        if interfaces:
            filtered["interfaces"] = interfaces
        pending = device.get("interface_pending_config")
        if pending:
            filtered["interface_pending_config"] = pending
        return filtered

    return dict(device)


def _format_report(
    diagnostics: dict,
    level: str,
    description: str,
    device_filter: str = "",
) -> str:
    diagnostics = _filter_report_diagnostics(diagnostics, device_filter)
    lines = []
    lines.append("## Description")
    lines.append("")
    lines.append(description if description else "_No description provided._")
    lines.append("")
    lines.append("## Environment")
    lines.append("")
    lines.append(f"netaudio {diagnostics['version']} | Python {diagnostics['python']} | {diagnostics['platform']}")
    lines.append(f"Devices: {diagnostics['device_count']}")
    lines.append("")

    filtered_devices = []
    for device in diagnostics.get("devices", []):
        filtered = _filter_device(device, level)
        filtered_devices.append(filtered)

        if level == "full":
            name = device.get("name", device.get("server_name", "unknown"))
        else:
            name = filtered.get("dante_model", "unknown")

        ip_address = filtered.get("ipv4", "")
        model = filtered.get("dante_model", "")
        firmware = filtered.get("firmware_version", "")
        software = filtered.get("software_version", "")
        sample_rate = filtered.get("sample_rate", "")
        link_speed_mbps = filtered.get("link_speed_mbps")

        header = f"**{name}**"
        if ip_address:
            header += f" ({ip_address})"
        lines.append(header)
        lines.append(f"  Model: {model} | FW: {firmware} | SW: {software} | Rate: {sample_rate}")
        if link_speed_mbps is not None:
            lines.append(f"  Link speed: {link_speed_mbps} Mbps")

        interfaces = filtered.get("interfaces")
        if interfaces:
            for index, iface in enumerate(interfaces):
                mode = iface.get("mode", "")
                iface_ip = iface.get("ip_address", "")
                netmask = iface.get("netmask", "")
                gateway = iface.get("gateway", "")
                dns_server = iface.get("dns_server", "")
                lines.append(f"  Interface {index}: {mode} {iface_ip}/{netmask} gw={gateway} dns={dns_server}")

        if level == "full":
            channels = device.get("channels", {})
            transmitters = channels.get("transmitters", {})
            receivers = channels.get("receivers", {})
            if transmitters:
                tx_names = [
                    transmitters[key].get("friendly_name") or transmitters[key].get("name", "")
                    for key in sorted(transmitters.keys(), key=int)
                ]
                lines.append(f"  TX: {', '.join(tx_names)}")
            if receivers:
                rx_names = [
                    receivers[key].get("friendly_name") or receivers[key].get("name", "")
                    for key in sorted(receivers.keys(), key=int)
                ]
                lines.append(f"  RX: {', '.join(rx_names)}")

            subscriptions = device.get("subscriptions", [])
            if subscriptions:
                active = [s for s in subscriptions if s.get("tx_device")]
                if active:
                    lines.append(f"  Subscriptions: {len(active)} active")
                    for subscription in active:
                        lines.append(
                            f"    {subscription.get('rx_channel', '')}@{subscription.get('rx_device', '')} <- {subscription.get('tx_channel', '')}@{subscription.get('tx_device', '')}"
                        )

        lines.append("")

    lines.append("<details>")
    lines.append("<summary>Raw diagnostic JSON</summary>")
    lines.append("")
    lines.append("```json")
    raw = {
        "version": diagnostics["version"],
        "python": diagnostics["python"],
        "platform": diagnostics["platform"],
        "device_count": diagnostics["device_count"],
        "devices": filtered_devices,
    }
    lines.append(json.dumps(raw, indent=2, default=str))
    lines.append("```")
    lines.append("")
    lines.append("</details>")

    return "\n".join(lines)


def _collect_bundle(session_ref: str) -> tuple[str, int] | None:
    import base64
    from netaudio.dante.packet_store import PacketStore
    from netaudio.dante.protocol_verifier import export_session_bundle
    import tempfile

    store = PacketStore()
    try:
        session_id = _resolve_session_ref(store, session_ref)
        if session_id is None:
            return None

        with tempfile.TemporaryDirectory() as tmpdir:
            bundle_path = export_session_bundle(store, session_id, output_dir=tmpdir)
            bundle_bytes = bundle_path.read_bytes()
            encoded = base64.b64encode(bundle_bytes).decode("ascii")
            return encoded, len(bundle_bytes)
    finally:
        store.close()


def _resolve_session_ref(store, ref: str) -> int | None:
    if ref == "active" or ref == "latest":
        session = store.get_latest_session(active_only=(ref == "active"))
        return session["id"] if session else None

    try:
        return int(ref)
    except ValueError:
        pass

    for session in store.list_sessions(limit=100):
        if session.get("name") == ref:
            return session["id"]

    return None


def _format_bundle_section(encoded: str, bundle_size: int, session_ref: str) -> str:
    lines = []
    lines.append("")
    lines.append("<details>")
    lines.append(f"<summary>Provenance bundle ({bundle_size:,} bytes, session: {session_ref})</summary>")
    lines.append("")
    lines.append("To decode: `echo '<base64>' | base64 -d > bundle.tar.gz`")
    lines.append("")
    lines.append("```")
    for offset in range(0, len(encoded), 76):
        lines.append(encoded[offset : offset + 76])
    lines.append("```")
    lines.append("")
    lines.append("</details>")
    return "\n".join(lines)


@app.command("create", help="Create a GitHub issue with diagnostic context.")
def report_create(
    title: str = typer.Option(..., "--title", "-t", help="Issue title."),
    description: str = typer.Option(..., "--description", "-d", help="Description of the problem."),
    level: str = typer.Option("minimal", "--level", "-l", help="Privacy level: minimal, network, full."),
    device_filter: str = typer.Option(
        "", "--filter", "-f", help="Only include matching devices (comma-separated globs against name, IP, MAC, model)."
    ),
    session: Optional[str] = typer.Option(
        None, "--session", "-s", help="Attach packets from a capture session (ID or name)."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the report without creating an issue."),
):
    if level not in PRIVACY_LEVELS:
        typer.echo(f"Error: level must be one of: {', '.join(PRIVACY_LEVELS)}", err=True)
        raise typer.Exit(code=1)

    if not description.strip():
        typer.echo("Error: description must not be empty", err=True)
        raise typer.Exit(code=1)

    typer.echo("Collecting device diagnostics...", err=True)
    diagnostics = asyncio.run(_collect_diagnostics())
    body = _format_report(diagnostics, level, description, device_filter=device_filter)

    if session:
        typer.echo(f"Collecting bundle for session '{session}'...", err=True)
        result = _collect_bundle(session)
        if result:
            encoded, bundle_size = result
            body += _format_bundle_section(encoded, bundle_size, session)
        else:
            typer.echo(f"Warning: session '{session}' not found, skipping bundle", err=True)

    if dry_run:
        typer.echo(f"Title: {title}")
        typer.echo("")
        typer.echo(body)
        return

    _submit_issue(title, body)


@app.command("levels", help="List the privacy levels available for report content.")
def report_levels():
    for level in PRIVACY_LEVELS:
        typer.echo(f"{level}: {PRIVACY_DESCRIPTIONS[level]}")


def _submit_issue(title: str, body: str) -> None:
    if not _gh_available():
        typer.echo("Error: gh (GitHub CLI) is not installed. Install it from https://cli.github.com/", err=True)
        raise typer.Exit(code=1)
    if not _gh_authenticated():
        typer.echo("Error: gh is not authenticated. Run: gh auth login", err=True)
        raise typer.Exit(code=1)

    result = subprocess.run(
        ["gh", "issue", "create", "--repo", REPO, "--title", title, "--body", body],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        typer.echo(f"Error creating issue: {result.stderr}", err=True)
        raise typer.Exit(code=1)

    issue_url = result.stdout.strip()
    typer.echo(f"Created: {issue_url}", err=True)
