import typer

from netaudio.cli_support.output import output_table
from netaudio.cli_support.selection import sort_devices
from netaudio.dante.ptpv1_uuid import canonical_ptpv1_uuid
from netaudio.dante.device_serializer import DanteDeviceSerializer


def _matching_leader_name(entries, follower):
    ptpv1_master_uuid = canonical_ptpv1_uuid(follower.get("ptpv1_master_uuid"))
    if ptpv1_master_uuid in (None, "000000000000"):
        return None
    candidates = [
        entry
        for entry in entries
        if (entry.get("clock_role") or "").lower() == "leader"
        and canonical_ptpv1_uuid(entry.get("ptpv1_device_uuid")) == ptpv1_master_uuid
    ]
    if len(candidates) != 1:
        return None
    return candidates[0].get("name") or None


async def run_clock(application, devices) -> None:
    from netaudio.cli_support.execution import _load_display_devices

    devices = await _load_display_devices(application)
    entries = [
        dict(DanteDeviceSerializer.to_json(device), server_name=server_name)
        for server_name, device in sort_devices(devices)
    ]

    if not entries:
        typer.echo("No device found.", err=True)
        raise typer.Exit(code=1)

    headers = ["Name", "Role", "Preferred Leader", "Current master", "Synchronization", "Mute", "Server Name"]
    rows = []
    json_data = {}

    for entry in entries:
        name = entry.get("name") or ""
        role = entry.get("clock_role") or ""
        if entry.get("preferred_leader") is True:
            preferred = "yes"
        elif entry.get("preferred_leader") is False:
            preferred = "no"
        else:
            preferred = ""
        matched_leader = _matching_leader_name(entries, entry) if role.lower() == "follower" else None
        status = entry.get("clock_status") or {}
        rows.append(
            [
                name,
                role,
                preferred,
                matched_leader or "",
                status.get("synchronization", "unknown"),
                "; ".join(status.get("mute_reasons") or []),
                entry.get("server_name") or "",
            ]
        )
        json_data[entry.get("server_name") or name] = {
            "name": name,
            "role": role,
            "preferred_leader": entry.get("preferred_leader"),
            "ptpv1_device_uuid": entry.get("ptpv1_device_uuid"),
            "ptpv1_master_uuid": entry.get("ptpv1_master_uuid"),
            "leader": matched_leader,
            "clock_status": status,
            "ptpv1_grandmaster_uuid": entry.get("ptpv1_grandmaster_uuid"),
        }

    output_table(headers, rows, json_data=json_data)


def clock():
    """Show PTP clock status (leader, followers, preferred leader)."""
    from netaudio.cli_support.execution import run_command

    run_command(run_clock, discover_devices=False)
