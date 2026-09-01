import asyncio

import typer

from netaudio._common_output import output_table
from netaudio._common_selection import sort_devices
from netaudio.dante.clock_identity import canonical_clock_identity
from netaudio.dante.device_serializer import DanteDeviceSerializer


def _matching_leader_name(entries, follower):
    leader_clock_identity = canonical_clock_identity(follower.get("leader_clock_identity"))
    if leader_clock_identity is None:
        return None
    candidates = [
        entry
        for entry in entries
        if (entry.get("clock_role") or "").lower() == "leader"
        and canonical_clock_identity(entry.get("clock_identity")) == leader_clock_identity
    ]
    if len(candidates) != 1:
        return None
    return candidates[0].get("name") or None


def clock():
    """Show PTP clock status (leader, followers, preferred leader)."""

    async def run():
        from netaudio._common import _load_display_devices

        devices = await _load_display_devices()
        entries = [
            dict(DanteDeviceSerializer.to_json(device), server_name=server_name)
            for server_name, device in sort_devices(devices)
        ]

        if not entries:
            typer.echo("No device found.", err=True)
            raise typer.Exit(code=1)

        headers = ["Name", "Role", "Preferred Leader", "Sync to Leader", "Server Name"]
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
            rows.append([name, role, preferred, matched_leader or "", entry.get("server_name") or ""])
            json_data[entry.get("server_name") or name] = {
                "name": name,
                "role": role,
                "preferred_leader": entry.get("preferred_leader"),
                "clock_identity": entry.get("clock_identity"),
                "leader_clock_identity": entry.get("leader_clock_identity"),
                "leader": matched_leader,
            }

        output_table(headers, rows, json_data=json_data)

    asyncio.run(run())
