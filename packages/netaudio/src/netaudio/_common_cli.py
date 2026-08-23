from netaudio.icons import icon


HEADER_ICONS = {
    "Name": "name",
    "IP Address": "ip",
    "IP": "ip",
    "MAC Address": "mac",
    "Clock MAC": "mac",
    "Model": "model",
    "TX": "tx",
    "RX": "rx",
    "Last Seen": "last_seen",
    "Server Name": "server",
    "Manufacturer": "manufacturer",
    "Product Version": "version",
    "Board": "board",
    "Firmware": "firmware",
    "Software": "software",
    "Sample Rate": "sample_rate",
    "Encoding": "encoding",
    "Bit Depth": "bit_depth",
    "Latency": "latency",
    "Flows": "flow",
    "Bluetooth": "bluetooth",
    "Status": "status",
    "Label": "label",
    "Summary": "summary",
    "Reported": "reported",
    "Updated": "updated",
    "Sessions": "session",
    "Tags": "tag",
    "Context": "context",
    "RX Channel": "rx",
    "RX Device": "device",
    "TX Channel": "tx",
    "TX Device": "device",
    "#": "number",
    "Friendly Name": "friendly_name",
    "Role": "role",
    "Grandmaster": "grandmaster",
    "Direction": "direction",
    "Channel": "channel",
    "Channel Name": "channel",
    "Level": "level",
    "Timestamp": "wall_time",
    "Online": "online",
    "Receiving": "receiving",
}


def _iconize_headers(headers: list[str]) -> list[str]:
    return [f"{icon(HEADER_ICONS[header])}{header}" if header in HEADER_ICONS else header for header in headers]


def _get_state():
    from netaudio.cli import state

    return state
