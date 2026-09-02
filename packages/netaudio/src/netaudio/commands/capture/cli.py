import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.commands.capture import collection, live, packets, sessions

app = typer.Typer(
    help="Capture and replay Dante traffic.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)
packet_app = typer.Typer(
    help="Inspect individual captured packets.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)
session_app = typer.Typer(help="Manage capture sessions.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)

app.add_typer(packet_app, name="packet")
app.add_typer(session_app, name="session")

app.command("clear")(packets.clear)
app.command("collect", help="Consume packets from a Redis stream into the capture database.")(collection.collect)
app.command("follow", help="Print packets from a Redis stream as they arrive.")(collection.follow)
app.command("live", help="Capture Dante traffic from a network interface into the capture database.")(live.live)
app.command("marker", help="Add a marker to a capture session timeline.")(sessions.marker)
app.command("replay", help="Resend a captured packet and listen for responses.")(live.replay)
packet_app.command("diff")(packets.packet_diff)
packet_app.command("list")(packets.packet_list)
packet_app.command("show", help="Show captured packets with annotated dissection.")(packets.packet_show)
packet_app.command("state-diff")(packets.packet_state_diff)
session_app.command("list", help="List capture sessions.")(sessions.session_list)
session_app.command("packets", help="List packets captured in a session.")(sessions.session_packets)
session_app.command("rename")(sessions.session_rename)
session_app.command("show", help="Show a session's marker timeline.")(sessions.session_show)
session_app.command("start", help="Start a new capture session.")(sessions.session_start)
session_app.command("stop", help="Stop a capture session.")(sessions.session_stop)
