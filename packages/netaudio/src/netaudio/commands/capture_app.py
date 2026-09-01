import typer

from netaudio._common_cli import HELP_CONTEXT_SETTINGS


app = typer.Typer(
    help="Capture and replay Dante traffic.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)
session_app = typer.Typer(help="Manage capture sessions.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS)
app.add_typer(session_app, name="session")
packet_app = typer.Typer(
    help="Inspect individual captured packets.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)
app.add_typer(packet_app, name="packet")
