import typer


app = typer.Typer(help="Capture and replay Dante traffic.", no_args_is_help=True)
session_app = typer.Typer(help="Manage capture sessions.", no_args_is_help=True)
app.add_typer(session_app, name="session")
packet_app = typer.Typer(help="Inspect individual captured packets.", no_args_is_help=True)
app.add_typer(packet_app, name="packet")
