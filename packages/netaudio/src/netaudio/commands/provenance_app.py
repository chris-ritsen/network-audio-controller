import typer

from netaudio._common_cli import HELP_CONTEXT_SETTINGS


app = typer.Typer(
    help="Wire-observation provenance workflows.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)
