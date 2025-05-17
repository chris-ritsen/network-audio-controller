import typer

from ._http import run_http_server
from ._mdns import run_mdns_daemon

app = typer.Typer(
    name="server",
    help="Commands to run various servers (e.g., HTTP, mDNS).",
    no_args_is_help=True,
)

app.command(name="http", help="Run an HTTP server for API control.")(run_http_server)

app.command(name="mdns", help="Run an mDNS and Dante message monitoring daemon.")(
    run_mdns_daemon
)
