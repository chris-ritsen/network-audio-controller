import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.commands.firmware import archive, cramfs, database, dissection, image

app = typer.Typer(
    help="Analyze Dante firmware (.dnt) files.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)

app.command("build-brooklyn2-image")(image.firmware_build_brooklyn2_image)
app.command("db")(database.firmware_db)
app.command("extract")(archive.firmware_extract)
app.command("hexdump")(dissection.firmware_hexdump)
app.command("info")(archive.firmware_info)
app.command("password")(cramfs.firmware_password)
app.command("rootfs")(cramfs.firmware_rootfs)
app.command("sections")(archive.firmware_sections)
