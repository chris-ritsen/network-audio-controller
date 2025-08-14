from cleo.application import Application
from cleo.commands.command import Command
from netaudio import version

from typing import Callable

COMMANDS = [
    "config",
    "device list",
    "channel list",
    "server http",
    "server mdns",
    "subscription add",
    "subscription remove",
    "subscription list"
]

# Fix Windows issue, See: https://stackoverflow.com/q/58718659/
try:
    from signal import signal, SIGPIPE, SIG_DFL

    signal(SIGPIPE, SIG_DFL)
except ImportError:  # If SIGPIPE is not available (win32),
    pass  # we don't have to do anything to ignore it.


def load_command(name: str) -> Callable[[], Command]:
    from importlib import import_module

    def _load() -> Command:
        words = name.split(" ")
        module = import_module("netaudio.console.commands." + ".".join(words))
        command_class = getattr(module, "".join(c.title() for c in words) + "Command")
        command: Command = command_class()
        return command

    return _load


def main() -> int:
    application = Application("netaudio", version.version)

    for command in COMMANDS:
        application.add(load_command(command)())

    return application.run()


if __name__ == "__main__":
    main()
