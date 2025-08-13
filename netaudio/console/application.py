from cleo.application import Application
from netaudio import version

from netaudio.console.commands import (
    ChannelCommand,
    ConfigCommand,
    DeviceCommand,
    ServerCommand,
    SubscriptionCommand,
)

# Fix Windows issue, See: https://stackoverflow.com/q/58718659/
try:
    from signal import signal, SIGPIPE, SIG_DFL
    signal(SIGPIPE, SIG_DFL)
except ImportError:  # If SIGPIPE is not available (win32),
    pass             # we don't have to do anything to ignore it. 


def main() -> int:
    commands = [
        ChannelCommand,
        ConfigCommand,
        DeviceCommand,
        ServerCommand,
        SubscriptionCommand,
    ]

    application = Application("netaudio", version.version)

    for command in commands:
        application.add(command())

        for subcommand in command.commands if command.commands else []:
            application.add(subcommand)

    return application.run()


if __name__ == "__main__":
    main()
