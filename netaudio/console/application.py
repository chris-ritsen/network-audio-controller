from signal import signal, SIGPIPE, SIG_DFL
from cleo.application import Application
from netaudio import version

from netaudio.console.commands import (
    ChannelCommand,
    ConfigCommand,
    DeviceCommand,
    ServerCommand,
    SubscriptionCommand,
)

signal(SIGPIPE, SIG_DFL)


def main() -> int:
    commands = [
        ChannelCommand,
        ConfigCommand,
        DeviceCommand,
        ServerCommand,
        SubscriptionCommand,
    ]

    application = Application("netaudio", version.version, complete=True)

    for command in commands:
        application.add(command())

    return application.run()


if __name__ == "__main__":
    main()
