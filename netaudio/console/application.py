from signal import signal, SIGPIPE, SIG_DFL
from cleo.application import Application

from netaudio.console.commands import (
    ChannelCommand,
    ConfigCommand,
    DeviceCommand,
    SubscriptionCommand,
)

signal(SIGPIPE, SIG_DFL)


def main() -> int:
    application = Application("netaudio", "0.0.10", complete=True)
    application.add(ChannelCommand())
    application.add(ConfigCommand())
    application.add(DeviceCommand())
    application.add(SubscriptionCommand())

    return application.run()


if __name__ == "__main__":
    main()
