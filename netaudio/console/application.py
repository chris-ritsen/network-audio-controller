from cleo.application import Application

from netaudio.console.commands import (
    ChannelCommand,
    ConfigCommand,
    DeviceCommand,
    SubscriptionCommand,
)

def main() -> int:
    application = Application("netaudio", "", complete=True)
    application.add(ChannelCommand())
    application.add(ConfigCommand())
    application.add(DeviceCommand())
    application.add(SubscriptionCommand())

    return application.run()


if __name__ == "__main__":
    main()
