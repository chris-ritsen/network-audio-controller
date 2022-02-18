from cleo.application import Application

from netaudio.console.commands import ChannelCommand, DeviceCommand, ListCommand, SubscriptionCommand


def main() -> int:
    application = Application('netaudio', '0.0.1', complete=True)
    application.add(ChannelCommand())
    application.add(DeviceCommand())
    application.add(ListCommand())
    application.add(SubscriptionCommand())

    return application.run()


if __name__ == '__main__':
    main()
