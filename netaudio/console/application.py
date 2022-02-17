from cleo.application import Application

from netaudio.console.commands import DeviceCommand, ListCommand, SubscriptionCommand


def main() -> int:
    application = Application('netaudio', '0.0.1', complete=True)
    application.add(DeviceCommand())
    application.add(SubscriptionCommand())
    application.add(ListCommand())

    return application.run()


if __name__ == '__main__':
    main()
