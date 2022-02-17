from cleo import Command
from cleo.helpers import option

from .add import SubscriptionAddCommand
from .list import SubscriptionListCommand
from .remove import SubscriptionRemoveCommand


class SubscriptionCommand(Command):
    name = 'subscription'
    description = 'Control subscriptions'
    commands = [SubscriptionAddCommand(), SubscriptionListCommand(), SubscriptionRemoveCommand()]


    def handle(self):
        return self.call('help', self._config.name)
