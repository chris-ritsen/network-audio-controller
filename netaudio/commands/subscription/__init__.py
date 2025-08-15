from .add import subscription_add
from .remove import subscription_remove
from .list import subscription_list

class SubscriptionCommands(object):
    """
    Subscription methods: connect channel matrix
    """
    def __init__(self):
        self.add = subscription_add
        self.remove = subscription_remove
        self.list = subscription_list
