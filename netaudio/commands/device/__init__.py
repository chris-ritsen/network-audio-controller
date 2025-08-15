from .list import device_list
from .configure import device_configure

class DeviceCommands(object):
    """
    Device methods
    """

    def __init__(self):
        self.list = device_list
        self.configure = device_configure