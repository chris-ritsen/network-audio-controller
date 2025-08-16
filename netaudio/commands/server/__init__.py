from .http import run_server as http_run_server
# from .mdns import run_server as mdns_run_server

class ServerCommands(object):
    """
    Server methods
    """
    def __init__(self):
        self.http = http_run_server
        # self.mdns = mdns_run_server
