import sys

# from .dante.browser import DanteBrowser
# from .dante.channel import DanteChannel
# from .dante.control import DanteControl
# from .dante.device import DanteDevice
# from .dante.multicast import DanteMulticast
# from .dante.subscription import DanteSubscription

__author__ = "Chris Ritsen"
__maintainer__ = "Chris Ritsen <chris.ritsen@gmail.com>"

if sys.version_info <= (3, 9):
    raise ImportError("Python version > 3.9 required.")

def main():
    from netaudio.commands import run_cli
    import signal

    def handler(signum, frame):
        sys.exit(sys.exit(0))

    signal.signal(signal.SIGINT, handler)

    sys.exit(run_cli())

if __name__ == "__main__":
    main()
