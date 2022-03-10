#!/usr/bin/env python3

import signal
import sys


def handler(signum, frame):
    sys.exit(main())


if __name__ == "__main__":
    from netaudio.console.application import main

    signal.signal(signal.SIGINT, handler)

    sys.exit(main())
