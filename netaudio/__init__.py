import sys

def main():
    from netaudio.commands import run_cli
    import signal

    def handler(signum, frame):
        sys.exit(sys.exit(0))

    signal.signal(signal.SIGINT, handler)

    sys.exit(run_cli())

if __name__ == "__main__":
    main()
