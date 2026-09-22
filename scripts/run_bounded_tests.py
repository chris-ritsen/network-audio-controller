import os
import signal
import subprocess
import sys


def handle_termination(signal_number, stack_frame):
    raise KeyboardInterrupt


def run_tests(arguments, timeout_seconds=300):
    process = subprocess.Popen(arguments, start_new_session=True)
    try:
        return process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        print("error: Validation stopped at its time limit; checks are incomplete.", file=sys.stderr)
        return 124
    except KeyboardInterrupt:
        return 130
    finally:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                process.wait()
            process.wait()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_termination)
    if len(sys.argv) < 2:
        sys.exit("Usage: run_bounded_tests.py COMMAND [ARGUMENT ...]")
    sys.exit(run_tests(sys.argv[1:]))
