__all__ = ["CaptureDaemon"]


def __getattr__(name: str):
    if name == "CaptureDaemon":
        from netaudio.capture.daemon import CaptureDaemon

        return CaptureDaemon
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
