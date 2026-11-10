__all__ = ["__version__"]


def __getattr__(name: str):
    if name == "__version__":
        from importlib.metadata import version

        return version("netaudio")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
