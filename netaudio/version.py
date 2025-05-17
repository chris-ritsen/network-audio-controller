import importlib.metadata

try:
    version = importlib.metadata.version("netaudio")
except importlib.metadata.PackageNotFoundError:
    version = "unknown"
