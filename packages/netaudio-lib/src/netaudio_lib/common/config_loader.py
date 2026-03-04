import os
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

from netaudio_lib.dante.packet_store import DEFAULT_DB_PATH


def _as_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _coalesce(*values):
    for value in values:
        if value is not None:
            return value
    return None


def default_config_path() -> Path:
    env_path = os.environ.get("NETAUDIO_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve()

    if sys.platform == "win32":
        base = os.environ.get("APPDATA")
        if base:
            root = Path(base)
        else:
            root = Path.home() / "AppData" / "Roaming"
    elif sys.platform == "darwin":
        root = Path.home() / "Library" / "Application Support"
    else:
        xdg_home = os.environ.get("XDG_CONFIG_HOME")
        if xdg_home:
            root = Path(xdg_home).expanduser()
        else:
            root = Path.home() / ".config"

    return (root / "netaudio" / "config.toml").resolve()


def load_capture_profile(config: str | None, profile: str | None) -> tuple[dict, Path]:
    config_path = Path(config).expanduser().resolve() if config else default_config_path()
    explicit_config = config is not None

    if not config_path.exists():
        if explicit_config or profile is not None:
            raise ValueError(f"Capture config file not found: {config_path}")
        return {}, config_path

    if tomllib is None:
        raise ValueError("TOML parser unavailable. Install 'tomli' or use Python 3.11+.")

    try:
        data = tomllib.loads(config_path.read_text())
    except Exception as exception:
        raise ValueError(f"Failed to parse capture config {config_path}: {exception}")

    if not isinstance(data, dict):
        raise ValueError(f"Capture config {config_path} must contain a TOML table.")

    selected_profile = None
    profiles_section = data.get("profiles")
    active_profile = profile or data.get("active_profile")

    if isinstance(profiles_section, dict):
        if active_profile:
            raw = profiles_section.get(active_profile)
            if raw is None:
                raise ValueError(f"Capture profile {active_profile!r} not found in {config_path}")
            if not isinstance(raw, dict):
                raise ValueError(f"Capture profile {active_profile!r} must be a TOML table in {config_path}")
            selected_profile = raw
        elif "default" in profiles_section and isinstance(profiles_section["default"], dict):
            selected_profile = profiles_section["default"]
        elif profiles_section:
            first_key = next(iter(profiles_section))
            first_value = profiles_section[first_key]
            if not isinstance(first_value, dict):
                raise ValueError(f"Capture profile {first_key!r} must be a TOML table in {config_path}")
            selected_profile = first_value
    elif profiles_section is not None:
        raise ValueError(f"Capture config {config_path} has invalid [profiles] section.")

    if selected_profile is not None:
        return selected_profile, config_path

    if any(key in data for key in ("redis", "capture", "paths")):
        return data, config_path

    return {}, config_path


def resolve_db_from_config(db: str | None, profile_cfg: dict) -> str:
    paths_cfg = _as_dict(profile_cfg.get("paths"))
    capture_cfg = _as_dict(profile_cfg.get("capture"))
    raw = _coalesce(db, paths_cfg.get("db"), capture_cfg.get("db"), DEFAULT_DB_PATH)
    return str(Path(str(raw)).expanduser())
