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


def config_search_paths() -> list[Path]:
    paths = []

    env_path = os.environ.get("NETAUDIO_CONFIG")
    if env_path:
        return [Path(env_path).expanduser().resolve()]

    home = Path.home()

    paths.append(home / ".netaudio" / "config.toml")

    xdg_home = os.environ.get("XDG_CONFIG_HOME")
    if xdg_home:
        paths.append(Path(xdg_home).expanduser() / "netaudio" / "config.toml")
    paths.append(home / ".config" / "netaudio" / "config.toml")

    if sys.platform == "darwin":
        paths.append(home / "Library" / "Application Support" / "netaudio" / "config.toml")
    elif sys.platform == "win32":
        base = os.environ.get("APPDATA")
        if base:
            paths.append(Path(base) / "netaudio" / "config.toml")
        else:
            paths.append(home / "AppData" / "Roaming" / "netaudio" / "config.toml")

    seen = set()
    deduplicated = []
    for path in paths:
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            deduplicated.append(resolved)

    return deduplicated


def default_config_path() -> Path:
    for path in config_search_paths():
        if path.exists():
            return path

    candidates = config_search_paths()
    return candidates[0]


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


def set_config_value(key: str, value: str | None) -> Path:
    config_path = default_config_path()

    if not config_path.exists():
        if value is None:
            return config_path
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(f'{key} = "{value}"\n')
        return config_path

    lines = config_path.read_text().splitlines(keepends=True)

    if tomllib is not None:
        data = tomllib.loads(config_path.read_text())
        active_profile = data.get("active_profile")
        if active_profile:
            section_header = f"[profiles.{active_profile}]"
        else:
            section_header = None
    else:
        section_header = None

    target_section_found = section_header is None
    key_pattern = f"{key} ="
    key_replaced = False
    result_lines = []

    for line in lines:
        stripped = line.strip()

        if section_header and stripped == section_header:
            target_section_found = True
            result_lines.append(line)
            continue

        if target_section_found and not key_replaced and stripped.startswith(key_pattern):
            if value is not None:
                result_lines.append(f'{key} = "{value}"\n')
            key_replaced = True
            continue

        if target_section_found and not key_replaced and stripped.startswith("[") and stripped != section_header:
            if value is not None:
                result_lines.append(f'{key} = "{value}"\n')
                key_replaced = True

        result_lines.append(line)

    if not key_replaced and value is not None:
        if not result_lines or not result_lines[-1].endswith("\n"):
            result_lines.append("\n")
        result_lines.append(f'{key} = "{value}"\n')

    config_path.write_text("".join(result_lines))
    return config_path


def get_config_value(key: str) -> tuple[str | None, Path]:
    config_path = default_config_path()
    if not config_path.exists():
        return None, config_path

    profile_cfg, _ = load_capture_profile(None, None)
    return profile_cfg.get(key), config_path


def resolve_db_from_config(db: str | None, profile_cfg: dict) -> str:
    paths_cfg = _as_dict(profile_cfg.get("paths"))
    capture_cfg = _as_dict(profile_cfg.get("capture"))
    raw = _coalesce(db, paths_cfg.get("db"), capture_cfg.get("db"), DEFAULT_DB_PATH)
    return str(Path(str(raw)).expanduser())
