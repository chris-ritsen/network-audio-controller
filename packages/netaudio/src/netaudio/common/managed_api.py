from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


DEFAULT_DDM_REFRESH_INTERVAL = 10.0


@dataclass(frozen=True)
class ManagedAPIConfiguration:
    url: str | None
    credential: str | None
    credential_file: Path | None
    refresh_interval: float
    requested_enabled: bool | None = None

    @property
    def enabled(self) -> bool:
        if self.requested_enabled is False:
            return False
        return bool(self.url and (self.credential or self.credential_file))

    @property
    def configuration_error(self) -> str | None:
        if self.requested_enabled is False:
            return None
        if not self.url and not self.credential and self.credential_file is None:
            if self.requested_enabled:
                return "DDM is enabled but no URL or credential is configured"
            return None
        if not self.url:
            return "DDM credential is configured but ddm.url is missing"
        if not self.credential and self.credential_file is None:
            return "DDM URL is configured but no credential is available"
        if self.credential and self.credential_file is not None:
            return "Configure either a DDM credential value or credential file, not both"
        return None


def _optional_string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _refresh_interval(value: object) -> float:
    if value is None:
        return DEFAULT_DDM_REFRESH_INTERVAL
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("ddm.refresh_interval must be a number")
    try:
        interval = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError("ddm.refresh_interval must be a number") from error
    if not math.isfinite(interval) or not 1 <= interval <= 3600:
        raise ValueError("ddm.refresh_interval must be between 1 and 3600 seconds")
    return interval


def _optional_bool(value: object, name: str) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{name} must be true or false")


def resolve_managed_api_configuration(
    profile: Mapping[str, object] | None,
    environ: Mapping[str, str] | None = None,
    base_directory: Path | None = None,
) -> ManagedAPIConfiguration:
    environment = os.environ if environ is None else environ
    profile_mapping = profile if isinstance(profile, Mapping) else {}
    ddm_value = profile_mapping.get("ddm")
    ddm_mapping = ddm_value if isinstance(ddm_value, Mapping) else {}

    url = _optional_string(environment.get("NETAUDIO_DDM_URL")) or _optional_string(ddm_mapping.get("url"))
    environment_credential = _optional_string(environment.get("NETAUDIO_DDM_API_KEY"))
    environment_credential_file = _optional_string(environment.get("NETAUDIO_DDM_API_KEY_FILE"))
    if environment_credential is not None or environment_credential_file is not None:
        credential = environment_credential
        credential_file_value = environment_credential_file
    else:
        credential = _optional_string(ddm_mapping.get("api_key"))
        credential_file_value = _optional_string(ddm_mapping.get("api_key_file"))
    enabled_value: object = environment.get("NETAUDIO_DDM_ENABLED")
    if enabled_value is None:
        enabled_value = ddm_mapping.get("enabled")
    refresh_value: object = environment.get("NETAUDIO_DDM_REFRESH_INTERVAL")
    if refresh_value is None:
        refresh_value = ddm_mapping.get("refresh_interval", ddm_mapping.get("refresh_interval_seconds"))

    credential_file = Path(credential_file_value).expanduser() if credential_file_value else None
    if credential_file is not None and not credential_file.is_absolute() and base_directory is not None:
        credential_file = (base_directory / credential_file).resolve()
    return ManagedAPIConfiguration(
        url=url,
        credential=credential,
        credential_file=credential_file,
        refresh_interval=_refresh_interval(refresh_value),
        requested_enabled=_optional_bool(enabled_value, "ddm.enabled"),
    )


__all__ = [
    "DEFAULT_DDM_REFRESH_INTERVAL",
    "ManagedAPIConfiguration",
    "resolve_managed_api_configuration",
]
