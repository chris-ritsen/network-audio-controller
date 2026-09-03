from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


DEFAULT_DDM_REFRESH_INTERVAL = 10.0
DDM_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


@dataclass(frozen=True)
class ManagedAPIConfiguration:
    url: str | None
    credential: str | None
    credential_file: Path | None
    refresh_interval: float
    requested_enabled: bool | None = None
    name: str = "default"

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


@dataclass(frozen=True)
class DDMContextConfiguration:
    name: str
    server: str
    domain_id: str
    domain_name: str | None = None


@dataclass(frozen=True)
class DDMConfiguration:
    servers: dict[str, ManagedAPIConfiguration]
    contexts: dict[str, DDMContextConfiguration]
    default_context: str | None = None

    def context(self, name: str) -> DDMContextConfiguration:
        try:
            return self.contexts[name]
        except KeyError as error:
            available = ", ".join(sorted(self.contexts)) or "none"
            raise ValueError(f"unknown DDM context {name!r}; available contexts: {available}") from error

    def server(self, name: str) -> ManagedAPIConfiguration:
        try:
            return self.servers[name]
        except KeyError as error:
            available = ", ".join(sorted(self.servers)) or "none"
            raise ValueError(f"unknown DDM server profile {name!r}; available profiles: {available}") from error

    def selected_server(self, context_name: str | None = None) -> ManagedAPIConfiguration:
        selected = context_name or self.default_context
        if selected is not None:
            return self.server(self.context(selected).server)
        if len(self.servers) == 1:
            return next(iter(self.servers.values()))
        if not self.servers:
            return ManagedAPIConfiguration(None, None, None, DEFAULT_DDM_REFRESH_INTERVAL)
        raise ValueError("multiple DDM server profiles are configured; select one with --context")


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


def _name(value: object, description: str) -> str:
    name = _optional_string(value)
    if name is None or DDM_NAME_PATTERN.fullmatch(name) is None:
        raise ValueError(f"{description} must start with a letter or digit and contain only letters, digits, ._- characters")
    return name


def _server_configuration(
    name: str,
    mapping: Mapping[str, object],
    *,
    base_directory: Path | None,
    default_refresh: object = None,
    default_enabled: object = None,
    environment: Mapping[str, str] | None = None,
) -> ManagedAPIConfiguration:
    environment = environment or {}
    url = _optional_string(environment.get("NETAUDIO_DDM_URL")) or _optional_string(mapping.get("url"))
    environment_credential = _optional_string(environment.get("NETAUDIO_DDM_CREDENTIAL")) or _optional_string(
        environment.get("NETAUDIO_DDM_API_KEY")
    )
    environment_credential_file = _optional_string(
        environment.get("NETAUDIO_DDM_CREDENTIAL_FILE")
    ) or _optional_string(environment.get("NETAUDIO_DDM_API_KEY_FILE"))
    if environment_credential is not None or environment_credential_file is not None:
        credential = environment_credential
        credential_file_value = environment_credential_file
    else:
        credential = _optional_string(mapping.get("credential")) or _optional_string(mapping.get("api_key"))
        credential_file_value = _optional_string(mapping.get("credential_file")) or _optional_string(
            mapping.get("api_key_file")
        )
    credential_file = Path(credential_file_value).expanduser() if credential_file_value else None
    if credential_file is not None and not credential_file.is_absolute() and base_directory is not None:
        credential_file = (base_directory / credential_file).resolve()
    enabled_value: object = environment.get("NETAUDIO_DDM_ENABLED")
    if enabled_value is None:
        enabled_value = mapping.get("enabled", default_enabled)
    refresh_value: object = environment.get("NETAUDIO_DDM_REFRESH_INTERVAL")
    if refresh_value is None:
        refresh_value = mapping.get(
            "refresh_interval",
            mapping.get("refresh_interval_seconds", default_refresh),
        )
    return ManagedAPIConfiguration(
        url=url,
        credential=credential,
        credential_file=credential_file,
        refresh_interval=_refresh_interval(refresh_value),
        requested_enabled=_optional_bool(enabled_value, f"ddm.servers.{name}.enabled"),
        name=name,
    )


def resolve_ddm_configuration(
    profile: Mapping[str, object] | None,
    environ: Mapping[str, str] | None = None,
    base_directory: Path | None = None,
) -> DDMConfiguration:
    environment = os.environ if environ is None else environ
    profile_mapping = profile if isinstance(profile, Mapping) else {}
    ddm_value = profile_mapping.get("ddm")
    ddm_mapping = ddm_value if isinstance(ddm_value, Mapping) else {}
    servers: dict[str, ManagedAPIConfiguration] = {}

    has_flat_connection = any(
        _optional_string(environment.get(key)) is not None
        for key in (
            "NETAUDIO_DDM_URL",
            "NETAUDIO_DDM_CREDENTIAL",
            "NETAUDIO_DDM_CREDENTIAL_FILE",
            "NETAUDIO_DDM_API_KEY",
            "NETAUDIO_DDM_API_KEY_FILE",
        )
    ) or any(
        ddm_mapping.get(key) is not None
        for key in ("url", "credential", "credential_file", "api_key", "api_key_file")
    )
    if has_flat_connection or ddm_mapping.get("enabled") is not None:
        servers["default"] = _server_configuration(
            "default",
            ddm_mapping,
            base_directory=base_directory,
            environment=environment,
        )

    server_values = ddm_mapping.get("servers")
    if server_values is not None and not isinstance(server_values, Mapping):
        raise ValueError("ddm.servers must be a table")
    for raw_name, value in (server_values or {}).items():
        name = _name(raw_name, "DDM server profile name")
        if not isinstance(value, Mapping):
            raise ValueError(f"ddm.servers.{name} must be a table")
        servers[name] = _server_configuration(
            name,
            value,
            base_directory=base_directory,
            default_refresh=ddm_mapping.get("refresh_interval", ddm_mapping.get("refresh_interval_seconds")),
            default_enabled=ddm_mapping.get("enabled"),
        )

    contexts: dict[str, DDMContextConfiguration] = {}
    context_targets: dict[tuple[str, str], str] = {}
    context_values = ddm_mapping.get("contexts")
    if context_values is not None and not isinstance(context_values, Mapping):
        raise ValueError("ddm.contexts must be a table")
    for raw_name, value in (context_values or {}).items():
        name = _name(raw_name, "DDM context name")
        if not isinstance(value, Mapping):
            raise ValueError(f"ddm.contexts.{name} must be a table")
        server_name = _name(value.get("server"), f"ddm.contexts.{name}.server")
        if server_name not in servers:
            raise ValueError(f"ddm.contexts.{name} references unknown server profile {server_name!r}")
        domain_id = _optional_string(value.get("domain_id"))
        if domain_id is None:
            raise ValueError(f"ddm.contexts.{name}.domain_id must be a non-empty string")
        target = (server_name, domain_id)
        if target in context_targets:
            raise ValueError(
                f"ddm.contexts.{name} duplicates server and domain target from context {context_targets[target]!r}"
            )
        context_targets[target] = name
        contexts[name] = DDMContextConfiguration(
            name=name,
            server=server_name,
            domain_id=domain_id,
            domain_name=_optional_string(value.get("domain_name")),
        )

    default_context = _optional_string(ddm_mapping.get("default_context"))
    if default_context is not None and default_context not in contexts:
        raise ValueError(f"ddm.default_context references unknown context {default_context!r}")
    return DDMConfiguration(servers=servers, contexts=contexts, default_context=default_context)


def resolve_managed_api_configuration(
    profile: Mapping[str, object] | None,
    environ: Mapping[str, str] | None = None,
    base_directory: Path | None = None,
    context_name: str | None = None,
) -> ManagedAPIConfiguration:
    return resolve_ddm_configuration(profile, environ, base_directory).selected_server(context_name)


__all__ = [
    "DEFAULT_DDM_REFRESH_INTERVAL",
    "DDMConfiguration",
    "DDMContextConfiguration",
    "DDM_NAME_PATTERN",
    "ManagedAPIConfiguration",
    "resolve_ddm_configuration",
    "resolve_managed_api_configuration",
]
