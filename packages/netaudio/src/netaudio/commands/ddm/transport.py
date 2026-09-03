from __future__ import annotations

import asyncio
from typing import Any, Mapping

import typer

from netaudio.daemon.client import execute_ddm_graphql_on_daemon
from netaudio.ddm import ManagedAPIClient, ManagedAPIError

NOT_CONFIGURED_MESSAGE = (
    "Dante Domain Manager is not configured. Run 'netaudio ddm login', configure a DDM context, "
    "or run the netaudio daemon on a host that has one."
)


def fail(message: str, code: int = 1) -> None:
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=code)


def configured_client() -> ManagedAPIClient | None:
    from netaudio.cli_support.context import _get_state
    from netaudio.common.config_loader import default_config_path, load_config_document
    from netaudio.common.managed_api import resolve_managed_api_configuration

    configuration = resolve_managed_api_configuration(
        load_config_document(),
        base_directory=default_config_path().parent,
        context_name=_get_state().ddm_context,
    )
    error = configuration.configuration_error
    if error:
        fail(error)
    if not configuration.enabled:
        return None
    if configuration.credential is not None:
        return ManagedAPIClient(configuration.url or "", credential=configuration.credential)
    return ManagedAPIClient(configuration.url or "", credential_file=configuration.credential_file)


def execute(query: str, variables: Mapping[str, Any] | None = None, operation_name: str | None = None) -> dict:
    client = configured_client()
    if client is not None:
        try:
            return client.execute(query, variables, operation_name).to_json()
        except ManagedAPIError as exception:
            fail(str(exception))
    from netaudio.cli_support.context import _get_state

    status, data = asyncio.run(
        execute_ddm_graphql_on_daemon(
            query,
            dict(variables or {}),
            operation_name,
            context=_get_state().ddm_context,
        )
    )
    if status is None:
        fail(NOT_CONFIGURED_MESSAGE)
    if status != 200 or data is None:
        detail = (data or {}).get("error") if isinstance(data, dict) else None
        fail(detail or f"netaudio daemon returned HTTP {status} for the Managed API request")
    return data


__all__ = ["NOT_CONFIGURED_MESSAGE", "configured_client", "execute", "fail"]
