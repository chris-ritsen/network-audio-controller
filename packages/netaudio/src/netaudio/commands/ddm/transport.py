from __future__ import annotations

import asyncio
from typing import Any, Mapping, NoReturn

import typer

from netaudio.cli_support.context import _get_state
from netaudio.daemon.client import call_daemon


def fail(message: str, code: int = 1) -> NoReturn:
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=code)


def execute(query: str, variables: Mapping[str, Any] | None = None, operation_name: str | None = None) -> dict:
    return asyncio.run(
        call_daemon(
            "POST",
            "/ddm/graphql",
            {
                "context": _get_state().ddm_context,
                "operation_name": operation_name,
                "query": query,
                "variables": dict(variables or {}),
            },
        )
    )
