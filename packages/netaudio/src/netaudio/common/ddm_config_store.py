from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Mapping

from netaudio.common.managed_api import DDM_NAME_PATTERN
from netaudio.common.config_loader import tomllib
from netaudio.common.managed_api import resolve_ddm_configuration


_ASSIGNMENT = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*=")


def _validate_name(value: str, description: str) -> str:
    if DDM_NAME_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f"{description} must start with a letter or digit and contain only letters, digits, ._- characters"
        )
    return value


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _named_table_header(text: str, prefix: str, name: str) -> str:
    quoted = f"[{prefix}.{_toml_string(name)}]"
    bare = f"[{prefix}.{name}]"
    existing = {line.strip() for line in text.splitlines()}
    if quoted in existing:
        return quoted
    if "." not in name and bare in existing:
        return bare
    return quoted


def _upsert_table(text: str, header: str, values: Mapping[str, str | bool | None]) -> str:
    lines = text.splitlines(keepends=True)
    start = next((index for index, line in enumerate(lines) if line.strip() == header), None)
    if start is None:
        if lines and not lines[-1].endswith(("\n", "\r")):
            lines[-1] += "\n"
        if lines and lines[-1].strip():
            lines.append("\n")
        lines.append(f"{header}\n")
        for key, value in values.items():
            if value is not None:
                rendered = str(value).lower() if isinstance(value, bool) else _toml_string(value)
                lines.append(f"{key} = {rendered}\n")
        return "".join(lines)

    end = next(
        (index for index in range(start + 1, len(lines)) if lines[index].lstrip().startswith("[")),
        len(lines),
    )
    pending = dict(values)
    output = lines[: start + 1]
    for line in lines[start + 1 : end]:
        match = _ASSIGNMENT.match(line)
        key = match.group(1) if match else None
        if key not in pending:
            output.append(line)
            continue
        value = pending.pop(key)
        if value is not None:
            rendered = str(value).lower() if isinstance(value, bool) else _toml_string(value)
            output.append(f"{key} = {rendered}\n")
    if output and output[-1].strip() == "":
        separator = output.pop()
    else:
        separator = None
    for key, value in pending.items():
        if value is not None:
            rendered = str(value).lower() if isinstance(value, bool) else _toml_string(value)
            output.append(f"{key} = {rendered}\n")
    if separator is not None:
        output.append(separator)
    output.extend(lines[end:])
    return "".join(output)


def _write_atomic(path: Path, text: str) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as output:
            descriptor = -1
            output.write(text)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        temporary_path.unlink(missing_ok=True)
        raise


def save_ddm_context(
    path: Path,
    *,
    server_name: str,
    url: str,
    credential_file: Path,
    context_name: str,
    domain_id: str,
    domain_name: str | None,
    make_default: bool,
) -> None:
    server_name = _validate_name(server_name, "DDM server profile name")
    context_name = _validate_name(context_name, "DDM context name")
    destination = path.expanduser().resolve()
    try:
        credential_value = str(credential_file.expanduser().resolve().relative_to(destination.parent))
    except ValueError:
        credential_value = str(credential_file.expanduser().resolve())
    try:
        text = destination.read_text(encoding="utf-8")
    except FileNotFoundError:
        text = ""
    text = _upsert_table(
        text,
        _named_table_header(text, "ddm.servers", server_name),
        {
            "url": url,
            "credential_file": credential_value,
            "credential": None,
            "enabled": True,
        },
    )
    text = _upsert_table(
        text,
        _named_table_header(text, "ddm.contexts", context_name),
        {
            "server": server_name,
            "domain_id": domain_id,
            "domain_name": domain_name,
        },
    )
    if make_default:
        text = _upsert_table(text, "[ddm]", {"default_context": context_name})
    _write_atomic(destination, text)


def save_ddm_server(path: Path, *, name: str, url: str, credential: str) -> None:
    name = _validate_name(name, "DDM server profile name")
    destination = path.expanduser().resolve()
    credential_path = destination.parent / "credentials" / f"{name}.credential"
    _write_atomic(credential_path, credential + "\n")
    text = destination.read_text(encoding="utf-8") if destination.exists() else ""
    text = _upsert_table(
        text,
        _named_table_header(text, "ddm.servers", name),
        {
            "url": url,
            "credential_file": str(credential_path),
            "credential": None,
            "enabled": True,
        },
    )
    _write_atomic(destination, text)


def logout_ddm_server(path: Path, name: str) -> None:
    name = _validate_name(name, "DDM server profile name")
    destination = path.expanduser().resolve()
    text = destination.read_text(encoding="utf-8")
    text = _upsert_table(
        text,
        _named_table_header(text, "ddm.servers", name),
        {
            "credential": None,
            "credential_file": None,
            "enabled": False,
        },
    )
    _write_atomic(destination, text)


def _remove_table(text: str, header: str) -> str:
    lines = text.splitlines(keepends=True)
    start = next((index for index, line in enumerate(lines) if line.strip() == header), None)
    if start is None:
        raise ValueError("This configuration format cannot be edited from the app")
    end = next((index for index in range(start + 1, len(lines)) if lines[index].lstrip().startswith("[")), len(lines))
    return "".join(lines[:start] + lines[end:])


def edit_ddm_server(path: Path, *, current_name: str, name: str | None, url: str | None) -> None:
    destination = path.expanduser().resolve()
    text = destination.read_text(encoding="utf-8")
    configuration = resolve_ddm_configuration(tomllib.loads(text), base_directory=destination.parent)
    existing = configuration.server(current_name)
    if name is not None:
        _validate_name(name, "DDM server profile name")
        if name != current_name and name in configuration.servers:
            raise ValueError("A profile with that name already exists")
    old_header = _named_table_header(text, "ddm.servers", current_name)
    if name is None:
        text = _remove_table(text, old_header)
    else:
        if old_header not in {line.strip() for line in text.splitlines()}:
            raise ValueError("This configuration format cannot be edited from the app")
        new_header = f"[ddm.servers.{_toml_string(name)}]"
        text = "".join(
            new_header + "\n" if line.strip() == old_header else line for line in text.splitlines(keepends=True)
        )
        values = {"url": url}
        if url != existing.url:
            values.update(credential_file=None, credential=None, enabled=False)
        text = _upsert_table(text, new_header, values)
    for context in configuration.contexts.values():
        if context.server != current_name:
            continue
        header = _named_table_header(text, "ddm.contexts", context.name)
        if name is None or url != existing.url:
            text = _remove_table(text, header)
            if configuration.default_context == context.name:
                text = _upsert_table(text, "[ddm]", {"default_context": None})
        else:
            text = _upsert_table(text, header, {"server": name})
    resolve_ddm_configuration(tomllib.loads(text), base_directory=destination.parent)
    _write_atomic(destination, text)


def set_default_ddm_context(path: Path, context_name: str) -> None:
    context_name = _validate_name(context_name, "DDM context name")
    destination = path.expanduser().resolve()
    try:
        text = destination.read_text(encoding="utf-8")
    except FileNotFoundError:
        text = ""
    _write_atomic(destination, _upsert_table(text, "[ddm]", {"default_context": context_name}))


__all__ = ["save_ddm_context", "set_default_ddm_context"]
