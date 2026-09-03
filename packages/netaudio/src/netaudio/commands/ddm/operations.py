from __future__ import annotations

import enum
import inspect
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import typer

from netaudio.commands.ddm.render import is_sensitive_name, render_result
from netaudio.commands.ddm.transport import execute, fail
from netaudio.ddm import Field, InputValidationError, InputValue, Schema, TypeReference, command_name

PYTHON_SCALAR_TYPES = {"Boolean": bool, "Float": float, "ID": str, "Int": int, "String": str}
SENTENCE_END = re.compile(r"(?<=[.!?])\s|\n")
MUTATION_RESOURCES = (
    "admin",
    "clocking-group",
    "device",
    "devices",
    "domain",
    "fqdn",
    "license",
    "user",
)
QUERY_COMMAND_NAMES = {"me": "current-user"}
CONFIRMATION_REQUIRED_MUTATIONS = frozenset(
    {
        "DevicesUnenroll",
        "DomainExternalSdpDescriptorRemove",
        "DomainRemove",
        "LicenseActivate",
        "UserAPIKeyRemove",
    }
)


def summary(description: str | None) -> str:
    text = " ".join((description or "").split())
    if not text:
        return ""
    return SENTENCE_END.split(text, maxsplit=1)[0].strip()


@dataclass(frozen=True)
class OptionSpecification:
    graphql_name: str
    identifier: str
    is_json: bool
    reference: TypeReference
    is_secret_file: bool = False


def _identifier(graphql_name: str) -> str:
    return re.sub(r"\W", "_", command_name(graphql_name))


def _action_name(value: str) -> str:
    for suffix, verb in (
        ("-configure", "configure"),
        ("-activate", "activate"),
        ("-assign", "assign"),
        ("-remove", "remove"),
        ("-add", "add"),
        ("-set", "set"),
    ):
        if value.endswith(suffix):
            subject = value[: -len(suffix)]
            return f"{verb}-{subject}" if subject else verb
    return value


def operation_command_path(operation: str, graphql_name: str) -> tuple[str, ...]:
    name = command_name(graphql_name)
    if operation == "query":
        return "read", QUERY_COMMAND_NAMES.get(name, name)
    if operation != "mutation":
        raise ValueError(f"unsupported GraphQL operation {operation!r}")
    for resource in MUTATION_RESOURCES:
        prefix = f"{resource}-"
        if name.startswith(prefix):
            return "write", resource, _action_name(name[len(prefix) :])
    return "write", "other", _action_name(name)


def _enum_type(schema: Schema, name: str) -> type[enum.Enum]:
    values = schema.type(name).enum_values
    return enum.Enum(name, {value: value for value in values}, type=str)


def _leaf_python_type(schema: Schema, reference: TypeReference) -> type | None:
    schema_type = schema.type(reference.named)
    if schema_type.kind == "ENUM":
        return _enum_type(schema, schema_type.name)
    if schema_type.kind == "SCALAR":
        return PYTHON_SCALAR_TYPES.get(schema_type.name, str)
    return None


def _option(schema: Schema, value: InputValue) -> tuple[type, Any, OptionSpecification]:
    reference = value.type
    required = reference.is_required
    unwrapped = reference.unwrapped
    flag = f"--{command_name(value.name)}"
    rendered = reference.render()
    description = f"{summary(value.description)} " if value.description else ""
    sensitive = is_sensitive_name(value.name)
    if sensitive:
        identifier = f"{_identifier(value.name)}_file"
        annotation = Path if required else Optional[Path]
        default = typer.Option(
            ... if required else None,
            f"{flag}-file",
            help=f"[{rendered}] {description}Read from this file instead of the command line.".rstrip(),
        )
        return annotation, default, OptionSpecification(value.name, identifier, False, reference, True)
    if unwrapped.kind == "LIST":
        item = (unwrapped.of_type or unwrapped).unwrapped
        item_type = _leaf_python_type(schema, item)
        if item_type is not None:
            annotation: Any = list[item_type] if required else Optional[list[item_type]]
            default = typer.Option(... if required else None, flag, help=f"[{rendered}] {description}Repeatable.")
            return annotation, default, OptionSpecification(value.name, _identifier(value.name), False, reference)
        fields = ", ".join(f"{field.name}: {field.type.render()}" for field in schema.type(item.named).input_fields)
        annotation = str if required else Optional[str]
        default = typer.Option(
            ... if required else None,
            flag,
            help=f"[{rendered}] {description}JSON list of objects with fields {fields}.",
        )
        return annotation, default, OptionSpecification(value.name, _identifier(value.name), True, reference)
    leaf_type = _leaf_python_type(schema, unwrapped)
    if leaf_type is not None:
        annotation = leaf_type if required else Optional[leaf_type]
        declarations = [flag]
        if leaf_type is bool:
            declarations = [f"{flag}/--no-{command_name(value.name)}"]
        default = typer.Option(
            ... if required else None,
            *declarations,
            help=f"[{rendered}] {description}".rstrip(),
        )
        return annotation, default, OptionSpecification(value.name, _identifier(value.name), False, reference)
    fields = ", ".join(f"{field.name}: {field.type.render()}" for field in schema.type(unwrapped.named).input_fields)
    annotation = str if required else Optional[str]
    default = typer.Option(
        ... if required else None,
        flag,
        help=f"{description}JSON object with fields {fields} ({rendered}).",
    )
    return annotation, default, OptionSpecification(value.name, _identifier(value.name), True, reference)


def _plain_value(value: Any) -> Any:
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, list):
        return [_plain_value(item) for item in value]
    return value


def _decode_json(specification: OptionSpecification, value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError as exception:
        fail(f"--{command_name(specification.graphql_name)} must be valid JSON: {exception}")


def _read_secret(specification: OptionSpecification, path: Path) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exception:
        fail(f"could not read --{command_name(specification.graphql_name)}-file: {exception}")
    if not value:
        fail(f"--{command_name(specification.graphql_name)}-file is empty")
    return value


def _flattened_input(schema: Schema, field: Field) -> InputValue | None:
    if len(field.arguments) != 1:
        return None
    argument = field.arguments[0]
    if argument.name != "input" or schema.type(argument.type.named).kind != "INPUT_OBJECT":
        return None
    return argument


def _help_text(schema: Schema, operation: str, field: Field) -> str:
    del schema
    text = summary(field.description) or f"{operation.capitalize()} {field.name}."
    text = f"{text} Returns {field.type.render()}."
    if field.is_deprecated:
        text = f"{text} Deprecated."
    return text


def register_operation(
    group: typer.Typer,
    schema: Schema,
    operation: str,
    field: Field,
    *,
    command: str | None = None,
) -> None:
    flattened = _flattened_input(schema, field)
    values = tuple(schema.type(flattened.type.named).input_fields) if flattened else field.arguments
    parameters = []
    annotations: dict[str, Any] = {}
    specifications: list[OptionSpecification] = []
    for value in values:
        annotation, default, specification = _option(schema, value)
        parameters.append(
            inspect.Parameter(
                specification.identifier,
                inspect.Parameter.KEYWORD_ONLY,
                default=default,
                annotation=annotation,
            )
        )
        annotations[specification.identifier] = annotation
        specifications.append(specification)
    requires_confirmation = operation == "mutation" and field.name in CONFIRMATION_REQUIRED_MUTATIONS
    if requires_confirmation:
        parameters.append(
            inspect.Parameter(
                "yes",
                inspect.Parameter.KEYWORD_ONLY,
                default=typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
                annotation=bool,
            )
        )
        annotations["yes"] = bool
    parameters.append(
        inspect.Parameter(
            "print_query",
            inspect.Parameter.KEYWORD_ONLY,
            default=typer.Option(
                False,
                "--print-query",
                help="Print the GraphQL document and variables instead of sending them.",
            ),
            annotation=bool,
        )
    )
    annotations["print_query"] = bool
    document = schema.operation_document(operation, field)
    result_type = field.type.named

    def callback(**arguments: Any) -> None:
        print_query = arguments.pop("print_query", False)
        confirmed = arguments.pop("yes", False)
        provided: dict[str, Any] = {}
        for specification in specifications:
            raw = arguments.get(specification.identifier)
            if raw is None:
                continue
            if specification.is_secret_file:
                provided[specification.graphql_name] = _read_secret(specification, raw)
            else:
                provided[specification.graphql_name] = (
                    _decode_json(specification, raw) if specification.is_json else _plain_value(raw)
                )
        try:
            if flattened is not None:
                variables = {"input": schema.coerce_input(flattened.type, provided, "input")}
            else:
                variables = {
                    argument.name: schema.coerce_input(argument.type, provided.get(argument.name), argument.name)
                    for argument in field.arguments
                    if argument.name in provided or argument.type.is_required
                }
        except InputValidationError as exception:
            fail(str(exception))
        if print_query:
            from netaudio.commands.ddm.render import redact_sensitive_values

            typer.echo(document)
            typer.echo(json.dumps(redact_sensitive_values(variables), indent=2, sort_keys=True))
            return
        if requires_confirmation and not confirmed:
            typer.confirm(f"Proceed with {command_name(field.name)}?", abort=True)
        response = execute(document, variables, field.name[:1].upper() + field.name[1:])
        render_result(response, field.name, result_type, require_ok=operation == "mutation")

    callback.__signature__ = inspect.Signature(parameters)
    callback.__annotations__ = annotations
    callback.__name__ = _identifier(field.name)
    callback.__doc__ = _help_text(schema, operation, field)
    group.command(command or command_name(field.name), help=_help_text(schema, operation, field))(callback)


def register_schema_operations(
    group: typer.Typer,
    schema: Schema,
    *,
    excluded_fields: frozenset[str] = frozenset(),
) -> None:
    read_group = typer.Typer(help="Run read-only queries from the DDM GraphQL schema.", no_args_is_help=True)
    write_group = typer.Typer(
        help="Run DDM GraphQL mutations that can change servers, domains, or devices.",
        no_args_is_help=True,
    )
    group.add_typer(read_group, name="read")
    group.add_typer(write_group, name="write")

    for field in schema.query_fields:
        if field.name not in excluded_fields:
            _, command = operation_command_path("query", field.name)
            register_operation(read_group, schema, "query", field, command=command)

    resource_groups: dict[str, typer.Typer] = {}
    for field in schema.mutation_fields:
        if field.name in excluded_fields:
            continue
        _, resource, command = operation_command_path("mutation", field.name)
        resource_group = resource_groups.get(resource)
        if resource_group is None:
            resource_group = typer.Typer(
                help=f"Run {resource.replace('-', ' ')} mutations from the DDM GraphQL schema.",
                no_args_is_help=True,
            )
            resource_groups[resource] = resource_group
            write_group.add_typer(resource_group, name=resource)
        register_operation(resource_group, schema, "mutation", field, command=command)


__all__ = [
    "CONFIRMATION_REQUIRED_MUTATIONS",
    "operation_command_path",
    "register_operation",
    "register_schema_operations",
]
