from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from typing import Any, Mapping

BUILTIN_SCALAR_TYPES = frozenset({"Boolean", "Float", "ID", "Int", "String"})
COMPOSITE_KINDS = frozenset({"INTERFACE", "OBJECT", "UNION"})
LEAF_KINDS = frozenset({"ENUM", "SCALAR"})
MAXIMUM_SELECTION_DEPTH = 1
SCHEMA_RESOURCE = "schema.json"


class SchemaError(ValueError):
    pass


class InputValidationError(SchemaError):
    pass


@dataclass(frozen=True)
class TypeReference:
    kind: str
    name: str | None = None
    of_type: TypeReference | None = None

    @classmethod
    def from_introspection(cls, value: Mapping[str, Any]) -> TypeReference:
        inner = value.get("ofType")
        return cls(
            kind=value["kind"],
            name=value.get("name"),
            of_type=cls.from_introspection(inner) if inner else None,
        )

    @property
    def is_list(self) -> bool:
        return self.unwrapped.kind == "LIST"

    @property
    def is_required(self) -> bool:
        return self.kind == "NON_NULL"

    @property
    def named(self) -> str:
        reference = self
        while reference.name is None:
            if reference.of_type is None:
                raise SchemaError("type reference has no named type")
            reference = reference.of_type
        return reference.name

    def render(self) -> str:
        if self.kind == "NON_NULL" and self.of_type is not None:
            return f"{self.of_type.render()}!"
        if self.kind == "LIST" and self.of_type is not None:
            return f"[{self.of_type.render()}]"
        return self.name or self.kind

    @property
    def unwrapped(self) -> TypeReference:
        if self.kind == "NON_NULL" and self.of_type is not None:
            return self.of_type
        return self


@dataclass(frozen=True)
class InputValue:
    description: str | None
    name: str
    type: TypeReference


@dataclass(frozen=True)
class Field:
    arguments: tuple[InputValue, ...]
    description: str | None
    is_deprecated: bool
    name: str
    type: TypeReference


@dataclass(frozen=True)
class SchemaType:
    description: str | None
    enum_values: tuple[str, ...]
    fields: tuple[Field, ...]
    input_fields: tuple[InputValue, ...]
    kind: str
    name: str
    possible_types: tuple[str, ...]


def command_name(graphql_name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "-", graphql_name).lower()


def operation_name(field_name: str) -> str:
    return field_name[:1].upper() + field_name[1:]


def _parse_input_value(value: Mapping[str, Any]) -> InputValue:
    return InputValue(
        description=value.get("description"),
        name=value["name"],
        type=TypeReference.from_introspection(value["type"]),
    )


def _parse_field(value: Mapping[str, Any]) -> Field:
    return Field(
        arguments=tuple(_parse_input_value(argument) for argument in value.get("args") or ()),
        description=value.get("description"),
        is_deprecated=bool(value.get("isDeprecated")),
        name=value["name"],
        type=TypeReference.from_introspection(value["type"]),
    )


def _parse_type(value: Mapping[str, Any]) -> SchemaType:
    return SchemaType(
        description=value.get("description"),
        enum_values=tuple(item["name"] for item in value.get("enumValues") or ()),
        fields=tuple(_parse_field(field) for field in value.get("fields") or ()),
        input_fields=tuple(_parse_input_value(field) for field in value.get("inputFields") or ()),
        kind=value["kind"],
        name=value["name"],
        possible_types=tuple(item["name"] for item in value.get("possibleTypes") or ()),
    )


def _coerce_scalar(scalar_name: str, value: Any, path: str) -> Any:
    if scalar_name == "Boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in {"false", "true"}:
            return value.lower() == "true"
        raise InputValidationError(f"{path} must be true or false")
    if scalar_name == "Int":
        if isinstance(value, bool):
            raise InputValidationError(f"{path} must be an integer")
        if isinstance(value, int):
            return value
        if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
            return int(value)
        raise InputValidationError(f"{path} must be an integer")
    if scalar_name == "Float":
        if isinstance(value, bool):
            raise InputValidationError(f"{path} must be a number")
        if isinstance(value, (int, float)):
            return value
        try:
            return float(value)
        except (TypeError, ValueError) as error:
            raise InputValidationError(f"{path} must be a number") from error
    if scalar_name in {"ID", "String"}:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise InputValidationError(f"{path} must be a string")
        return str(value)
    return value


class Schema:
    def __init__(self, document: Mapping[str, Any]):
        schema = document["__schema"]
        self.types: dict[str, SchemaType] = {
            item["name"]: _parse_type(item) for item in schema["types"] if not item["name"].startswith("__")
        }
        self.query_type_name: str = schema["queryType"]["name"]
        mutation_type = schema.get("mutationType")
        self.mutation_type_name: str | None = mutation_type["name"] if mutation_type else None
        subscription_type = schema.get("subscriptionType")
        self.subscription_type_name: str | None = subscription_type["name"] if subscription_type else None

    @classmethod
    def load(cls) -> Schema:
        return _load_bundled_schema()

    def coerce_input(self, reference: TypeReference, value: Any, path: str) -> Any:
        if reference.kind == "NON_NULL":
            if value is None:
                raise InputValidationError(f"{path} is required ({reference.render()})")
            return self.coerce_input(reference.of_type or reference, value, path)
        if value is None:
            return None
        if reference.kind == "LIST":
            items = value if isinstance(value, list) else [value]
            return [
                self.coerce_input(reference.of_type or reference, item, f"{path}[{index}]")
                for index, item in enumerate(items)
            ]
        schema_type = self.type(reference.named)
        if schema_type.kind == "ENUM":
            if not isinstance(value, str) or value not in schema_type.enum_values:
                raise InputValidationError(f"{path} must be one of {', '.join(schema_type.enum_values)}")
            return value
        if schema_type.kind == "SCALAR":
            return _coerce_scalar(schema_type.name, value, path)
        if schema_type.kind == "INPUT_OBJECT":
            return self._coerce_input_object(schema_type, value, path)
        raise InputValidationError(f"{path} has unsupported input type {schema_type.name}")

    def describe_field(self, field: Field) -> list[str]:
        lines = [f"{field.name}: {field.type.render()}"]
        if field.description:
            lines.append(f"  {field.description}")
        for argument in field.arguments:
            lines.append(f"  {argument.name}: {argument.type.render()}")
        return lines

    def describe_type(self, name: str) -> list[str]:
        schema_type = self.type(name)
        lines = [f"{schema_type.kind.lower()} {schema_type.name}"]
        if schema_type.description:
            lines.append(f"  {schema_type.description}")
        for field in schema_type.fields:
            arguments = ", ".join(f"{argument.name}: {argument.type.render()}" for argument in field.arguments)
            suffix = f"({arguments})" if arguments else ""
            lines.append(f"  {field.name}{suffix}: {field.type.render()}")
        for input_field in schema_type.input_fields:
            lines.append(f"  {input_field.name}: {input_field.type.render()}")
        for value in schema_type.enum_values:
            lines.append(f"  {value}")
        for possible in schema_type.possible_types:
            lines.append(f"  ... on {possible}")
        return lines

    def field(self, operation: str, name: str) -> Field:
        fields = self.query_fields if operation == "query" else self.mutation_fields
        for field in fields:
            if name in {field.name, command_name(field.name)}:
                return field
        raise SchemaError(f"unknown {operation} field {name}")

    @property
    def mutation_fields(self) -> tuple[Field, ...]:
        if self.mutation_type_name is None:
            return ()
        return tuple(sorted(self.type(self.mutation_type_name).fields, key=lambda field: command_name(field.name)))

    def operation_document(self, operation: str, field: Field) -> str:
        variables = ", ".join(f"${argument.name}: {argument.type.render()}" for argument in field.arguments)
        arguments = ", ".join(f"{argument.name}: ${argument.name}" for argument in field.arguments)
        head = f"{operation} {operation_name(field.name)}"
        if variables:
            head = f"{head}({variables})"
        call = field.name
        if arguments:
            call = f"{call}({arguments})"
        selection = self.operation_selection_set(operation, field.type.named)
        if selection:
            call = f"{call} {selection}"
        return f"{head} {{ {call} }}"

    def operation_selection_set(self, operation: str, type_name: str) -> str:
        if operation != "mutation":
            return self.selection_set(type_name)
        schema_type = self.type(type_name)
        parts = []
        for field in schema_type.fields:
            if field.name in {"password", "recoveryCode", "token", "keyToken"}:
                continue
            if self.type(field.type.named).kind in LEAF_KINDS:
                parts.append(field.name)
            elif field.name == "error":
                selection = self._field_selection(field, 1, frozenset({type_name}))
                if selection is not None:
                    parts.append(selection)
        return "{ " + " ".join(parts) + " }" if parts else ""

    @property
    def query_fields(self) -> tuple[Field, ...]:
        return tuple(sorted(self.type(self.query_type_name).fields, key=lambda field: command_name(field.name)))

    def selection_set(self, type_name: str, depth: int = MAXIMUM_SELECTION_DEPTH) -> str:
        if self.type(type_name).kind not in COMPOSITE_KINDS:
            return ""
        parts = self._selection_parts(type_name, depth, frozenset())
        if not parts:
            return ""
        return "{ " + " ".join(parts) + " }"

    def type(self, name: str) -> SchemaType:
        try:
            return self.types[name]
        except KeyError:
            raise SchemaError(f"unknown type {name}") from None

    def _coerce_input_object(self, schema_type: SchemaType, value: Any, path: str) -> dict[str, Any]:
        expected = ", ".join(f"{field.name}: {field.type.render()}" for field in schema_type.input_fields)
        if not isinstance(value, Mapping):
            raise InputValidationError(f"{path} must be an object with fields {expected}")
        known = {field.name for field in schema_type.input_fields}
        unknown = sorted(set(value) - known)
        if unknown:
            raise InputValidationError(f"{path} has unknown fields {', '.join(unknown)}; expected {expected}")
        result: dict[str, Any] = {}
        for input_field in schema_type.input_fields:
            present = input_field.name in value
            coerced = self.coerce_input(input_field.type, value.get(input_field.name), f"{path}.{input_field.name}")
            if present or coerced is not None:
                result[input_field.name] = coerced
        return result

    def _field_selection(self, field: Field, depth: int, ancestors: frozenset[str]) -> str | None:
        target = self.type(field.type.named)
        if target.kind in LEAF_KINDS:
            return field.name
        if depth <= 0 or target.name in ancestors:
            return None
        inner = self._selection_parts(target.name, depth - 1, ancestors)
        if not inner:
            return None
        return f"{field.name} {{ {' '.join(inner)} }}"

    def _selection_parts(self, type_name: str, depth: int, ancestors: frozenset[str]) -> list[str]:
        schema_type = self.type(type_name)
        lineage = ancestors | {type_name}
        parts: list[str] = []
        if schema_type.kind in {"INTERFACE", "UNION"}:
            parts.append("__typename")
        for field in schema_type.fields:
            if any(argument.type.is_required for argument in field.arguments):
                continue
            selection = self._field_selection(field, depth, lineage)
            if selection is not None:
                parts.append(selection)
        if schema_type.kind in {"INTERFACE", "UNION"}:
            for possible in schema_type.possible_types:
                if possible in lineage:
                    continue
                inline = [part for part in self._selection_parts(possible, depth, lineage) if part not in parts]
                if inline:
                    parts.append(f"... on {possible} {{ {' '.join(inline)} }}")
        return parts


@lru_cache(maxsize=1)
def _load_bundled_schema() -> Schema:
    document = json.loads(resources.files(__package__).joinpath(SCHEMA_RESOURCE).read_text(encoding="utf-8"))
    return Schema(document)


__all__ = [
    "BUILTIN_SCALAR_TYPES",
    "Field",
    "InputValidationError",
    "InputValue",
    "MAXIMUM_SELECTION_DEPTH",
    "Schema",
    "SchemaError",
    "SchemaType",
    "TypeReference",
    "command_name",
    "operation_name",
]
