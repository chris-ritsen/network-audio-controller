from __future__ import annotations

import argparse
import json
import keyword
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = {
    "inputs": ROOT / "packages/netaudio/src/netaudio/core/_requests.py",
    "outputs": ROOT / "packages/netaudio/src/netaudio/core/_types.py",
}


def generate(schemas: dict) -> str:
    definitions = {}

    def register(name, schema):
        if not name.isidentifier() or keyword.iskeyword(name):
            raise ValueError(f"invalid type name: {name}")

        schema = {key: value for key, value in schema.items() if key not in {"$defs", "$schema", "title"}}

        if name in definitions and definitions[name] != schema:
            raise ValueError(f"conflicting schemas for {name}")

        definitions[name] = schema

    for name, schema in schemas.items():
        register(name, schema)

        for child_name, child in schema.get("$defs", {}).items():
            register(child_name, child)

    emitted = set()
    visiting = set()
    declarations = []
    uses_json = False

    def annotation(schema, name_hint=None):
        nonlocal uses_json

        if schema is True or schema == {}:
            uses_json = True
            return "JsonValue"

        if not isinstance(schema, dict):
            raise ValueError("unconstrained schemas have no concrete client type")

        if "$ref" in schema:
            reference = schema["$ref"]

            if not reference.startswith("#/$defs/"):
                raise ValueError(f"unsupported schema reference: {reference}")

            name = reference.removeprefix("#/$defs/")
            emit(name)
            return name

        for union in ("anyOf", "oneOf"):
            if union in schema:
                variants = []

                for index, part in enumerate(schema[union]):
                    tags = (
                        [
                            field["const"]
                            for field in part.get("properties", {}).values()
                            if isinstance(field, dict) and isinstance(field.get("const"), str)
                        ]
                        if isinstance(part, dict)
                        else []
                    )
                    suffix = tags[0].title().replace("_", "") if len(tags) == 1 else f"Variant{index}"
                    variants.append(annotation(part, f"{name_hint}{suffix}"))

                return f"_typing.Union[{', '.join(variants)}]"

        if "const" in schema:
            return f"_typing.Literal[{schema['const']!r}]"

        if "enum" in schema:
            return f"_typing.Literal[{', '.join(repr(value) for value in schema['enum'])}]"

        kind = schema.get("type")

        if isinstance(kind, list):
            return f"_typing.Union[{', '.join(annotation({**schema, 'type': item}, name_hint) for item in kind)}]"

        if kind == "array" and "items" in schema:
            return f"list[{annotation(schema['items'], f'{name_hint}Item')}]"

        if kind == "object" and "properties" in schema and name_hint:
            register(name_hint, schema)
            emit(name_hint)
            return name_hint

        if kind == "object" and "properties" not in schema:
            patterns = schema.get("patternProperties", {})

            if len(patterns) == 1 and schema.get("additionalProperties") is False:
                return f"dict[str, {annotation(next(iter(patterns.values())))}]"

            if not patterns and "additionalProperties" in schema:
                return f"dict[str, {annotation(schema['additionalProperties'])}]"

        scalars = {"integer": "int", "number": "float", "string": "str", "boolean": "bool", "null": "None"}

        if kind in scalars:
            return scalars[kind]

        raise ValueError(f"unsupported schema shape: {schema}")

    def emit(name):
        if name in emitted:
            return

        if name in visiting or name not in definitions:
            raise ValueError(f"recursive or missing schema: {name}")

        visiting.add(name)
        schema = definitions[name]

        if schema.get("type") == "object" and "properties" in schema:
            fields = []
            required = set(schema.get("required", []))

            for field, value in sorted(schema["properties"].items()):
                if not field.isidentifier() or keyword.iskeyword(field):
                    raise ValueError(f"invalid field name: {field}")

                field_type = annotation(value, f"{name}{field.title().replace('_', '')}")

                if field not in required:
                    field_type = f"_extensions.NotRequired[{field_type}]"

                fields.append(f"    {field}: {field_type}")

            declaration = f"class {name}(_extensions.TypedDict):\n" + "\n".join(fields or ["    pass"])
        else:
            declaration = f"{name} = {annotation(schema, name)}"

        visiting.remove(name)
        emitted.add(name)
        declarations.append(declaration)

    for name in sorted(definitions):
        emit(name)

    json_type = (
        'JsonValue = _typing.Union[None, bool, int, float, str, list["JsonValue"], dict[str, "JsonValue"]]\n\n\n'
        if uses_json
        else ""
    )

    return (
        "# Generated from Rust schemas by scripts/generate_core_types.py. Do not edit.\n"
        "import typing as _typing\nimport typing_extensions as _extensions\n\n\n"
        + json_type
        + "\n\n\n".join(declarations)
        + "\n"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--schema-json", type=Path)
    args = parser.parse_args()

    if args.schema_json:
        schemas = json.loads(args.schema_json.read_text())
    else:
        result = subprocess.run(
            [
                "cargo",
                "run",
                "--locked",
                *(["--offline"] if args.offline else []),
                "--quiet",
                "--manifest-path",
                str(ROOT / "packages/netaudio-core/Cargo.toml"),
                "--features",
                "schema",
                "--bin",
                "export-schema",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
        schemas = json.loads(result.stdout)

    for contract, output in OUTPUTS.items():
        source = subprocess.run(
            [sys.executable, "-m", "ruff", "format", "--stdin-filename", str(output), "-"],
            input=generate(schemas[contract]),
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout

        if args.check:
            if not output.exists() or output.read_text() != source:
                raise SystemExit(f"{output.name} is stale; run scripts/generate_core_types.py")
        else:
            output.write_text(source)


if __name__ == "__main__":
    main()
