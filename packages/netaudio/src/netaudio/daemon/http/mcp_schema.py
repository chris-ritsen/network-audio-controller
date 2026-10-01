from __future__ import annotations

import difflib
import json
import re

DEVICE_DESCRIPTION = "Device name, server name, or inventory ID as returned by list_devices."
CONFIRMATION_DESCRIPTION = "Set true only after the user authorized this change. Leave it out to see what would change without changing anything."
TYPE_NAMES = {
    "array": "a list",
    "boolean": "true or false",
    "integer": "a whole number",
    "number": "a number",
    "object": "an object",
    "string": "text",
}
ARGUMENT_ALIASES = {"direction": "channel_type", "channel_name": "channel", "label": "name", "new_name": "name"}
CHANNEL_ARGUMENT = {
    "oneOf": [{"type": "integer", "minimum": 1}, {"type": "string", "minLength": 1}],
    "description": "Channel number or unique label.",
}


def device_property() -> dict:
    return {"type": "string", "description": DEVICE_DESCRIPTION}


def object_schema(properties: dict, required: list[str]) -> dict:
    return {"type": "object", "properties": properties, "required": required, "additionalProperties": False}


def with_confirmation(schema: dict) -> dict:
    properties = dict(schema["properties"])
    properties["confirmed"] = {"type": "boolean", "description": CONFIRMATION_DESCRIPTION}
    required = [key for key in schema["required"] if key != "confirmed"]
    return object_schema(properties, required)


def _has_type(value, expected) -> bool:
    if isinstance(expected, list):
        return any(_has_type(value, option) for option in expected)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "string":
        return isinstance(value, str)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    return True


def _describe(schema: dict) -> str:
    if "oneOf" in schema:
        return " or ".join(_describe(option) for option in schema["oneOf"])
    if "enum" in schema:
        return "one of " + ", ".join(json.dumps(item) for item in schema["enum"])
    expected = schema.get("type")
    if isinstance(expected, list):
        return " or ".join(TYPE_NAMES.get(option, option) for option in expected)
    return TYPE_NAMES.get(expected, "a valid value")


def _join(path: str, key) -> str:
    if isinstance(key, int):
        return f"{path}[{key}]"
    return f"{path}.{key}" if path else str(key)


def close_matches(word: str, candidates, limit: int = 3) -> list[str]:
    candidates = [candidate for candidate in candidates if isinstance(candidate, str)]
    key = word.casefold()
    contained = [
        candidate for candidate in candidates if key and (key in candidate.casefold() or candidate.casefold() in key)
    ]
    if contained:
        return contained[:limit]
    folded = {candidate.casefold(): candidate for candidate in candidates}
    return [folded[match] for match in difflib.get_close_matches(key, list(folded), n=limit, cutoff=0.6)]


def schema_errors(schema: dict, value, path: str = "", ignore_required: frozenset[str] = frozenset()) -> list[str]:
    label = path or "arguments"
    if "oneOf" in schema:
        if any(not schema_errors(option, value, path) for option in schema["oneOf"]):
            return []
        return [f"{label} must be {_describe(schema)}; got {json.dumps(value, default=str)}"]
    expected = schema.get("type")
    if expected and not _has_type(value, expected):
        options = expected if isinstance(expected, list) else [expected]
        wanted = " or ".join(TYPE_NAMES.get(option, option) for option in options)
        return [f"{label} must be {wanted}; got {json.dumps(value, default=str)}"]
    if "enum" in schema and value not in schema["enum"]:
        return [f"{label} must be {_describe(schema)}; got {json.dumps(value, default=str)}"]
    errors = []
    if _has_type(value, "number"):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{label} must be at least {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{label} must be at most {schema['maximum']}")
        if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
            errors.append(f"{label} must be greater than {schema['exclusiveMinimum']}")
    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            errors.append(f"{label} must not be empty" if schema["minLength"] == 1 else f"{label} is too short")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(f"{label} must be at most {schema['maxLength']} characters; it has {len(value)}")
        if "pattern" in schema and not re.search(schema["pattern"], value):
            errors.append(f"{label} must match {schema['pattern']}")
    if isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            errors.append(f"{label} needs at least {schema['minItems']} item(s)")
        if "items" in schema:
            for index, item in enumerate(value):
                errors.extend(schema_errors(schema["items"], item, _join(label, index)))
    if isinstance(value, dict) and "properties" in schema:
        properties = schema["properties"]
        if schema.get("additionalProperties") is False:
            for key in sorted(set(value) - set(properties)):
                alias = ARGUMENT_ALIASES.get(key)
                suggestion = [alias] if alias in properties else close_matches(key, properties, limit=1)
                hint = f" (did you mean {suggestion[0]}?)" if suggestion else ""
                errors.append(f"{_join(path, key)} is not accepted{hint}")
        for key in schema.get("required", []):
            if key not in value and key not in ignore_required:
                errors.append(f"{_join(path, key)} is required")
        for key, item in value.items():
            if key in properties:
                errors.extend(schema_errors(properties[key], item, _join(path, key)))
    return errors
