from typing import Union, get_args

import pytest

from scripts.generate_core_types import generate, generate_protocols


def test_generated_types_preserve_required_nullable_fields_and_named_references():
    schemas = {
        "Result": {
            "type": "object",
            "properties": {
                "count": {"type": ["integer", "null"]},
                "items": {"type": "array", "items": {"$ref": "#/$defs/Item"}},
            },
            "required": ["count", "items"],
            "$defs": {"Item": {"type": "object", "properties": {"name": {"type": "string"}}}},
        }
    }
    source = generate(schemas)
    namespace = {}
    exec(compile(source, "generated_types.py", "exec"), namespace)

    assert namespace["Result"].__required_keys__ == frozenset({"count", "items"})
    assert namespace["Item"].__optional_keys__ == frozenset({"name"})
    hints = namespace["Result"].__annotations__
    assert hints["count"] == Union[int, None]
    assert get_args(hints["items"]) == (namespace["Item"],)


@pytest.mark.parametrize("schema", [{"allOf": [{"type": "string"}]}, {"$ref": "https://example.invalid/type"}])
def test_unsupported_type_shapes_are_not_silently_widened(schema):
    with pytest.raises(ValueError):
        generate({"Broken": schema})


def test_conflicting_named_types_are_rejected():
    with pytest.raises(ValueError, match="conflicting"):
        generate(
            {
                "First": {"type": "string", "$defs": {"Shared": {"type": "integer"}}},
                "Second": {"type": "string", "$defs": {"Shared": {"type": "string"}}},
            }
        )


def test_tagged_variants_keep_their_discriminators_and_required_fields():
    source = generate(
        {
            "Request": {
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {"mode": {"const": "dhcp", "type": "string"}},
                        "required": ["mode"],
                    },
                    {
                        "type": "object",
                        "properties": {"mode": {"const": "static", "type": "string"}, "address": {"type": "string"}},
                        "required": ["mode", "address"],
                    },
                ]
            }
        }
    )
    namespace = {}
    exec(compile(source, "generated_types.py", "exec"), namespace)
    dynamic, static = get_args(namespace["Request"])

    assert dynamic.__required_keys__ == frozenset({"mode"})
    assert static.__required_keys__ == frozenset({"mode", "address"})
    assert get_args(dynamic.__annotations__["mode"]) == ("dhcp",)
    assert get_args(static.__annotations__["mode"]) == ("static",)


def test_typed_maps_preserve_value_contracts_and_reject_untyped_objects():
    source = generate(
        {
            "ObservedValues": {"type": "object", "additionalProperties": {"type": "integer"}},
            "PropertyValues": {
                "type": "object",
                "patternProperties": {r"^\d+$": True},
                "additionalProperties": False,
            },
            "Reports": {
                "type": "object",
                "additionalProperties": {"$ref": "#/$defs/Report"},
                "$defs": {"Report": {"type": "object", "properties": {"accepted": {"type": "boolean"}}}},
            },
        }
    )
    namespace = {}
    exec(compile(source, "generated_types.py", "exec"), namespace)

    assert namespace["ObservedValues"] == dict[str, int]
    report_type = namespace["Report"]
    assert namespace["Reports"] == dict[str, report_type]
    assert get_args(namespace["PropertyValues"]) == (str, namespace["JsonValue"])

    with pytest.raises(ValueError, match="unsupported schema"):
        generate({"Unspecified": {"type": "object"}})


def test_explicit_json_values_do_not_erase_neighboring_field_types():
    source = generate(
        {
            "Request": {
                "type": "object",
                "properties": {"state": True, "enabled": {"type": "boolean"}},
                "required": ["state", "enabled"],
            }
        }
    )
    namespace = {}
    exec(compile(source, "generated_types.py", "exec"), namespace)

    hints = namespace["Request"].__annotations__
    assert hints["state"] == namespace["JsonValue"]
    assert hints["enabled"] is bool


def test_input_omission_and_output_nullability_have_distinct_contracts():
    properties = {"source": {"type": ["string", "null"]}}
    request = generate({"Request": {"type": "object", "properties": properties}})
    response = generate({"Response": {"type": "object", "properties": properties, "required": ["source"]}})
    namespace = {}
    exec(compile(request + "\n" + response, "generated_types.py", "exec"), namespace)

    assert namespace["Request"].__optional_keys__ == frozenset({"source"})
    assert namespace["Response"].__required_keys__ == frozenset({"source"})


def test_tagged_request_with_json_payload_preserves_both_types():
    source = generate(
        {
            "Request": {
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {"kind": {"const": "command"}, "payload": True},
                        "required": ["kind", "payload"],
                    }
                ]
            }
        }
    )
    namespace = {}
    exec(compile(source, "generated_types.py", "exec"), namespace)
    request = namespace["Request"]

    assert request.__required_keys__ == frozenset({"kind", "payload"})
    assert get_args(request.__annotations__["kind"]) == ("command",)
    assert request.__annotations__["payload"] == namespace["JsonValue"]


def test_protocol_catalog_generation_preserves_native_classification():
    namespace = {}
    source = generate_protocols(
        [
            {"protocol_id": 10, "family": "ARC", "modern_arc": True},
            {"protocol_id": 11, "family": "CMC", "modern_arc": False},
            {"protocol_id": 12, "family": "ARC", "modern_arc": False},
        ]
    )
    exec(compile(source, "protocols.py", "exec"), namespace)

    assert namespace["ARC_PROTOCOL_IDS"] == (10, 12)
    assert namespace["MODERN_ARC_PROTOCOL_IDS"] == (10,)
    assert namespace["CAPTURE_PROTOCOL_IDS"] == (10, 11, 12)
    assert namespace["PROTOCOL_LABELS"] == {10: "ARC", 11: "CMC", 12: "ARC"}


@pytest.mark.parametrize(
    "records",
    [
        [{"protocol_id": 10, "family": "unexpected", "modern_arc": False}],
        [{"protocol_id": True, "family": "ARC", "modern_arc": False}],
        [{"protocol_id": 10, "family": "ARC", "modern_arc": False}] * 2,
    ],
)
def test_protocol_catalog_rejects_ambiguous_or_unsupported_metadata(records):
    with pytest.raises(ValueError):
        generate_protocols(records)
