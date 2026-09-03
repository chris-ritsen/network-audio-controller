import json

import pytest
from netaudio.ddm import InputValidationError, Schema, SchemaError, TypeReference, command_name, operation_name


@pytest.fixture(scope="module")
def schema():
    return Schema.load()


def test_bundled_schema_has_the_documented_roots(schema):
    assert [field.name for field in schema.query_fields] == [
        "deviceEntitlements",
        "domain",
        "domains",
        "me",
        "unenrolledDevices",
    ]
    assert len(schema.mutation_fields) == 31
    assert schema.subscription_type_name is None


def test_command_names_are_kebab_case_and_unique(schema):
    query_names = [command_name(field.name) for field in schema.query_fields]
    mutation_names = [command_name(field.name) for field in schema.mutation_fields]
    assert query_names == sorted(query_names)
    assert mutation_names == sorted(mutation_names)
    assert len(query_names + mutation_names) == len(set(query_names + mutation_names))
    assert (
        command_name("DeviceClockingPTPV1UnicastDelayRequestSet") == "device-clocking-ptpv1-unicast-delay-request-set"
    )
    assert command_name("unenrolledDevices") == "unenrolled-devices"
    assert operation_name("unenrolledDevices") == "UnenrolledDevices"


def test_every_input_reference_resolves_to_a_named_type(schema):
    for schema_type in schema.types.values():
        for field in schema_type.input_fields + schema_type.fields:
            assert field.type.named in schema.types, (schema_type.name, field.name)
    reference = schema.type("DeviceRxChannelsSubscriptionSetInput").input_fields[1].type
    assert reference.render() == "[DeviceRxChannelsSubscriptionInput!]!"
    assert reference.is_required and reference.is_list


def test_operation_documents_select_only_argument_free_leaf_fields(schema):
    document = schema.operation_document("query", schema.field("query", "domain"))
    assert document.startswith("query Domain($id: ID) { domain(id: $id) { ")
    assert "devices { id name" in document
    assert "(" not in document.split("{", 2)[2]
    mutation = schema.operation_document("mutation", schema.field("mutation", "device-name-set"))
    assert mutation == "mutation DeviceNameSet($input: DeviceNameSetInput!) { DeviceNameSet(input: $input) { ok } }"
    assert len(document) < 1_500
    assert "devices { id name" in document
    assert "devices { id name domain" not in document


def test_mutation_documents_omit_secret_results(schema):
    document = schema.operation_document("mutation", schema.field("mutation", "UserAPIKeyAdd"))

    assert document.endswith("{ UserAPIKeyAdd(input: $input) { ok } }")
    assert "keyToken" not in document


def test_interface_selections_use_inline_fragments_for_possible_types(schema):
    selection = schema.selection_set("DeviceParameter")
    assert selection.startswith("{ __typename")
    assert "... on DeviceParameterDiscrete { options }" in selection


def test_selection_sets_do_not_recurse_into_ancestor_types(schema):
    selection = schema.selection_set("Domain", depth=2)
    assert selection.count("domain {") == 0
    assert schema.selection_set("String") == ""


def test_input_coercion_validates_nested_objects_lists_enums_and_scalars(schema):
    reference = schema.type("DeviceRxChannelsSubscriptionSetInput")
    input_reference = TypeReference("NON_NULL", None, TypeReference("INPUT_OBJECT", reference.name))
    value = schema.coerce_input(
        input_reference,
        {
            "deviceId": "001dc1fffe50692e:0",
            "subscriptions": {"rxChannelIndex": "1", "subscribedDevice": "lx-dante", "subscribedChannel": "01"},
            "allowSubscriptionToNonExistentDevice": "true",
        },
        "input",
    )
    assert value == {
        "deviceId": "001dc1fffe50692e:0",
        "subscriptions": [{"rxChannelIndex": 1, "subscribedDevice": "lx-dante", "subscribedChannel": "01"}],
        "allowSubscriptionToNonExistentDevice": True,
    }
    with pytest.raises(InputValidationError, match="input.subscriptions\\[0\\].rxChannelIndex must be an integer"):
        schema.coerce_input(
            input_reference,
            {
                "deviceId": "x",
                "subscriptions": [{"rxChannelIndex": "one", "subscribedDevice": "a", "subscribedChannel": "b"}],
            },
            "input",
        )
    with pytest.raises(InputValidationError, match="input.deviceId is required"):
        schema.coerce_input(input_reference, {"subscriptions": []}, "input")
    with pytest.raises(InputValidationError, match="unknown fields bogus"):
        schema.coerce_input(input_reference, {"deviceId": "x", "subscriptions": [], "bogus": 1}, "input")
    with pytest.raises(SchemaError, match="unknown type"):
        schema.type("Nope")


def test_enum_inputs_must_match_declared_values(schema):
    enum_name = next(name for name, item in schema.types.items() if item.kind == "ENUM" and item.enum_values)
    values = schema.type(enum_name).enum_values
    reference = TypeReference("ENUM", enum_name)
    assert schema.coerce_input(reference, values[0], "value") == values[0]
    with pytest.raises(InputValidationError, match="must be one of"):
        schema.coerce_input(reference, "not-a-value", "value")


def test_schema_fixture_is_stable_json(schema):
    from importlib import resources

    document = json.loads(resources.files("netaudio.ddm").joinpath("schema.json").read_text())
    assert document["__schema"]["queryType"]["name"] == schema.query_type_name
    assert json.dumps(document, indent=1, sort_keys=True) == resources.files("netaudio.ddm").joinpath(
        "schema.json"
    ).read_text().rstrip("\n")
