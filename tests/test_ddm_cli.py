import json

import click
import pytest
from netaudio.cli import app
from netaudio.commands.ddm import cli as ddm_cli
from netaudio.commands.ddm import operations
from typer.testing import CliRunner

runner = CliRunner()


def _invoke(*arguments):
    return runner.invoke(app, ["ddm", *arguments])


def test_every_schema_operation_is_a_command():
    result = _invoke("schema")
    assert result.exit_code == 0, result.output
    for expected in ("device-name-set", "devices-enroll", "domain-add", "unenrolled-devices", "me"):
        assert expected in result.output


def test_schema_describes_one_type():
    result = _invoke("schema", "DeviceNameSetInput")
    assert result.exit_code == 0, result.output
    assert "deviceId: ID!" in result.output
    assert "name: String!" in result.output
    missing = _invoke("schema", "Nope")
    assert missing.exit_code == 1
    assert "unknown type Nope" in missing.output


def test_print_query_shows_document_and_flattened_input_variables():
    result = _invoke("device-name-set", "--device-id", "001dc1fffe50692e:0", "--name", "stage-left", "--print-query")
    assert result.exit_code == 0, result.output
    document, variables = result.output.split("\n", 1)
    assert document == "mutation DeviceNameSet($input: DeviceNameSetInput!) { DeviceNameSet(input: $input) { ok } }"
    assert json.loads(variables) == {"input": {"deviceId": "001dc1fffe50692e:0", "name": "stage-left"}}


def test_json_list_options_are_validated_against_the_schema():
    subscriptions = json.dumps([{"rxChannelIndex": 1, "subscribedDevice": "lx-dante", "subscribedChannel": "01"}])
    result = _invoke(
        "device-rx-channels-subscription-set",
        "--device-id",
        "001dc1fffe50692e:0",
        "--subscriptions",
        subscriptions,
        "--allow-subscription-to-non-existent-device",
        "--print-query",
    )
    assert result.exit_code == 0, result.output
    variables = json.loads(result.output.split("\n", 1)[1])
    assert variables["input"]["subscriptions"] == [
        {"rxChannelIndex": 1, "subscribedDevice": "lx-dante", "subscribedChannel": "01"}
    ]
    assert variables["input"]["allowSubscriptionToNonExistentDevice"] is True

    invalid = _invoke(
        "device-rx-channels-subscription-set",
        "--device-id",
        "x",
        "--subscriptions",
        '[{"rxChannelIndex": "one"}]',
        "--print-query",
    )
    assert invalid.exit_code == 1
    assert "input.subscriptions[0].rxChannelIndex must be an integer" in invalid.output

    malformed = _invoke(
        "device-rx-channels-subscription-set", "--device-id", "x", "--subscriptions", "{", "--print-query"
    )
    assert malformed.exit_code == 1
    assert "--subscriptions must be valid JSON" in malformed.output


def test_required_options_are_enforced_by_the_parser():
    result = _invoke("device-name-set", "--name", "x", "--print-query")
    assert result.exit_code != 0
    assert "--device-id" in result.output


def test_query_arguments_become_variables():
    result = _invoke("domain", "--id", "abc", "--print-query")
    assert result.exit_code == 0, result.output
    document, variables = result.output.split("\n", 1)
    assert document.startswith("query Domain($id: ID) { domain(id: $id) {")
    assert json.loads(variables) == {"id": "abc"}


def test_typed_commands_render_tables_from_responses(monkeypatch):
    def fake_execute(query, variables=None, operation_name=None):
        assert operation_name == "Domains"
        return {
            "data": {
                "domains": [
                    {
                        "id": "d1",
                        "name": "test",
                        "devices": [{"id": "x"}, None],
                        "status": {
                            "summary": "OK",
                            "clocking": "OK",
                            "connectivity": "OK",
                            "latency": "OK",
                            "subscriptions": "OK",
                        },
                    }
                ]
            },
            "errors": [],
        }

    monkeypatch.setattr(operations, "execute", fake_execute)
    result = _invoke("domains")
    assert result.exit_code == 0, result.output
    assert "test" in result.output and "d1" in result.output
    assert " 1 " in result.output


def test_rejected_mutations_exit_nonzero_with_the_server_message(monkeypatch):
    def fake_execute(query, variables=None, operation_name=None):
        return {"data": {"DeviceNameSet": {"ok": False, "error": {"code": "X", "message": "name taken"}}}, "errors": []}

    monkeypatch.setattr(operations, "execute", fake_execute)
    result = _invoke("device-name-set", "--device-id", "d", "--name", "n")
    assert result.exit_code == 1
    assert "DeviceNameSet was rejected: name taken" in result.output


def test_graphql_errors_are_reported_and_fail_without_data(monkeypatch):
    def fake_execute(query, variables=None, operation_name=None):
        return {"data": None, "errors": [{"message": "Unauthorized", "path": ["domains"]}]}

    monkeypatch.setattr(ddm_cli, "execute", fake_execute)
    result = _invoke("graphql", "{ domains { id } }")
    assert result.exit_code == 1
    assert "Managed API error at domains: Unauthorized" in result.output


def test_raw_graphql_passes_variables_and_prints_data(monkeypatch, tmp_path):
    captured = {}

    def fake_execute(query, variables=None, operation_name=None):
        captured.update(query=query, variables=variables, operation_name=operation_name)
        return {"data": {"me": None}, "errors": []}

    monkeypatch.setattr(ddm_cli, "execute", fake_execute)
    document = tmp_path / "query.graphql"
    document.write_text("query Me { me { id } }")
    result = _invoke("graphql", "--file", str(document), "--variables", '{"a": 1}', "--operation-name", "Me")
    assert result.exit_code == 0, result.output
    assert captured == {"query": "query Me { me { id } }", "variables": {"a": 1}, "operation_name": "Me"}
    assert json.loads(result.output) == {"me": None}
    bad = _invoke("graphql", "{ me { id } }", "--variables", "[1]")
    assert bad.exit_code == 1
    assert "--variables must be a JSON object" in bad.output


def test_unconfigured_transport_explains_how_to_configure(monkeypatch):
    from netaudio.commands.ddm import transport

    monkeypatch.setattr(transport, "configured_client", lambda: None)

    async def unavailable(*arguments, **options):
        return None, None

    monkeypatch.setattr(transport, "execute_ddm_graphql_on_daemon", unavailable)
    with pytest.raises(click.exceptions.Exit):
        transport.execute("{ me { id } }")
