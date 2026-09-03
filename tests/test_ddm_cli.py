import json
import stat

import click
import pytest
from netaudio.cli import app
from netaudio.commands.ddm import cli as ddm_cli
from netaudio.commands.ddm import operations
from netaudio.ddm import AuthenticationError
from typer.testing import CliRunner

runner = CliRunner()


def _invoke(*arguments, **options):
    return runner.invoke(app, ["ddm", *arguments], **options)


def test_every_schema_operation_is_a_command():
    result = _invoke("schema")
    assert result.exit_code == 0, result.output
    for expected in ("device-name-set", "devices-enroll", "domain-add", "unenrolled-devices", "me"):
        assert expected in result.output


def test_password_login_uses_a_hidden_prompt_and_saves_the_token(monkeypatch, tmp_path):
    captured = {}
    credential_file = tmp_path / "ddm-token"

    def fake_authenticate(url, username, password, *, allow_insecure_http):
        captured.update(
            url=url,
            username=username,
            password=password,
            allow_insecure_http=allow_insecure_http,
        )
        return "session-token"

    monkeypatch.setattr(ddm_cli, "authenticate_with_password", fake_authenticate)
    result = _invoke(
        "login",
        "--url",
        "http://manager.example/graphql",
        "--username",
        "operator",
        "--credential-file",
        str(credential_file),
        "--allow-insecure-http",
        input="private-password\n",
    )

    assert result.exit_code == 0, result.output
    assert captured == {
        "url": "http://manager.example/graphql",
        "username": "operator",
        "password": "private-password",
        "allow_insecure_http": True,
    }
    assert credential_file.read_text(encoding="ascii") == "session-token\n"
    assert stat.S_IMODE(credential_file.stat().st_mode) == 0o600
    assert "session-token" not in result.output
    assert "private-password" not in result.output


def test_password_login_does_not_replace_a_credential_after_rejection(monkeypatch, tmp_path):
    credential_file = tmp_path / "ddm-token"
    credential_file.write_text("existing-token\n", encoding="ascii")

    def reject(*arguments, **options):
        raise AuthenticationError("DDM rejected username/password login")

    monkeypatch.setattr(ddm_cli, "authenticate_with_password", reject)
    result = _invoke(
        "login",
        "--url",
        "https://manager.example/graphql",
        "--username",
        "operator",
        "--credential-file",
        str(credential_file),
        input="private-password\n",
    )

    assert result.exit_code == 1
    assert credential_file.read_text(encoding="ascii") == "existing-token\n"
    assert "private-password" not in result.output


def test_generated_password_mutation_is_replaced_by_the_dedicated_login_command():
    help_result = _invoke("--help")
    login_help = _invoke("login", "--help")

    assert help_result.exit_code == 0
    assert "user-login-with-password" not in help_result.output
    assert "login" in help_result.output
    assert login_help.exit_code == 0
    assert "--password" not in login_help.output


def test_ddm_discover_renders_correlated_services(monkeypatch):
    class Service:
        def __init__(self, port):
            self.port = port

    class Server:
        server_name = "ddm.local."
        ipv4_addresses = ("192.168.1.217",)
        controller_service = Service(8443)
        device_service = Service(8000)

        def to_json(self):
            return {"server_name": self.server_name}

    async def fake_discover(*, timeout, interfaces=None):
        assert timeout == 0.25
        assert interfaces is None
        return (Server(),)

    monkeypatch.setattr(ddm_cli, "discover_ddm_servers", fake_discover)

    result = _invoke("discover", "--timeout", "0.25")

    assert result.exit_code == 0, result.output
    assert "ddm.local." in result.output
    assert "192.168.1.217" in result.output
    assert "8443" in result.output
    assert "8000" in result.output


def test_managed_identify_discovers_ddm_and_keeps_password_out_of_output(monkeypatch):
    captured = {}

    class Service:
        port = 8443

    class Server:
        server_name = "ddm.local."
        controller_service = Service()

    async def fake_discover(*, timeout, interfaces=None):
        assert timeout == 2.0
        assert interfaces is None
        return (Server(),)

    def fake_identify(server, username, password, device_id, mac, **options):
        captured.update(
            server=server,
            username=username,
            password=password,
            device_id=device_id,
            mac=mac,
            options=options,
        )

    monkeypatch.setattr(ddm_cli, "discover_ddm_servers", fake_discover)
    monkeypatch.setattr(ddm_cli.core, "host_mac", lambda: bytes.fromhex("842f5774e86d"))
    monkeypatch.setattr(ddm_cli, "identify_managed_device", fake_identify)

    result = _invoke(
        "identify",
        "001dc1fffe507b8d:0",
        "--username",
        "operator",
        "--insecure-tls",
        input="private-password\n",
    )

    assert result.exit_code == 0, result.output
    assert captured == {
        "server": "ddm.local",
        "username": "operator",
        "password": "private-password",
        "device_id": "001dc1fffe507b8d:0",
        "mac": bytes.fromhex("842f5774e86d"),
        "options": {
            "auth_port": 8443,
            "timeout": 10.0,
            "ca_file": None,
            "insecure_tls": True,
        },
    }
    assert "Identified managed device 001dc1fffe507b8d:0 through ddm.local." in result.output
    assert "private-password" not in result.output


def test_managed_identify_uses_an_explicit_server_without_discovery(monkeypatch):
    def unexpected_discovery(**options):
        raise AssertionError("discovery should not run")

    captured = {}
    monkeypatch.setattr(ddm_cli, "discover_ddm_servers", unexpected_discovery)
    monkeypatch.setattr(ddm_cli.core, "host_mac", lambda: b"\x01\x02\x03\x04\x05\x06")
    monkeypatch.setattr(
        ddm_cli, "identify_managed_device", lambda *args, **kwargs: captured.update(args=args, kwargs=kwargs)
    )

    result = _invoke(
        "identify",
        "001dc1fffe507b8d",
        "--username",
        "operator",
        "--server",
        "192.0.2.10",
        "--auth-port",
        "9443",
        input="private-password\n",
    )

    assert result.exit_code == 0, result.output
    assert captured["args"][:4] == ("192.0.2.10", "operator", "private-password", "001dc1fffe507b8d")
    assert captured["kwargs"]["auth_port"] == 9443
    assert "private-password" not in result.output


def test_managed_identify_accepts_an_api_key_from_a_hidden_prompt(monkeypatch):
    captured = {}
    api_key = "00000000-0000-4000-8000-000000000000"
    monkeypatch.setattr(ddm_cli.core, "host_mac", lambda: b"\x01\x02\x03\x04\x05\x06")
    monkeypatch.setattr(
        ddm_cli,
        "identify_managed_device_with_api_key",
        lambda *args, **kwargs: captured.update(args=args, kwargs=kwargs),
    )

    result = _invoke(
        "identify",
        "001dc1fffe507b8d:0",
        "--api-key",
        "--server",
        "192.0.2.10",
        input=f"{api_key}\n",
    )

    assert result.exit_code == 0, result.output
    assert captured["args"][:3] == ("192.0.2.10", api_key, "001dc1fffe507b8d:0")
    assert api_key not in result.output


def test_managed_identify_rejects_ambiguous_authentication_options(monkeypatch):
    monkeypatch.setattr(ddm_cli.core, "host_mac", lambda: b"\x01\x02\x03\x04\x05\x06")

    result = _invoke(
        "identify",
        "001dc1fffe507b8d:0",
        "--username",
        "operator",
        "--api-key",
        "--server",
        "192.0.2.10",
    )

    assert result.exit_code == 1
    assert "not both" in result.output


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
