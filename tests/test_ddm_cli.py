import json
import stat
from types import SimpleNamespace

import click
import pytest
from netaudio.cli import app
from netaudio.commands.ddm import cli as ddm_cli
from netaudio.commands.ddm import operations
from netaudio.ddm import AuthenticationError
from netaudio.ddm.models import Domain, Inventory
from typer.testing import CliRunner

runner = CliRunner()


def _invoke(*arguments, **options):
    return runner.invoke(app, ["ddm", *arguments], **options)


def _invoke_api(*arguments, **options):
    return _invoke("api", *arguments, **options)


def test_every_schema_operation_is_a_command():
    result = _invoke_api("schema")
    assert result.exit_code == 0, result.output
    for expected in (
        "write device set-name",
        "write devices enroll",
        "write domain add",
        "read unenrolled-devices",
        "read current-user",
    ):
        assert expected in result.output


def test_password_login_uses_a_hidden_prompt_and_saves_the_token(monkeypatch, tmp_path):
    captured = {}
    credential_file = tmp_path / "ddm-token"
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_file))

    def fake_authenticate(url, username, password):
        captured.update(
            url=url,
            username=username,
            password=password,
        )
        return "session-token"

    class FakeClient:
        def __init__(self, url, *, credential):
            assert url == "http://manager.example/graphql"
            assert credential == "session-token"

        def inventory(self):
            domain = Domain(id="domain-1", name="Studio", status=None, devices=())
            return SimpleNamespace(data=Inventory(domains=(domain,), unenrolled_devices=()), errors=())

    monkeypatch.setattr(ddm_cli, "authenticate_with_password", fake_authenticate)
    monkeypatch.setattr(ddm_cli, "ManagedAPIClient", FakeClient)
    result = _invoke(
        "login",
        "--url",
        "http://manager.example/graphql",
        "--username",
        "operator",
        "--save-credential",
        str(credential_file),
        input="private-password\n",
    )

    assert result.exit_code == 0, result.output
    assert captured == {
        "url": "http://manager.example/graphql",
        "username": "operator",
        "password": "private-password",
    }
    assert credential_file.read_text(encoding="ascii") == "session-token\n"
    assert stat.S_IMODE(credential_file.stat().st_mode) == 0o600
    assert "session-token" not in result.output
    assert "private-password" not in result.output
    assert "Saved DDM context" in result.output
    assert 'default_context = "manager.example-Studio"' in config_file.read_text()


def test_password_login_does_not_replace_a_credential_after_rejection(monkeypatch, tmp_path):
    credential_file = tmp_path / "ddm-token"
    credential_file.write_text("existing-token\n", encoding="ascii")
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_file))

    def reject(*arguments, **options):
        raise AuthenticationError("DDM rejected username/password login")

    monkeypatch.setattr(ddm_cli, "authenticate_with_password", reject)
    result = _invoke(
        "login",
        "--url",
        "https://manager.example/graphql",
        "--username",
        "operator",
        "--save-credential",
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
    assert "--api-key-file" not in login_help.output
    assert "--server-profile" in login_help.output
    assert "--credential-file" in login_help.output


def test_managed_api_operations_exist_only_under_the_api_group():
    top_level = _invoke("schema")
    nested = _invoke_api("schema")

    assert top_level.exit_code == 2
    assert "No such command 'schema'" in top_level.output
    assert nested.exit_code == 0


def test_managed_api_help_uses_read_write_and_resource_groups():
    api_help = _invoke_api("--help")
    read_help = _invoke_api("read", "--help")
    write_help = _invoke_api("write", "--help")
    device_help = _invoke_api("write", "device", "--help")
    flat_command = _invoke_api("device-name-set")

    assert api_help.exit_code == 0
    assert "read" in api_help.output and "write" in api_help.output
    assert "device-name-set" not in api_help.output
    assert read_help.exit_code == 0 and "current-user" in read_help.output and "domains" in read_help.output
    assert write_help.exit_code == 0 and "device" in write_help.output and "domain" in write_help.output
    assert device_help.exit_code == 0 and "set-name" in device_help.output
    assert flat_command.exit_code == 2 and "No such command 'device-name-set'" in flat_command.output


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


def test_login_discovers_server_resolves_graphql_endpoint_and_saves_selected_domain(monkeypatch, tmp_path):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_file))
    discovered = SimpleNamespace(
        server_name="ddm.local.",
        ipv4_addresses=("192.168.1.217",),
        controller_service=SimpleNamespace(port=8443),
    )

    async def fake_discover(*, timeout, interfaces=None):
        assert timeout == 0.25
        assert interfaces is None
        return (discovered,)

    class FakeControllerClient:
        def __init__(self, server, *, port, timeout):
            assert (server, port, timeout) == ("192.168.1.217", 8443, 0.25)

        def endpoints(self):
            return SimpleNamespace(graphql_url="http://ddm.local/graphql")

    class FakeManagedClient:
        def __init__(self, url, *, credential):
            assert url == "http://ddm.local/graphql"
            assert credential == "session-token"

        def inventory(self):
            domain = Domain(id="domain-1", name="test", status=None, devices=())
            return SimpleNamespace(data=Inventory(domains=(domain,), unenrolled_devices=()), errors=())

    monkeypatch.setattr(ddm_cli, "discover_ddm_servers", fake_discover)
    monkeypatch.setattr(ddm_cli, "ControllerAPIClient", FakeControllerClient)
    monkeypatch.setattr(ddm_cli, "authenticate_with_password", lambda *_args: "session-token")
    monkeypatch.setattr(ddm_cli, "ManagedAPIClient", FakeManagedClient)

    result = _invoke("login", "--username", "admin", "--discovery-timeout", "0.25", input="password\n")

    assert result.exit_code == 0, result.output
    document = config_file.read_text()
    assert '[ddm.servers."ddm.local"]' in document
    assert 'url = "http://ddm.local/graphql"' in document
    assert '[ddm.contexts."ddm.local-test"]' in document
    assert 'domain_id = "domain-1"' in document
    configuration, _ = ddm_cli._ddm_configuration()
    assert configuration.context("ddm.local-test").server == "ddm.local"


def test_login_can_use_an_existing_credential_file_without_password_authentication(monkeypatch, tmp_path):
    config_file = tmp_path / "config.toml"
    credential_path = tmp_path / "existing-api-key"
    credential_path.write_text("key\n", encoding="ascii")
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_file))

    class FakeManagedClient:
        def __init__(self, url, *, credential_file):
            assert url == "https://manager.example/graphql"
            assert credential_file == credential_path

        def inventory(self):
            domain = Domain(id="domain-1", name="Studio", status=None, devices=())
            return SimpleNamespace(data=Inventory(domains=(domain,), unenrolled_devices=()), errors=())

    monkeypatch.setattr(ddm_cli, "ManagedAPIClient", FakeManagedClient)
    monkeypatch.setattr(
        ddm_cli,
        "authenticate_with_password",
        lambda *_args: pytest.fail("password authentication should not run"),
    )

    result = _invoke(
        "login",
        "--url",
        "https://manager.example/graphql",
        "--server-profile",
        "studio",
        "--credential-file",
        str(credential_path),
    )

    assert result.exit_code == 0, result.output
    assert 'credential_file = "existing-api-key"' in config_file.read_text()


def test_context_commands_list_show_override_and_change_the_default(monkeypatch, tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        """
[ddm]
default_context = "east-main"

[ddm.servers.manager]
url = "https://manager.example/graphql"
credential_file = "key"

[ddm.contexts.east-main]
server = "manager"
domain_id = "east"
domain_name = "East"

[ddm.contexts.west-main]
server = "manager"
domain_id = "west"
domain_name = "West"
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_file))

    listed = _invoke("context", "list")
    current = _invoke("context", "current")
    overridden = runner.invoke(app, ["--context", "west-main", "ddm", "context", "current"])
    changed = _invoke("context", "use", "west-main")

    assert listed.exit_code == 0 and "east-main" in listed.output and "west-main" in listed.output
    assert current.exit_code == 0 and current.output.strip() == "east-main"
    assert overridden.exit_code == 0 and overridden.output.strip() == "west-main"
    assert changed.exit_code == 0
    assert 'default_context = "west-main"' in config_file.read_text()


def test_login_will_not_retarget_an_existing_server_profile(monkeypatch, tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        """
[ddm.servers.studio]
url = "https://expected.example/graphql"
credential_file = "credential"
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_file))
    monkeypatch.setattr(
        ddm_cli,
        "authenticate_with_password",
        lambda *_args: pytest.fail("authentication must not run against a retargeted profile"),
    )

    result = _invoke(
        "login",
        "--url",
        "https://wrong.example/graphql",
        "--server-profile",
        "studio",
        "--username",
        "operator",
    )

    assert result.exit_code == 1
    assert "choose a new --server-profile name" in result.output


def test_identify_is_not_a_separate_ddm_command():
    result = _invoke("identify")

    assert result.exit_code == 2
    assert "No such command 'identify'" in result.output


def test_schema_describes_one_type():
    result = _invoke_api("schema", "DeviceNameSetInput")
    assert result.exit_code == 0, result.output
    assert "deviceId: ID!" in result.output
    assert "name: String!" in result.output
    missing = _invoke_api("schema", "Nope")
    assert missing.exit_code == 1
    assert "unknown type Nope" in missing.output


def test_print_query_shows_document_and_flattened_input_variables():
    result = _invoke_api(
        "write",
        "device",
        "set-name",
        "--device-id",
        "001dc1fffe50692e:0",
        "--name",
        "stage-left",
        "--print-query",
    )
    assert result.exit_code == 0, result.output
    document, variables = result.output.split("\n", 1)
    assert document == "mutation DeviceNameSet($input: DeviceNameSetInput!) { DeviceNameSet(input: $input) { ok } }"
    assert json.loads(variables) == {"input": {"deviceId": "001dc1fffe50692e:0", "name": "stage-left"}}


def test_json_list_options_are_validated_against_the_schema():
    subscriptions = json.dumps([{"rxChannelIndex": 1, "subscribedDevice": "lx-dante", "subscribedChannel": "01"}])
    result = _invoke_api(
        "write",
        "device",
        "set-rx-channels-subscription",
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

    invalid = _invoke_api(
        "write",
        "device",
        "set-rx-channels-subscription",
        "--device-id",
        "x",
        "--subscriptions",
        '[{"rxChannelIndex": "one"}]',
        "--print-query",
    )
    assert invalid.exit_code == 1
    assert "input.subscriptions[0].rxChannelIndex must be an integer" in invalid.output

    malformed = _invoke_api(
        "write",
        "device",
        "set-rx-channels-subscription",
        "--device-id",
        "x",
        "--subscriptions",
        "{",
        "--print-query",
    )
    assert malformed.exit_code == 1
    assert "--subscriptions must be valid JSON" in malformed.output


def test_required_options_are_enforced_by_the_parser():
    result = _invoke_api("write", "device", "set-name", "--name", "x", "--print-query")
    assert result.exit_code != 0
    assert "--device-id" in result.output


def test_query_arguments_become_variables():
    result = _invoke_api("read", "domain", "--id", "abc", "--print-query")
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
    result = _invoke_api("read", "domains")
    assert result.exit_code == 0, result.output
    assert "test" in result.output and "d1" in result.output
    assert " 1 " in result.output


def test_rejected_mutations_exit_nonzero_with_the_server_message(monkeypatch):
    def fake_execute(query, variables=None, operation_name=None):
        return {"data": {"DeviceNameSet": {"ok": False, "error": {"code": "X", "message": "name taken"}}}, "errors": []}

    monkeypatch.setattr(operations, "execute", fake_execute)
    result = _invoke_api("write", "device", "set-name", "--device-id", "d", "--name", "n")
    assert result.exit_code == 1
    assert "DeviceNameSet was rejected: name taken" in result.output


def test_graphql_errors_are_reported_and_fail_without_data(monkeypatch):
    def fake_execute(query, variables=None, operation_name=None):
        return {"data": None, "errors": [{"message": "Unauthorized", "path": ["domains"]}]}

    monkeypatch.setattr(ddm_cli, "execute", fake_execute)
    result = _invoke_api("graphql", "{ domains { id } }")
    assert result.exit_code == 1
    assert "Managed API error at domains: Unauthorized" in result.output


def test_bare_graphql_command_displays_help():
    result = _invoke_api("graphql")

    assert result.exit_code == 2
    assert "Usage: netaudio ddm api graphql" in result.output
    assert "--variables" in result.output
    assert "pass a GraphQL document" not in result.output


def test_raw_graphql_passes_variables_and_prints_data(monkeypatch, tmp_path):
    captured = {}

    def fake_execute(query, variables=None, operation_name=None):
        captured.update(query=query, variables=variables, operation_name=operation_name)
        return {"data": {"me": None}, "errors": []}

    monkeypatch.setattr(ddm_cli, "execute", fake_execute)
    document = tmp_path / "query.graphql"
    document.write_text("query Me { me { id } }")
    result = _invoke_api("graphql", "--file", str(document), "--variables", '{"a": 1}', "--operation-name", "Me")
    assert result.exit_code == 0, result.output
    assert captured == {"query": "query Me { me { id } }", "variables": {"a": 1}, "operation_name": "Me"}
    assert json.loads(result.output) == {"me": None}
    bad = _invoke_api("graphql", "{ me { id } }", "--variables", "[1]")
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


def test_daemon_graphql_proxy_receives_the_selected_context(monkeypatch):
    from netaudio.cli import state
    from netaudio.commands.ddm import transport

    monkeypatch.setattr(transport, "configured_client", lambda: None)
    captured = {}

    async def execute_on_daemon(query, variables, operation_name, *, context):
        captured.update(query=query, variables=variables, operation_name=operation_name, context=context)
        return 200, {"data": {"me": None}, "errors": []}

    monkeypatch.setattr(transport, "execute_ddm_graphql_on_daemon", execute_on_daemon)
    original = state.ddm_context
    state.ddm_context = "west-main"
    try:
        result = transport.execute("query Me { me { id } }", operation_name="Me")
    finally:
        state.ddm_context = original

    assert result["data"] == {"me": None}
    assert captured["context"] == "west-main"
