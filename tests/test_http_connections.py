import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from netaudio.common.ddm_config_store import save_ddm_server
from netaudio.daemon.http import connections
from netaudio.ddm.client import GraphQLResult, ManagedAPIClient
from tests.http_api_test_support import make_http_server, post


@pytest.fixture
def connection_server(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    monkeypatch.setattr(connections, "default_config_path", lambda: path)
    server = make_http_server()
    server.application._managed_transports = {"old": object()}
    server.managed_inventory = SimpleNamespace(reconfigure=AsyncMock())
    server.publish_inventory_snapshot = AsyncMock()
    return server, path


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["api_key", "password", "saved"])
async def test_login_validates_credentials_then_persists_without_exposing_them(connection_server, monkeypatch, method):
    server, path = connection_server
    token = "test-only-token"
    if method == "saved":
        save_ddm_server(path, name="studio", url="https://ddm.example/graphql", credential=token)
    login = Mock(return_value=token)
    monkeypatch.setattr(connections, "authenticate_with_password", login)
    inventory = AsyncMock(return_value=SimpleNamespace(data=object(), errors=()))
    monkeypatch.setattr(ManagedAPIClient, "inventory_async", inventory)
    status, body = await post(
        server,
        "/ddm/login",
        {
            "server": "studio",
            "url": "https://ddm.example/graphql",
            "method": method,
            "username": "operator",
            "password": "test-only-password",
            "api_key": token,
        },
    )
    assert status == 200
    assert body["servers"] == [{"name": "studio", "url": "https://ddm.example/graphql", "configured": True}]
    assert token not in str(body) and token not in path.read_text()
    assert (path.parent / "credentials/studio.credential").read_text().strip() == token
    if os.name == "posix":
        assert not ((path.parent / "credentials/studio.credential").stat().st_mode & 0o077)
    inventory.assert_awaited_once()
    assert login.call_count == (1 if method == "password" else 0)
    server.managed_inventory.reconfigure.assert_awaited_once()
    assert server.application._managed_transports == {}


@pytest.mark.asyncio
async def test_profile_url_mismatch_never_sends_credentials(connection_server, monkeypatch):
    server, path = connection_server
    save_ddm_server(path, name="studio", url="https://ddm.example/graphql", credential="test-token")
    login = Mock()
    monkeypatch.setattr(connections, "authenticate_with_password", login)
    status, _ = await post(
        server, "/ddm/login", {"server": "studio", "url": "https://other.example/graphql", "method": "password"}
    )
    assert status == 400
    login.assert_not_called()
    server.managed_inventory.reconfigure.assert_not_awaited()


@pytest.mark.asyncio
async def test_rejected_login_does_not_save_profile(connection_server, monkeypatch):
    server, path = connection_server
    monkeypatch.setattr(
        ManagedAPIClient, "inventory_async", AsyncMock(return_value=SimpleNamespace(data=None, errors=("denied",)))
    )
    status, _ = await post(
        server,
        "/ddm/login",
        {"server": "studio", "url": "https://ddm.example/graphql", "method": "api_key", "api_key": "test-token"},
    )
    assert status == 400
    assert not path.exists()
    server.managed_inventory.reconfigure.assert_not_awaited()


@pytest.mark.asyncio
async def test_logout_disconnects_only_selected_server(connection_server):
    server, path = connection_server
    for name in ("studio", "venue"):
        save_ddm_server(path, name=name, url=f"https://{name}.example/graphql", credential="test-token")
    status, body = await post(server, "/ddm/logout", {"server": "studio"})
    assert status == 200
    profiles = {item["name"]: item for item in body["servers"]}
    assert not profiles["studio"]["configured"]
    assert profiles["venue"]["configured"]
    configuration = server.managed_inventory.reconfigure.call_args.args[0]
    assert configuration.server("studio").credential_file is None
    assert configuration.server("venue").credential_file is not None
    server.publish_inventory_snapshot.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["enroll", "unenroll"])
async def test_enrollment_targets_server_and_preserves_configuration(action):
    server = make_http_server()
    operation = "DevicesEnroll" if action == "enroll" else "DevicesUnenroll"
    client = SimpleNamespace(execute_async=AsyncMock(return_value=GraphQLResult({operation: {"ok": True}}, ())))
    service = SimpleNamespace(enabled=True, refresh=AsyncMock(return_value=True), client=client)
    other = SimpleNamespace(enabled=True, refresh=AsyncMock(), client=SimpleNamespace(execute_async=AsyncMock()))
    server.managed_inventory = SimpleNamespace(
        services={"studio": service, "venue": other}, domains=lambda: [{"id": "domain", "ddm_server_profile": "studio"}]
    )
    server._serialized_devices = lambda: {
        "target": {
            "ddm_server_profile": "studio",
            "ddm_device_id": "device",
            "online": True,
            "ddm_domain_id": "domain" if action == "unenroll" else None,
        }
    }
    server.publish_inventory_snapshot = AsyncMock()
    status, body = await post(
        server, "/ddm/enrollment", {"server": "studio", "device_id": "device", "action": action, "domain_id": "domain"}
    )
    assert status == 200 and body["accepted"]
    args = client.execute_async.call_args.args
    expected = {"deviceIds": ["device"], "clearConfig": False}
    if action == "enroll":
        expected["domainId"] = "domain"
    assert args[1] == {"input": expected}
    assert args[2] == operation
    assert service.refresh.await_count == 2
    other.client.execute_async.assert_not_awaited()
    server.publish_inventory_snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_enrollment_rejects_domain_from_another_server():
    server = make_http_server()
    client = SimpleNamespace(execute_async=AsyncMock())
    service = SimpleNamespace(enabled=True, refresh=AsyncMock(return_value=True), client=client)
    server.managed_inventory = SimpleNamespace(
        services={"studio": service}, domains=lambda: [{"id": "domain", "ddm_server_profile": "venue"}]
    )
    server._serialized_devices = lambda: {
        "target": {"ddm_server_profile": "studio", "ddm_device_id": "device", "online": True}
    }
    status, _ = await post(
        server,
        "/ddm/enrollment",
        {"server": "studio", "device_id": "device", "action": "enroll", "domain_id": "domain"},
    )
    assert status == 400
    client.execute_async.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [True, False])
async def test_create_domain_uses_selected_server_and_reports_rejection(accepted):
    server = make_http_server()
    client = SimpleNamespace(
        execute_async=AsyncMock(
            return_value=GraphQLResult({"DomainAdd": {"ok": accepted, "domain": {"id": "new", "name": "Control"}}}, ())
        )
    )
    service = SimpleNamespace(enabled=True, refresh=AsyncMock(return_value=True), client=client)
    other_client = SimpleNamespace(execute_async=AsyncMock())
    server.managed_inventory = SimpleNamespace(
        services={"studio": service, "venue": SimpleNamespace(enabled=True, client=other_client)}
    )
    server.publish_inventory_snapshot = AsyncMock()
    status, body = await post(server, "/ddm/domains", {"server": "studio", "name": " Control "})
    assert status == (201 if accepted else 409)
    assert client.execute_async.call_args.args[1] == {"input": {"name": "Control", "icon": "OTHER"}}
    other_client.execute_async.assert_not_awaited()
    assert service.refresh.await_count == int(accepted)
    assert server.publish_inventory_snapshot.await_count == int(accepted)
    if accepted:
        assert body["domain"]["name"] == "Control"


@pytest.mark.asyncio
async def test_create_domain_rejects_blank_name_without_sending_request():
    server = make_http_server()
    server.managed_inventory = SimpleNamespace(services={})
    status, _ = await post(server, "/ddm/domains", {"server": "studio", "name": "   "})
    assert status == 400


@pytest.mark.asyncio
@pytest.mark.parametrize("action,operation", [("rename", "DomainUpdate"), ("remove", "DomainRemove")])
@pytest.mark.parametrize("accepted", [True, False])
async def test_update_domain_uses_exact_server_and_requires_acceptance(action, operation, accepted):
    from tests.http_api_test_support import FakeWriter

    server = make_http_server()
    client = SimpleNamespace(execute_async=AsyncMock(return_value=GraphQLResult({operation: {"ok": accepted}}, ())))
    service = SimpleNamespace(enabled=True, refresh=AsyncMock(), client=client)
    other = SimpleNamespace(enabled=True, client=SimpleNamespace(execute_async=AsyncMock()))
    server.managed_inventory = SimpleNamespace(
        services={"studio": service, "other": other},
        domains=lambda: [{"id": "domain-1", "ddm_server_profile": "studio"}],
    )
    server.publish_inventory_snapshot = AsyncMock()
    writer = FakeWriter()
    await server._handle_ddm_update_domain(
        writer, {"server": "studio", "domain_id": "domain-1", "action": action, "name": " New Name "}
    )
    status, body = writer.response()
    assert status == (200 if accepted else 409)
    assert client.execute_async.call_args.args[2] == operation
    expected = {"id": "domain-1", "name": "New Name"} if action == "rename" else {"id": "domain-1"}
    assert client.execute_async.call_args.args[1] == {"input": expected}
    assert service.refresh.await_count == int(accepted)
    assert server.publish_inventory_snapshot.await_count == int(accepted)
    other.client.execute_async.assert_not_awaited()
    assert body.get("accepted", False) == accepted


@pytest.mark.asyncio
async def test_domain_update_rejects_another_servers_domain():
    from tests.http_api_test_support import FakeWriter

    server = make_http_server()
    client = SimpleNamespace(execute_async=AsyncMock())
    server.managed_inventory = SimpleNamespace(
        services={"studio": SimpleNamespace(enabled=True, client=client)},
        domains=lambda: [{"id": "domain-1", "ddm_server_profile": "other"}],
    )
    writer = FakeWriter()
    await server._handle_ddm_update_domain(writer, {"server": "studio", "domain_id": "domain-1", "action": "remove"})
    assert writer.response()[0] == 400
    client.execute_async.assert_not_awaited()
