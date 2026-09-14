import base64
import hashlib
import json
import secrets
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest

from netaudio.common import preferences
from netaudio.daemon import mcp_oauth
from netaudio.daemon.mcp_oauth import SCOPE_READ, SCOPE_WRITE, set_login_secret
from tests.http_api_test_support import FakeWriter, make_device, make_http_server

PUBLIC_HEADERS = {"host": "netaudio.app", "x-forwarded-proto": "https", "cf-connecting-ip": "203.0.113.9"}
REDIRECT = "https://claude.ai/api/mcp/auth_callback"
SECRET = "correct horse battery staple"


@pytest.fixture(autouse=True)
def isolated_preferences(monkeypatch, tmp_path):
    monkeypatch.setattr(preferences, "default_config_path", lambda: tmp_path / "config.toml")


async def request(server, method, path, body=None, headers=PUBLIC_HEADERS, peer=("203.0.113.9", 5000)):
    writer = FakeWriter(peer=peer)
    if isinstance(body, dict) and "content-type" in headers and "json" in headers["content-type"]:
        body = json.dumps(body).encode()
    elif isinstance(body, dict):
        body = urlencode(body).encode()
    await server._route(method, path, body, writer, None, headers)
    raw = bytes(writer.data).decode()
    head, _, payload = raw.partition("\r\n\r\n")
    status = int(head.split(" ")[1])
    response_headers = {
        name.strip().lower(): value.strip() for line in head.split("\r\n")[1:] for name, value in [line.split(":", 1)]
    }
    if response_headers.get("content-type", "").startswith("application/json") and payload:
        payload = json.loads(payload)
    return status, payload, response_headers


def make_server():
    device = make_device(server_name="lx-dante.local.", name="lx-dante")
    return make_http_server(devices={device.server_name: device})


def pkce():
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    return verifier, challenge


async def register(server):
    status, body, _ = await request(
        server,
        "POST",
        "/oauth/register",
        {"client_name": "Claude", "redirect_uris": [REDIRECT], "token_endpoint_auth_method": "none"},
        headers={**PUBLIC_HEADERS, "content-type": "application/json"},
    )
    assert status == 201, body
    return body["client_id"]


async def authorize(server, client_id, challenge, scope=SCOPE_WRITE, secret=SECRET):
    form = {
        "client_id": client_id,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "decision": "allow",
        "redirect_uri": REDIRECT,
        "response_type": "code",
        "scope": scope,
        "secret": secret,
        "state": "xyz",
    }
    return await request(server, "POST", "/oauth/authorize", form)


@pytest.mark.asyncio
async def test_metadata_uses_the_public_url_or_forwarded_host():
    server = make_server()
    status, body, _ = await request(server, "GET", "/.well-known/oauth-authorization-server")
    assert status == 200
    assert body["issuer"] == "https://netaudio.app"
    assert body["token_endpoint"] == "https://netaudio.app/oauth/token"
    assert body["code_challenge_methods_supported"] == ["S256"]

    status, body, _ = await request(server, "GET", "/.well-known/oauth-protected-resource/mcp")
    assert body["resource"] == "https://netaudio.app/mcp"
    assert body["authorization_servers"] == ["https://netaudio.app"]

    mcp_oauth.save_public_url("https://control.example.org/")
    status, body, _ = await request(
        server, "GET", "/.well-known/oauth-authorization-server", headers={"host": "10.0.0.5:9000"}
    )
    assert body["issuer"] == "https://control.example.org"


@pytest.mark.asyncio
async def test_unauthenticated_mcp_requests_point_at_resource_metadata():
    server = make_server()
    status, body, headers = await request(
        server,
        "POST",
        "/mcp",
        {"jsonrpc": "2.0", "id": 1, "method": "ping"},
        headers={**PUBLIC_HEADERS, "content-type": "application/json"},
    )
    assert status == 401
    assert (
        'resource_metadata="https://netaudio.app/.well-known/oauth-protected-resource"' in headers["www-authenticate"]
    )


@pytest.mark.asyncio
async def test_registration_validates_redirect_uris():
    server = make_server()
    status, body, _ = await request(
        server,
        "POST",
        "/oauth/register",
        {"redirect_uris": ["http://evil.example/cb"]},
        headers={**PUBLIC_HEADERS, "content-type": "application/json"},
    )
    assert status == 400
    assert body["error"] == "invalid_redirect_uri"
    status, body, _ = await request(
        server,
        "POST",
        "/oauth/register",
        {"redirect_uris": ["http://localhost:3000/cb"]},
        headers={**PUBLIC_HEADERS, "content-type": "application/json"},
    )
    assert status == 201
    assert body["token_endpoint_auth_method"] == "none"
    assert "client_secret" not in body


@pytest.mark.asyncio
async def test_authorization_page_requires_a_login_secret_and_pkce():
    server = make_server()
    client_id = await register(server)
    query = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": REDIRECT,
            "response_type": "code",
            "code_challenge": "abc",
            "code_challenge_method": "S256",
            "state": "s",
        }
    )
    status, body, _ = await request(server, "GET", f"/oauth/authorize?{query}")
    assert status == 503

    set_login_secret(SECRET)
    status, body, headers = await request(server, "GET", f"/oauth/authorize?{query}")
    assert status == 200
    assert headers["content-type"].startswith("text/html")
    assert "Claude" in body and 'name="secret"' in body and "form-action 'self'" in headers["content-security-policy"]

    query = urlencode({"client_id": client_id, "redirect_uri": REDIRECT, "response_type": "code", "state": "s"})
    status, body, _ = await request(server, "GET", f"/oauth/authorize?{query}")
    assert status == 400
    assert body["error"] == "invalid_request"

    query = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": "https://other.example/cb",
            "response_type": "code",
            "code_challenge": "abc",
            "code_challenge_method": "S256",
        }
    )
    status, body, _ = await request(server, "GET", f"/oauth/authorize?{query}")
    assert status == 400


@pytest.mark.asyncio
async def test_full_authorization_code_flow_then_tool_call_and_refresh():
    server = make_server()
    set_login_secret(SECRET)
    client_id = await register(server)
    verifier, challenge = pkce()

    status, body, headers = await authorize(server, client_id, challenge)
    assert status == 302
    location = urlsplit(headers["location"])
    assert location.netloc == "claude.ai"
    query = parse_qs(location.query)
    assert query["state"] == ["xyz"]
    code = query["code"][0]

    status, body, _ = await request(
        server,
        "POST",
        "/oauth/token",
        {
            "grant_type": "authorization_code",
            "client_id": client_id,
            "code": code,
            "redirect_uri": REDIRECT,
            "code_verifier": "wrong-verifier-that-is-long-enough-to-pass-length-check",
        },
    )
    assert status == 400 and body["error"] == "invalid_grant"

    status, body, headers = await authorize(server, client_id, challenge)
    code = parse_qs(urlsplit(headers["location"]).query)["code"][0]
    status, tokens, _ = await request(
        server,
        "POST",
        "/oauth/token",
        {
            "grant_type": "authorization_code",
            "client_id": client_id,
            "code": code,
            "redirect_uri": REDIRECT,
            "code_verifier": verifier,
        },
    )
    assert status == 200, tokens
    assert tokens["token_type"] == "Bearer" and tokens["scope"] == SCOPE_WRITE

    status, body, _ = await request(
        server,
        "POST",
        "/oauth/token",
        {
            "grant_type": "authorization_code",
            "client_id": client_id,
            "code": code,
            "redirect_uri": REDIRECT,
            "code_verifier": verifier,
        },
    )
    assert body["error"] == "invalid_grant"

    mcp_headers = {
        **PUBLIC_HEADERS,
        "content-type": "application/json",
        "authorization": f"Bearer {tokens['access_token']}",
    }
    status, body, _ = await request(
        server,
        "POST",
        "/mcp",
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "identify", "arguments": {"device": "lx-dante"}},
        },
        headers=mcp_headers,
    )
    assert status == 200 and body["result"]["isError"] is False
    server.application.identify.assert_awaited_once()

    status, refreshed, _ = await request(
        server,
        "POST",
        "/oauth/token",
        {"grant_type": "refresh_token", "client_id": client_id, "refresh_token": tokens["refresh_token"]},
    )
    assert status == 200 and refreshed["access_token"] != tokens["access_token"]
    status, body, _ = await request(
        server,
        "POST",
        "/oauth/token",
        {"grant_type": "refresh_token", "client_id": client_id, "refresh_token": tokens["refresh_token"]},
    )
    assert body["error"] == "invalid_grant"

    status, body, _ = await request(server, "POST", "/oauth/revoke", {"token": refreshed["access_token"]})
    assert status == 200
    status, body, _ = await request(
        server,
        "POST",
        "/mcp",
        {"jsonrpc": "2.0", "id": 1, "method": "ping"},
        headers={**mcp_headers, "authorization": f"Bearer {refreshed['access_token']}"},
    )
    assert status == 401


@pytest.mark.asyncio
async def test_read_only_scope_blocks_writes_but_allows_resources():
    server = make_server()
    set_login_secret(SECRET)
    client_id = await register(server)
    verifier, challenge = pkce()
    status, body, headers = await authorize(server, client_id, challenge, scope=SCOPE_READ)
    code = parse_qs(urlsplit(headers["location"]).query)["code"][0]
    status, tokens, _ = await request(
        server,
        "POST",
        "/oauth/token",
        {
            "grant_type": "authorization_code",
            "client_id": client_id,
            "code": code,
            "redirect_uri": REDIRECT,
            "code_verifier": verifier,
        },
    )
    assert tokens["scope"] == SCOPE_READ
    mcp_headers = {
        **PUBLIC_HEADERS,
        "content-type": "application/json",
        "authorization": f"Bearer {tokens['access_token']}",
    }

    status, body, _ = await request(
        server,
        "POST",
        "/mcp",
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "identify", "arguments": {"device": "lx-dante"}},
        },
        headers=mcp_headers,
    )
    assert body["result"]["isError"] is True
    assert SCOPE_WRITE in body["result"]["structuredContent"]["error"]
    server.application.identify.assert_not_awaited()

    status, body, _ = await request(
        server, "POST", "/mcp", {"jsonrpc": "2.0", "id": 2, "method": "resources/list"}, headers=mcp_headers
    )
    assert status == 200 and body["result"]["resources"]


@pytest.mark.asyncio
async def test_wrong_secret_is_rate_limited_and_denial_redirects():
    server = make_server()
    set_login_secret(SECRET)
    client_id = await register(server)
    _, challenge = pkce()
    for _ in range(5):
        status, body, _ = await authorize(server, client_id, challenge, secret="nope")
        assert status == 200 and "not right" in body
    status, body, _ = await authorize(server, client_id, challenge, secret=SECRET)
    assert status == 429

    status, body, headers = await request(
        server,
        "POST",
        "/oauth/authorize",
        {
            "client_id": client_id,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "decision": "deny",
            "redirect_uri": REDIRECT,
            "response_type": "code",
            "state": "s",
        },
    )
    assert status == 302
    assert parse_qs(urlsplit(headers["location"]).query)["error"] == ["access_denied"]


def test_login_secret_and_public_url_validation():
    with pytest.raises(ValueError):
        set_login_secret("short")
    set_login_secret(SECRET)
    assert mcp_oauth.verify_login_secret(SECRET)
    assert not mcp_oauth.verify_login_secret(SECRET + "x")
    with pytest.raises(ValueError):
        mcp_oauth.save_public_url("netaudio.app")
    assert mcp_oauth.save_public_url("https://netaudio.app/") == "https://netaudio.app"
