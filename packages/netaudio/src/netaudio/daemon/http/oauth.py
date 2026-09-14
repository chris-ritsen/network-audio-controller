from __future__ import annotations

import html
import json
import logging
from urllib.parse import parse_qs, urlencode, urlsplit

from netaudio.daemon.mcp_oauth import (
    SCOPE_READ,
    SCOPE_WRITE,
    SCOPES,
    OAuthError,
    OAuthStore,
    login_secret_configured,
    normalize_scope,
    read_public_url,
    verify_login_secret,
)

logger = logging.getLogger("netaudio")

AUTHORIZATION_SERVER_METADATA_PATH = "/.well-known/oauth-authorization-server"
AUTHORIZE_PATH = "/oauth/authorize"
PROTECTED_RESOURCE_METADATA_PATH = "/.well-known/oauth-protected-resource"
REGISTER_PATH = "/oauth/register"
REVOKE_PATH = "/oauth/revoke"
TOKEN_PATH = "/oauth/token"

OAUTH_PATHS = {AUTHORIZE_PATH, REGISTER_PATH, REVOKE_PATH, TOKEN_PATH}
STATUS_TEXT = {
    200: "OK",
    201: "Created",
    302: "Found",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    405: "Method Not Allowed",
    429: "Too Many Requests",
    503: "Service Unavailable",
}

LOGIN_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>netaudio access</title>
<style>
body {{ font: 16px/1.5 -apple-system, system-ui, sans-serif; background: #111; color: #eee; margin: 0; padding: 24px; }}
main {{ max-width: 28rem; margin: 8vh auto; }}
h1 {{ font-size: 1.4rem; margin: 0 0 0.5rem; }}
p {{ margin: 0.5rem 0; color: #bbb; }}
label {{ display: block; margin-top: 1.2rem; }}
input[type=password] {{ width: 100%; box-sizing: border-box; font-size: 1rem; padding: 0.6rem; margin-top: 0.3rem; border-radius: 8px; border: 1px solid #444; background: #1c1c1e; color: #eee; }}
fieldset {{ border: 1px solid #333; border-radius: 8px; margin-top: 1.2rem; padding: 0.6rem 1rem; }}
legend {{ color: #bbb; padding: 0 0.3rem; }}
.actions {{ display: flex; gap: 0.8rem; margin-top: 1.4rem; }}
button {{ font-size: 1rem; padding: 0.6rem 1.2rem; border-radius: 8px; border: 0; cursor: pointer; }}
button.allow {{ background: #0a84ff; color: white; }}
button.deny {{ background: #2c2c2e; color: #eee; }}
.error {{ color: #ff6b6b; }}
</style>
</head>
<body>
<main>
<h1>{client} wants to control your Dante network</h1>
<p>through netaudio on {host}.</p>
{error}
<form method="post" action="{action}">
{hidden}
<fieldset>
<legend>Allow it to</legend>
<label><input type="radio" name="scope" value="{scope_write}" {write_checked}> read and change devices, routing and settings</label>
<label><input type="radio" name="scope" value="{scope_read}" {read_checked}> only read devices, routes, levels and issues</label>
</fieldset>
<label>Login secret<input type="password" name="secret" autocomplete="current-password" autofocus required></label>
<div class="actions">
<button class="allow" name="decision" value="allow">Allow</button>
<button class="deny" name="decision" value="deny">Deny</button>
</div>
</form>
</main>
</body>
</html>
"""


class DaemonOAuthHandlers:
    oauth_store: OAuthStore

    def oauth_base_url(self, headers) -> str:
        configured = read_public_url()
        if configured:
            return configured
        headers = headers or {}
        scheme = headers.get("x-forwarded-proto", "http").split(",")[0].strip() or "http"
        host = headers.get("x-forwarded-host") or headers.get("host") or f"localhost:{self.port}"
        return f"{scheme}://{host.split(',')[0].strip()}"

    def oauth_metadata(self, headers) -> dict:
        base = self.oauth_base_url(headers)
        return {
            "authorization_endpoint": base + AUTHORIZE_PATH,
            "code_challenge_methods_supported": ["S256"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "issuer": base,
            "registration_endpoint": base + REGISTER_PATH,
            "response_types_supported": ["code"],
            "revocation_endpoint": base + REVOKE_PATH,
            "scopes_supported": list(SCOPES),
            "token_endpoint": base + TOKEN_PATH,
            "token_endpoint_auth_methods_supported": ["none", "client_secret_post"],
        }

    def protected_resource_metadata(self, headers) -> dict:
        base = self.oauth_base_url(headers)
        return {
            "authorization_servers": [base],
            "bearer_methods_supported": ["header"],
            "resource": base + "/mcp",
            "scopes_supported": list(SCOPES),
        }

    @staticmethod
    def is_oauth_path(path: str) -> bool:
        return (
            path in OAUTH_PATHS
            or path.startswith(PROTECTED_RESOURCE_METADATA_PATH)
            or path.startswith(AUTHORIZATION_SERVER_METADATA_PATH)
        )

    async def _handle_oauth(self, method: str, raw_path: str, body, writer, headers) -> None:
        headers = headers or {}
        path, _, query_string = raw_path.partition("?")
        try:
            if path.startswith(PROTECTED_RESOURCE_METADATA_PATH) and method == "GET":
                self._write_oauth(writer, 200, self.protected_resource_metadata(headers))
            elif path.startswith(AUTHORIZATION_SERVER_METADATA_PATH) and method == "GET":
                self._write_oauth(writer, 200, self.oauth_metadata(headers))
            elif path == REGISTER_PATH and method == "POST":
                self._write_oauth(writer, 201, self.oauth_store.register_client(_json_body(body)))
            elif path == AUTHORIZE_PATH and method == "GET":
                await self._oauth_authorize_page(writer, headers, parse_qs(query_string), error=None)
            elif path == AUTHORIZE_PATH and method == "POST":
                await self._oauth_authorize_decision(writer, headers, _form_body(body, headers))
            elif path == TOKEN_PATH and method == "POST":
                self._write_oauth(writer, 200, self._oauth_token(_form_body(body, headers)))
            elif path == REVOKE_PATH and method == "POST":
                self.oauth_store.revoke(_form_body(body, headers).get("token"))
                self._write_oauth(writer, 200, {})
            else:
                self._write_oauth(writer, 405 if self.is_oauth_path(path) else 404, {"error": "not found"})
        except OAuthError as exception:
            self._write_oauth(
                writer, exception.status, {"error": exception.error, "error_description": exception.description}
            )

    def _oauth_request(self, query: dict) -> dict:
        def first(name: str) -> str | None:
            values = query.get(name)
            return values[0] if values else None

        client_id, record = self.oauth_store.client(first("client_id"))
        redirect_uri = first("redirect_uri")
        if redirect_uri not in record["redirect_uris"]:
            raise OAuthError("invalid_request", "redirect_uri is not registered for this client")
        if first("response_type") != "code":
            raise OAuthError("unsupported_response_type", "only the code response type is supported")
        challenge = first("code_challenge")
        if not challenge or first("code_challenge_method") != "S256":
            raise OAuthError("invalid_request", "PKCE with S256 is required")
        return {
            "client_id": client_id,
            "client_name": record["client_name"],
            "code_challenge": challenge,
            "redirect_uri": redirect_uri,
            "scope": normalize_scope(first("scope")),
            "state": first("state") or "",
        }

    async def _oauth_authorize_page(self, writer, headers, query: dict, error: str | None) -> None:
        if not login_secret_configured():
            raise OAuthError(
                "temporarily_unavailable", "no login secret is configured; run `netaudio daemon mcp-login-secret`", 503
            )
        request = self._oauth_request(query)
        hidden = "".join(
            f'<input type="hidden" name="{name}" value="{html.escape(request[name], quote=True)}">'
            for name in ("client_id", "code_challenge", "redirect_uri", "scope", "state")
        )
        hidden += '<input type="hidden" name="response_type" value="code"><input type="hidden" name="code_challenge_method" value="S256">'
        page = LOGIN_PAGE.format(
            action=AUTHORIZE_PATH,
            client=html.escape(request["client_name"]),
            error=f'<p class="error">{html.escape(error)}</p>' if error else "",
            hidden=hidden,
            host=html.escape(urlsplit(self.oauth_base_url(headers)).netloc),
            read_checked="checked" if request["scope"] == SCOPE_READ else "",
            scope_read=SCOPE_READ,
            scope_write=SCOPE_WRITE,
            write_checked="checked" if request["scope"] == SCOPE_WRITE else "",
        )
        self._write_oauth(writer, 200, page, content_type="text/html; charset=utf-8")

    async def _oauth_authorize_decision(self, writer, headers, form: dict) -> None:
        request = self._oauth_request({key: [value] for key, value in form.items()})
        redirect = request["redirect_uri"]
        if form.get("decision") != "allow":
            self._oauth_redirect(writer, redirect, {"error": "access_denied", "state": request["state"]})
            return
        address = _client_address(writer, headers)
        if not self.oauth_store.login_allowed(address):
            raise OAuthError("access_denied", "too many login attempts; try again in a few minutes", 429)
        if not verify_login_secret(form.get("secret") or ""):
            self.oauth_store.record_login_failure(address)
            logger.warning(f"MCP login failed from {address} for client {request['client_name']}")
            await self._oauth_authorize_page(
                writer, headers, {key: [value] for key, value in form.items()}, error="That login secret is not right."
            )
            return
        code = self.oauth_store.issue_code(request["client_id"], redirect, request["code_challenge"], request["scope"])
        logger.info(f"MCP client {request['client_name']} authorized from {address} with scope {request['scope']}")
        self._oauth_redirect(writer, redirect, {"code": code, "state": request["state"]})

    def _oauth_token(self, form: dict) -> dict:
        grant_type = form.get("grant_type")
        client_id, record = self.oauth_store.authenticate_client(form.get("client_id"), form.get("client_secret"))
        if grant_type == "authorization_code":
            scope = self.oauth_store.redeem_code(
                form.get("code"), client_id, form.get("redirect_uri"), form.get("code_verifier")
            )
            return self.oauth_store.issue_tokens(client_id, record["client_name"], scope)
        if grant_type == "refresh_token":
            return self.oauth_store.refresh(form.get("refresh_token"), client_id)
        raise OAuthError("unsupported_grant_type", "grant_type must be authorization_code or refresh_token")

    def _oauth_redirect(self, writer, redirect_uri: str, parameters: dict) -> None:
        parameters = {key: value for key, value in parameters.items() if value}
        separator = "&" if urlsplit(redirect_uri).query else "?"
        location = f"{redirect_uri}{separator}{urlencode(parameters)}"
        head = f"HTTP/1.1 302 Found\r\nLocation: {location}\r\nCache-Control: no-store\r\nContent-Length: 0\r\n\r\n"
        writer.write(head.encode())

    def _write_oauth(self, writer, status: int, payload, content_type: str = "application/json") -> None:
        body = payload.encode() if isinstance(payload, str) else json.dumps(payload, default=str).encode()
        head = (
            f"HTTP/1.1 {status} {STATUS_TEXT.get(status, 'Error')}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Cache-Control: no-store\r\n"
            "Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'\r\n"
            "Referrer-Policy: no-referrer\r\n"
            "X-Content-Type-Options: nosniff\r\n"
        )
        if status == 401:
            head += 'WWW-Authenticate: Bearer realm="netaudio"\r\n'
        writer.write(head.encode() + b"\r\n" + body)


def _json_body(body) -> dict:
    try:
        value = json.loads(body) if body else {}
    except ValueError as exception:
        raise OAuthError("invalid_request", f"invalid json: {exception}") from exception
    if not isinstance(value, dict):
        raise OAuthError("invalid_request", "body must be a json object")
    return value


def _form_body(body, headers) -> dict:
    content_type = (headers or {}).get("content-type", "")
    if "application/json" in content_type:
        return _json_body(body)
    text = body.decode() if isinstance(body, (bytes, bytearray)) else (body or "")
    return {key: values[0] for key, values in parse_qs(text, keep_blank_values=True).items()}


def _client_address(writer, headers) -> str:
    for name in ("cf-connecting-ip", "x-forwarded-for"):
        value = (headers or {}).get(name)
        if value:
            return value.split(",")[0].strip()
    peername = writer.get_extra_info("peername")
    return peername[0] if peername else "unknown"
