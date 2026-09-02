from __future__ import annotations

import asyncio
import http.client
import json
import math
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import SplitResult, urlsplit

from netaudio.ddm.models import GraphQLIssue, Inventory, ModelDecodeError, parse_inventory
from netaudio.ddm.queries import INVENTORY_OPERATION_NAME, INVENTORY_QUERY

DEFAULT_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 10.0
MAXIMUM_CREDENTIAL_BYTES = 64 * 1024
MAXIMUM_RESPONSE_LIMIT_BYTES = 16 * 1024 * 1024
MAXIMUM_TIMEOUT_SECONDS = 60.0


class ManagedAPIError(RuntimeError):
    pass


class CredentialError(ManagedAPIError):
    pass


class HTTPStatusError(ManagedAPIError):
    def __init__(self, status: int, body: bytes):
        self.body = body
        self.status = status
        super().__init__(f"Managed API returned HTTP {status}")


class JSONResponseError(ManagedAPIError):
    pass


class ResponseShapeError(ManagedAPIError):
    pass


class ResponseTooLargeError(ManagedAPIError):
    pass


class TransportError(ManagedAPIError):
    pass


@dataclass(frozen=True)
class ManagedAPIRequest:
    url: str
    headers: Mapping[str, str]
    body: bytes
    timeout: float
    max_response_bytes: int


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    body: bytes


@dataclass(frozen=True)
class GraphQLResult:
    data: Mapping[str, Any] | None
    errors: tuple[GraphQLIssue, ...]

    @property
    def failed(self) -> bool:
        return self.data is None

    @property
    def successful(self) -> bool:
        return self.data is not None and not self.errors

    def to_json(self) -> dict[str, Any]:
        return {
            "data": dict(self.data) if self.data is not None else None,
            "errors": [dict(issue.raw) for issue in self.errors],
        }


@dataclass(frozen=True)
class InventoryResult:
    data: Inventory | None
    errors: tuple[GraphQLIssue, ...]
    raw_data: Mapping[str, Any] | None

    @property
    def failed(self) -> bool:
        return self.data is None

    @property
    def partial(self) -> bool:
        return self.data is not None and bool(self.errors)

    @property
    def successful(self) -> bool:
        return self.data is not None and not self.errors


Transport = Callable[[ManagedAPIRequest], HTTPResponse]


def _validate_url(url: str) -> SplitResult:
    if not isinstance(url, str):
        raise ValueError("Managed API URL must be a string")
    endpoint = urlsplit(url)
    if endpoint.scheme not in {"http", "https"} or endpoint.hostname is None:
        raise ValueError("Managed API URL must use http or https and include a host")
    if endpoint.path != "/graphql" or endpoint.query or endpoint.fragment:
        raise ValueError("Managed API URL path must be exactly /graphql")
    if endpoint.username is not None or endpoint.password is not None:
        raise ValueError("Managed API URL must not contain credentials")
    try:
        endpoint.port
    except ValueError as error:
        raise ValueError("Managed API URL contains an invalid port") from error
    return endpoint


def _validate_timeout(timeout: float) -> float:
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout):
        raise ValueError("timeout must be a finite number")
    if not 0 < timeout <= MAXIMUM_TIMEOUT_SECONDS:
        raise ValueError(f"timeout must be between 0 and {MAXIMUM_TIMEOUT_SECONDS:g} seconds")
    return float(timeout)


def _validate_response_limit(max_response_bytes: int) -> int:
    if isinstance(max_response_bytes, bool) or not isinstance(max_response_bytes, int):
        raise ValueError("response limit must be an integer")
    if not 1 <= max_response_bytes <= MAXIMUM_RESPONSE_LIMIT_BYTES:
        raise ValueError("response limit must be between 1 byte and 16 MiB")
    return max_response_bytes


def _validate_credential(credential: str) -> str:
    if not isinstance(credential, str) or not credential:
        raise CredentialError("Managed API credential is empty")
    if len(credential) > MAXIMUM_CREDENTIAL_BYTES:
        raise CredentialError("Managed API credential is too large")
    if any(ord(character) < 0x21 or ord(character) > 0x7E for character in credential):
        raise CredentialError("Managed API credential has an invalid format")
    return credential


def _connection(endpoint: SplitResult, timeout: float) -> http.client.HTTPConnection:
    host = endpoint.hostname
    if host is None:
        raise TransportError("Managed API URL has no host")
    port = endpoint.port or (443 if endpoint.scheme == "https" else 80)
    if endpoint.scheme == "https":
        return http.client.HTTPSConnection(host, port=port, timeout=timeout, context=ssl.create_default_context())
    return http.client.HTTPConnection(host, port=port, timeout=timeout)


def _default_transport(request: ManagedAPIRequest) -> HTTPResponse:
    endpoint = urlsplit(request.url)
    connection = _connection(endpoint, request.timeout)
    try:
        connection.request("POST", endpoint.path, body=request.body, headers=dict(request.headers))
        response = connection.getresponse()
        body = response.read(request.max_response_bytes + 1)
        status = response.status
    except (OSError, ValueError, http.client.HTTPException) as error:
        raise TransportError(f"Managed API transport failed: {error}") from error
    finally:
        connection.close()
    if len(body) > request.max_response_bytes:
        raise ResponseTooLargeError("Managed API response exceeded the configured limit")
    return HTTPResponse(status=status, body=body)


class ManagedAPIClient:
    def __init__(
        self,
        url: str,
        *,
        credential: str | None = None,
        credential_file: str | Path | None = None,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        transport: Transport | None = None,
    ):
        self.url = url
        self._endpoint = _validate_url(url)
        if (credential is None) == (credential_file is None):
            raise ValueError("provide exactly one of credential or credential_file")
        self._credential = _validate_credential(credential) if credential is not None else None
        self.credential_file = Path(credential_file) if credential_file is not None else None
        self.max_response_bytes = _validate_response_limit(max_response_bytes)
        self.timeout = _validate_timeout(timeout)
        self.transport = transport or _default_transport

    def execute(
        self,
        query: str,
        variables: Mapping[str, Any] | None = None,
        operation_name: str | None = None,
    ) -> GraphQLResult:
        if not isinstance(query, str) or not query.strip():
            raise ValueError("GraphQL query must be a non-empty string")
        credential = self._read_credential()
        payload: dict[str, Any] = {}
        if operation_name is not None:
            payload["operationName"] = operation_name
        payload["query"] = query
        payload["variables"] = dict(variables or {})
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = ManagedAPIRequest(
            url=self.url,
            headers={
                "Accept": "application/json",
                "Authorization": credential,
                "Content-Type": "application/json",
                "Connection": "close",
            },
            body=body,
            timeout=self.timeout,
            max_response_bytes=self.max_response_bytes,
        )
        response = self._send(request)
        return self._decode_result(response)

    async def execute_async(
        self,
        query: str,
        variables: Mapping[str, Any] | None = None,
        operation_name: str | None = None,
    ) -> GraphQLResult:
        return await asyncio.to_thread(self.execute, query, variables, operation_name)

    def inventory(self) -> InventoryResult:
        result = self.execute(INVENTORY_QUERY, {}, INVENTORY_OPERATION_NAME)
        if result.data is None:
            return InventoryResult(data=None, errors=result.errors, raw_data=None)
        try:
            inventory = parse_inventory(result.data, allow_missing_roots=bool(result.errors))
        except ModelDecodeError as error:
            raise ResponseShapeError(str(error)) from error
        return InventoryResult(data=inventory, errors=result.errors, raw_data=dict(result.data))

    async def inventory_async(self) -> InventoryResult:
        return await asyncio.to_thread(self.inventory)

    def _decode_result(self, response: HTTPResponse) -> GraphQLResult:
        decoded = self._decode_json(response.body)
        errors = self._decode_errors(decoded.get("errors", []))
        if "data" not in decoded:
            if errors:
                return GraphQLResult(data=None, errors=errors)
            raise ResponseShapeError("Managed API response omitted data")
        raw_data = decoded["data"]
        if raw_data is None:
            return GraphQLResult(data=None, errors=errors)
        if not isinstance(raw_data, Mapping):
            raise ResponseShapeError("Managed API data must be an object or null")
        return GraphQLResult(data=dict(raw_data), errors=errors)

    def _read_credential(self) -> str:
        if self._credential is not None:
            return self._credential
        credential_file = self.credential_file
        if credential_file is None:
            raise CredentialError("Managed API credential is not configured")
        try:
            credential = credential_file.read_text(encoding="ascii").rstrip("\r\n")
        except (OSError, UnicodeError) as error:
            raise CredentialError("Managed API credential file could not be read") from error
        return _validate_credential(credential)

    def _send(self, request: ManagedAPIRequest) -> HTTPResponse:
        try:
            response = self.transport(request)
        except ManagedAPIError:
            raise
        except (OSError, ValueError, RuntimeError, http.client.HTTPException) as error:
            raise TransportError(f"Managed API transport failed: {error}") from error
        if not isinstance(response, HTTPResponse):
            raise TransportError("Managed API transport returned an invalid response")
        if (
            isinstance(response.status, bool)
            or not isinstance(response.status, int)
            or not isinstance(response.body, bytes)
        ):
            raise TransportError("Managed API transport returned an invalid response")
        if len(response.body) > self.max_response_bytes:
            raise ResponseTooLargeError("Managed API response exceeded the configured limit")
        if not 200 <= response.status < 300:
            raise HTTPStatusError(response.status, response.body)
        return response

    @staticmethod
    def _decode_errors(value: Any) -> tuple[GraphQLIssue, ...]:
        if not isinstance(value, list):
            raise ResponseShapeError("Managed API GraphQL errors must be a list")
        if any(not isinstance(item, Mapping) for item in value):
            raise ResponseShapeError("Managed API GraphQL errors must contain only objects")
        try:
            return tuple(GraphQLIssue.from_mapping(item) for item in value)
        except ModelDecodeError as error:
            raise ResponseShapeError(str(error)) from error

    @staticmethod
    def _decode_json(body: bytes) -> Mapping[str, Any]:
        try:
            decoded = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise JSONResponseError("Managed API returned invalid JSON") from error
        if not isinstance(decoded, Mapping):
            raise ResponseShapeError("Managed API response must be an object")
        return decoded


__all__ = [
    "CredentialError",
    "GraphQLResult",
    "HTTPResponse",
    "HTTPStatusError",
    "InventoryResult",
    "JSONResponseError",
    "ManagedAPIClient",
    "ManagedAPIError",
    "ManagedAPIRequest",
    "ResponseShapeError",
    "ResponseTooLargeError",
    "Transport",
    "TransportError",
]
