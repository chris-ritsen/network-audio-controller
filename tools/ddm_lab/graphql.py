"""Small read-only client for the lab DDM's public GraphQL endpoint."""

from __future__ import annotations

import http.client
import json
import math
import ssl
import textwrap
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit


class DDMGraphQLError(RuntimeError):
    pass


class HTTPStatusError(DDMGraphQLError):
    def __init__(self, status: int):
        self.status = status
        super().__init__(f"DDM GraphQL returned HTTP {status}")


class GraphQLResponseError(DDMGraphQLError):
    pass


class ReadOnlyOperation(str, Enum):
    HEALTH = "health"
    SCHEMA = "schema"
    INVENTORY = "inventory"


class ManagedMutation(str, Enum):
    DEVICES_ENROLL = "devices_enroll"
    DEVICES_UNENROLL = "devices_unenroll"


@dataclass(frozen=True)
class GraphQLResult:
    operation: ReadOnlyOperation | ManagedMutation
    data: Any
    errors: tuple[dict[str, Any], ...] = ()

    @property
    def partial(self) -> bool:
        return bool(self.errors)


@dataclass(frozen=True)
class HTTPRequest:
    url: str
    headers: dict[str, str]
    body: bytes
    timeout: float
    max_response_bytes: int


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    body: bytes


Transport = Callable[[HTTPRequest], HTTPResponse]


HEALTH_QUERY = "query NetAudioDDMHealth { __typename }"

SCHEMA_QUERY = textwrap.dedent(
    """
    query NetAudioDDMSchema {
      __schema {
        queryType { name fields { name args { name defaultValue type { ...TypeRef } } type { ...TypeRef } } }
        mutationType { name fields { name args { name defaultValue type { ...TypeRef } } type { ...TypeRef } } }
        types {
          kind
          name
          fields { name args { name defaultValue type { ...TypeRef } } type { ...TypeRef } }
          inputFields { name defaultValue type { ...TypeRef } }
          enumValues { name }
        }
      }
    }
    fragment TypeRef on __Type {
      kind
      name
      ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } }
    }
    """
).strip()

INVENTORY_QUERY = textwrap.dedent(
    """
    query NetAudioDDMInventory {
      domains {
        id
        name
        status { clocking connectivity latency subscriptions summary }
        devices {
          id
          name
          domainId
          enrolmentState
          interfaces { macAddress address }
          connection { state lastChanged }
        }
      }
      unenrolledDevices {
        id
        name
        enrolmentState
        interfaces { macAddress address }
        connection { state lastChanged }
      }
    }
    """
).strip()

DEVICES_ENROLL_MUTATION = textwrap.dedent(
    """
    mutation NetAudioDevicesEnroll($input: DevicesEnrollInput!) {
      DevicesEnroll(input: $input) { ok }
    }
    """
).strip()

DEVICES_UNENROLL_MUTATION = textwrap.dedent(
    """
    mutation NetAudioDevicesUnenroll($input: DevicesUnenrollInput!) {
      DevicesUnenroll(input: $input) { ok }
    }
    """
).strip()

OPERATIONS = {
    ReadOnlyOperation.HEALTH: ("NetAudioDDMHealth", HEALTH_QUERY),
    ReadOnlyOperation.SCHEMA: ("NetAudioDDMSchema", SCHEMA_QUERY),
    ReadOnlyOperation.INVENTORY: ("NetAudioDDMInventory", INVENTORY_QUERY),
}


def _default_transport(request: HTTPRequest) -> HTTPResponse:
    endpoint = urlsplit(request.url)
    host = endpoint.hostname
    if host is None:
        raise DDMGraphQLError("DDM URL has no host")
    port = endpoint.port or (443 if endpoint.scheme == "https" else 80)
    connection: http.client.HTTPConnection
    if endpoint.scheme == "https":
        connection = http.client.HTTPSConnection(
            host,
            port=port,
            timeout=request.timeout,
            context=ssl.create_default_context(),
        )
    else:
        connection = http.client.HTTPConnection(host, port=port, timeout=request.timeout)
    try:
        connection.request("POST", "/graphql", body=request.body, headers=request.headers)
        response = connection.getresponse()
        body = response.read(request.max_response_bytes + 1)
    except (OSError, ValueError, http.client.HTTPException) as error:
        raise DDMGraphQLError("DDM GraphQL transport failed") from error
    finally:
        connection.close()
    if len(body) > request.max_response_bytes:
        raise DDMGraphQLError("DDM GraphQL response exceeded the configured limit")
    return HTTPResponse(status=response.status, body=body)


class DDMGraphQLClient:
    """Run only the three fixed read-only operations used by this harness."""

    def __init__(
        self,
        url: str,
        key_file: str | Path,
        *,
        timeout: float = 5.0,
        max_response_bytes: int = 2 * 1024 * 1024,
        transport: Transport | None = None,
    ):
        endpoint = urlsplit(url)
        if endpoint.scheme not in {"http", "https"} or endpoint.hostname is None:
            raise ValueError("DDM URL must use http or https and include a host")
        if endpoint.path != "/graphql" or endpoint.query or endpoint.fragment:
            raise ValueError("DDM URL path must be exactly /graphql")
        if endpoint.username is not None or endpoint.password is not None:
            raise ValueError("DDM URL must not contain credentials")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout):
            raise ValueError("timeout must be a finite number")
        if not 0 < timeout <= 60:
            raise ValueError("timeout must be between 0 and 60 seconds")
        if isinstance(max_response_bytes, bool) or not 1 <= max_response_bytes <= 16 * 1024 * 1024:
            raise ValueError("response limit must be between 1 byte and 16 MiB")
        self.url = url
        self.key_file = Path(key_file)
        self.timeout = float(timeout)
        self.max_response_bytes = max_response_bytes
        self.transport = transport or _default_transport

    def health(self) -> GraphQLResult:
        return self.execute(ReadOnlyOperation.HEALTH)

    def schema(self) -> GraphQLResult:
        return self.execute(ReadOnlyOperation.SCHEMA)

    def inventory(self) -> GraphQLResult:
        return self.execute(ReadOnlyOperation.INVENTORY)

    def enroll_devices(
        self,
        domain_id: str,
        device_ids: list[str],
        *,
        clear_config: bool = False,
    ) -> GraphQLResult:
        domain_id = self._identifier(domain_id, "domain ID")
        identifiers = self._identifiers(device_ids)
        return self._execute_document(
            ManagedMutation.DEVICES_ENROLL,
            "NetAudioDevicesEnroll",
            DEVICES_ENROLL_MUTATION,
            {
                "input": {
                    "domainId": domain_id,
                    "deviceIds": identifiers,
                    "clearConfig": bool(clear_config),
                }
            },
        )

    def unenroll_devices(
        self,
        device_ids: list[str],
        *,
        clear_config: bool = False,
    ) -> GraphQLResult:
        return self._execute_document(
            ManagedMutation.DEVICES_UNENROLL,
            "NetAudioDevicesUnenroll",
            DEVICES_UNENROLL_MUTATION,
            {
                "input": {
                    "deviceIds": self._identifiers(device_ids),
                    "clearConfig": bool(clear_config),
                }
            },
        )

    def execute(self, operation: ReadOnlyOperation) -> GraphQLResult:
        if not isinstance(operation, ReadOnlyOperation):
            raise ValueError("operation is not in the read-only allowlist")
        operation_name, document = OPERATIONS[operation]
        return self._execute_document(operation, operation_name, document, {})

    @staticmethod
    def _identifier(value: Any, label: str) -> str:
        if not isinstance(value, str) or not value or len(value) > 1024:
            raise ValueError(f"{label} must be a non-empty string of at most 1024 characters")
        return value

    @classmethod
    def _identifiers(cls, values: Any) -> list[str]:
        if not isinstance(values, list) or not values or len(values) > 64:
            raise ValueError("device IDs must be a non-empty list of at most 64 entries")
        return [cls._identifier(value, "device ID") for value in values]

    def _execute_document(
        self,
        operation: ReadOnlyOperation | ManagedMutation,
        operation_name: str,
        document: str,
        variables: dict[str, Any],
    ) -> GraphQLResult:
        try:
            key = self.key_file.read_text(encoding="ascii").rstrip("\r\n")
        except (OSError, UnicodeError) as error:
            raise DDMGraphQLError("DDM API key could not be read") from error
        if not key:
            raise DDMGraphQLError("DDM API key file is empty")
        if any(ord(character) < 0x21 or ord(character) > 0x7E for character in key):
            raise DDMGraphQLError("DDM API key has an invalid format")
        body = json.dumps(
            {"operationName": operation_name, "query": document, "variables": variables},
            separators=(",", ":"),
        ).encode("utf-8")
        response = self.transport(
            HTTPRequest(
                url=self.url,
                headers={
                    "Accept": "application/json",
                    "Authorization": key,
                    "Content-Type": "application/json",
                    "Connection": "close",
                },
                body=body,
                timeout=self.timeout,
                max_response_bytes=self.max_response_bytes,
            )
        )
        if not 200 <= response.status < 300:
            raise HTTPStatusError(response.status)
        if len(response.body) > self.max_response_bytes:
            raise DDMGraphQLError("DDM GraphQL response exceeded the configured limit")
        try:
            decoded = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DDMGraphQLError("DDM GraphQL returned invalid JSON") from error
        if not isinstance(decoded, dict):
            raise DDMGraphQLError("DDM GraphQL response is not an object")
        raw_errors = decoded.get("errors", [])
        if not isinstance(raw_errors, list) or any(not isinstance(item, dict) for item in raw_errors):
            raise DDMGraphQLError("DDM GraphQL errors have an invalid shape")
        errors = tuple(raw_errors)
        if errors and decoded.get("data") is None:
            raise GraphQLResponseError(f"DDM GraphQL returned {len(errors)} error(s) without usable data")
        if "data" not in decoded:
            raise DDMGraphQLError("DDM GraphQL response omitted data")
        return GraphQLResult(operation=operation, data=decoded["data"], errors=errors)


__all__ = [
    "DDMGraphQLClient",
    "DDMGraphQLError",
    "GraphQLResponseError",
    "GraphQLResult",
    "HTTPRequest",
    "HTTPResponse",
    "HTTPStatusError",
    "ManagedMutation",
    "ReadOnlyOperation",
]
