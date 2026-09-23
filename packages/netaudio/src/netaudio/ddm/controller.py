from __future__ import annotations

import http.client
import ipaddress
import math
import socket
import ssl
import time
import urllib.parse
from typing import Any, Callable

from netaudio import core
from netaudio.core import _requests, _types
from netaudio.core._protocols import CONTROLLER_AUTH_PORT as DEFAULT_AUTH_PORT, CONTROLLER_VERSIONS_PATH


DEFAULT_TIMEOUT_SECONDS = 10.0
MAXIMUM_RESPONSE_BYTES = 1024 * 1024


class ControllerServiceError(RuntimeError):
    pass


class ControllerAuthenticationError(ControllerServiceError):
    pass


class DAPISessionError(ControllerServiceError):
    pass


def _validate_timeout(timeout: float) -> float:
    if not math.isfinite(timeout) or timeout <= 0 or timeout > 60:
        raise ValueError("timeout must be greater than 0 and no more than 60 seconds")
    return timeout


def _port(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise ControllerServiceError(f"DDM returned an invalid {name}")
    return value


class ControllerAPIClient:
    def __init__(
        self,
        server: str,
        *,
        port: int = DEFAULT_AUTH_PORT,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ):
        if not server or "/" in server:
            raise ValueError("server must be a hostname or IP address")
        self.server = server.rstrip(".")
        self.port = _port(port, "authentication port")
        self.timeout = _validate_timeout(timeout)
        self.ssl_context = ssl._create_unverified_context()

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        connection = http.client.HTTPSConnection(
            self.server,
            self.port,
            timeout=self.timeout,
            context=self.ssl_context,
        )
        try:
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            response_body = response.read(MAXIMUM_RESPONSE_BYTES + 1)
        except (OSError, ssl.SSLError, http.client.HTTPException) as exception:
            raise ControllerServiceError(f"DDM Controller API request failed: {exception}") from exception
        finally:
            connection.close()
        if len(response_body) > MAXIMUM_RESPONSE_BYTES:
            raise ControllerServiceError("DDM Controller API response exceeded the configured limit")
        if response.status == 401:
            raise ControllerAuthenticationError("DDM rejected the username or password")
        if not 200 <= response.status < 300:
            raise ControllerServiceError(f"DDM Controller API returned HTTP {response.status}")
        return response_body

    def _bootstrap(self) -> tuple[_types.ControllerApiRoutes, _types.ControllerEndpoints]:
        try:
            routes = core.controller_api_routes(
                self._request("GET", CONTROLLER_VERSIONS_PATH, headers={"Accept": "application/json"})
            )
            endpoints = core.controller_endpoints(
                self._request("GET", routes["endpoints"], headers={"Accept": "application/json"})
            )
        except core.NetaudioCoreError as exception:
            raise ControllerServiceError(str(exception)) from exception

        return routes, endpoints

    def endpoints(self) -> _types.ControllerEndpoints:
        return self._bootstrap()[1]

    def login(self, username: str, password: str) -> _types.ControllerLogin:
        routes, advertised = self._bootstrap()
        form = urllib.parse.urlencode({"username": username, "password": password}).encode("ascii")
        body = self._request(
            "POST",
            routes["login"],
            body=form,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        try:
            login = core.controller_login(body)
        except core.NetaudioCoreError as exception:
            raise ControllerServiceError(str(exception)) from exception

        if login is None:
            raise ControllerAuthenticationError("DDM returned an unsupported Controller authentication token")

        if login["endpoints"] != advertised:
            raise ControllerServiceError("DDM changed its advertised endpoints during login")

        return login


SocketConnector = Callable[[str, int, ssl.SSLContext, float], Any]


def _connect_tls(server: str, port: int, context: ssl.SSLContext, timeout: float):
    raw_socket = socket.create_connection((server, port), timeout=timeout)
    try:
        return context.wrap_socket(raw_socket, server_hostname=server)
    except Exception:
        raw_socket.close()
        raise


def normalize_device_id(device_id: str) -> str:
    identity = core.device_identity({"kind": "managed_device", "value": device_id})

    if identity is None:
        raise ValueError("device ID must be 16 hexadecimal digits, optionally followed by :0")

    return identity


def normalize_domain_id(domain_id: str) -> str:
    normalized = core.device_identity({"kind": "managed_domain", "value": domain_id})

    if normalized is None:
        raise ValueError("domain ID must be 32 hexadecimal digits")

    return normalized


def _validate_api_key(api_key: str) -> str:
    try:
        core.validate_managed_credential({"kind": "api_key", "value": api_key})
    except core.NetaudioCoreError as exception:
        raise ControllerAuthenticationError(exception.detail or str(exception)) from exception

    return api_key


class DAPISession:
    def __init__(
        self,
        server: str,
        port: int,
        ssl_context: ssl.SSLContext,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        connector: SocketConnector = _connect_tls,
    ):
        self.server = server
        self.port = _port(port, "Controller service port")
        self.ssl_context = ssl_context
        self.timeout = _validate_timeout(timeout)
        self.connector = connector
        self.socket = None
        self.notification_socket = None
        self.local_ipv4 = None
        self._state: _types.ManagedSessionState | None = None

    def __enter__(self):
        try:
            self.socket = self.connector(self.server, self.port, self.ssl_context, self.timeout)
            local_address = self.socket.getsockname()[0]
            self.local_ipv4 = ipaddress.IPv4Address(local_address).packed
            self.notification_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.notification_socket.bind((local_address, 0))
        except (OSError, ssl.SSLError) as exception:
            if self.notification_socket is not None:
                self.notification_socket.close()
                self.notification_socket = None
            if self.socket is not None:
                self.socket.close()
                self.socket = None
            raise DAPISessionError(f"could not connect to the DDM Controller service: {exception}") from exception
        return self

    def __exit__(self, _type, _value, _traceback):
        if self.notification_socket is not None:
            self.notification_socket.close()
            self.notification_socket = None
        if self.socket is not None:
            self.socket.close()
            self.socket = None
        self.local_ipv4 = None
        self._state = None

    def _send(self, frame: bytes) -> None:
        if self.socket is None:
            raise DAPISessionError("DDM Controller session is not connected")
        try:
            self.socket.sendall(frame)
        except OSError as exception:
            raise DAPISessionError(f"could not send a DDM Controller message: {exception}") from exception

    def _read_exactly(self, length: int, deadline: float) -> bytes:
        if self.socket is None:
            raise DAPISessionError("DDM Controller session is not connected")
        output = bytearray()
        while len(output) < length:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DAPISessionError("timed out waiting for a DDM Controller response")
            self.socket.settimeout(remaining)
            try:
                chunk = self.socket.recv(length - len(output))
            except (OSError, socket.timeout) as exception:
                raise DAPISessionError("timed out waiting for a DDM Controller response") from exception
            if not chunk:
                raise DAPISessionError("DDM closed the Controller session")
            output.extend(chunk)
        return bytes(output)

    def _exchange(
        self, credential: str, operation: _requests.ManagedOperation, expected_domain_id: str | None
    ) -> bytes | None:
        if self.notification_socket is None or self.local_ipv4 is None:
            raise DAPISessionError("DDM Controller notification socket is not open")

        deadline = time.monotonic() + self.timeout
        request: _requests.ManagedSessionRequest = {
            "action": "begin",
            "state": self._state,
            "credential": credential,
            "local_ipv4": str(ipaddress.IPv4Address(self.local_ipv4)),
            "notification_port": self.notification_socket.getsockname()[1],
            "expected_domain_id": expected_domain_id,
            "operation": operation,
        }

        while True:
            try:
                result = core.advance_managed_session(request)
            except core.NetaudioCoreError as exception:
                raise DAPISessionError(exception.detail or str(exception)) from exception

            self._state = result["state"]

            for frame in result["outgoing"]:
                self._send(bytes(frame))

            if result["complete"]:
                packet = result["packet_hex"]
                return bytes.fromhex(packet) if packet is not None else None

            request = {
                "action": "receive",
                "state": self._state,
                "data": list(self._read_exactly(result["receive_bytes"], deadline)),
            }

    def identify(
        self,
        credential: str,
        device_id: str,
        host_mac: bytes,
        expected_domain_id: str | None = None,
    ) -> None:
        self._exchange(
            credential, {"kind": "identify", "device_id": device_id, "host_mac": list(host_mac)}, expected_domain_id
        )

    def query_arc(
        self,
        credential: str,
        device_id: str,
        arc_packet: bytes,
        expected_domain_id: str | None = None,
    ) -> bytes:
        response = self._exchange(
            credential, {"kind": "arc", "device_id": device_id, "packet": list(arc_packet)}, expected_domain_id
        )

        if response is None:
            raise DAPISessionError("DDM ARC exchange completed without a response")

        return response

    def reboot(
        self,
        credential: str,
        device_id: str,
        host_mac: bytes,
        expected_domain_id: str | None = None,
    ) -> None:
        self._exchange(
            credential, {"kind": "reboot", "device_id": device_id, "host_mac": list(host_mac)}, expected_domain_id
        )

    def query_settings(
        self,
        credential: str,
        device_id: str,
        settings_packet: bytes,
        expected_response_opcode: int,
        expected_domain_id: str | None = None,
    ) -> bytes:
        response = self._exchange(
            credential,
            {
                "kind": "settings",
                "device_id": device_id,
                "packet": list(settings_packet),
                "response_opcode": expected_response_opcode,
            },
            expected_domain_id,
        )

        if response is None:
            raise DAPISessionError("DDM settings exchange completed without a publication")

        return response


def identify_managed_device(
    server: str,
    username: str,
    password: str,
    device_id: str,
    host_mac: bytes,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    expected_domain_id: str | None = None,
) -> None:
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
    )
    login = api.login(username, password)
    with DAPISession(
        api.server,
        login["endpoints"]["service_port"],
        api.ssl_context,
        timeout=timeout,
    ) as session:
        session.identify(login["auth_token"], device_id, host_mac, expected_domain_id)


def identify_managed_device_with_api_key(
    server: str,
    api_key: str,
    device_id: str,
    host_mac: bytes,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    expected_domain_id: str | None = None,
) -> None:
    api_key = _validate_api_key(api_key)
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
    )
    endpoints = api.endpoints()
    with DAPISession(
        api.server,
        endpoints["service_port"],
        api.ssl_context,
        timeout=timeout,
    ) as session:
        session.identify(api_key, device_id, host_mac, expected_domain_id)


def reboot_managed_device_with_api_key(
    server: str,
    api_key: str,
    device_id: str,
    host_mac: bytes,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    expected_domain_id: str | None = None,
) -> None:
    api_key = _validate_api_key(api_key)
    api = ControllerAPIClient(server, port=auth_port, timeout=timeout)
    endpoints = api.endpoints()
    with DAPISession(api.server, endpoints["service_port"], api.ssl_context, timeout=timeout) as session:
        session.reboot(api_key, device_id, host_mac, expected_domain_id)


def query_managed_arc(
    server: str,
    username: str,
    password: str,
    device_id: str,
    arc_packet: bytes,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    expected_domain_id: str | None = None,
) -> bytes:
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
    )
    login = api.login(username, password)
    with DAPISession(
        api.server,
        login["endpoints"]["service_port"],
        api.ssl_context,
        timeout=timeout,
    ) as session:
        return session.query_arc(login["auth_token"], device_id, arc_packet, expected_domain_id)


def query_managed_arc_with_api_key(
    server: str,
    api_key: str,
    device_id: str,
    arc_packet: bytes,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    expected_domain_id: str | None = None,
) -> bytes:
    api_key = _validate_api_key(api_key)
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
    )
    endpoints = api.endpoints()
    with DAPISession(
        api.server,
        endpoints["service_port"],
        api.ssl_context,
        timeout=timeout,
    ) as session:
        return session.query_arc(api_key, device_id, arc_packet, expected_domain_id)


def query_managed_settings(
    server: str,
    username: str,
    password: str,
    device_id: str,
    settings_packet: bytes,
    expected_response_opcode: int,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    expected_domain_id: str | None = None,
) -> bytes:
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
    )
    login = api.login(username, password)
    with DAPISession(
        api.server,
        login["endpoints"]["service_port"],
        api.ssl_context,
        timeout=timeout,
    ) as session:
        return session.query_settings(
            login["auth_token"],
            device_id,
            settings_packet,
            expected_response_opcode,
            expected_domain_id,
        )


def query_managed_settings_with_api_key(
    server: str,
    api_key: str,
    device_id: str,
    settings_packet: bytes,
    expected_response_opcode: int,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    expected_domain_id: str | None = None,
) -> bytes:
    api_key = _validate_api_key(api_key)
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
    )
    endpoints = api.endpoints()
    with DAPISession(
        api.server,
        endpoints["service_port"],
        api.ssl_context,
        timeout=timeout,
    ) as session:
        return session.query_settings(
            api_key,
            device_id,
            settings_packet,
            expected_response_opcode,
            expected_domain_id,
        )
