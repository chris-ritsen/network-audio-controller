from __future__ import annotations

import http.client
import ipaddress
import json
import math
import socket
import ssl
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from netaudio import core


DEFAULT_AUTH_PORT = 8443
DEFAULT_TIMEOUT_SECONDS = 10.0
MAXIMUM_RESPONSE_BYTES = 1024 * 1024
MAXIMUM_DAPI_FRAME_BYTES = 1024 * 1024
DAPI_RESPONSE_MARKER = bytes.fromhex("b91a3725")


class ControllerServiceError(RuntimeError):
    pass


class ControllerAuthenticationError(ControllerServiceError):
    pass


class DAPISessionError(ControllerServiceError):
    pass


@dataclass(frozen=True)
class ControllerEndpoints:
    service_port: int
    device_port: int
    graphql_url: str


@dataclass(frozen=True)
class ControllerLogin:
    auth_token: str
    endpoints: ControllerEndpoints


def _validate_timeout(timeout: float) -> float:
    if not math.isfinite(timeout) or timeout <= 0 or timeout > 60:
        raise ValueError("timeout must be greater than 0 and no more than 60 seconds")
    return timeout


def _port(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise ControllerServiceError(f"DDM returned an invalid {name}")
    return value


def _decode_object(body: bytes) -> dict[str, Any]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exception:
        raise ControllerServiceError("DDM returned invalid JSON") from exception
    if not isinstance(value, dict):
        raise ControllerServiceError("DDM response must be a JSON object")
    return value


def _decode_endpoints(body: bytes) -> ControllerEndpoints:
    value = _decode_object(body)
    graphql_url = value.get("graphQl")
    if not isinstance(graphql_url, str) or not graphql_url:
        raise ControllerServiceError("DDM returned an invalid GraphQL endpoint")
    return ControllerEndpoints(
        service_port=_port(value.get("servicePort"), "Controller service port"),
        device_port=_port(value.get("devicePort"), "device service port"),
        graphql_url=graphql_url,
    )


class ControllerAPIClient:
    def __init__(
        self,
        server: str,
        *,
        port: int = DEFAULT_AUTH_PORT,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        ca_file: Path | None = None,
        insecure_tls: bool = False,
    ):
        if not server or "/" in server:
            raise ValueError("server must be a hostname or IP address")
        self.server = server.rstrip(".")
        self.port = _port(port, "authentication port")
        self.timeout = _validate_timeout(timeout)
        if insecure_tls:
            self.ssl_context = ssl._create_unverified_context()
        else:
            self.ssl_context = ssl.create_default_context(cafile=str(ca_file) if ca_file else None)

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

    def versions(self) -> tuple[str, ...]:
        body = self._request("GET", "/dapi", headers={"Accept": "application/json"})
        try:
            value = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exception:
            raise ControllerServiceError("DDM returned invalid JSON") from exception
        if not isinstance(value, list) or any(not isinstance(version, str) for version in value):
            raise ControllerServiceError("DDM returned an invalid Controller API version list")
        return tuple(value)

    def endpoints(self) -> ControllerEndpoints:
        return _decode_endpoints(self._request("GET", "/dapi/v2/endpoints", headers={"Accept": "application/json"}))

    def login(self, username: str, password: str) -> ControllerLogin:
        if "v2" not in self.versions():
            raise ControllerServiceError("DDM does not advertise the observed v2 Controller API")
        advertised = self.endpoints()
        form = urllib.parse.urlencode({"username": username, "password": password}).encode("ascii")
        body = self._request(
            "POST",
            "/dapi/v2/login",
            body=form,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        value = _decode_object(body)
        auth_token = value.get("authToken")
        try:
            encoded_auth_token = auth_token.encode("ascii") if isinstance(auth_token, str) else b""
        except UnicodeEncodeError:
            encoded_auth_token = b""
        if len(encoded_auth_token) != 43:
            raise ControllerAuthenticationError("DDM returned an unsupported Controller authentication token")
        returned = _decode_endpoints(body)
        if returned != advertised:
            raise ControllerServiceError("DDM changed its advertised endpoints during login")
        return ControllerLogin(auth_token=auth_token, endpoints=returned)


SocketConnector = Callable[[str, int, ssl.SSLContext, float], Any]


def _connect_tls(server: str, port: int, context: ssl.SSLContext, timeout: float):
    raw_socket = socket.create_connection((server, port), timeout=timeout)
    try:
        return context.wrap_socket(raw_socket, server_hostname=server)
    except Exception:
        raw_socket.close()
        raise


def normalize_device_id(device_id: str) -> str:
    base, separator, process = device_id.lower().partition(":")
    if separator and process != "0":
        raise ValueError("managed Identify currently supports only device process :0")
    if len(base) != 16 or any(character not in "0123456789abcdef" for character in base):
        raise ValueError("device ID must be 16 hexadecimal digits, optionally followed by :0")
    return base


def _validate_api_key(api_key: str) -> str:
    try:
        encoded = api_key.encode("ascii")
    except (AttributeError, UnicodeEncodeError) as exception:
        raise ControllerAuthenticationError("DDM API key must be an ASCII string") from exception
    if (
        len(encoded) != 36
        or any(encoded[index] != ord("-") for index in (8, 13, 18, 23))
        or any(
            byte not in b"0123456789abcdefABCDEF" for index, byte in enumerate(encoded) if index not in (8, 13, 18, 23)
        )
    ):
        raise ControllerAuthenticationError("DDM API key must use the observed UUID format")
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
        self.wrapper_id = 5
        self.initialized = False
        self.target_selectors: dict[str, int] = {}

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
        self.initialized = False
        self.target_selectors.clear()

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

    def _read_frame(self, deadline: float) -> bytes:
        header = self._read_exactly(12, deadline)
        if header[:4] != DAPI_RESPONSE_MARKER:
            raise DAPISessionError("DDM returned an unsupported Controller frame marker")
        payload_length = int.from_bytes(header[8:12], "big")
        if payload_length > MAXIMUM_DAPI_FRAME_BYTES:
            raise DAPISessionError("DDM Controller frame exceeded the configured limit")
        frame = header + self._read_exactly(payload_length, deadline)
        announcement = self._parse("dapi_service_announcement", frame)
        if announcement is not None:
            self._send(core.build_dapi_service_acknowledgement(frame))
        return frame

    @staticmethod
    def _parse(kind: str, frame: bytes) -> dict[str, Any] | None:
        try:
            value = core.parse_response(kind, frame)
        except core.NetaudioCoreError:
            return None
        return value if isinstance(value, dict) else None

    def _next_wrapper_id(self) -> int:
        self.wrapper_id = (self.wrapper_id + 1) & 0xFFFF
        if self.wrapper_id == 0:
            self.wrapper_id = 1
        return self.wrapper_id

    def _initialize(self, credential: str, deadline: float) -> None:
        if self.initialized:
            return
        self._send(core.build_dapi_session_open())
        self._send(core.build_dapi_authentication(credential))

        domain_id = None
        while domain_id is None:
            description = self._parse("dapi_session_description", self._read_frame(deadline))
            if description is not None:
                try:
                    domain_id = bytes.fromhex(description["domain_id"])
                except (KeyError, TypeError, ValueError) as exception:
                    raise DAPISessionError("DDM returned an invalid session description") from exception
        for subscription_id in range(2, 6):
            self._send(core.build_dapi_domain_subscription(domain_id, subscription_id))
        if self.notification_socket is None or self.local_ipv4 is None:
            raise DAPISessionError("DDM Controller notification socket is not open")
        notification_port = self.notification_socket.getsockname()[1]
        self._send(
            core.build_dapi_inventory_initialization(
                domain_id,
                core.next_message_id(),
                notification_port,
                self.local_ipv4,
            )
        )
        self._send(core.build_dapi_device_inventory_subscription(domain_id))
        self.initialized = True

    def _target_selector(self, target_id: str, deadline: float) -> int:
        cached = self.target_selectors.get(target_id)
        if cached is not None:
            return cached
        while True:
            announcement = self._parse("dapi_device_announcement", self._read_frame(deadline))
            if announcement is None:
                continue
            announced_id = announcement.get("device_id")
            candidate = announcement.get("target_selector")
            if (
                isinstance(announced_id, str)
                and isinstance(candidate, int)
                and not isinstance(candidate, bool)
                and 0 <= candidate <= 65535
            ):
                self.target_selectors[announced_id] = candidate
                if announced_id == target_id:
                    return candidate

    def identify(self, credential: str, device_id: str, host_mac: bytes) -> None:
        target_id = normalize_device_id(device_id)
        deadline = time.monotonic() + self.timeout
        self._initialize(credential, deadline)
        target_selector = self._target_selector(target_id, deadline)

        self._send(
            core.build_dapi_identify(
                target_selector,
                self._next_wrapper_id(),
                core.next_message_id(),
                host_mac,
            )
        )
        while True:
            confirmation = self._parse("dapi_identify_confirmation", self._read_frame(deadline))
            if confirmation is not None and confirmation.get("device_id") == target_id:
                return

    def query_arc(self, credential: str, device_id: str, arc_packet: bytes) -> bytes:
        target_id = normalize_device_id(device_id)
        if len(arc_packet) < 10:
            raise ValueError("ARC request packet is too short")
        expected_transaction_id = int.from_bytes(arc_packet[4:6], "big")
        expected_opcode = int.from_bytes(arc_packet[6:8], "big")
        deadline = time.monotonic() + self.timeout
        self._initialize(credential, deadline)
        target_selector = self._target_selector(target_id, deadline)
        wrapper_id = self._next_wrapper_id()
        self._send(core.build_dapi_arc_request(target_selector, wrapper_id, arc_packet))

        while True:
            response = self._parse("dapi_arc_response", self._read_frame(deadline))
            if response is None or response.get("wrapper_id") != wrapper_id:
                continue
            if response.get("transaction_id") != expected_transaction_id or response.get("opcode") != expected_opcode:
                raise DAPISessionError("DDM returned a mismatched managed ARC response")
            packet_hex = response.get("packet_hex")
            if not isinstance(packet_hex, str):
                raise DAPISessionError("DDM returned an invalid managed ARC response")
            try:
                return bytes.fromhex(packet_hex)
            except ValueError as exception:
                raise DAPISessionError("DDM returned an invalid managed ARC response") from exception

    def query_settings(
        self,
        credential: str,
        device_id: str,
        settings_packet: bytes,
        expected_response_opcode: int,
    ) -> bytes:
        target_id = normalize_device_id(device_id)
        if (
            isinstance(expected_response_opcode, bool)
            or not isinstance(expected_response_opcode, int)
            or not 0 <= expected_response_opcode <= 0xFFFF
        ):
            raise ValueError("expected settings response opcode must fit in 16 bits")
        deadline = time.monotonic() + self.timeout
        self._initialize(credential, deadline)
        target_selector = self._target_selector(target_id, deadline)
        wrapper_id = self._next_wrapper_id()
        self._send(core.build_dapi_settings_request(target_selector, wrapper_id, settings_packet))

        acknowledged = False
        matching_packet: bytes | None = None
        while not acknowledged or matching_packet is None:
            frame = self._read_frame(deadline)
            acknowledgement = self._parse("dapi_settings_acknowledgement", frame)
            if acknowledgement is not None and acknowledgement.get("wrapper_id") == wrapper_id:
                acknowledged = True
            publication = self._parse("dapi_settings_publication", frame)
            if (
                publication is not None
                and publication.get("device_id") == target_id
                and publication.get("opcode") == expected_response_opcode
            ):
                packet_hex = publication.get("packet_hex")
                if not isinstance(packet_hex, str):
                    raise DAPISessionError("DDM returned an invalid managed settings publication")
                try:
                    matching_packet = bytes.fromhex(packet_hex)
                except ValueError as exception:
                    raise DAPISessionError("DDM returned an invalid managed settings publication") from exception
        return matching_packet


def identify_managed_device(
    server: str,
    username: str,
    password: str,
    device_id: str,
    host_mac: bytes,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ca_file: Path | None = None,
    insecure_tls: bool = False,
) -> None:
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
        ca_file=ca_file,
        insecure_tls=insecure_tls,
    )
    login = api.login(username, password)
    with DAPISession(
        api.server,
        login.endpoints.service_port,
        api.ssl_context,
        timeout=timeout,
    ) as session:
        session.identify(login.auth_token, device_id, host_mac)


def identify_managed_device_with_api_key(
    server: str,
    api_key: str,
    device_id: str,
    host_mac: bytes,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ca_file: Path | None = None,
    insecure_tls: bool = False,
) -> None:
    api_key = _validate_api_key(api_key)
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
        ca_file=ca_file,
        insecure_tls=insecure_tls,
    )
    if "v2" not in api.versions():
        raise ControllerServiceError("DDM does not advertise the observed v2 Controller API")
    endpoints = api.endpoints()
    with DAPISession(
        api.server,
        endpoints.service_port,
        api.ssl_context,
        timeout=timeout,
    ) as session:
        session.identify(api_key, device_id, host_mac)


def query_managed_arc(
    server: str,
    username: str,
    password: str,
    device_id: str,
    arc_packet: bytes,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ca_file: Path | None = None,
    insecure_tls: bool = False,
) -> bytes:
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
        ca_file=ca_file,
        insecure_tls=insecure_tls,
    )
    login = api.login(username, password)
    with DAPISession(
        api.server,
        login.endpoints.service_port,
        api.ssl_context,
        timeout=timeout,
    ) as session:
        return session.query_arc(login.auth_token, device_id, arc_packet)


def query_managed_arc_with_api_key(
    server: str,
    api_key: str,
    device_id: str,
    arc_packet: bytes,
    *,
    auth_port: int = DEFAULT_AUTH_PORT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ca_file: Path | None = None,
    insecure_tls: bool = False,
) -> bytes:
    api_key = _validate_api_key(api_key)
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
        ca_file=ca_file,
        insecure_tls=insecure_tls,
    )
    if "v2" not in api.versions():
        raise ControllerServiceError("DDM does not advertise the observed v2 Controller API")
    endpoints = api.endpoints()
    with DAPISession(
        api.server,
        endpoints.service_port,
        api.ssl_context,
        timeout=timeout,
    ) as session:
        return session.query_arc(api_key, device_id, arc_packet)


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
    ca_file: Path | None = None,
    insecure_tls: bool = False,
) -> bytes:
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
        ca_file=ca_file,
        insecure_tls=insecure_tls,
    )
    login = api.login(username, password)
    with DAPISession(
        api.server,
        login.endpoints.service_port,
        api.ssl_context,
        timeout=timeout,
    ) as session:
        return session.query_settings(
            login.auth_token,
            device_id,
            settings_packet,
            expected_response_opcode,
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
    ca_file: Path | None = None,
    insecure_tls: bool = False,
) -> bytes:
    api_key = _validate_api_key(api_key)
    api = ControllerAPIClient(
        server,
        port=auth_port,
        timeout=timeout,
        ca_file=ca_file,
        insecure_tls=insecure_tls,
    )
    if "v2" not in api.versions():
        raise ControllerServiceError("DDM does not advertise the observed v2 Controller API")
    endpoints = api.endpoints()
    with DAPISession(
        api.server,
        endpoints.service_port,
        api.ssl_context,
        timeout=timeout,
    ) as session:
        return session.query_settings(
            api_key,
            device_id,
            settings_packet,
            expected_response_opcode,
        )
