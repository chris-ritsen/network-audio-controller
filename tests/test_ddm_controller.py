import json
import ssl

import pytest

from netaudio.ddm import controller


def test_controller_login_negotiates_v2_and_uses_form_data(monkeypatch):
    requests = []

    class Client(controller.ControllerAPIClient):
        def _request(self, method, path, *, body=None, headers=None):
            requests.append((method, path, body, headers))
            if path == "/dapi":
                return json.dumps(["v2", "v1"]).encode()
            if path == "/dapi/v2/endpoints":
                return json.dumps(
                    {"servicePort": 8001, "devicePort": 8000, "graphQl": "http://ddm.example/graphql"}
                ).encode()
            return json.dumps(
                {
                    "authToken": "x" * 43,
                    "servicePort": 8001,
                    "devicePort": 8000,
                    "graphQl": "http://ddm.example/graphql",
                }
            ).encode()

    login = Client("ddm.example").login("operator name", "private&value")

    assert login.auth_token == "x" * 43
    assert login.endpoints.service_port == 8001
    assert requests == [
        ("GET", "/dapi", None, {"Accept": "application/json"}),
        ("GET", "/dapi/v2/endpoints", None, {"Accept": "application/json"}),
        (
            "POST",
            "/dapi/v2/login",
            b"username=operator+name&password=private%26value",
            {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
        ),
    ]


def test_controller_login_fails_closed_on_an_unobserved_token_shape(monkeypatch):
    client = controller.ControllerAPIClient("ddm.example")
    responses = iter(
        [
            b'["v2"]',
            b'{"servicePort":8001,"devicePort":8000,"graphQl":"http://ddm/graphql"}',
            b'{"authToken":"short","servicePort":8001,"devicePort":8000,"graphQl":"http://ddm/graphql"}',
        ]
    )
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: next(responses))

    with pytest.raises(controller.ControllerAuthenticationError, match="unsupported"):
        client.login("operator", "private")


def _response(payload: bytes) -> bytes:
    return controller.DAPI_RESPONSE_MARKER + (2).to_bytes(4, "big") + len(payload).to_bytes(4, "big") + payload


class FakeSocket:
    def __init__(self, incoming: bytes):
        self.incoming = bytearray(incoming)
        self.sent = []
        self.timeouts = []
        self.closed = False

    def sendall(self, data):
        self.sent.append(data)

    def recv(self, length):
        result = bytes(self.incoming[:length])
        del self.incoming[:length]
        return result

    def settimeout(self, timeout):
        self.timeouts.append(timeout)

    def getsockname(self):
        return ("127.0.0.1", 12345)

    def close(self):
        self.closed = True


@pytest.mark.parametrize("target_selector", (0, 2))
def test_dapi_session_authenticates_maps_the_device_and_confirms_identify(monkeypatch, target_selector):
    incoming = b"".join((_response(b"session"), _response(b"announcement"), _response(b"confirmation")))
    fake_socket = FakeSocket(incoming)
    built = []

    monkeypatch.setattr(controller.core, "build_dapi_session_open", lambda: b"open")
    monkeypatch.setattr(controller.core, "build_dapi_authentication", lambda token: b"auth:" + token.encode())

    def subscription(domain_id, subscription_id):
        frame = b"subscribe:" + domain_id + bytes([subscription_id])
        built.append(frame)
        return frame

    monkeypatch.setattr(controller.core, "build_dapi_domain_subscription", subscription)
    monkeypatch.setattr(
        controller.core,
        "build_dapi_device_inventory_subscription",
        lambda domain_id: b"device-inventory:" + domain_id,
    )
    monkeypatch.setattr(
        controller.core,
        "build_dapi_inventory_initialization",
        lambda domain_id, first_message_id, notification_port, local_ipv4: (
            b"inventory:" + domain_id + bytes([first_message_id]) + notification_port.to_bytes(2, "big") + local_ipv4
        ),
    )
    message_ids = iter((7, 8))
    monkeypatch.setattr(controller.core, "next_message_id", lambda: next(message_ids))
    monkeypatch.setattr(
        controller.core,
        "build_dapi_service_acknowledgement",
        lambda frame: b"ack:" + frame[-12:],
    )
    monkeypatch.setattr(
        controller.core,
        "build_dapi_identify",
        lambda target_selector, wrapper_id, message_id, mac: (
            f"identify:{target_selector}:{wrapper_id}:{message_id}:".encode() + mac
        ),
    )

    def parse(kind, frame):
        if kind == "dapi_session_description" and frame.endswith(b"session"):
            return {"domain_id": "00" * 16}
        if kind == "dapi_device_announcement" and frame.endswith(b"announcement"):
            return {"device_id": "001dc1fffe507b8d", "target_selector": target_selector}
        if kind == "dapi_service_announcement" and frame.endswith(b"announcement"):
            return {"message_id": 12}
        if kind == "dapi_identify_confirmation" and frame.endswith(b"confirmation"):
            return {"device_id": "001dc1fffe507b8d"}
        return None

    monkeypatch.setattr(controller.core, "parse_response", parse)
    context = ssl.create_default_context()

    def connector(server, port, ssl_context, timeout):
        return fake_socket

    with controller.DAPISession("ddm.example", 8001, context, connector=connector) as session:
        session.identify("x" * 43, "001dc1fffe507b8d:0", bytes.fromhex("842f5774e86d"))

    assert fake_socket.sent[:2] == [b"open", b"auth:" + b"x" * 43]
    assert fake_socket.sent[2:6] == built
    assert fake_socket.sent[6].startswith(b"inventory:" + b"\x00" * 16 + b"\x07")
    assert int.from_bytes(fake_socket.sent[6][-6:-4], "big") > 0
    assert fake_socket.sent[6][-4:] == bytes([127, 0, 0, 1])
    assert fake_socket.sent[7] == b"device-inventory:" + b"\x00" * 16
    assert fake_socket.sent[8] == b"ack:announcement"
    assert fake_socket.sent[-1] == f"identify:{target_selector}:6:8:".encode() + bytes.fromhex("842f5774e86d")
    assert fake_socket.closed is True


def test_dapi_session_correlates_a_managed_arc_response(monkeypatch):
    incoming = b"".join(
        (
            _response(b"session"),
            _response(b"announcement"),
            _response(b"unrelated-arc"),
            _response(b"matched-arc"),
        )
    )
    fake_socket = FakeSocket(incoming)
    monkeypatch.setattr(controller.core, "build_dapi_session_open", lambda: b"open")
    monkeypatch.setattr(controller.core, "build_dapi_authentication", lambda token: b"auth:" + token.encode())
    monkeypatch.setattr(
        controller.core, "build_dapi_domain_subscription", lambda domain_id, index: b"sub" + bytes([index])
    )
    monkeypatch.setattr(controller.core, "build_dapi_device_inventory_subscription", lambda domain_id: b"devices")
    monkeypatch.setattr(
        controller.core,
        "build_dapi_inventory_initialization",
        lambda domain_id, first_message_id, notification_port, local_ipv4: b"inventory",
    )
    monkeypatch.setattr(controller.core, "build_dapi_service_acknowledgement", lambda frame: b"announcement-ack")
    monkeypatch.setattr(controller.core, "next_message_id", lambda: 7)
    monkeypatch.setattr(
        controller.core,
        "build_dapi_arc_request",
        lambda target_selector, wrapper_id, packet: f"arc:{target_selector}:{wrapper_id}:".encode() + packet,
    )

    def parse(kind, frame):
        if kind == "dapi_session_description" and frame.endswith(b"session"):
            return {"domain_id": "00" * 16}
        if kind == "dapi_device_announcement" and frame.endswith(b"announcement"):
            return {"device_id": "001dc1fffe50692e", "target_selector": 0}
        if kind == "dapi_service_announcement" and frame.endswith(b"announcement"):
            return {"message_id": 12}
        if kind == "dapi_arc_response" and frame.endswith(b"unrelated-arc"):
            return {
                "wrapper_id": 99,
                "transaction_id": 114,
                "opcode": 0x1000,
                "packet_hex": "2809000a007210000001",
            }
        if kind == "dapi_arc_response" and frame.endswith(b"matched-arc"):
            return {
                "wrapper_id": 6,
                "transaction_id": 114,
                "opcode": 0x1000,
                "packet_hex": "2809000a007210000001",
            }
        return None

    monkeypatch.setattr(controller.core, "parse_response", parse)
    request = bytes.fromhex("2809000a007210000000")
    with controller.DAPISession(
        "ddm.example",
        8001,
        ssl.create_default_context(),
        connector=lambda *args: fake_socket,
    ) as session:
        response = session.query_arc("x" * 43, "001dc1fffe50692e:0", request)

    assert response == bytes.fromhex("2809000a007210000001")
    assert fake_socket.sent[-1] == b"arc:0:6:" + request


def test_dapi_session_rejects_a_mismatched_inner_arc_response(monkeypatch):
    session = controller.DAPISession(
        "ddm.example",
        8001,
        ssl.create_default_context(),
        connector=lambda *args: FakeSocket(b""),
    )
    session.initialized = True
    session.target_selectors["001dc1fffe50692e"] = 0
    monkeypatch.setattr(controller.core, "build_dapi_arc_request", lambda *args: b"arc")
    monkeypatch.setattr(
        session,
        "_read_frame",
        lambda deadline: _response(b"mismatched"),
    )
    monkeypatch.setattr(
        controller.core,
        "parse_response",
        lambda kind, frame: {
            "wrapper_id": 6,
            "transaction_id": 115,
            "opcode": 0x1000,
            "packet_hex": "2809000a007310000001",
        },
    )

    with session, pytest.raises(controller.DAPISessionError, match="mismatched"):
        session.query_arc(
            "x" * 43,
            "001dc1fffe50692e",
            bytes.fromhex("2809000a007210000000"),
        )


def test_dapi_settings_query_requires_both_publication_and_transport_ack(monkeypatch):
    fake_socket = FakeSocket(b"".join((_response(b"publication"), _response(b"ack"))))
    session = controller.DAPISession(
        "ddm.example",
        8001,
        ssl.create_default_context(),
        connector=lambda *args: fake_socket,
    )
    session.initialized = True
    session.target_selectors["001dc1fffe50692e"] = 0
    monkeypatch.setattr(
        controller.core,
        "build_dapi_settings_request",
        lambda target_selector, wrapper_id, packet: f"settings:{target_selector}:{wrapper_id}:".encode() + packet,
    )

    def parse(kind, frame):
        if kind == "dapi_settings_publication" and frame.endswith(b"publication"):
            return {
                "device_id": "001dc1fffe50692e",
                "opcode": 0x1007,
                "packet_hex": "ffff001c00010000001dc1fffe50692e417564696e61746507381007",
            }
        if kind == "dapi_settings_acknowledgement" and frame.endswith(b"ack"):
            return {"wrapper_id": 6}
        return None

    monkeypatch.setattr(controller.core, "parse_response", parse)
    with session:
        response = session.query_settings(
            "x" * 43,
            "001dc1fffe50692e",
            b"settings-request",
            0x1007,
        )

    assert response == bytes.fromhex("ffff001c00010000001dc1fffe50692e417564696e61746507381007")
    assert fake_socket.sent == [b"settings:0:6:settings-request"]


def test_api_key_identify_skips_password_login(monkeypatch):
    captured = {}

    class API:
        server = "ddm.example"
        ssl_context = ssl.create_default_context()

        def __init__(self, server, **options):
            captured.update(api_server=server, api_options=options)

        def versions(self):
            return ("v2",)

        def endpoints(self):
            return controller.ControllerEndpoints(8001, 8000, "http://ddm.example/graphql")

        def login(self, *args):
            raise AssertionError("API-key authentication must not use password login")

    class Session:
        def __init__(self, server, port, context, **options):
            captured.update(session_server=server, session_port=port, session_options=options)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def identify(self, credential, device_id, host_mac, expected_domain_id=None):
            captured.update(
                credential=credential,
                device_id=device_id,
                host_mac=host_mac,
                expected_domain_id=expected_domain_id,
            )

    monkeypatch.setattr(controller, "ControllerAPIClient", API)
    monkeypatch.setattr(controller, "DAPISession", Session)
    api_key = "00000000-0000-4000-8000-000000000000"

    controller.identify_managed_device_with_api_key(
        "ddm.example",
        api_key,
        "001dc1fffe507b8d:0",
        bytes.fromhex("842f5774e86d"),
    )

    assert captured["credential"] == api_key
    assert captured["device_id"] == "001dc1fffe507b8d:0"
    assert captured["session_port"] == 8001
    assert captured["expected_domain_id"] is None


def test_dapi_session_rejects_an_authenticated_domain_other_than_the_selected_context(monkeypatch):
    session = controller.DAPISession("ddm.example", 8001, ssl.create_default_context())
    monkeypatch.setattr(controller.core, "build_dapi_session_open", lambda: b"open")
    monkeypatch.setattr(controller.core, "build_dapi_authentication", lambda credential: b"auth")
    monkeypatch.setattr(session, "_send", lambda frame: None)
    monkeypatch.setattr(session, "_read_frame", lambda deadline: b"session")
    monkeypatch.setattr(
        session,
        "_parse",
        lambda kind, frame: {"domain_id": "00" * 16} if kind == "dapi_session_description" else None,
    )

    with pytest.raises(controller.DAPISessionError, match="selected domain.*expected"):
        session._initialize("credential", float("inf"), "11" * 16)


@pytest.mark.parametrize(
    "api_key",
    ("short", "000000000000-4000-8000-000000000000", "zzzzzzzz-0000-4000-8000-000000000000"),
)
def test_api_key_identify_rejects_unobserved_key_shapes(api_key):
    with pytest.raises(controller.ControllerAuthenticationError, match="UUID format"):
        controller.identify_managed_device_with_api_key(
            "ddm.example",
            api_key,
            "001dc1fffe507b8d:0",
            bytes.fromhex("842f5774e86d"),
        )


@pytest.mark.parametrize(
    "value",
    ["", "001dc1", "001dc1fffe507b8z", "001dc1fffe507b8d:1"],
)
def test_managed_device_id_is_strict(value):
    with pytest.raises(ValueError):
        controller.normalize_device_id(value)


def test_dapi_session_rejects_an_unobserved_frame_marker():
    fake_socket = FakeSocket(b"unsupported!" + b"ignored")
    session = controller.DAPISession(
        "ddm.example",
        8001,
        ssl.create_default_context(),
        connector=lambda *args: fake_socket,
    )
    with session, pytest.raises(controller.DAPISessionError, match="frame marker"):
        session._read_frame(float("inf"))


def test_core_managed_arc_and_settings_codecs_use_the_capture_backed_layouts():
    arc_packet = controller.core.build_command(
        {"command": "channel_count", "protocol_id": 0x2809, "transaction_id": 0x0072}
    )
    assert arc_packet == bytes.fromhex("2809000a007210000000")
    assert controller.core.build_dapi_arc_request(0, 27, arc_packet) == bytes.fromhex(
        "b91a37260000000200000038001800011000020808400100000000000841000000000009"
        "0020200400040008001b0000000a0014000000002809000a0072100000000000"
    )
    rejected_arc_response = bytes.fromhex(
        "b91a37250000000200000038001800011000030808400106001500090841010000000005"
        "002020040004000800230000000a0014000000002809000a007a232000310000"
    )
    assert controller.core.parse_response("dapi_arc_response", rejected_arc_response) == {
        "wrapper_id": 35,
        "protocol_id": 0x2809,
        "transaction_id": 0x007A,
        "opcode": 0x2320,
        "result_code": 0x0031,
        "packet_hex": "2809000a007a23200031",
        "alignment_bytes_hex": "0000",
    }

    acknowledgement = bytes.fromhex(
        "b91a372500000002000000240018000110000308084001060015000a0841010000000004000c200200040000002d0000"
    )
    assert controller.core.parse_response("dapi_settings_acknowledgement", acknowledgement) == {"wrapper_id": 45}
    settings_packet = bytes.fromhex("ffff0024002d7e3f842f5774e86d0000417564696e617465073a10060000006400000000")
    assert controller.core.build_dapi_settings_request(0, 52, settings_packet) == bytes.fromhex(
        "b91a3726000000020000005400180001100002080840010000000000084100000000000a"
        "003c20020004000c00340000002400180000000000000000"
        "ffff0024002d7e3f842f5774e86d0000417564696e617465073a10060000006400000000"
    )
    publication = bytes.fromhex(
        "b91a3725000000020000006400180001100003080841ffffffff00000840010600150010"
        "004c20030004000c000000000024002800020018000000006176696f2d696e7075742d32"
        "002c0003ffff00241c400000001dc1fffe50692e417564696e6174650738100700000000"
        "00000000"
    )
    parsed_publication = controller.core.parse_response("dapi_settings_publication", publication)
    assert parsed_publication == {
        "wrapper_id": 0,
        "target_name": "avio-input-2",
        "device_id": "001dc1fffe50692e",
        "message_id": 0x1C40,
        "opcode": 0x1007,
        "packet_hex": "ffff00241c400000001dc1fffe50692e417564696e617465073810070000000000000000",
    }
