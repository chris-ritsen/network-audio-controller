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

    assert login["auth_token"] == "x" * 43
    assert login["endpoints"]["service_port"] == 8001
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


@pytest.mark.parametrize("versions", [b'["v3"]', b"[]", b'["v2", true]', b'{"version":"v2"}', b"invalid"])
def test_endpoint_bootstrap_rejects_unknown_or_malformed_versions_before_contacting_endpoints(monkeypatch, versions):
    client = controller.ControllerAPIClient("ddm.example")
    paths = []

    def request(method, path, **kwargs):
        paths.append(path)
        return versions

    monkeypatch.setattr(client, "_request", request)

    with pytest.raises(controller.ControllerServiceError):
        client.endpoints()

    assert paths == ["/dapi"]


def test_controller_login_rejects_changed_advertised_endpoints(monkeypatch):
    client = controller.ControllerAPIClient("ddm.example")
    responses = iter(
        [
            b'["v2"]',
            b'{"servicePort":8001,"devicePort":8000,"graphQl":"http://ddm/graphql"}',
            json.dumps(
                {"authToken": "x" * 43, "servicePort": 8002, "devicePort": 8000, "graphQl": "http://ddm/graphql"}
            ).encode(),
        ]
    )
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: next(responses))

    with pytest.raises(controller.ControllerServiceError, match="changed"):
        client.login("operator", "private")


@pytest.mark.parametrize("key", ["x" * 36, "z" * 8 + "-0000-4000-8000-000000000000", "0" * 36])
def test_authentication_codec_rejects_the_same_invalid_api_keys_as_controller(key):
    with pytest.raises(controller.ControllerAuthenticationError):
        controller._validate_api_key(key)

    with pytest.raises(controller.core.NetaudioCoreError):
        controller.core.build_dapi_authentication(key)


@pytest.mark.parametrize("token", ["x" * 36, "é" * 43, "x" * 42, "x" * 44])
def test_native_controller_token_validation_does_not_accept_other_credential_shapes(token):
    with pytest.raises(controller.core.NetaudioCoreError):
        controller.core.validate_managed_credential({"kind": "controller_token", "value": token})


def _response(payload: bytes) -> bytes:
    return bytes.fromhex("b91a372500000002") + len(payload).to_bytes(4, "big") + payload


class FakeSocket:
    def __init__(self, incoming: bytes, chunk_size=None):
        self.incoming = bytearray(incoming)
        self.chunk_size = chunk_size
        self.sent = []
        self.timeouts = []
        self.closed = False

    def sendall(self, data):
        self.sent.append(data)

    def recv(self, length):
        if self.chunk_size is not None:
            length = min(length, self.chunk_size)
        result = bytes(self.incoming[:length])
        del self.incoming[:length]
        return result

    def settimeout(self, timeout):
        self.timeouts.append(timeout)

    def getsockname(self):
        return ("127.0.0.1", 12345)

    def close(self):
        self.closed = True


def _session_description():
    # Synthetic session using the envelope exercised by the native codec tests.
    frame = bytearray(160)
    frame[:12] = bytes.fromhex("b91a37250000000300000094")
    for offset, value in ((12, 0x18), (36, 0x7C), (42, 12), (44, 3), (46, 20), (64, 0x7FF)):
        frame[offset : offset + 2] = value.to_bytes(2, "big")
    return bytes(frame)


def _device_announcement(target_selector):
    payload = bytearray(78)
    payload[:8] = bytes.fromhex("0018000110000208")
    payload[26:34] = bytes.fromhex("200b00040018000c")
    payload[52:54] = target_selector.to_bytes(2, "big")
    payload.extend(b"_netaudio-cmc._udp\x00id=001dc1fffe50692e")
    payload[24:26] = (len(payload) - 24).to_bytes(2, "big")
    return _response(payload)


@pytest.mark.parametrize("target_selector", (0, 2))
@pytest.mark.parametrize("chunk_size", (None, 3))
@pytest.mark.parametrize("matching_confirmation", (False, True))
@pytest.mark.parametrize("announcement_first", (False, True))
def test_dapi_session_authenticates_maps_the_device_and_confirms_identify(
    target_selector, chunk_size, matching_confirmation, announcement_first
):
    announcement = _device_announcement(target_selector)
    confirmation = _settings_publication().replace(bytes.fromhex("07381007"), bytes.fromhex("07380062"))
    wrong_device = confirmation.replace(bytes.fromhex("001dc1fffe50692e"), bytes.fromhex("001dc1fffe50692f"))
    startup = announcement + _session_description() if announcement_first else _session_description() + announcement
    incoming = startup + wrong_device
    if matching_confirmation:
        incoming += confirmation
    fake_socket = FakeSocket(incoming, chunk_size)
    context = ssl.create_default_context()

    def connector(server, port, ssl_context, timeout):
        return fake_socket

    with controller.DAPISession("ddm.example", 8001, context, connector=connector) as session:
        if matching_confirmation:
            session.identify("x" * 43, "001dc1fffe50692e:0", bytes.fromhex("842f5774e86d"))
        else:
            with pytest.raises(controller.DAPISessionError, match="closed"):
                session.identify("x" * 43, "001dc1fffe50692e:0", bytes.fromhex("842f5774e86d"))

    assert fake_socket.sent[:2] == [
        bytes.fromhex("b91a3726000000050000000400000000"),
        bytes.fromhex("b91a3726000000010000002b") + b"x" * 43,
    ]
    acknowledgement_index = 2 if announcement_first else 3
    assert fake_socket.sent[acknowledgement_index] == controller.core.build_dapi_service_acknowledgement(announcement)
    assert len(fake_socket.sent) == 5
    request = controller.core.parse_response("dapi_settings_request", fake_socket.sent[-1])
    assert request["target_selector"] == target_selector
    assert request["wrapper_id"] == 6
    assert request["opcode"] == 0x63
    assert int.from_bytes(bytes.fromhex(request["packet_hex"])[4:6], "big") != 0
    assert fake_socket.closed is True


@pytest.mark.parametrize("previous,expected", [(0, 6), (6, 7), (65534, 65535), (65535, 1)])
def test_managed_wrapper_sequence_reserves_initialization_ids_and_skips_zero(previous, expected):
    assert controller.core.next_dapi_wrapper_id(previous) == expected


@pytest.mark.parametrize("previous", [-1, 65536, True, None, "6"])
def test_managed_wrapper_sequence_rejects_values_that_ctypes_would_coerce(previous):
    with pytest.raises(ValueError):
        controller.core.next_dapi_wrapper_id(previous)


def _managed_arc_response(*, wrapper_id=6, protocol=0x2809, transaction=114, opcode=0x1000):
    # Synthetic response using the layout exercised by the codec fixture below.
    packet = b"".join(value.to_bytes(2, "big") for value in (protocol, 10, transaction, opcode, 1))
    payload = (
        bytes.fromhex("0018000110000308084001060015000908410100000000050020200400040008")
        + wrapper_id.to_bytes(2, "big")
        + bytes.fromhex("0000000a001400000000")
        + packet
        + bytes(2)
    )
    return _response(payload)


def test_dapi_session_correlates_a_managed_arc_response():
    incoming = _managed_arc_response(wrapper_id=99) + _managed_arc_response(protocol=0x2801) + _managed_arc_response()
    fake_socket = FakeSocket(_session_description() + _device_announcement(0) + incoming)
    session = controller.DAPISession(
        "ddm.example", 8001, ssl.create_default_context(), connector=lambda *args: fake_socket
    )
    request = bytes.fromhex("2809000a007210000000")

    with session:
        response = session.query_arc("x" * 43, "001dc1fffe50692e:0", request)

    assert response == bytes.fromhex("2809000a007210000001")
    assert fake_socket.sent[-1] == controller.core.build_dapi_arc_request(0, 6, request)


@pytest.mark.parametrize("changed", [{"transaction": 115}, {"opcode": 0x1001}])
def test_dapi_session_rejects_a_mismatched_inner_arc_response(changed):
    session = controller.DAPISession(
        "ddm.example",
        8001,
        ssl.create_default_context(),
        connector=lambda *args: FakeSocket(
            _session_description() + _device_announcement(0) + _managed_arc_response(**changed)
        ),
    )

    with session, pytest.raises(controller.DAPISessionError, match="mismatched"):
        session.query_arc("x" * 43, "001dc1fffe50692e", bytes.fromhex("2809000a007210000000"))


def _settings_acknowledgement(wrapper_id=6):
    return _response(
        bytes.fromhex("0018000110000308084001060015000a0841010000000004000c200200040000")
        + wrapper_id.to_bytes(2, "big")
        + bytes(2)
    )


def _settings_publication():
    return bytes.fromhex(
        "b91a3725000000020000006400180001100003080841ffffffff00000840010600150010"
        "004c20030004000c000000000024002800020018000000006176696f2d696e7075742d32"
        "002c0003ffff00241c400000001dc1fffe50692e417564696e6174650738100700000000"
        "00000000"
    )


@pytest.mark.parametrize("missing", ["packet_hex", "response_opcode"])
def test_managed_settings_state_requires_complete_snapshot(missing):
    request = {
        "device_id": "001dc1fffe50692e",
        "wrapper_id": 6,
        "response_opcode": 0x1007,
        "frame": list(_settings_acknowledgement()),
    }
    result = controller.core.advance_managed_settings(request)
    state = result["state"]
    del state[missing]

    with pytest.raises(controller.core.NetaudioCoreError):
        controller.core.advance_managed_settings({**request, "state": state})


@pytest.mark.parametrize("publication_first", [False, True])
@pytest.mark.parametrize("omitted", [None, "acknowledgement", "publication"])
def test_dapi_settings_query_requires_both_publication_and_transport_ack(publication_first, omitted):
    publication = _settings_publication()
    wrong_target = publication.replace(bytes.fromhex("001dc1fffe50692e"), bytes.fromhex("001dc1fffe50692f"))
    wrong_opcode = publication.replace(bytes.fromhex("07381007"), bytes.fromhex("07381008"))
    events = [("acknowledgement", _settings_acknowledgement()), ("publication", publication)]

    if publication_first:
        events.reverse()

    incoming = _settings_acknowledgement(99) + wrong_target + wrong_opcode
    incoming += b"".join(frame for name, frame in events if name != omitted)
    fake_socket = FakeSocket(_session_description() + _device_announcement(0) + incoming)
    session = controller.DAPISession(
        "ddm.example", 8001, ssl.create_default_context(), connector=lambda *args: fake_socket
    )
    request = bytes.fromhex("ffff0024002d7e3f842f5774e86d0000417564696e617465073a10060000006400000000")

    with session:
        if omitted is not None:
            with pytest.raises(controller.DAPISessionError, match="closed"):
                session.query_settings("x" * 43, "001dc1fffe50692e", request, 0x1007)
        else:
            response = session.query_settings("x" * 43, "001dc1fffe50692e", request, 0x1007)
            assert response.hex() == "ffff00241c400000001dc1fffe50692e417564696e617465073810070000000000000000"

    assert fake_socket.sent[-1] == controller.core.build_dapi_settings_request(0, 6, request)


@pytest.mark.parametrize("operation", ["identify", "reboot"])
def test_api_key_device_operation_skips_password_login(monkeypatch, operation):
    captured = {}

    class API:
        server = "ddm.example"
        ssl_context = ssl.create_default_context()

        def __init__(self, server, **options):
            captured.update(api_server=server, api_options=options)

        def endpoints(self):
            return {"service_port": 8001, "device_port": 8000, "graphql_url": "http://ddm.example/graphql"}

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

        reboot = identify

    monkeypatch.setattr(controller, "ControllerAPIClient", API)
    monkeypatch.setattr(controller, "DAPISession", Session)
    api_key = "00000000-0000-4000-8000-000000000000"

    getattr(controller, f"{operation}_managed_device_with_api_key")(
        "ddm.example",
        api_key,
        "001dc1fffe507b8d:0",
        bytes.fromhex("842f5774e86d"),
    )

    assert captured["credential"] == api_key
    assert captured["device_id"] == "001dc1fffe507b8d:0"
    assert captured["session_port"] == 8001
    assert captured["expected_domain_id"] is None


@pytest.mark.parametrize("matching_ack", [True, False])
def test_managed_reboot_requires_correlated_ack_without_retry(matching_ack):
    incoming = _settings_acknowledgement(5) + (_settings_acknowledgement() if matching_ack else b"")
    fake_socket = FakeSocket(_session_description() + _device_announcement(2) + incoming)
    session = controller.DAPISession(
        "ddm.example", 8001, ssl.create_default_context(), connector=lambda *args: fake_socket
    )

    with session:
        if matching_ack:
            session.reboot("x" * 43, "001dc1fffe50692e:0", b"\x01\x02\x03\x04\x05\x06", "00" * 16)
        else:
            with pytest.raises(controller.DAPISessionError, match="closed"):
                session.reboot("x" * 43, "001dc1fffe50692e:0", b"\x01\x02\x03\x04\x05\x06", "00" * 16)

    assert len(fake_socket.sent) == 5
    request = controller.core.parse_response("dapi_settings_request", fake_socket.sent[-1])
    assert request["target_selector"] == 2
    assert request["wrapper_id"] == 6
    packet = bytes.fromhex(request["packet_hex"])
    message_id = int.from_bytes(packet[4:6], "big")
    assert packet == controller.core.build_command(
        {"command": "reboot", "host_mac": "010203040506", "message_id": message_id}
    )


def test_managed_reboot_rejects_an_unobserved_controller_version(monkeypatch):
    def request(self, method, path, **kwargs):
        assert path == "/dapi"
        return b'["v3"]'

    monkeypatch.setattr(controller.ControllerAPIClient, "_request", request)
    with pytest.raises(controller.ControllerServiceError, match="observed v2"):
        controller.reboot_managed_device_with_api_key(
            "ddm.example", "00000000-0000-4000-8000-000000000000", "001dc1fffe50692e", bytes(6)
        )


def test_dapi_session_rejects_an_authenticated_domain_other_than_the_selected_context():
    fake_socket = FakeSocket(_session_description(), chunk_size=3)
    with controller.DAPISession(
        "ddm.example", 8001, ssl.create_default_context(), connector=lambda *args: fake_socket
    ) as session:
        with pytest.raises(controller.DAPISessionError, match="selected domain.*expected"):
            session.identify("x" * 43, "001dc1fffe50692e", bytes(6), "11" * 16)

    assert len(fake_socket.sent) == 2
    assert fake_socket.closed


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


@pytest.mark.parametrize(
    "header",
    [b"unsupported!", bytes.fromhex("b91a37250000000200100001"), bytes.fromhex("b91a37260000000200000007")],
)
def test_dapi_session_rejects_invalid_frame_header_without_reading_payload(header):
    fake_socket = FakeSocket(header + b"ignored")
    session = controller.DAPISession(
        "ddm.example",
        8001,
        ssl.create_default_context(),
        connector=lambda *args: fake_socket,
    )
    with session, pytest.raises(controller.DAPISessionError, match="frame"):
        session.identify("x" * 43, "001dc1fffe50692e", bytes(6))

    assert fake_socket.incoming == b"ignored"


@pytest.mark.parametrize(
    "packet",
    [
        b"",
        bytes.fromhex("2809000a007210000001"),
        bytes.fromhex("1234000a007210000000"),
        bytes.fromhex("2809000b007210000000"),
    ],
)
def test_dapi_session_rejects_invalid_arc_request_before_initialization(packet):
    fake_socket = FakeSocket(b"")

    with controller.DAPISession(
        "ddm.example", 8001, ssl.create_default_context(), connector=lambda *args: fake_socket
    ) as session:
        with pytest.raises((ValueError, controller.DAPISessionError), match="ARC request"):
            session.query_arc("x" * 43, "001dc1fffe50692e", packet)

    assert fake_socket.sent == []


def test_managed_session_reuses_initialization_and_native_wrapper_sequence():
    fake_socket = FakeSocket(
        _device_announcement(2) + _session_description() + _managed_arc_response() + _managed_arc_response(wrapper_id=7)
    )
    packet = bytes.fromhex("2809000a007210000000")

    with controller.DAPISession(
        "ddm.example", 8001, ssl.create_default_context(), connector=lambda *args: fake_socket
    ) as session:
        first = session.query_arc("x" * 43, "001dc1fffe50692e", packet, "00" * 16)
        second = session.query_arc("x" * 43, "001dc1fffe50692e", packet, "00" * 16)

        with pytest.raises(controller.DAPISessionError, match="selected domain.*expected"):
            session.query_arc("x" * 43, "001dc1fffe50692e", packet, "11" * 16)

    assert first == second == bytes.fromhex("2809000a007210000001")
    assert len(fake_socket.sent) == 6
    assert fake_socket.sent[-2:] == [controller.core.build_dapi_arc_request(2, wrapper, packet) for wrapper in (6, 7)]


@pytest.mark.parametrize("opcode", [-1, 65536, True, "4096"])
def test_managed_session_rejects_invalid_settings_operation_before_sending(opcode):
    fake_socket = FakeSocket(b"")
    packet = bytes.fromhex("ffff0024002d7e3f842f5774e86d0000417564696e617465073a10060000006400000000")

    with controller.DAPISession(
        "ddm.example", 8001, ssl.create_default_context(), connector=lambda *args: fake_socket
    ) as session:
        with pytest.raises((ValueError, controller.DAPISessionError)):
            session.query_settings("x" * 43, "001dc1fffe50692e", packet, opcode)

    assert fake_socket.sent == []


def test_native_managed_session_requires_initialization_and_bounds_received_chunks():
    result = controller.core.advance_managed_session(
        {
            "action": "begin",
            "state": None,
            "credential": "x" * 43,
            "notification_port": 12345,
            "local_ipv4": "127.0.0.1",
            "expected_domain_id": None,
            "operation": {"kind": "identify", "device_id": "001dc1fffe50692e", "host_mac": [0] * 6},
        }
    )
    assert result["receive_bytes"] == 12
    assert not result["complete"]

    with pytest.raises(controller.core.NetaudioCoreError):
        controller.core.advance_managed_session({"action": "receive", "state": result["state"], "data": [0] * 13})

    with pytest.raises(controller.core.NetaudioCoreError, match="in progress"):
        controller.core.advance_managed_session(
            {
                "action": "begin",
                "state": result["state"],
                "credential": "x" * 43,
                "notification_port": 12345,
                "local_ipv4": "127.0.0.1",
                "expected_domain_id": None,
                "operation": {"kind": "identify", "device_id": "001dc1fffe50692e", "host_mac": [0] * 6},
            }
        )


def test_core_managed_arc_and_settings_codecs_use_the_capture_backed_layouts():
    arc_packet = controller.core.build_command(
        {"command": "channel_count", "protocol_id": 0x2809, "message_id": 0x0072}
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
