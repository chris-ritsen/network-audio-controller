from __future__ import annotations

import json
from collections import deque

import pytest

from netaudio.ddm import (
    AuthenticationError,
    CredentialError,
    HTTPResponse,
    HTTPStatusError,
    JSONResponseError,
    ManagedAPIClient,
    ResponseShapeError,
    ResponseTooLargeError,
    TransportError,
    authenticate_with_password,
)


CREDENTIAL = "managed-api-key"


class FakeTransport:
    def __init__(self, *responses):
        self.responses = deque(responses)
        self.requests = []

    def __call__(self, request):
        self.requests.append(request)
        response = self.responses.popleft()
        if isinstance(response, Exception):
            raise response
        return response


def _response(payload, status=200):
    return HTTPResponse(status=status, body=json.dumps(payload).encode("utf-8"))


def _client(*responses, credential=CREDENTIAL, **kwargs):
    transport = FakeTransport(*responses)
    client = ManagedAPIClient(
        "http://manager.example/graphql",
        credential=credential,
        transport=transport,
        **kwargs,
    )
    return client, transport


def test_password_login_bootstraps_without_an_authorization_header():
    transport = FakeTransport(_response({"data": {"UserLoginWithPassword": {"ok": True, "token": "session-token"}}}))

    token = authenticate_with_password(
        "https://manager.example/graphql",
        "operator",
        "private-password",
        transport=transport,
    )

    assert token == "session-token"
    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert "Authorization" not in request.headers
    assert json.loads(request.body) == {
        "operationName": "UserLoginWithPassword",
        "query": (
            "mutation UserLoginWithPassword($input: UserLoginWithPasswordInput!) "
            "{ UserLoginWithPassword(input: $input) { ok token } }"
        ),
        "variables": {"input": {"email": "operator", "password": "private-password"}},
    }


def test_password_login_rejects_http_without_an_explicit_opt_in():
    transport = FakeTransport(_response({"data": {"UserLoginWithPassword": {"ok": True, "token": "session-token"}}}))

    with pytest.raises(ValueError, match="allow_insecure_http"):
        authenticate_with_password(
            "http://manager.example/graphql",
            "operator",
            "private-password",
            transport=transport,
        )

    assert transport.requests == []


def test_password_login_can_explicitly_use_a_lab_http_endpoint():
    transport = FakeTransport(_response({"data": {"UserLoginWithPassword": {"ok": True, "token": "session-token"}}}))

    token = authenticate_with_password(
        "http://manager.example/graphql",
        "operator",
        "private-password",
        allow_insecure_http=True,
        transport=transport,
    )

    assert token == "session-token"


@pytest.mark.parametrize(
    "payload",
    [
        {"data": {"UserLoginWithPassword": {"ok": False, "token": None}}},
        {"data": None, "errors": [{"message": "invalid login"}]},
    ],
)
def test_password_login_rejection_does_not_echo_the_password(payload):
    password = "private-password"
    transport = FakeTransport(_response(payload))

    with pytest.raises(AuthenticationError) as failure:
        authenticate_with_password(
            "https://manager.example/graphql",
            "operator",
            password,
            transport=transport,
        )

    assert password not in str(failure.value)


@pytest.mark.parametrize(
    "payload",
    [
        {"data": {"UserLoginWithPassword": None}},
        {"data": {"UserLoginWithPassword": {"ok": True, "token": None}}},
        {"data": {"UserLoginWithPassword": {"ok": "yes", "token": "session-token"}}},
    ],
)
def test_password_login_validates_the_response_shape(payload):
    transport = FakeTransport(_response(payload))

    with pytest.raises(ResponseShapeError):
        authenticate_with_password(
            "https://manager.example/graphql",
            "operator",
            "private-password",
            transport=transport,
        )


def _parameter():
    return {
        "__typename": "DeviceParameterDiscrete",
        "id": "parameter-1",
        "path": "/inputs/CH1",
        "key": "sensitivity",
        "value": "0 dBV",
        "label": "Input Sensitivity",
        "settable": True,
        "defaultValue": "+24 dBu",
        "units": None,
        "applyMode": None,
        "group": None,
        "renderingHint": None,
        "options": ["+24 dBu", "0 dBV"],
    }


def _device(*, device_id="device-1", name="input", enrolled=True):
    return {
        "id": device_id,
        "name": name,
        "domainId": "domain-1" if enrolled else None,
        "type": "MANAGED",
        "enrolmentState": "ENROLLED" if enrolled else "UNENROLLED",
        "identity": {
            "id": f"identity-{device_id}",
            "instanceId": f"instance-{device_id}",
            "defaultName": "DEVICE-default",
            "actualName": name,
            "productModelId": "model-id",
            "productModelName": "Adapter",
            "productVersion": "1.2.3",
            "productSoftwareVersion": None,
            "danteVersion": "4.2.4.1",
            "danteHardwareVersion": "4.2.3.4",
        },
        "manufacturer": {"id": "manufacturer-1", "name": "Example"},
        "platform": {"id": "platform-1", "name": "Platform", "platformId": "504c4154464f524d"},
        "product": {"id": "product-1", "name": "Adapter"},
        "interfaces": [
            {
                "id": "interface-1",
                "macAddress": "00:11:22:33:44:55",
                "address": "192.0.2.10",
                "netmask": 24,
                "subnet": "192.0.2.0",
            }
        ],
        "connection": {"id": "connection-1", "state": "READY", "lastChanged": "2026-08-26T12:00:00Z"},
        "clockPreferences": {
            "id": "preferences-1",
            "externalWordClock": False,
            "leader": True,
            "unicastClocking": False,
            "v1UnicastDelayRequests": False,
        },
        "capabilities": {
            "id": "capabilities-1",
            "CAN_WRITE_PREFERRED_MASTER": True,
            "CAN_WRITE_EXT_WORD_CLOCK": False,
            "CAN_WRITE_SLAVE_ONLY": False,
            "CAN_WRITE_UNICAST_DELAY_REQUESTS": False,
            "CAN_UNICAST_CLOCKING": False,
            "DDM_V_1_1_CLOCK_MESSAGES_SUPPORTED": True,
            "CAN_ENCRYPT_MEDIA": False,
            "CAN_RESET": True,
            "RTP_AUDIO_SUPPORTED": True,
            "RTP_AUDIO_SUPPORT_SUPPRESSED": False,
            "mediaTypes": 8,
        },
        "clockingState": {
            "id": "clock-state-1",
            "locked": "LOCKED",
            "grandLeader": False,
            "followerWithoutLeader": False,
            "multicastLeader": False,
            "unicastLeader": False,
            "unicastFollower": False,
            "muteStatus": "NOT_MUTED",
            "frequencyOffset": 0,
        },
        "status": {
            "id": "status-1",
            "summary": "OK",
            "clocking": "OK",
            "connectivity": "OK",
            "latency": "OK",
            "subscriptions": "OK",
            "alertMessage": {
                "id": "alert-1",
                "connectivity": None,
                "clocking": None,
                "latency": None,
                "subscriptions": None,
            },
        },
        "rxChannels": [
            {
                "id": "rx-1",
                "index": 1,
                "enabled": True,
                "name": None,
                "subscribedDevice": "source",
                "subscribedChannel": "CH1",
                "status": "DYNAMIC",
                "statusMessage": "Active subscription",
                "summary": "CONNECTED",
                "mediaType": "AUDIO",
                "encryptionScheme": "NONE",
                "canSubscribeSelf": False,
                "signalPresence": {"id": "presence-rx-1", "leveldBFS": -61.5, "status": "MUTE"},
            }
        ],
        "txChannels": [
            {
                "id": "tx-1",
                "index": 1,
                "name": "CH1",
                "mediaType": "AUDIO",
                "encryptionPolicy": None,
                "signalPresence": {"id": "presence-tx-1", "leveldBFS": 0, "status": "CLIPPING"},
            }
        ],
        "parameters": [],
        "inputs": [{"__typename": "DeviceInput", "id": "input-1", "key": "CH1", "parameters": [_parameter()]}],
        "outputs": [],
    }


def _inventory_payload():
    enrolled = _device()
    unmanaged = _device(device_id="device-2", name="unmanaged", enrolled=False)
    unmanaged["status"] = None
    unmanaged["signalPresence"] = None
    unmanaged["parameters"] = None
    unmanaged["inputs"] = None
    unmanaged["outputs"] = None
    unmanaged["rxChannels"][0]["signalPresence"] = None
    unmanaged["txChannels"][0]["signalPresence"] = None
    return {
        "data": {
            "domains": [
                {
                    "id": "domain-1",
                    "name": "test",
                    "status": {
                        "summary": "OK",
                        "clocking": "OK",
                        "connectivity": "OK",
                        "latency": "OK",
                        "subscriptions": "OK",
                    },
                    "devices": [enrolled],
                }
            ],
            "unenrolledDevices": [unmanaged],
        }
    }


def test_inventory_is_one_fixed_read_only_request_with_raw_authorization():
    client, transport = _client(_response(_inventory_payload()))

    result = client.inventory()

    assert result.successful is True
    assert result.partial is False
    assert len(transport.requests) == 1
    request = transport.requests[0]
    body = json.loads(request.body)
    query = body["query"]
    assert request.headers["Authorization"] == CREDENTIAL
    assert request.headers["Authorization"] != f"Bearer {CREDENTIAL}"
    assert CREDENTIAL.encode() not in request.body
    assert body["operationName"] == "NetAudioManagedInventory"
    assert "domains" in query
    assert "unenrolledDevices" in query
    assert "signalPresence" in query
    assert "parameters" in query
    assert "statusMessage" in query
    assert "mutation " not in query


def test_inventory_decodes_documented_channels_presence_and_parameters():
    client, _ = _client(_response(_inventory_payload()))

    result = client.inventory()

    domain = result.data.domains[0]
    device = domain.devices[0]
    assert domain.name == "test"
    assert device.capabilities.can_reset is True
    assert device.connection.state == "READY"
    assert device.rx_channels[0].name is None
    assert device.rx_channels[0].status == "DYNAMIC"
    assert device.rx_channels[0].signal_presence.level_dbfs == -61.5
    assert device.tx_channels[0].signal_presence.status == "CLIPPING"
    parameter = device.inputs[0].parameters[0]
    assert parameter.typename == "DeviceParameterDiscrete"
    assert parameter.value == "0 dBV"
    assert parameter.options == ("+24 dBu", "0 dBV")


def test_unenrolled_records_keep_unavailable_detail_distinct_from_empty_detail():
    client, _ = _client(_response(_inventory_payload()))

    unmanaged = client.inventory().data.unenrolled_devices[0]

    assert unmanaged.status is None
    assert unmanaged.parameters is None
    assert unmanaged.inputs is None
    assert unmanaged.outputs is None
    assert unmanaged.rx_channels[0].signal_presence is None
    assert unmanaged.tx_channels[0].signal_presence is None


def test_partial_data_and_complete_graphql_errors_are_preserved():
    error = {
        "message": "unenrolled inventory unavailable",
        "path": ["unenrolledDevices"],
        "extensions": {"code": "TEMPORARY"},
    }
    client, _ = _client(_response({"data": {"domains": []}, "errors": [error]}))

    result = client.inventory()

    assert result.partial is True
    assert result.successful is False
    assert result.failed is False
    assert result.data.domains == ()
    assert result.data.unenrolled_devices is None
    assert result.errors[0].message == "unenrolled inventory unavailable"
    assert result.errors[0].raw == error
    assert result.raw_data == {"domains": []}


def test_graphql_errors_without_data_are_a_typed_failed_result():
    client, _ = _client(_response({"errors": [{"message": "not authorized"}]}))

    result = client.inventory()

    assert result.data is None
    assert result.failed is True
    assert result.partial is False
    assert result.errors[0].message == "not authorized"


@pytest.mark.asyncio
async def test_async_inventory_uses_the_same_typed_operation():
    client, transport = _client(_response(_inventory_payload()))

    result = await client.inventory_async()

    assert result.successful is True
    assert len(transport.requests) == 1


def test_credential_file_is_reloaded_and_newlines_are_removed(tmp_path):
    credential_file = tmp_path / "managed-api-key"
    credential_file.write_text(CREDENTIAL + "\r\n", encoding="ascii")
    transport = FakeTransport(_response(_inventory_payload()), _response(_inventory_payload()))
    client = ManagedAPIClient(
        "https://manager.example/graphql",
        credential_file=credential_file,
        transport=transport,
    )

    client.inventory()
    credential_file.write_text("rotated-key\n", encoding="ascii")
    client.inventory()

    assert transport.requests[0].headers["Authorization"] == CREDENTIAL
    assert transport.requests[1].headers["Authorization"] == "rotated-key"


@pytest.mark.parametrize(
    "url",
    [
        "ftp://manager.example/graphql",
        "http://manager.example/",
        "http://manager.example/graphql?query=value",
        "http://user:password@manager.example/graphql",
    ],
)
def test_endpoint_must_be_a_public_graphql_url(url):
    with pytest.raises(ValueError):
        ManagedAPIClient(url, credential=CREDENTIAL)


def test_exactly_one_credential_source_is_required(tmp_path):
    credential_file = tmp_path / "key"
    credential_file.write_text(CREDENTIAL, encoding="ascii")

    with pytest.raises(ValueError, match="exactly one"):
        ManagedAPIClient("http://manager.example/graphql")
    with pytest.raises(ValueError, match="exactly one"):
        ManagedAPIClient(
            "http://manager.example/graphql",
            credential=CREDENTIAL,
            credential_file=credential_file,
        )


def test_malformed_credentials_fail_without_echoing_secret(tmp_path):
    secret = "managed-key\r\ninjected"
    credential_file = tmp_path / "key"
    credential_file.write_text(secret, encoding="ascii")
    client = ManagedAPIClient(
        "http://manager.example/graphql",
        credential_file=credential_file,
        transport=FakeTransport(_response(_inventory_payload())),
    )

    with pytest.raises(CredentialError) as failure:
        client.inventory()

    assert secret not in str(failure.value)


def test_transport_http_json_size_and_shape_failures_are_distinct():
    transport_client, _ = _client(OSError("offline"))
    http_client, _ = _client(HTTPResponse(status=403, body=b"forbidden"))
    json_client, _ = _client(HTTPResponse(status=200, body=b"not json"))
    size_client, _ = _client(HTTPResponse(status=200, body=b"x" * 33), max_response_bytes=32)
    shape_client, _ = _client(_response({"data": []}))

    with pytest.raises(TransportError):
        transport_client.inventory()
    with pytest.raises(HTTPStatusError) as status_failure:
        http_client.inventory()
    assert status_failure.value.status == 403
    assert status_failure.value.body == b"forbidden"
    with pytest.raises(JSONResponseError):
        json_client.inventory()
    with pytest.raises(ResponseTooLargeError):
        size_client.inventory()
    with pytest.raises(ResponseShapeError):
        shape_client.inventory()


def test_invalid_graphql_error_shape_is_not_silently_discarded():
    client, _ = _client(_response({"data": {"domains": [], "unenrolledDevices": []}, "errors": ["bad"]}))

    with pytest.raises(ResponseShapeError, match="errors"):
        client.inventory()


def test_complete_response_must_contain_both_inventory_roots():
    client, _ = _client(_response({"data": {"domains": []}}))

    with pytest.raises(ResponseShapeError, match="requested root"):
        client.inventory()


@pytest.mark.parametrize("root", ["domains", "unenrolledDevices"])
def test_complete_response_inventory_roots_must_be_non_null(root):
    data = {"domains": [], "unenrolledDevices": []}
    data[root] = None
    client, _ = _client(_response({"data": data}))

    with pytest.raises(ResponseShapeError, match="non-null lists"):
        client.inventory()


def test_complete_response_preserves_nullable_domain_items():
    client, _ = _client(_response({"data": {"domains": [None], "unenrolledDevices": []}}))

    result = client.inventory()

    assert result.successful is True
    assert result.data.domains == (None,)


def test_partial_response_preserves_nullable_domain_items():
    client, _ = _client(
        _response(
            {
                "data": {"domains": [None], "unenrolledDevices": []},
                "errors": [{"message": "domain unavailable"}],
            }
        )
    )

    result = client.inventory()

    assert result.partial is True
    assert result.data.domains == (None,)


@pytest.mark.parametrize("root", ["domains", "unenrolledDevices"])
def test_graphql_errors_allow_null_inventory_roots(root):
    data = {"domains": [], "unenrolledDevices": []}
    data[root] = None
    client, _ = _client(_response({"data": data, "errors": [{"message": f"{root} unavailable"}]}))

    result = client.inventory()

    assert result.partial is True
    assert getattr(result.data, "unenrolled_devices" if root == "unenrolledDevices" else root) is None
