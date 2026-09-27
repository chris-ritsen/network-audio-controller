import json

import pytest

from netaudio.daemon.http.api import _encode_sse
from netaudio.daemon.http.configuration import DaemonConfigurationHandlers


def test_session_conversion_does_not_copy_unchanged_device_payloads():
    from netaudio.daemon.http.json_values import browser_values

    devices = {"receiver": {"channels": [{"number": number} for number in range(256)]}}
    payload = {"devices": devices, "flows": [{"session_id": 9007199254740993}]}
    converted = browser_values(payload)
    assert converted["devices"] is devices
    assert converted["flows"][0]["session_id"] == "9007199254740993"
    assert payload["flows"][0]["session_id"] == 9007199254740993
    assert browser_values(devices) is devices


@pytest.mark.parametrize("session_id", [9007199254740993, 18446744073709551615])
def test_sse_preserves_external_session_identity(session_id):
    payload = {
        "event": "external_flow_changed",
        "identity": {"session_id": session_id},
        "flow": {"sdp": {"session_id": session_id}},
    }
    decoded = json.loads(_encode_sse(payload).decode().removeprefix("data: "))
    assert decoded["identity"]["session_id"] == str(session_id)
    assert decoded["flow"]["sdp"]["session_id"] == str(session_id)


@pytest.mark.asyncio
async def test_http_preserves_nested_receiver_session_identity():
    class Writer:
        def write(self, body):
            self.body = body

        async def drain(self):
            pass

    writer = Writer()
    await DaemonConfigurationHandlers()._send_json(
        writer, {"flows": [{"external_identity": {"session_id": 9007199254740993}}]}
    )
    assert (
        json.loads(writer.body.split(b"\r\n\r\n", 1)[1])["flows"][0]["external_identity"]["session_id"]
        == "9007199254740993"
    )
