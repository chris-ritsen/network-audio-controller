import pytest
from typer.testing import CliRunner

from netaudio.commands import shure
from netaudio.commands import shure_transport


runner = CliRunner()


class FakeSocket:
    def __init__(self, responses):
        self.responses = list(responses)
        self.sent = []

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, traceback):
        return False

    def sendall(self, payload):
        self.sent.append(payload)

    def settimeout(self, timeout):
        self.timeout = timeout

    def recv(self, size):
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        return b""


def _resolve_rep(monkeypatch):
    monkeypatch.setattr(
        shure,
        "_resolve_target",
        lambda host, protocol, port: (host or "192.0.2.10", shure.Protocol.rep, port or 2202),
    )


def test_response_parsers_preserve_values_with_spaces():
    assert shure_transport._parse_ad4d_line("< REP DEVICE_ID {Studio Rack A} >") == (
        None,
        "DEVICE_ID",
        "Studio Rack A",
    )
    assert shure_transport._parse_p10t_line("< REPORT DEVICE_NAME Studio Rack A >") == (
        None,
        "DEVICE_NAME",
        "Studio Rack A",
    )


def test_p10t_probe_preserves_device_names_with_spaces(monkeypatch):
    connection_socket = FakeSocket([b"< REPORT DEVICE_NAME Studio Rack A >\r\n", b""])
    monkeypatch.setattr(
        shure_transport.socket,
        "create_connection",
        lambda *args, **kwargs: connection_socket,
    )

    assert shure_transport._probe_device("192.0.2.10") == (
        shure.Protocol.report,
        "Studio Rack A",
        "Studio Rack A",
    )


def test_strict_send_raises_for_explicit_device_error(monkeypatch):
    connection_socket = FakeSocket([b"< REP ERR INVALID_VALUE >\r\n", b""])
    monkeypatch.setattr(
        shure_transport.socket,
        "create_connection",
        lambda *args, **kwargs: connection_socket,
    )

    with pytest.raises(shure_transport.ShureCommandRejected, match="INVALID_VALUE"):
        shure._send(
            "192.0.2.10",
            2202,
            shure.Protocol.rep,
            command="SET DEVICE_ID {bad}",
            expect_key="DEVICE_ID",
            require_response=True,
            allow_no_response=True,
        )


def test_required_get_raises_when_no_matching_response(monkeypatch):
    connection_socket = FakeSocket([b""])
    monkeypatch.setattr(
        shure_transport.socket,
        "create_connection",
        lambda *args, **kwargs: connection_socket,
    )

    with pytest.raises(shure.ShureCommandTimeout, match="DEVICE_ID"):
        shure._send(
            "192.0.2.10",
            2202,
            shure.Protocol.rep,
            command="GET DEVICE_ID",
            expect_key="DEVICE_ID",
            require_response=True,
        )


def test_required_bulk_query_raises_when_no_usable_response(monkeypatch):
    connection_socket = FakeSocket([b""])
    monkeypatch.setattr(
        shure_transport.socket,
        "create_connection",
        lambda *args, **kwargs: connection_socket,
    )

    with pytest.raises(shure.ShureCommandTimeout, match="no usable bulk response"):
        shure._send(
            "192.0.2.10",
            2202,
            shure.Protocol.rep,
            bulk=True,
            require_response=True,
        )


def test_silent_set_response_is_allowed_before_readback(monkeypatch):
    connection_socket = FakeSocket([b""])
    monkeypatch.setattr(
        shure_transport.socket,
        "create_connection",
        lambda *args, **kwargs: connection_socket,
    )

    assert (
        shure._send(
            "192.0.2.10",
            2202,
            shure.Protocol.rep,
            command="SET DEVICE_ID {Studio}",
            expect_key="DEVICE_ID",
            require_response=True,
            allow_no_response=True,
        )
        is None
    )


def test_get_timeout_is_a_clean_nonzero_cli_failure(monkeypatch):
    _resolve_rep(monkeypatch)
    monkeypatch.setattr(
        shure,
        "_send",
        lambda *args, **kwargs: (_ for _ in ()).throw(shure.ShureCommandTimeout("no MODEL response")),
    )

    result = runner.invoke(shure.app, ["get", "MODEL", "192.0.2.10"])

    assert result.exit_code == 1
    assert "Error: no MODEL response" in result.output
    assert "Traceback" not in result.output


def test_set_uses_get_readback_and_reports_verified_success(monkeypatch):
    _resolve_rep(monkeypatch)
    calls = []

    def send(*args, **kwargs):
        calls.append(kwargs)
        if kwargs["command"].startswith("SET"):
            return None
        return "Studio Rack"

    monkeypatch.setattr(shure, "_send", send)

    result = runner.invoke(shure.app, ["set", "DEVICE_ID", "Studio Rack", "192.0.2.10"])

    assert result.exit_code == 0
    assert [call["command"] for call in calls] == [
        "SET DEVICE_ID {Studio Rack}",
        "GET DEVICE_ID",
    ]
    assert calls[0]["allow_no_response"] is True
    assert calls[1]["allow_no_response"] is False
    assert all(call["require_response"] is True for call in calls)
    assert "Set DEVICE_ID to 'Studio Rack' (verified)" in result.output


def test_set_readback_mismatch_is_a_nonzero_failure(monkeypatch):
    _resolve_rep(monkeypatch)
    responses = iter((None, "Old Name"))
    monkeypatch.setattr(shure, "_send", lambda *args, **kwargs: next(responses))

    result = runner.invoke(shure.app, ["set", "DEVICE_ID", "New Name", "192.0.2.10"])

    assert result.exit_code == 1
    assert "verification failed" in result.output
    assert "device reports 'Old Name'" in result.output
    assert "(verified)" not in result.output


def test_set_error_response_stops_before_readback(monkeypatch):
    _resolve_rep(monkeypatch)
    calls = []

    def send(*args, **kwargs):
        calls.append(kwargs["command"])
        raise shure_transport.ShureCommandRejected("device rejected the value")

    monkeypatch.setattr(shure, "_send", send)

    result = runner.invoke(shure.app, ["set", "DEVICE_ID", "New Name", "192.0.2.10"])

    assert result.exit_code == 1
    assert calls == ["SET DEVICE_ID {New Name}"]
    assert "Error: device rejected the value" in result.output


@pytest.mark.parametrize("value", ["safe> <GET MODEL", "line\nbreak", "nul\x00byte", "bad}brace"])
def test_set_rejects_values_that_can_break_wire_framing(monkeypatch, value):
    _resolve_rep(monkeypatch)
    sent = False

    def send(*_args, **_kwargs):
        nonlocal sent
        sent = True

    monkeypatch.setattr(shure, "_send", send)

    result = runner.invoke(shure.app, ["set", "DEVICE_ID", value, "192.0.2.10"])

    assert result.exit_code == 1
    assert "invalid value for DEVICE_ID" in result.output
    assert not sent


def _shure_device(ip, name):
    return shure.ShureDevice(
        ip=ip,
        mac="00:0e:dd:00:00:01",
        protocol=shure.Protocol.rep,
        model="AD4D",
        name=name,
    )


def _reset_shure_cli_state(monkeypatch):
    from netaudio.cli import OutputFormat, state

    monkeypatch.setattr(state, "output_format", OutputFormat.plain)
    monkeypatch.setattr(state, "names", [])
    monkeypatch.setattr(state, "hosts", [])


def test_discovery_contains_probe_worker_exceptions(monkeypatch):
    monkeypatch.setattr(
        shure_transport,
        "get_shure_neighbor_entries",
        lambda: [("192.0.2.10", "00:0e:dd:00:00:01"), ("192.0.2.11", "00:0e:dd:00:00:02")],
    )

    def probe(ip):
        if ip.endswith("10"):
            raise RuntimeError("synthetic worker failure")
        return shure.Protocol.rep, "AD4D", "Healthy"

    monkeypatch.setattr(shure_transport, "_probe_device", probe)

    devices = shure._discover_shure_devices()

    assert [device.ip for device in devices] == ["192.0.2.11"]


def test_device_list_aggregates_firmware_failures_without_traceback(monkeypatch):
    _reset_shure_cli_state(monkeypatch)
    first = _shure_device("192.0.2.10", "Rack A")
    second = _shure_device("192.0.2.11", "Rack B")
    monkeypatch.setattr(shure, "_discover_shure_devices", lambda: [first, second])

    async def no_dante_devices():
        return {}

    monkeypatch.setattr(shure, "_get_dante_devices", no_dante_devices)

    def send(host, *_args, **_kwargs):
        if host == first.ip:
            return "1.2.3"
        raise shure.ShureCommandTimeout("device returned no matching response for FW_VER")

    monkeypatch.setattr(shure, "_send", send)

    result = runner.invoke(shure.app, ["device", "list"])

    assert result.exit_code == 1
    assert "1.2.3" in result.output
    assert "ERROR" in result.output
    assert "firmware query failed for Rack B" in result.output
    assert "FW_VER" in result.output
    assert "Traceback" not in result.output


def test_device_list_reports_dante_enrichment_failure_after_shure_rows(monkeypatch):
    _reset_shure_cli_state(monkeypatch)
    device = _shure_device("192.0.2.10", "Rack A")
    monkeypatch.setattr(shure, "_discover_shure_devices", lambda: [device])
    monkeypatch.setattr(shure, "_send", lambda *_args, **_kwargs: "1.2.3")

    async def fail_dante_devices():
        raise OSError("synthetic daemon failure")

    monkeypatch.setattr(shure, "_get_dante_devices", fail_dante_devices)

    result = runner.invoke(shure.app, ["device", "list"])

    assert result.exit_code == 1
    assert "Rack A" in result.output
    assert "1.2.3" in result.output
    assert "Dante correlation query failed: synthetic daemon failure" in result.output
    assert "Traceback" not in result.output


def test_device_list_contains_saved_correlation_read_failure(monkeypatch):
    _reset_shure_cli_state(monkeypatch)
    device = _shure_device("192.0.2.10", "Rack A")
    monkeypatch.setattr(shure, "_discover_shure_devices", lambda: [device])
    monkeypatch.setattr(shure, "_send", lambda *_args, **_kwargs: "1.2.3")
    monkeypatch.setattr(shure, "_load_correlation", lambda *_args: (_ for _ in ()).throw(UnicodeError("bad config")))

    async def no_dante_devices():
        return {}

    monkeypatch.setattr(shure, "_get_dante_devices", no_dante_devices)

    result = runner.invoke(shure.app, ["device", "list"])

    assert result.exit_code == 1
    assert "Rack A" in result.output
    assert "saved correlation lookup failed for Rack A" in result.output
    assert "bad config" in result.output
    assert "Traceback" not in result.output


def test_channel_list_keeps_successes_and_aggregates_device_errors(monkeypatch):
    _reset_shure_cli_state(monkeypatch)
    first = _shure_device("192.0.2.10", "Rack A")
    second = _shure_device("192.0.2.11", "Rack B")
    monkeypatch.setattr(shure, "_discover_shure_devices", lambda: [first, second])

    def send(host, *_args, **_kwargs):
        if host == first.ip:
            return {
                "DEVICE_ID": "Rack A",
                "MODEL": "AD4D",
                1: {"CHAN_NAME": "Vocal"},
            }
        raise shure_transport.ShureCommandRejected("device rejected bulk query: ERR BUSY")

    monkeypatch.setattr(shure, "_send", send)

    result = runner.invoke(shure.app, ["channel", "list"])

    assert result.exit_code == 1
    assert "Rack A (192.0.2.10)" in result.output
    assert "Vocal" in result.output
    assert "channel query failed for Rack B" in result.output
    assert "ERR BUSY" in result.output
    assert "Traceback" not in result.output


def test_device_show_missing_bulk_response_is_clean_nonzero(monkeypatch):
    _reset_shure_cli_state(monkeypatch)
    _resolve_rep(monkeypatch)
    monkeypatch.setattr(
        shure,
        "_send",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            shure.ShureCommandTimeout("device returned no usable bulk response")
        ),
    )

    result = runner.invoke(shure.app, ["device", "show", "192.0.2.10"])

    assert result.exit_code == 1
    assert "device query failed for 192.0.2.10" in result.output
    assert "no usable bulk response" in result.output
    assert "Traceback" not in result.output


@pytest.mark.asyncio
async def test_correlation_rejects_p10t_before_receiver_queries(monkeypatch):
    monkeypatch.setattr(
        shure,
        "_resolve_target",
        lambda host, protocol, port: ("192.0.2.10", shure.Protocol.report, 2202),
    )
    monkeypatch.setattr(
        shure,
        "_get_active_shure_channels",
        lambda *args: pytest.fail("P10T correlation must not query receiver channels"),
    )

    with pytest.raises(shure.typer.Exit):
        await shure._correlate_async(
            "192.0.2.10",
            shure.Protocol.report,
            2202,
            None,
            1.0,
            1,
            False,
        )
