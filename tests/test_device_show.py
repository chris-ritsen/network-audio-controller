import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

import netaudio._capture as capture_module
import netaudio._common as common_module
from netaudio.commands import device as device_commands
from netaudio.dante.channel import DanteChannel
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.subscription import DanteSubscription


runner = CliRunner()


@pytest.mark.parametrize(
    ("link_speed_mbps", "expected"),
    [(100, "100 Mbps"), (1000, "1 Gbps"), (2500, "2.5 Gbps"), (10000, "10 Gbps")],
)
def test_format_link_speed_preserves_generic_numeric_values(link_speed_mbps, expected):
    assert device_commands._format_link_speed(link_speed_mbps) == expected


def test_format_channel_count_does_not_turn_unknown_into_zero():
    assert device_commands._format_channel_count({}, None) == "unknown"
    assert device_commands._format_channel_count({}, 0) == "0"
    assert device_commands._format_channel_count({1: object()}, 64) == "1"


def make_show_device() -> DanteDevice:
    device = DanteDevice(server_name="LX-DANTE-081258.local.")
    device.name = "lx-dante"
    device.ipv4 = "192.168.1.108"
    device.mac_address = "001dc10812580000"
    device.model_id = "LX-DANTE"
    device.manufacturer = "Digigram"
    device.firmware_version = "4.0.1"
    device.software_version = "4.0.0"
    device.link_speed_mbps = 1000
    device.tx_count = 128
    device.rx_count = 128
    device.sample_rate = 48_000
    device.supported_sample_rates = [44_100, 48_000, 96_000]
    device.encoding = 24
    device.supported_encodings = [16, 24, 32]
    device.latency = 1.0
    device.active_latency = 1.0
    device.configured_latency = 2.0
    device.default_latency = 1.0
    device.min_latency = 0.15
    device.max_latency = 21.333334
    device.aes67_current = False
    device.aes67_configured = False
    device.is_locked = False
    device.preferred_leader = True
    device.services = {
        "lx-dante._netaudio-arc._udp.local.": {
            "type": "_netaudio-arc._udp.local.",
            "port": 4440,
            "ipv4": "192.168.1.108",
        }
    }

    rx_channel = DanteChannel()
    rx_channel.channel_type = "rx"
    rx_channel.device = device
    rx_channel.number = 1
    rx_channel.name = "input-1"
    device.rx_channels[1] = rx_channel

    tx_channel = DanteChannel()
    tx_channel.channel_type = "tx"
    tx_channel.device = device
    tx_channel.number = 1
    tx_channel.name = "output-1"
    device.tx_channels[1] = tx_channel

    subscription = DanteSubscription()
    subscription.rx_channel_name = "input-1"
    subscription.rx_device_name = "lx-dante"
    subscription.tx_channel_name = "output-1"
    subscription.tx_device_name = "source"
    subscription.status_code = 0x0009
    subscription.rx_channel_status_code = 0x0009
    device.subscriptions = [subscription]
    return device


@pytest.fixture(autouse=True)
def reset_cli_state():
    from netaudio.cli import OutputFormat, state

    original_state = (
        list(state.names),
        list(state.hosts),
        list(state.server_names),
        list(state.macs),
        state.output_format,
        state.no_color,
        state.timeout_explicit,
        state.verbose,
    )
    state.names = []
    state.hosts = []
    state.server_names = []
    state.macs = []
    state.output_format = OutputFormat.plain
    state.no_color = True
    state.timeout_explicit = False
    state.verbose = False
    try:
        yield
    finally:
        (
            state.names,
            state.hosts,
            state.server_names,
            state.macs,
            state.output_format,
            state.no_color,
            state.timeout_explicit,
            state.verbose,
        ) = original_state


def test_device_show_plain_is_concise_and_formats_capabilities(monkeypatch):
    device = make_show_device()

    async def load_device(include_channels):
        assert include_channels is False
        return device.server_name, device

    monkeypatch.setattr(device_commands, "_load_device_for_show", load_device)

    result = runner.invoke(device_commands.app, ["show"])

    assert result.exit_code == 0
    assert "Name                    lx-dante" in result.output
    assert "Channels                128 TX / 128 RX" in result.output
    assert "Link Speed              1 Gbps" in result.output
    assert "Supported Sample Rates  44.1, 48, 96 kHz" in result.output
    assert "Supported Encodings     PCM16, PCM24, PCM32" in result.output
    assert "Latency Range           0.15-21.3333 ms" in result.output
    assert "'channels':" not in result.output
    assert "subscriptions" not in result.output
    assert "_netaudio-arc" not in result.output


def test_device_show_plain_labels_unknown_capabilities(monkeypatch):
    device = make_show_device()
    device.supported_sample_rates = None
    device.encoding = None
    device.supported_encodings = None

    async def load_device(include_channels):
        assert include_channels is False
        return device.server_name, device

    monkeypatch.setattr(device_commands, "_load_device_for_show", load_device)

    result = runner.invoke(device_commands.app, ["show"])

    assert result.exit_code == 0
    assert "Supported Sample Rates  unknown" in result.output
    assert "Encoding                unknown" in result.output
    assert "Supported Encodings     unknown" in result.output


def test_device_show_plain_labels_known_unsupported_aes67(monkeypatch):
    device = make_show_device()
    device.aes67_supported = False

    async def load_device(include_channels):
        assert include_channels is False
        return device.server_name, device

    monkeypatch.setattr(device_commands, "_load_device_for_show", load_device)

    result = runner.invoke(device_commands.app, ["show"])

    assert result.exit_code == 0
    assert "AES67                   unsupported" in result.output


@pytest.mark.asyncio
async def test_control_population_reports_failures_instead_of_discarding_them(caplog):
    device = make_show_device()
    device.tx_channels = {}
    device.rx_channels = {}
    device.populate_from_core = AsyncMock(side_effect=RuntimeError("synthetic population failure"))

    with pytest.raises(RuntimeError, match="failed to populate controls"):
        await common_module._populate_controls({device.server_name: device})

    await common_module._populate_controls({device.server_name: device}, strict=False)

    assert "synthetic population failure" in caplog.text


def test_device_show_plain_formats_gain_capability_without_channel_inventory(monkeypatch):
    device = make_show_device()
    device.gain_device_type = "input"
    device.gain_levels = [5, 1]
    device.supported_gain_levels = [1, 2, 3, 4, 5]

    async def load_device(include_channels):
        assert include_channels is False
        return device.server_name, device

    monkeypatch.setattr(device_commands, "_load_device_for_show", load_device)

    result = runner.invoke(device_commands.app, ["show"])

    assert result.exit_code == 0
    assert "Reference Controls      input" in result.output
    assert "Reference Levels        1: -10 dBV, 2: +24 dBu" in result.output
    assert "Reference Options       +24 dBu, +4 dBu, 0 dBu, 0 dBV, -10 dBV" in result.output


def test_device_show_plain_does_not_render_unknown_channel_counts_as_zero(monkeypatch):
    device = make_show_device()
    device.tx_count = None
    device.rx_count = None
    device.tx_channels = {}
    device.rx_channels = {}

    async def load_device(include_channels):
        return device.server_name, device

    monkeypatch.setattr(device_commands, "_load_device_for_show", load_device)

    result = runner.invoke(device_commands.app, ["show"])

    assert result.exit_code == 0
    assert "Channels                unknown TX / unknown RX" in result.output


def test_device_show_json_preserves_full_serializer(monkeypatch):
    from netaudio.cli import OutputFormat, state

    device = make_show_device()

    async def load_device(include_channels):
        assert include_channels is True
        return device.server_name, device

    monkeypatch.setattr(device_commands, "_load_device_for_show", load_device)
    state.output_format = OutputFormat.json

    result = runner.invoke(device_commands.app, ["show"])

    assert result.exit_code == 0
    assert json.loads(result.output) == json.loads(json.dumps(DanteDeviceSerializer.to_json(device), default=str))


def test_device_show_csv_is_a_two_column_summary(monkeypatch):
    from netaudio.cli import OutputFormat, state

    device = make_show_device()

    async def load_device(include_channels):
        assert include_channels is False
        return device.server_name, device

    monkeypatch.setattr(device_commands, "_load_device_for_show", load_device)
    state.output_format = OutputFormat.csv

    result = runner.invoke(device_commands.app, ["show"])

    assert result.exit_code == 0
    assert result.output.startswith("Field,Value\n")
    assert "Name,lx-dante" in result.output
    assert "channels" not in result.output


def test_device_list_verbose_labels_known_unsupported_aes67(monkeypatch):
    from netaudio.cli import state

    device = make_show_device()
    device.aes67_supported = False

    async def discover():
        return {device.server_name: device}

    async def populate_controls(_devices, strict):
        assert strict is False

    monkeypatch.setattr(device_commands, "_discover", discover)
    monkeypatch.setattr(device_commands, "_populate_controls", populate_controls)
    monkeypatch.setattr(device_commands, "_collect_lock_state", lambda _devices: None)
    state.verbose = True

    result = runner.invoke(device_commands.app, ["list"])

    assert result.exit_code == 0
    assert "unsupported" in result.output


@pytest.mark.asyncio
async def test_show_loader_uses_exact_name_resolution_and_selected_population(monkeypatch):
    from netaudio.cli import state

    device = make_show_device()
    device.name = ""

    class FakeApplication:
        def __init__(self):
            self.started = False
            self.stopped = False
            self.populated_devices = None
            self.include_channels = None

        async def startup(self):
            self.started = True

        async def shutdown(self):
            self.stopped = True

        async def discover_named_device(self, device_name, timeout):
            assert device_name == "lx-dante"
            assert timeout == 2.0
            return {device.server_name: device}

        async def populate_device_names(
            self,
            devices,
            request_timeout_milliseconds,
            request_attempts,
        ):
            assert devices == {device.server_name: device}
            assert request_timeout_milliseconds == 500
            assert request_attempts == 1
            device.name = "lx-dante"

        async def wait_for_discovery(self, timeout):
            raise AssertionError("broad discovery should not run for a resolved literal name")

        async def populate_devices(self, devices, timeout, include_channels):
            self.populated_devices = devices
            self.include_channels = include_channels
            assert timeout == 2.0

    async def no_daemon_devices():
        return None

    application = FakeApplication()
    monkeypatch.setattr(common_module, "get_devices_from_daemon", no_daemon_devices)
    monkeypatch.setattr(common_module, "_make_dante_application", lambda packet_store, session_id: application)
    monkeypatch.setattr(capture_module, "open_capture_session", lambda: (None, None))
    state.names = ["lx-dante"]

    server_name, loaded_device = await common_module._load_device_for_show(include_channels=False)

    assert server_name == device.server_name
    assert loaded_device is device
    assert application.started is True
    assert application.stopped is True
    assert application.populated_devices == {device.server_name: device}
    assert application.include_channels is False


@pytest.mark.asyncio
async def test_show_loader_falls_back_to_bounded_discovery_after_literal_name_miss(monkeypatch):
    from netaudio.cli import state

    device = make_show_device()
    device.name = ""

    class FakeApplication:
        def __init__(self):
            self.discovery_attempts = 0
            self.broad_discovery_attempts = 0
            self.stopped = False

        async def startup(self):
            return None

        async def shutdown(self):
            self.stopped = True

        async def discover_named_device(self, device_name, timeout):
            assert device_name == "missing-device"
            self.discovery_attempts += 1
            return {}

        async def populate_device_names(
            self,
            devices,
            request_timeout_milliseconds,
            request_attempts,
        ):
            if devices:
                device.name = "missing-device"

        async def wait_for_discovery(self, timeout):
            assert timeout == 2.0
            self.broad_discovery_attempts += 1
            return {device.server_name: device}

        async def populate_devices(self, devices, timeout, include_channels):
            assert devices == {device.server_name: device}

    async def no_daemon_devices():
        return None

    application = FakeApplication()
    monkeypatch.setattr(common_module, "get_devices_from_daemon", no_daemon_devices)
    monkeypatch.setattr(common_module, "_make_dante_application", lambda packet_store, session_id: application)
    monkeypatch.setattr(capture_module, "open_capture_session", lambda: (None, None))
    state.names = ["missing-device"]

    server_name, loaded_device = await common_module._load_device_for_show(include_channels=False)

    assert server_name == device.server_name
    assert loaded_device is device
    assert application.discovery_attempts == 1
    assert application.broad_discovery_attempts == 1
    assert application.stopped is True


@pytest.mark.asyncio
async def test_show_loader_glob_falls_back_to_identity_discovery(monkeypatch):
    from netaudio.cli import state

    matching_device = make_show_device()
    matching_device.name = ""
    other_device = make_show_device()
    other_device.server_name = "other.local."
    other_device.name = ""

    class FakeApplication:
        async def startup(self):
            return None

        async def shutdown(self):
            return None

        async def discover_named_device(self, device_name, timeout):
            raise AssertionError("literal resolution should not run for a glob")

        async def wait_for_discovery(self, timeout):
            assert timeout == 2.0
            return {
                matching_device.server_name: matching_device,
                other_device.server_name: other_device,
            }

        async def populate_device_names(
            self,
            devices,
            request_timeout_milliseconds,
            request_attempts,
        ):
            assert request_timeout_milliseconds == 500
            assert request_attempts == 1
            matching_device.name = "lx-dante"
            other_device.name = "other"

        async def populate_devices(self, devices, timeout, include_channels):
            assert devices == {matching_device.server_name: matching_device}

    async def no_daemon_devices():
        return None

    monkeypatch.setattr(common_module, "get_devices_from_daemon", no_daemon_devices)
    monkeypatch.setattr(
        common_module,
        "_make_dante_application",
        lambda packet_store, session_id: FakeApplication(),
    )
    monkeypatch.setattr(capture_module, "open_capture_session", lambda: (None, None))
    state.names = ["lx-*"]

    server_name, loaded_device = await common_module._load_device_for_show(include_channels=False)

    assert server_name == matching_device.server_name
    assert loaded_device is matching_device


@pytest.mark.asyncio
async def test_summary_control_fetch_skips_channel_pages(monkeypatch):
    device = make_show_device()
    core_client = MagicMock()
    core_client.observer = None
    core_client.get_device_name.return_value = "lx-dante"
    core_client.get_channel_count.return_value = (128, 128, False)
    core_client.get_device_settings.return_value = {"sample_rate": 48_000}
    core_client.get_property_directory.return_value = None
    core_client.get_aes67_configured.return_value = False
    monkeypatch.setattr(device, "_core_client", lambda: core_client)

    controls = await device.fetch_controls_data(include_channels=False)

    assert controls["name"] == "lx-dante"
    assert controls["tx_count"] == 128
    assert controls["rx_count"] == 128
    core_client.get_rx_channels.assert_not_called()
    core_client.get_tx_channels.assert_not_called()


def test_instrumented_summary_fetch_skips_channel_pages(monkeypatch):
    fetch_rx_records = MagicMock()
    fetch_tx_records = MagicMock()

    def query_response(client, specification, port, parse_kind=None, starting_channel=None):
        if specification["command"] == "channel_count":
            return {"tx_count": 128, "rx_count": 128, "locked": False}
        if specification["command"] == "device_settings":
            return {"sample_rate": 48_000}
        if specification["command"] == "query_latency_config":
            return False
        raise AssertionError(f"Unexpected command: {specification['command']}")

    monkeypatch.setattr(capture_module, "fetch_device_name", lambda client, port: "lx-dante")
    monkeypatch.setattr(capture_module, "fetch_rx_records", fetch_rx_records)
    monkeypatch.setattr(capture_module, "fetch_tx_records", fetch_tx_records)
    monkeypatch.setattr(capture_module, "_query", query_response)

    controls = capture_module._fetch_instrumented(MagicMock(), 4440, include_channels=False)

    assert controls["name"] == "lx-dante"
    assert controls["counts"] == (128, 128, False)
    assert controls["rx"] == []
    assert controls["tx"] == []
    fetch_rx_records.assert_not_called()
    fetch_tx_records.assert_not_called()
