import importlib
import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from netaudio.daemon.dbus_state import (
    DANTE_PROPERTY_NAMES,
    SHURE_PROPERTY_NAMES,
    aes67_enabled,
    clock_port_rows,
    dbus_double,
    dbus_int32,
    dbus_string,
    dbus_uint,
    dbus_uint_list,
    latency_milliseconds,
    snapshot_dante_device,
    snapshot_shure_device,
    subscription_rows,
    transmitter_flow_rows,
)
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio.shure.device import ShureDeviceInfo, ShureDeviceType, ShureP10TChannel


@pytest.fixture
def dbus_interfaces_without_dependency(monkeypatch):
    module_name = "netaudio.daemon.dbus_interfaces"
    previous = sys.modules.pop(module_name, None)

    dbus_fast = types.ModuleType("dbus_fast")

    class PropertyAccess:
        READ = object()

    dbus_fast.PropertyAccess = PropertyAccess

    service = types.ModuleType("dbus_fast.service")

    class ServiceInterface:
        def __init__(self, interface_name):
            self.interface_name = interface_name

    def decorator(*args, **kwargs):
        def decorate(function):
            return function

        return decorate

    service.ServiceInterface = ServiceInterface
    service.dbus_property = decorator
    service.signal = decorator

    monkeypatch.setitem(sys.modules, "dbus_fast", dbus_fast)
    monkeypatch.setitem(sys.modules, "dbus_fast.service", service)

    try:
        yield importlib.import_module(module_name)
    finally:
        sys.modules.pop(module_name, None)
        if previous is not None:
            sys.modules[module_name] = previous


@pytest.fixture
def dbus_service_without_dependency(monkeypatch):
    module_names = ("netaudio.daemon.dbus_interfaces", "netaudio.daemon.dbus_service")
    previous = {name: sys.modules.pop(name, None) for name in module_names}

    dbus_fast = types.ModuleType("dbus_fast")

    class PropertyAccess:
        READ = object()

    class BusType:
        SESSION = object()

    dbus_fast.PropertyAccess = PropertyAccess
    dbus_fast.BusType = BusType

    service_api = types.ModuleType("dbus_fast.service")

    class ServiceInterface:
        def __init__(self, interface_name):
            self.interface_name = interface_name
            self.emitted = []

        def emit_properties_changed(self, changed):
            self.emitted.append(changed)

    def decorator(*args, **kwargs):
        def decorate(function):
            return function

        return decorate

    service_api.ServiceInterface = ServiceInterface
    service_api.dbus_property = decorator
    service_api.signal = decorator

    aio = types.ModuleType("dbus_fast.aio")

    class MissingMessageBus:
        def __init__(self, **kwargs):
            raise AssertionError("test must install a fake MessageBus")

    aio.MessageBus = MissingMessageBus

    monkeypatch.setitem(sys.modules, "dbus_fast", dbus_fast)
    monkeypatch.setitem(sys.modules, "dbus_fast.service", service_api)
    monkeypatch.setitem(sys.modules, "dbus_fast.aio", aio)

    try:
        importlib.import_module("netaudio.daemon.dbus_interfaces")
        yield importlib.import_module("netaudio.daemon.dbus_service")
    finally:
        for name in reversed(module_names):
            sys.modules.pop(name, None)
            if previous[name] is not None:
                sys.modules[name] = previous[name]


class FakeBus:
    def __init__(self, request_reply=1, fail_export_at=None, fail_connect=False):
        self.request_reply = request_reply
        self.fail_export_at = fail_export_at
        self.fail_connect = fail_connect
        self.export_calls = []
        self.unexport_calls = []
        self.disconnected = False

    async def connect(self):
        if self.fail_connect:
            raise RuntimeError("connect failed")
        return self

    async def request_name(self, name):
        self.requested_name = name
        return self.request_reply

    def export(self, path, interface):
        self.export_calls.append((path, interface))
        if self.fail_export_at == len(self.export_calls):
            raise RuntimeError("export failed")

    def unexport(self, path, interface=None):
        self.unexport_calls.append((path, interface))

    def disconnect(self):
        self.disconnected = True


def _daemon(devices=None, shure=None):
    return SimpleNamespace(
        devices=devices or {},
        shure=shure,
        application=SimpleNamespace(dispatcher=DanteEventDispatcher()),
    )


def test_latency_adapter_preserves_fractional_milliseconds():
    assert latency_milliseconds(0.15) == 0.15
    assert latency_milliseconds(21.333334) == 21.333334
    assert latency_milliseconds(None) == 0.0


def test_dbus_scalar_adapters_never_leak_none_or_out_of_range_values():
    assert dbus_string(None) == ""
    assert dbus_string(123) == "123"
    assert dbus_uint(None) == 0
    assert dbus_uint(-1) == 0
    assert dbus_uint(2**32) == 0
    assert dbus_uint(65535, bits=16) == 65535
    assert dbus_uint_list([48_000, None, -1]) == [48_000, 0, 0]
    assert dbus_int32(-(2**31)) == -(2**31)
    assert dbus_int32(2**31) == 0
    assert dbus_double(None) == 0.0
    assert dbus_double(float("nan")) == 0.0


def test_aes67_enabled_uses_applied_state_not_pending_configuration():
    device = DanteDevice()
    device.aes67_current = False
    device.aes67_configured = True
    assert aes67_enabled(device) is False

    device.aes67_current = True
    device.aes67_configured = False
    assert aes67_enabled(device) is True


def test_snapshot_preserves_all_latency_values_and_applied_aes67_state():
    device = DanteDevice(server_name="device.local.")
    device.latency = 0.15
    device.active_latency = 0.15
    device.configured_latency = 0.25
    device.default_latency = 1.0
    device.min_latency = 0.125
    device.max_latency = 21.333334
    device.supported_sample_rates = [48_000, 96_000]
    device.supported_encodings = [16, 24, 32]
    device.link_speed_mbps = 2500
    device.aes67_current = False
    device.aes67_configured = True
    device.aes67_supported = False
    device.aes67_multicast_prefix = "239.69.0.0"
    device.sample_rate_pullup_raw_value = 1
    device.requested_sample_rate_pullup_raw_value = 1
    device.supported_sample_rate_pullup_raw_values = [0, 1, 2, 3, 4]
    device.transmitter_flows = [
        {
            "flow_number": 1,
            "flow_type": "unicast",
            "channel_count": 1,
            "sample_rate": 48000,
            "encoding": 24,
            "destination_internet_protocol_version_four_address": "192.168.1.108",
            "destination_user_datagram_port": 14355,
            "subscriber_device_name": "lx-dante",
            "subscriber_flow_name": "1",
        }
    ]
    device.is_locked = None
    device.clock_frequency_offset_parts_per_billion = -25_473
    device.clock_source_code = 57044
    device.clock_subdomain = b"_DFLT" + bytes(11)
    device.clock_port_state_code = 0x0006
    device.clock_role = "Leader"
    device.clock_identity = "001dc150692e"
    device.leader_clock_identity = "001dc150692e"
    device.clock_port_records = [
        {
            "record_flags": 0,
            "link_down": False,
            "record_number": 1,
            "ptp_version": 1,
            "record_format_code": 2,
            "transport_path_code": 1,
            "transport_path": "multicast",
            "reserved_byte": 0,
            "network_interface_index": 2,
            "state_code": 6,
            "role": "Leader",
            "status_flags": 7,
        }
    ]

    snapshot = snapshot_dante_device(device)

    assert snapshot["latency"] == 0.15
    assert snapshot["active_latency"] == 0.15
    assert snapshot["configured_latency"] == 0.25
    assert snapshot["default_latency"] == 1.0
    assert snapshot["min_latency"] == 0.125
    assert snapshot["max_latency"] == 21.333334
    assert snapshot["supported_sample_rates"] == [48_000, 96_000]
    assert snapshot["supported_encodings"] == [16, 24, 32]
    assert snapshot["aes67_current"] is False
    assert snapshot["aes67_supported"] is False
    assert snapshot["aes67_support_known"] is True
    assert snapshot["aes67_multicast_prefix"] == "239.69.0.0"
    assert snapshot["sample_rate_pullup_raw_value"] == 1
    assert snapshot["requested_sample_rate_pullup_raw_value"] == 1
    assert snapshot["supported_sample_rate_pullup_raw_values"] == [0, 1, 2, 3, 4]
    assert snapshot["sample_rate_pullup_known"] is True
    assert snapshot["transmitter_flows"] == [(1, 1, "unicast", 48000, 24, "192.168.1.108", 14355, "lx-dante", "1")]
    assert transmitter_flow_rows(device) == snapshot["transmitter_flows"]
    assert snapshot["lock_state_known"] is False
    assert snapshot["clock_frequency_offset_parts_per_billion"] == -25_473
    assert snapshot["clock_source_code"] == 57044
    assert snapshot["clock_subdomain"] == "_DFLT"
    assert snapshot["clock_role"] == "Leader"
    assert snapshot["clock_identity"] == "001dc150692e"
    assert snapshot["leader_clock_identity"] == "001dc150692e"
    assert snapshot["clock_port_records"] == [(0, False, 1, 1, 2, 1, "multicast", 0, 2, 6, 7, "Leader")]
    assert clock_port_rows(device) == snapshot["clock_port_records"]
    assert "aes67_enabled" not in snapshot
    assert DANTE_PROPERTY_NAMES["min_latency"] == "MinLatency"
    assert DANTE_PROPERTY_NAMES["max_latency"] == "MaxLatency"
    assert DANTE_PROPERTY_NAMES["aes67_current"] == "Aes67Enabled"
    assert DANTE_PROPERTY_NAMES["aes67_multicast_prefix"] == "Aes67MulticastPrefix"
    assert DANTE_PROPERTY_NAMES["sample_rate_pullup_known"] == "SampleRatePullupKnown"
    assert DANTE_PROPERTY_NAMES["transmitter_flows"] == "TransmitterFlows"
    assert DANTE_PROPERTY_NAMES["clock_source_code"] == "ClockSourceCode"
    assert DANTE_PROPERTY_NAMES["clock_subdomain"] == "ClockSubdomain"
    assert DANTE_PROPERTY_NAMES["clock_identity"] == "ClockIdentity"
    assert DANTE_PROPERTY_NAMES["leader_clock_identity"] == "LeaderClockIdentity"
    assert set(snapshot) == set(DANTE_PROPERTY_NAMES)


def test_subscription_snapshot_handles_partial_optional_records():
    device = SimpleNamespace(
        subscriptions=[
            SimpleNamespace(
                rx_device=None,
                rx_device_name=None,
                rx_channel_name=None,
                tx_device_name="Sender",
                tx_channel_name="Left",
                status_code=None,
            ),
            SimpleNamespace(
                rx_device=SimpleNamespace(server_name="receiver.local."),
                rx_channel_name="Right",
                tx_device_name="Sender",
                tx_channel_name="Right",
                status_code=0xFFFF,
            ),
        ]
    )

    assert subscription_rows(device) == [
        ("", "", "Sender", "Left", 0),
        ("receiver.local.", "Right", "Sender", "Right", 0xFFFF),
    ]


def test_shure_snapshot_covers_every_exported_device_property():
    device = ShureDeviceInfo(
        ip="192.0.2.4",
        mac="00:0e:dd:00:00:01",
        device_type=ShureDeviceType.ad4d,
        name="Receiver",
        model=None,
    )

    snapshot = snapshot_shure_device(device)

    assert set(snapshot) == set(SHURE_PROPERTY_NAMES)
    assert snapshot["device_type"] == "ad4d"
    assert snapshot["model"] == ""


def test_interface_uses_double_latency_properties_and_applied_aes67(
    dbus_interfaces_without_dependency,
):
    module = dbus_interfaces_without_dependency
    device = DanteDevice()
    device.latency = 0.15
    device.active_latency = 0.15
    device.configured_latency = 0.25
    device.default_latency = 1.0
    device.min_latency = 0.125
    device.max_latency = 21.333334
    device.supported_sample_rates = [48_000, 96_000]
    device.supported_encodings = [16, 24, 32]
    device.link_speed_mbps = 2500
    device.aes67_current = False
    device.aes67_configured = True
    device.aes67_supported = False
    device.is_locked = None
    device.clock_frequency_offset_parts_per_billion = -1_601
    device.clock_port_state_code = 0x0009
    device.clock_role = "Follower"
    device.clock_identity = "001dc1510295"
    device.leader_clock_identity = "001dc150692e"
    device.clock_port_records = [
        {
            "record_flags": 1,
            "link_down": False,
            "record_number": 2,
            "ptp_version": 2,
            "record_format_code": 2,
            "transport_path_code": 1,
            "transport_path": "multicast",
            "reserved_byte": 0,
            "network_interface_index": 2,
            "state_code": 9,
            "role": "Follower",
            "status_flags": 7,
        }
    ]

    interface = module.DanteDeviceInterface(device)

    assert interface.Latency() == 0.15
    assert interface.ActiveLatency() == 0.15
    assert interface.ConfiguredLatency() == 0.25
    assert interface.DefaultLatency() == 1.0
    assert interface.MinLatency() == 0.125
    assert interface.MaxLatency() == 21.333334
    assert interface.SupportedSampleRates() == [48_000, 96_000]
    assert interface.SupportedEncodings() == [16, 24, 32]
    assert interface.LinkSpeedMbps() == 2500
    assert interface.Aes67Enabled() is False
    assert interface.Aes67Supported() is False
    assert interface.Aes67SupportKnown() is True
    assert interface.Aes67MulticastPrefix() == ""
    assert interface.SampleRatePullupKnown() is False
    assert interface.TransmitterFlows() == []
    assert interface.LockStateKnown() is False
    assert interface.ClockFrequencyOffsetPartsPerBillion() == -1_601
    assert interface.ClockRole() == "Follower"
    assert interface.ClockPortRecords() == [(1, False, 2, 2, 2, 1, "multicast", 0, 2, 9, 7, "Follower")]
    assert interface.ClockIdentity() == "001dc1510295"
    assert interface.LeaderClockIdentity() == "001dc150692e"
    assert module.DanteDeviceInterface.Latency.__annotations__["return"] == "d"
    assert module.DanteDeviceInterface.SupportedSampleRates.__annotations__["return"] == "au"
    assert module.DanteDeviceInterface.SupportedEncodings.__annotations__["return"] == "au"
    assert module.DanteDeviceInterface.LinkSpeedMbps.__annotations__["return"] == "u"
    assert module.DanteDeviceInterface.MinLatency.__annotations__["return"] == "d"
    assert module.DanteDeviceInterface.MaxLatency.__annotations__["return"] == "d"
    assert module.DanteDeviceInterface.ClockFrequencyOffsetPartsPerBillion.__annotations__["return"] == "i"
    assert module.DanteDeviceInterface.ClockPortRecords.__annotations__["return"] == "a(qbqyyysyuqqs)"
    assert module.DanteDeviceInterface.ClockSourceCode.__annotations__["return"] == "q"
    assert module.DanteDeviceInterface.ClockSubdomain.__annotations__["return"] == "s"
    assert module.DanteDeviceInterface.Aes67MulticastPrefix.__annotations__["return"] == "s"
    assert module.DanteDeviceInterface.SupportedSampleRatePullupRawValues.__annotations__["return"] == "au"
    assert module.DanteDeviceInterface.TransmitterFlows.__annotations__["return"] == "a(uusuususs)"
    assert module.DanteChannelInterface.FactoryName.__annotations__["return"] == "s"


def test_channel_interface_exposes_factory_name(dbus_interfaces_without_dependency):
    module = dbus_interfaces_without_dependency
    channel = SimpleNamespace(factory_name="CH1", number=1, name="01", friendly_name="01")
    interface = module.DanteChannelInterface(channel)
    assert interface.FactoryName() == "CH1"


def test_shure_p10t_channel_optional_fields_are_schema_safe(dbus_interfaces_without_dependency):
    module = dbus_interfaces_without_dependency
    channel = ShureP10TChannel(
        number=1,
        name=None,
        frequency=None,
        audio_in_level=-12,
        rf_mute=True,
    )

    interface = module.ShureChannelInterface(channel)

    assert interface.Number() == 1
    assert interface.Name() == ""
    assert interface.Frequency() == 0
    assert interface.AudioGain() == -12
    assert interface.AudioMute() is True
    assert interface.AudioLevelPeak() == 0
    assert interface.TransmitterConnected() is False


def test_dbus_object_paths_are_stable_and_collision_resistant(dbus_service_without_dependency):
    module = dbus_service_without_dependency

    assert module._safe_name("rack-a.local.") == "rack_a_local_efcd16cc3150"
    assert module._safe_name("rack-a.local.") != module._safe_name("rack_a.local.")
    assert module._safe_mac("00:0e:dd:00:00:01") == module._safe_mac("00-0e-dd-00-00-01")
    assert module._safe_mac("invalid-a") != module._safe_mac("invalid_a")


@pytest.mark.asyncio
async def test_dbus_start_stop_restart_is_idempotent_and_refreshes_snapshots(
    dbus_service_without_dependency,
    monkeypatch,
):
    module = dbus_service_without_dependency
    device = DanteDevice(server_name="rack.local.")
    device.name = "Before restart"
    daemon = _daemon({device.server_name: device})
    buses = [FakeBus(), FakeBus()]
    monkeypatch.setattr(module, "MessageBus", lambda **kwargs: buses.pop(0))
    service = module.DBusService(daemon)

    await service.start()
    await service.start()
    assert service._listeners_registered is True
    assert all(len(callbacks) == 1 for callbacks in daemon.application.dispatcher._listeners.values())

    await service.stop()
    await service.stop()
    assert service._bus is None
    assert service._prop_snapshots == {}
    assert all(callbacks == [] for callbacks in daemon.application.dispatcher._listeners.values())

    device.name = "After restart"
    await service.start()
    assert service._prop_snapshots[device.server_name]["name"] == "After restart"
    assert service._dante_interfaces[device.server_name].emitted == []
    await service.stop()


@pytest.mark.asyncio
async def test_dbus_partial_start_and_name_conflict_clean_up_bus(
    dbus_service_without_dependency,
    monkeypatch,
):
    module = dbus_service_without_dependency
    device = DanteDevice(server_name="rack.local.")
    daemon = _daemon({device.server_name: device})

    export_failure = FakeBus(fail_export_at=2)
    service = module.DBusService(daemon)
    connect_failure = FakeBus(fail_connect=True)
    monkeypatch.setattr(module, "MessageBus", lambda **kwargs: connect_failure)
    with pytest.raises(RuntimeError, match="connect failed"):
        await service.start()
    assert connect_failure.disconnected is True
    assert service._bus is None

    with pytest.raises(RuntimeError, match="export failed"):
        monkeypatch.setattr(module, "MessageBus", lambda **kwargs: export_failure)
        await service.start()
    assert export_failure.disconnected is True
    assert service._bus is None
    assert service._listeners_registered is False

    name_conflict = FakeBus(request_reply=3)
    monkeypatch.setattr(module, "MessageBus", lambda **kwargs: name_conflict)
    with pytest.raises(RuntimeError, match="name .* unavailable"):
        await service.start()
    assert name_conflict.disconnected is True
    assert service._bus is None


@pytest.mark.asyncio
async def test_dbus_dante_update_rebinds_device_and_emits_full_schema_changes(
    dbus_service_without_dependency,
):
    module = dbus_service_without_dependency
    old = DanteDevice(server_name="rack.local.")
    old.name = "Old"
    daemon = _daemon({old.server_name: old})
    service = module.DBusService(daemon)
    service._bus = FakeBus()
    assert service._export_dante_device(old.server_name, old) is True
    interface = service._dante_interfaces[old.server_name]

    replacement = DanteDevice(server_name=old.server_name)
    replacement.name = "New"
    replacement.model = "Model X"
    replacement.min_latency = 0.125
    replacement.max_latency = 21.333334
    replacement.last_seen = 1234.5
    daemon.devices[old.server_name] = replacement

    await service._on_dante_updated(DanteEvent(type=EventType.DEVICE_UPDATED, server_name=old.server_name))

    assert interface._device is replacement
    changed = interface.emitted[-1]
    assert changed["Name"] == "New"
    assert changed["Model"] == "Model X"
    assert changed["MinLatency"] == 0.125
    assert changed["MaxLatency"] == 21.333334
    assert changed["LastSeen"] == 1234.5


@pytest.mark.asyncio
async def test_dbus_dante_update_publishes_known_lock_state_becoming_unknown(
    dbus_service_without_dependency,
):
    module = dbus_service_without_dependency
    device = DanteDevice(server_name="rack.local.")
    device.is_locked = True
    daemon = _daemon({device.server_name: device})
    service = module.DBusService(daemon)
    service._bus = FakeBus()
    assert service._export_dante_device(device.server_name, device) is True
    interface = service._dante_interfaces[device.server_name]

    device.is_locked = None
    await service._on_dante_updated(DanteEvent(type=EventType.DEVICE_UPDATED, server_name=device.server_name))

    changed = interface.emitted[-1]
    assert changed["IsLocked"] is False
    assert changed["LockStateKnown"] is False


@pytest.mark.asyncio
async def test_dbus_failed_property_emit_keeps_snapshot_for_retry(
    dbus_service_without_dependency,
):
    module = dbus_service_without_dependency
    device = DanteDevice(server_name="rack.local.")
    device.name = "Old"
    daemon = _daemon({device.server_name: device})
    service = module.DBusService(daemon)
    service._bus = FakeBus()
    service._export_dante_device(device.server_name, device)
    interface = service._dante_interfaces[device.server_name]
    interface.emit_properties_changed = MagicMock(side_effect=(RuntimeError("bus busy"), None))
    device.name = "New"
    event = DanteEvent(type=EventType.DEVICE_UPDATED, server_name=device.server_name)

    await service._on_dante_updated(event)
    assert service._prop_snapshots[device.server_name]["name"] == "Old"
    await service._on_dante_updated(event)

    assert interface.emit_properties_changed.call_count == 2
    assert service._prop_snapshots[device.server_name]["name"] == "New"


@pytest.mark.asyncio
async def test_dbus_shure_update_rebinds_device_and_emits_real_changes(
    dbus_service_without_dependency,
):
    module = dbus_service_without_dependency
    mac = "00:0e:dd:00:00:01"
    old = ShureDeviceInfo(
        ip="192.0.2.4",
        mac=mac,
        device_type=ShureDeviceType.ad4d,
        name="Old",
    )
    shure = SimpleNamespace(devices={mac: old})
    daemon = _daemon(shure=shure)
    service = module.DBusService(daemon)
    service._bus = FakeBus()
    assert service._export_shure_device(mac, old) is True
    interface = service._shure_interfaces[mac]

    replacement = ShureDeviceInfo(
        ip="192.0.2.5",
        mac=mac,
        device_type=ShureDeviceType.ad4d,
        name="New",
        firmware_version="1.2.3",
    )
    shure.devices[mac] = replacement
    await service._on_shure_updated(DanteEvent(type=EventType.SHURE_DEVICE_UPDATED, device_name=mac))

    assert interface._device is replacement
    assert interface.emitted[-1] == {
        "Ip": "192.0.2.5",
        "Name": "New",
        "FirmwareVersion": "1.2.3",
    }
