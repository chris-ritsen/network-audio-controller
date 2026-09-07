from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from zeroconf import IPVersion

from netaudio import core
from netaudio.common import app_config
from netaudio.core import binding
from netaudio.dante import browser, core_transport, flows, lock
from netaudio.daemon import server


def adapter(name, *addresses):
    return SimpleNamespace(nice_name=name, ips=[SimpleNamespace(ip=address) for address in addresses])


@pytest.fixture
def selected_interface(monkeypatch):
    settings = app_config.AppSettings()
    settings.interface = "selected"
    adapters = [adapter("selected", "192.0.2.62")]
    monkeypatch.setattr(app_config.ifaddr, "get_adapters", lambda: adapters)
    for module in (browser, core_transport, flows, lock, server):
        monkeypatch.setattr(module, "app_settings", settings)
    return settings, adapters


def test_interface_address_is_resolved_fresh_and_never_falls_back(selected_interface):
    settings, adapters = selected_interface
    assert settings.interface_ip == "192.0.2.62"
    adapters[:] = [adapter("selected", "192.0.2.63")]
    assert settings.interface_ip == "192.0.2.63"
    adapters.clear()
    with pytest.raises(RuntimeError, match="was not found"):
        _ = settings.interface_ip
    adapters[:] = [adapter("selected", ("::1", 0, 0))]
    with pytest.raises(RuntimeError, match="has no IPv4 address"):
        _ = settings.interface_ip
    settings.interface = None
    assert settings.interface_ip is None


def test_library_discovery_uses_only_the_selected_address(selected_interface):
    settings, _ = selected_interface
    instance = browser.DanteBrowser(0)
    assert instance.get_zeroconf_kwargs() == {"ip_version": IPVersion.V4Only, "interfaces": ["192.0.2.62"]}
    settings.interface = None
    assert instance.get_zeroconf_kwargs() == {"ip_version": IPVersion.V4Only}


@pytest.mark.asyncio
async def test_library_discovery_does_not_start_when_the_interface_disappears(monkeypatch, selected_interface):
    _, adapters = selected_interface
    adapters.clear()
    constructor = Mock()
    monkeypatch.setattr(browser, "AsyncZeroconf", constructor)
    with pytest.raises(RuntimeError, match="was not found"):
        await browser.DanteBrowser(0).async_run()
    constructor.assert_not_called()


@pytest.mark.asyncio
async def test_control_cache_separates_source_addresses_and_rejects_missing_interface(monkeypatch, selected_interface):
    settings, adapters = selected_interface
    constructor = Mock(side_effect=lambda *args, **kwargs: Mock())
    monkeypatch.setattr(core, "CoreClient", constructor)
    transport = core_transport.CoreTransport()
    first = transport.client("192.0.2.10")
    assert transport.client("192.0.2.10") is first
    await transport.call("192.0.2.10", lambda client: client.get_device_name())
    first.get_device_name.assert_called_once_with()
    constructor.assert_called_once_with("192.0.2.10", arc_port=4440, timeout_ms=1000, attempts=3, local_ip="192.0.2.62")
    first.set_host_mac.assert_not_called()
    adapters[:] = [adapter("selected", "192.0.2.63")]
    second = transport.client("192.0.2.10")
    assert second is not first
    assert constructor.call_args.kwargs["local_ip"] == "192.0.2.63"
    adapters.clear()
    with pytest.raises(RuntimeError, match="was not found"):
        await transport.call("192.0.2.10", lambda client: client.get_device_name())
    assert constructor.call_count == 2
    settings.interface = None
    third = transport.client("192.0.2.10")
    assert third is not first and third is not second
    assert constructor.call_args.kwargs["local_ip"] is None
    transport.close()
    for client in (first, second, third):
        client.close.assert_called_once_with()


@pytest.mark.parametrize("local_ip", [None, "127.0.0.1"])
def test_python_constructor_passes_explicit_source_to_native(monkeypatch, local_ip):
    library = Mock()

    def create(device, source, port, timeout, attempts, handle):
        assert (device, source, port, timeout, attempts) == (
            b"127.0.0.1",
            local_ip.encode() if local_ip is not None else None,
            4440,
            1000,
            3,
        )
        handle._obj.value = 1
        return 0

    library.netaudio_client_new.side_effect = create
    monkeypatch.setattr(binding, "require", lambda: library)
    with core.CoreClient("127.0.0.1", local_ip=local_ip):
        pass
    library.netaudio_client_new.assert_called_once()
    library.netaudio_client_free.assert_called_once()


@pytest.mark.asyncio
async def test_direct_callers_pass_the_selected_source(monkeypatch, selected_interface):
    from unittest.mock import MagicMock

    native = MagicMock()
    native.__enter__.return_value = native
    native.lock.return_value = {"status": 0}
    constructor = Mock(return_value=native)
    monkeypatch.setattr(core, "CoreClient", constructor)
    monkeypatch.setattr(core, "build_command", lambda specification: b"packet")
    assert server._probe_device("192.0.2.10", 4540)
    await lock._device_lock_operation("192.0.2.10", "1234", b"k" * 32, lock.LOCK_OPERATION_LOCK)
    await flows._request("192.0.2.10", 4540, {"command": "channel_count"}, 100, 1)
    assert constructor.call_count == 3
    assert all(call.kwargs["local_ip"] == "192.0.2.62" for call in constructor.call_args_list)


@pytest.mark.parametrize("address", ["127.0.0.1\0ignored", "", "::1", "device.local", True, 2130706433])
def test_python_source_address_validation_precedes_native_constructor(monkeypatch, address):
    loader = Mock()
    monkeypatch.setattr(binding, "require", loader)
    with pytest.raises(ValueError):
        core.CoreClient("127.0.0.1", local_ip=address)
    loader.assert_not_called()


@pytest.mark.asyncio
async def test_direct_callers_never_open_a_client_for_a_missing_interface(monkeypatch, selected_interface):
    _, adapters = selected_interface
    adapters.clear()
    constructor = Mock()
    monkeypatch.setattr(core, "CoreClient", constructor)
    with pytest.raises(RuntimeError, match="was not found"):
        server._probe_device("192.0.2.10", 4540)
    with pytest.raises(RuntimeError, match="was not found"):
        await lock._device_lock_operation("192.0.2.10", "1234", b"k" * 32, lock.LOCK_OPERATION_LOCK)
    with pytest.raises(RuntimeError, match="was not found"):
        await flows._request("192.0.2.10", 4540, {"command": "channel_count"}, 100, 1)
    constructor.assert_not_called()
