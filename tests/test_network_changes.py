import asyncio
import socket
from unittest.mock import Mock

import pytest

from netaudio import network_changes, network_path
from netaudio.network_changes import NetworkChangeError, NetworkChangeMonitor


class FakeChangeSource:
    def __init__(self) -> None:
        self.callback = None
        self.closed = False

    def start(self, _loop, callback) -> None:
        self.callback = callback

    def emit(self) -> None:
        assert self.callback is not None
        self.callback()

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_monitor_shares_one_native_source_and_coalesces_event_bursts(monkeypatch):
    source = FakeChangeSource()
    invalidated = Mock()
    monkeypatch.setattr(network_path, "invalidate_platform_preferences", invalidated)
    monitor = NetworkChangeMonitor(source_factory=lambda: source, debounce_seconds=0)
    first = Mock()
    second = Mock()

    unsubscribe_first = monitor.subscribe(first)
    unsubscribe_second = monitor.subscribe(second)
    source.emit()
    source.emit()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    invalidated.assert_called_once_with()
    first.assert_called_once_with()
    second.assert_called_once_with()
    unsubscribe_first()
    assert source.closed is False
    unsubscribe_second()
    assert source.closed is True


class FakeNetlinkSocket:
    def __init__(self) -> None:
        self.bound = None
        self.closed = False

    def bind(self, address) -> None:
        self.bound = address

    def close(self) -> None:
        self.closed = True


def test_linux_source_subscribes_to_link_address_and_route_groups(monkeypatch):
    change_socket = FakeNetlinkSocket()
    constructor = Mock(return_value=change_socket)
    monkeypatch.setattr(network_changes.socket, "AF_NETLINK", 16, raising=False)
    monkeypatch.setattr(network_changes.socket, "NETLINK_ROUTE", 0, raising=False)
    monkeypatch.setattr(network_changes.socket, "socket", constructor)

    source = network_changes._linux_change_source()

    constructor.assert_called_once_with(16, socket.SOCK_RAW, 0)
    assert change_socket.bound == (
        0,
        network_changes.LINUX_RTMGRP_LINK
        | network_changes.LINUX_RTMGRP_IPV4_IFADDR
        | network_changes.LINUX_RTMGRP_IPV4_ROUTE,
    )
    source.close()
    assert change_socket.closed is True


def test_unknown_platform_fails_instead_of_silently_polling(monkeypatch):
    monkeypatch.setattr(network_changes.sys, "platform", "unsupported")

    with pytest.raises(NetworkChangeError, match="unsupported"):
        network_changes._platform_change_source()


@pytest.mark.asyncio
async def test_failed_native_source_does_not_retain_a_subscription():
    source = FakeChangeSource()
    attempts = iter((NetworkChangeError("unavailable"), source))

    def source_factory():
        result = next(attempts)
        if isinstance(result, Exception):
            raise result
        return result

    monitor = NetworkChangeMonitor(source_factory=source_factory, debounce_seconds=0)
    stale_callback = Mock()
    with pytest.raises(NetworkChangeError, match="unavailable"):
        monitor.subscribe(stale_callback)

    active_callback = Mock()
    unsubscribe = monitor.subscribe(active_callback)
    source.emit()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    stale_callback.assert_not_called()
    active_callback.assert_called_once_with()
    unsubscribe()
