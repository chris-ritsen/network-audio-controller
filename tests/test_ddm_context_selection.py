from netaudio.cli import state
from netaudio.cli_support.selection import filter_devices
from netaudio.dante.device import DanteDevice


def _device(name: str, context: str | None = None) -> DanteDevice:
    device = DanteDevice(name)
    device.name = name
    if context is not None:
        device.ddm_device_id = f"id-{name}"
        device.ddm_context = context
    return device


def test_context_filters_managed_devices_but_retains_local_unmanaged_devices():
    original = state.ddm_context
    try:
        state.ddm_context = "east-main"
        devices = {
            "east": _device("east", "east-main"),
            "west": _device("west", "west-main"),
            "local": _device("local"),
        }

        assert set(filter_devices(devices)) == {"east", "local"}
    finally:
        state.ddm_context = original
