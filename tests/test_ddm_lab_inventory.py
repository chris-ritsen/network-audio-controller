from __future__ import annotations

import pytest

from tools.ddm_lab.inventory import matching_inventory_device
from tools.ddm_lab.lifecycle import LabError


VIRTUAL_MAC = "02:00:00:00:00:01"
VIRTUAL_IPV4 = "192.0.2.10"


def _device(device_id: str, *, mac_address: str | None, address: str = VIRTUAL_IPV4) -> dict:
    interface = {"address": address}
    if mac_address is not None:
        interface["macAddress"] = mac_address
    return {
        "id": device_id,
        "name": f"device-{device_id}",
        "interfaces": [interface],
        "connection": {"state": "READY"},
    }


def test_ip_fallback_rejects_a_conflicting_nonempty_mac() -> None:
    inventory = [_device("physical-device", mac_address="00:11:22:33:44:55")]

    with pytest.raises(LabError, match="MAC that conflicts"):
        matching_inventory_device(inventory, VIRTUAL_MAC, {VIRTUAL_IPV4})


def test_ip_fallback_rejects_ambiguous_devices() -> None:
    inventory = [
        _device("virtual-a", mac_address=None),
        _device("virtual-b", mac_address=None),
    ]

    with pytest.raises(LabError, match="IPv4 address to more than one device"):
        matching_inventory_device(inventory, VIRTUAL_MAC, {VIRTUAL_IPV4})


@pytest.mark.parametrize("explicit_null", [False, True])
def test_unique_ip_fallback_accepts_a_device_without_a_reported_mac(explicit_null: bool) -> None:
    device = _device("virtual-device", mac_address=None)
    if explicit_null:
        device["interfaces"][0]["macAddress"] = None
    selected = matching_inventory_device(
        [device],
        VIRTUAL_MAC,
        {VIRTUAL_IPV4},
    )

    assert selected is not None
    assert selected["id"] == "virtual-device"
    assert selected["interfaces"] == [{"macAddress": None, "address": VIRTUAL_IPV4}]
