import json

import pytest

from netaudio.dante.device import DanteDevice
from netaudio.dante.device_kind import device_kind, mac_address_encodes_ipv4
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.virtual_device import VirtualDeviceConfig


def make_device(name: str, mac_address: str, ipv4: str = "192.168.1.50", **attributes) -> DanteDevice:
    device = DanteDevice(server_name=f"{name}.local.")
    device.name = name
    device.mac_address = mac_address
    device.ipv4 = ipv4
    for attribute_name, attribute_value in attributes.items():
        setattr(device, attribute_name, attribute_value)
    return device


def test_device_json_includes_kind():
    device = make_device("avio-usb-1", "001dc1fffe50368b", manufacturer="Audinate", dante_model="AVIO-USB")

    as_json = json.loads(json.dumps(DanteDeviceSerializer.to_json(device), default=str))

    assert as_json["kind"] == "hardware"


@pytest.mark.parametrize(
    ("name", "mac_address", "ipv4", "attributes", "expected"),
    [
        ("avio-usb-1", "001dc1fffe50368b", "192.168.1.247", {"manufacturer": "Audinate"}, "hardware"),
        ("ad4d", "000eddfd4e130000", "192.168.1.37", {"manufacturer": "Shure Inc."}, "hardware"),
        ("studio-media-b", "5254001234560000", "192.168.1.107", {"model_id": "APEC-TRIMEDIA"}, "emulated"),
        ("studio-media", "525400fffe123456", "192.168.1.38", {"model_id": "Crusoe"}, "emulated"),
        ("netaudio-page-probe", "0000c0a801f90000", "192.168.1.249", {"model_id": "DIOUSB"}, "emulated"),
        ("lab-probe", "00000a0000070000", "10.0.0.7", {"model_id": "DIOUSB"}, "emulated"),
        ("netaudio-page-probe2", "001dc1fffe000001", "192.168.1.249", {}, "emulated"),
        (
            "netaudio-virtual",
            "0000c0a801050000",
            "192.168.1.5",
            {"manufacturer": VirtualDeviceConfig().manufacturer, "model": VirtualDeviceConfig().model},
            "virtual",
        ),
        ("studio-virtual", "02a1b2c3d4e5", "192.168.1.6", {"dante_model": VirtualDeviceConfig().model}, "virtual"),
    ],
)
def test_device_kind_classification(name, mac_address, ipv4, attributes, expected):
    device = make_device(name, mac_address, ipv4, **attributes)

    assert device_kind(device) == expected
    assert device.kind == expected


def test_mac_address_encodes_ipv4_matches_the_device_address_outside_private_prefix():
    assert mac_address_encodes_ipv4("0000c0a801f90000", "192.168.1.249") is True
    assert mac_address_encodes_ipv4("00000a0000070000", "10.0.0.7") is True
    assert mac_address_encodes_ipv4("00000a0000070000", "10.0.0.8") is False
    assert mac_address_encodes_ipv4("001dc1fffe50368b", "192.168.1.247") is False
    assert mac_address_encodes_ipv4(None, "192.168.1.247") is False
