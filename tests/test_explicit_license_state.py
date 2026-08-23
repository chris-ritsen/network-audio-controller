import json

from netaudio.commands.device_display import _device_show_rows
from netaudio.dante.const import SERVICE_ARC, SERVICE_CMC
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer


def service(service_type: str, properties: dict) -> dict:
    return {
        "ipv4": "192.168.1.34",
        "name": f"a32.{service_type}",
        "port": 4440,
        "properties": properties,
        "server_name": "A32-19245c.local.",
        "type": service_type,
    }


def test_arc_unlicensed_marker_is_explicit_negative_state():
    device = DanteDevice(server_name="A32-19245c.local.")
    device.services = {"a32._netaudio-arc._udp.local.": service(SERVICE_ARC, {"unlicensed": None})}

    assert device.is_licensed is False


def test_marker_absence_does_not_assert_positive_license_state():
    device = DanteDevice(server_name="A32-19245c.local.")
    device.services = {"a32._netaudio-arc._udp.local.": service(SERVICE_ARC, {"router_vers": "4.0.1"})}

    assert device.is_licensed is None
    assert "is_licensed" not in DanteDeviceSerializer.to_json(device)


def test_marker_is_scoped_to_arc_service():
    device = DanteDevice(server_name="A32-19245c.local.")
    device.services = {"a32._netaudio-cmc._udp.local.": service(SERVICE_CMC, {"unlicensed": None})}

    assert device.is_licensed is None


def test_serialized_negative_state_is_derived_from_preserved_service_properties():
    device = DanteDevice(server_name="A32-19245c.local.")
    device.ipv4 = "192.168.1.34"
    device.services = {"a32._netaudio-arc._udp.local.": service(SERVICE_ARC, {"unlicensed": None})}

    serialized = DanteDeviceSerializer.to_json(device)
    restored = DanteDeviceSerializer.device_from_json(json.loads(json.dumps(serialized)))

    assert serialized["is_licensed"] is False
    assert restored.is_licensed is False


def test_device_show_displays_only_explicit_negative_state():
    device = DanteDevice(server_name="A32-19245c.local.")
    device.services = {"a32._netaudio-arc._udp.local.": service(SERVICE_ARC, {"unlicensed": None})}

    assert dict(_device_show_rows(device))["License State"] == "unlicensed"

    device.services = {"a32._netaudio-arc._udp.local.": service(SERVICE_ARC, {"router_vers": "4.0.1"})}

    assert "License State" not in dict(_device_show_rows(device))
