import pytest

from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.operation_availability import operation_availability, operation_availability_map, require_writable


def device_for(operation: str) -> DanteDevice:
    device = DanteDevice(server_name="device.local.")
    capability_fields = {
        "identify": "identify_supported",
        "sample_rate": "sample_rate_configuration_supported",
        "encoding": "encoding_configuration_supported",
        "sample_rate_pullup": "sample_rate_pullup_configuration_supported",
        "aes67": "aes67_configuration_supported",
        "static_ipv4": "static_ipv4_configuration_supported",
        "redundancy": "switch_redundancy_supported",
        "codec_control": "generic_codec_control_supported",
        "locking": "device_locking_supported",
    }
    setattr(device, capability_fields[operation], True)
    device.is_locked = False
    if operation == "sample_rate":
        device.sample_rate = 48_000
        device.sample_rate_update_mode = 2
        device.supported_sample_rates = [48_000, 96_000]
    elif operation == "encoding":
        device.encoding = 24
        device.encoding_update_mode = 1
        device.supported_encodings = [16, 24, 32]
    elif operation == "sample_rate_pullup":
        device.sample_rate_pullup_raw_value = 0
        device.sample_rate_pullup_update_mode = 2
        device.sample_rate_pullup_flags = 0
        device.supported_sample_rate_pullup_raw_values = [0, 1, 2]
    elif operation == "aes67":
        device.aes67_current = False
    elif operation == "static_ipv4":
        device.interfaces = [{"interface": "primary", "mode": "dynamic"}]
        device.static_ipv4_configuration_read_only = False
    elif operation == "redundancy":
        device.ipv4 = "192.0.2.10"
        device.control_transports = ["direct"]
        device.interface_status_protocol = 0x0724
        device.redundancy_advertised_support_source = {"fresh": True, "field_reported": True}
        device.redundancy_read_only_source = {"fresh": True, "field_reported": True}
        device.dante_redundancy = {
            "current": "switched",
            "configured": "switched",
            "state_fresh": True,
            "available_modes": [
                {"code": 0, "label": "Switched", "mode": "switched"},
                {"code": 1, "label": "Redundant", "mode": "redundant"},
            ],
            "available_modes_source": "interface_status_flag_cohort",
            "available_modes_fresh": True,
        }
        device.switch_redundancy_read_only = False
    elif operation == "codec_control":
        device.codec_parameters = [{"parameter_type": 1, "mode": 2, "values": [5]}]
        device.gain_adapter = {"device_type": "input", "supported_levels": [1, 2, 3, 4, 5]}
    return device


@pytest.mark.parametrize(
    "operation",
    [
        "identify",
        "sample_rate",
        "encoding",
        "sample_rate_pullup",
        "aes67",
        "static_ipv4",
        "redundancy",
        "codec_control",
        "locking",
    ],
)
def test_each_operation_requires_its_advertised_capability(operation):
    device = device_for(operation)
    available = operation_availability(device, operation)
    assert available.supported is True
    assert available.writable is True

    capability_field = {
        "identify": "identify_supported",
        "sample_rate": "sample_rate_configuration_supported",
        "encoding": "encoding_configuration_supported",
        "sample_rate_pullup": "sample_rate_pullup_configuration_supported",
        "aes67": "aes67_configuration_supported",
        "static_ipv4": "static_ipv4_configuration_supported",
        "redundancy": "switch_redundancy_supported",
        "codec_control": "generic_codec_control_supported",
        "locking": "device_locking_supported",
    }[operation]
    setattr(device, capability_field, False)
    unavailable = operation_availability(device, operation)
    assert unavailable.supported is False
    assert unavailable.readable == available.readable
    assert unavailable.writable is False
    assert "unsupported" in unavailable.reasons


def test_readable_component_does_not_create_support_or_writability():
    device = device_for("sample_rate")
    device.sample_rate_configuration_supported = False
    availability = operation_availability(device, "sample_rate", 48_000)
    assert availability.readable is True
    assert availability.writable is False


def test_configurable_components_require_known_writable_modes_and_advertised_values():
    device = device_for("sample_rate")
    assert operation_availability(device, "sample_rate", 96_000).writable is True

    device.sample_rate_update_mode = 0
    assert operation_availability(device, "sample_rate", 96_000).reasons == ("fixed",)
    device.sample_rate_update_mode = 77
    assert operation_availability(device, "sample_rate", 96_000).reasons == ("update_mode_unknown",)
    device.sample_rate_update_mode = 2
    assert operation_availability(device, "sample_rate", 44_100).reasons == ("value_not_advertised",)


def test_pullup_host_disable_flag_blocks_writes_and_empty_choices_remain_valid():
    device = device_for("sample_rate_pullup")
    device.sample_rate_pullup_flags = 1
    assert "host_disabled" in operation_availability(device, "sample_rate_pullup", 1).reasons

    device.sample_rate_pullup_flags = 0
    device.supported_sample_rate_pullup_raw_values = []
    assert operation_availability(device, "sample_rate_pullup", 99).writable is True


def test_network_read_only_masks_and_lock_state_block_writes():
    static = device_for("static_ipv4")
    static.static_ipv4_configuration_read_only = True
    assert "read_only" in operation_availability(static, "static_ipv4").reasons

    redundancy = device_for("redundancy")
    redundancy.switch_redundancy_read_only = True
    assert "read_only" in operation_availability(redundancy, "redundancy").reasons

    encoding = device_for("encoding")
    encoding.is_locked = True
    assert "device_locked" in operation_availability(encoding, "encoding", 24).reasons
    encoding.is_locked = None
    assert "lock_state_unknown" in operation_availability(encoding, "encoding", 24).reasons


def test_managed_operation_permission_is_explicit_and_operation_scoped():
    device = device_for("sample_rate")
    device.ddm_enrolment_state = "ENROLLED"
    device.managed_operation_permissions = {"encoding": True}
    unavailable = operation_availability(device, "sample_rate", 48_000)
    assert "managed_permission_missing" in unavailable.reasons

    device.managed_operation_permissions["sample_rate"] = False
    assert "managed_permission_denied" in operation_availability(device, "sample_rate", 48_000).reasons

    device.managed_operation_permissions["sample_rate"] = True
    assert operation_availability(device, "sample_rate", 48_000).writable is True


def test_codec_control_requires_an_established_device_adapter():
    device = device_for("codec_control")
    device.gain_adapter = None
    assert operation_availability(device, "codec_control").reasons == ("no_device_adapter",)


def test_require_writable_attempts_operations_whose_capability_is_unverified():
    device = device_for("identify")
    device.identify_supported = None

    assert operation_availability(device, "identify").reasons == ("capability_unknown",)
    require_writable(device, "identify")


def test_require_writable_reports_all_decision_reasons_and_serializer_exposes_map():
    device = device_for("encoding")
    device.encoding_configuration_supported = False
    device.encoding_update_mode = 0
    device.is_locked = True
    with pytest.raises(RuntimeError, match="unsupported, fixed, device_locked"):
        require_writable(device, "encoding", 24)

    serialized = DanteDeviceSerializer.to_json(device)
    assert serialized["operation_availability"] == operation_availability_map(device)
