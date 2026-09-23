import pytest

from netaudio import core


@pytest.mark.parametrize(
    "kind,value,expected",
    [
        ("mac", "00:1D:C1:FF:FE:50:69:2E", "001dc150692e"),
        ("ptpv1", [0, 29, 193, 80, 105, 46], "001dc150692e"),
        ("ptpv1", "00-1D-C1-50-69-2E", "001dc150692e"),
        ("ptpv1", [0] * 6, "000000000000"),
        ("ptpv1", [False] * 6, None),
        ("ptpv1", "001dc1fffe50692e", None),
        ("managed_device", "001DC1FFFE50692E:0", "001dc1fffe50692e"),
        ("managed_device", "001dc1fffe50692e:1", None),
        ("managed_device", "a" * 32, None),
        ("managed_inventory", "A" * 32, "a" * 32),
        ("managed_domain", "A" * 32, "a" * 32),
        ("managed_domain", "a" * 31, None),
        ("managed_primary", "00:1d:c1:50:69:2e", "001dc1fffe50692e"),
        ("managed_primary", "00:00:00:00:00:00", None),
        ("managed_primary", "01:00:00:00:00:22", None),
        ("managed_primary", None, None),
    ],
)
def test_native_identity_normalization_preserves_identity_kind(kind, value, expected):
    assert core.device_identity({"kind": kind, "value": value}) == expected


def test_native_identity_rejects_unknown_formats_instead_of_guessing():
    with pytest.raises(core.NetaudioCoreError):
        core.device_identity({"kind": "future_device", "value": "001dc150692e"})
