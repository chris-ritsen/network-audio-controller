use super::*;

#[test]
fn system_reset_modes_match_authentic_a32_frames() {
    let mac = [0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24];
    assert_eq!(
        build_reboot(mac, 0x18A4).unwrap(),
        decode_hexadecimal(
            "ffff002418a400003e42274cff240000417564696e617465073a00900000006400010000"
        )
    );
    assert_eq!(
        build_factory_reset(mac, 0x18A4).unwrap(),
        decode_hexadecimal(
            "ffff002418a400003e42274cff240000417564696e617465073a00900000006400010001"
        )
    );
    assert_eq!(
        build_factory_reset(mac, 0),
        Err(NetaudioError::InvalidSequence)
    );
}

#[test]
fn lock_reset_probe_matches_shipping_controller_request() {
    assert_eq!(
        build_probe_lock_reset_status([0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24], 0x18C1, 100,).unwrap(),
        decode_hexadecimal("ffff002018c100003e42274cff240000417564696e617465073a100800000064")
    );
}

#[test]
fn link_status_probe_matches_authentic_a32_request() {
    assert_eq!(
        build_probe_link_status([0x52, 0x55, 0x0A, 0x00, 0x02, 0x02], 0x0047).unwrap(),
        decode_hexadecimal(
            "ffff00380047000052550a0002020000417564696e617465073a004100000000000000000000000000000000000000000000000000000000"
        )
    );
    assert_eq!(
        build_probe_link_status([0x52, 0x55, 0x0A, 0x00, 0x02, 0x02], 0),
        Err(NetaudioError::InvalidSequence)
    );
}

#[test]
fn switch_configuration_probe_matches_shipping_controller_request() {
    assert_eq!(
        build_probe_switch_configuration([0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24], 0x97BE).unwrap(),
        decode_hexadecimal(
            "ffff002497be00003e42274cff240000417564696e617465073a00150000006400000000"
        )
    );
    assert_eq!(
        build_probe_switch_configuration([0x3E, 0x42, 0x27, 0x4C, 0xFF, 0x24], 0),
        Err(NetaudioError::InvalidSequence)
    );
}

#[test]
fn device_log_export_matches_authentic_a32_request() {
    assert_eq!(
        build_device_log_export([0x52, 0x55, 0x0A, 0x00, 0x02, 0x02], 1).unwrap(),
        decode_hexadecimal(
            "ffff00280001000052550a0002020000417564696e6174650724ff04000000004c4f475300010000"
        )
    );
    assert_eq!(
        build_device_log_export([0x52, 0x55, 0x0A, 0x00, 0x02, 0x02], 0),
        Err(NetaudioError::InvalidSequence)
    );
}

#[test]
fn capability_partition_export_matches_physical_a32_request() {
    assert_eq!(
        build_capability_partition_export([0xC2, 0x0F, 0x45, 0x68, 0x99, 0xF5], 1,).unwrap(),
        decode_hexadecimal(
            "ffff002800010000c20f456899f50000417564696e6174650724ff04000000004341503100020000"
        )
    );
    assert_eq!(
        build_capability_partition_export([0xC2, 0x0F, 0x45, 0x68, 0x99, 0xF5], 0),
        Err(NetaudioError::InvalidSequence)
    );
}

#[test]
fn clear_configuration_requests_match_authentic_a32_frames() {
    let mac = [0xFE, 0xC9, 0xCA, 0x09, 0xA6, 0xD5];
    assert_eq!(
        build_probe_clear_configuration_status(mac, 0x01ED).unwrap(),
        decode_hexadecimal(
            "ffff002401ed0000fec9ca09a6d50000417564696e617465073e00770000006400000000"
        )
    );
    assert_eq!(
        build_clear_all_configuration(mac, 0x01ED).unwrap(),
        decode_hexadecimal(
            "ffff002401ed0000fec9ca09a6d50000417564696e617465073e00770000006400000001"
        )
    );
    assert_eq!(
        build_clear_all_configuration_preserving_internet_protocol_settings(mac, 0x01ED).unwrap(),
        decode_hexadecimal(
            "ffff002401ed0000fec9ca09a6d50000417564696e617465073e00770000006400000002"
        )
    );
    assert_eq!(
        build_probe_clear_configuration_status(mac, 0),
        Err(NetaudioError::InvalidSequence)
    );
    assert_eq!(
        build_clear_all_configuration(mac, 0),
        Err(NetaudioError::InvalidSequence)
    );
    assert_eq!(
        build_clear_all_configuration_preserving_internet_protocol_settings(mac, 0),
        Err(NetaudioError::InvalidSequence)
    );
}
