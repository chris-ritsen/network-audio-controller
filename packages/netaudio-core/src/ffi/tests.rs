use super::*;
use crate::test_support::decode_hexadecimal;
use std::ffi::CString;

fn lock_token_call_with_pin(pin: &str, nonce: &[u8], key: &[u8]) -> (NetaudioStatus, Vec<u8>) {
    let pin = CString::new(pin).unwrap();
    let mut output = vec![0u8; 64];
    let mut output_length = 0usize;
    let status = unsafe {
        netaudio_lock_token(
            pin.as_ptr(),
            nonce.as_ptr(),
            nonce.len(),
            key.as_ptr(),
            key.len(),
            output.as_mut_ptr(),
            output.len(),
            &mut output_length,
        )
    };
    output.truncate(output_length);
    (status, output)
}

fn lock_token_call(nonce: &[u8], key: &[u8]) -> (NetaudioStatus, Vec<u8>) {
    lock_token_call_with_pin("1234", nonce, key)
}

fn client_lock_call(key: &[u8], locking: bool) -> NetaudioStatus {
    let inner = Client::new(
        "127.0.0.1".parse().unwrap(),
        4440,
        Duration::from_millis(1),
        1,
    )
    .unwrap();
    let mut client = NetaudioClient {
        inner: Mutex::new(inner),
    };
    let pin = CString::new("1234").unwrap();
    let mut output = vec![0u8; 64];
    let mut output_length = 0usize;

    if locking {
        unsafe {
            netaudio_client_lock(
                &mut client,
                pin.as_ptr(),
                key.as_ptr(),
                key.len(),
                output.as_mut_ptr(),
                output.len(),
                &mut output_length,
            )
        }
    } else {
        unsafe {
            netaudio_client_unlock(
                &mut client,
                pin.as_ptr(),
                key.as_ptr(),
                key.len(),
                output.as_mut_ptr(),
                output.len(),
                &mut output_length,
            )
        }
    }
}

fn build_command_status(json: &str) -> NetaudioStatus {
    let json = CString::new(json).unwrap();
    let mut output = vec![0u8; 256];
    let mut output_length = 0usize;
    unsafe {
        netaudio_build_command(
            json.as_ptr(),
            output.as_mut_ptr(),
            output.len(),
            &mut output_length,
        )
    }
}

fn parse_response_call(kind: &str, data: &[u8]) -> (NetaudioStatus, Vec<u8>) {
    let kind = CString::new(kind).unwrap();
    let mut output = vec![0u8; 1024];
    let mut output_length = 0usize;
    let status = unsafe {
        netaudio_parse_response(
            kind.as_ptr(),
            data.as_ptr(),
            data.len(),
            output.as_mut_ptr(),
            output.len(),
            &mut output_length,
        )
    };
    output.truncate(output_length);
    (status, output)
}

#[test]
fn status_name_handles_unknown_c_discriminants_without_enum_ub() {
    for status in [-1, 34, i32::MAX] {
        let name = unsafe { CStr::from_ptr(netaudio_status_name(status)) };
        assert_eq!(name.to_str().unwrap(), "unknown");
    }
    let ok = unsafe { CStr::from_ptr(netaudio_status_name(NetaudioStatus::Ok as i32)) };
    assert_eq!(ok.to_str().unwrap(), "ok");
    let invalid_sequence =
        unsafe { CStr::from_ptr(netaudio_status_name(NetaudioStatus::InvalidSequence as i32)) };
    assert_eq!(invalid_sequence.to_str().unwrap(), "invalid_sequence");
    let unsupported = unsafe {
        CStr::from_ptr(netaudio_status_name(
            NetaudioStatus::UnsupportedProtocolOperation as i32,
        ))
    };
    assert_eq!(
        unsupported.to_str().unwrap(),
        "unsupported_protocol_operation"
    );
    let internal_panic =
        unsafe { CStr::from_ptr(netaudio_status_name(NetaudioStatus::InternalPanic as i32)) };
    assert_eq!(internal_panic.to_str().unwrap(), "internal_panic");
}

#[test]
fn guard_panic_converts_a_panic_into_internal_panic_status() {
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let status = guard_panic(|| panic!("deliberate"));
    std::panic::set_hook(previous_hook);
    assert_eq!(status, NetaudioStatus::InternalPanic);
    assert_eq!(guard_panic(|| NetaudioStatus::Ok), NetaudioStatus::Ok);
}

#[test]
fn empty_byte_output_accepts_null_zero_capacity_buffer() {
    let mut output_length = usize::MAX;
    let status = unsafe { write_bytes(&[], ptr::null_mut(), 0, &mut output_length) };

    assert_eq!(status, NetaudioStatus::Ok);
    assert_eq!(output_length, 0);
}

#[test]
fn failed_client_creation_clears_output_pointer() {
    let invalid_address = CString::new("invalid address").unwrap();
    let mut client_pointer = std::ptr::NonNull::<NetaudioClient>::dangling().as_ptr();

    let status =
        unsafe { netaudio_client_new(invalid_address.as_ptr(), 4440, 1, 1, &mut client_pointer) };

    assert_eq!(status, NetaudioStatus::InvalidAddress);
    assert!(client_pointer.is_null());
}

#[test]
fn null_client_address_clears_output_pointer() {
    let mut client_pointer = std::ptr::NonNull::<NetaudioClient>::dangling().as_ptr();

    let status = unsafe { netaudio_client_new(ptr::null(), 4440, 1, 1, &mut client_pointer) };

    assert_eq!(status, NetaudioStatus::NullPointer);
    assert!(client_pointer.is_null());
}

#[test]
fn ipv6_client_creation_is_rejected_and_clears_output_pointer() {
    let address = CString::new("::1").unwrap();
    let mut client_pointer = std::ptr::NonNull::<NetaudioClient>::dangling().as_ptr();

    let status = unsafe { netaudio_client_new(address.as_ptr(), 4440, 1, 1, &mut client_pointer) };

    assert_eq!(status, NetaudioStatus::InvalidAddress);
    assert!(client_pointer.is_null());
}

#[test]
fn ffi_errors_clear_buffer_and_scalar_outputs() {
    let mut output = [0u8; 16];
    let mut output_length = usize::MAX;
    let status = unsafe {
        netaudio_build_command(
            ptr::null(),
            output.as_mut_ptr(),
            output.len(),
            &mut output_length,
        )
    };
    assert_eq!(status, NetaudioStatus::NullPointer);
    assert_eq!(output_length, 0);

    let kind = CString::new("device_name").unwrap();
    let malformed_response = [0u8; 10];
    output_length = usize::MAX;
    let status = unsafe {
        netaudio_parse_response(
            kind.as_ptr(),
            malformed_response.as_ptr(),
            malformed_response.len(),
            output.as_mut_ptr(),
            output.len(),
            &mut output_length,
        )
    };
    assert_eq!(status, NetaudioStatus::MalformedResponse);
    assert_eq!(output_length, 0);

    let mut tx_count = u16::MAX;
    let mut rx_count = u16::MAX;
    let mut locked = i32::MAX;
    let status = unsafe {
        netaudio_client_get_channel_count(
            ptr::null_mut(),
            &mut tx_count,
            &mut rx_count,
            &mut locked,
        )
    };
    assert_eq!(status, NetaudioStatus::NullPointer);
    assert_eq!(tx_count, 0);
    assert_eq!(rx_count, 0);
    assert_eq!(locked, -1);

    let mut aes67_state = i32::MAX;
    let status = unsafe { netaudio_client_get_aes67_configured(ptr::null_mut(), &mut aes67_state) };
    assert_eq!(status, NetaudioStatus::NullPointer);
    assert_eq!(aes67_state, -1);
}

#[test]
fn same_client_handle_serializes_concurrent_calls() {
    let client = Client::new(
        "127.0.0.1".parse().unwrap(),
        4440,
        Duration::from_millis(1),
        1,
    )
    .unwrap();
    let client_pointer = Box::into_raw(Box::new(NetaudioClient {
        inner: Mutex::new(client),
    }));
    let client_address = client_pointer as usize;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(5));
    let mut threads = Vec::new();

    for _ in 0..4 {
        let barrier = std::sync::Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..100 {
                assert_eq!(
                    unsafe {
                        netaudio_client_clear_wire_captures(client_address as *mut NetaudioClient)
                    },
                    NetaudioStatus::Ok
                );
            }
        }));
    }

    barrier.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    unsafe {
        netaudio_client_free(client_pointer);
    }
}

#[test]
fn interface_status_response_kind_serializes_expected_schema() {
    let mut data = [0u8; 0x40];
    data[0..2].copy_from_slice(&0xFFFFu16.to_be_bytes());
    data[2..4].copy_from_slice(&0x40u16.to_be_bytes());
    data[16..24].copy_from_slice(b"Audinate");
    data[24] = 0x07;
    data[26..28].copy_from_slice(&crate::responses::CONMON_OPCODE_INTERFACE_STATUS.to_be_bytes());
    data[36..40].copy_from_slice(&1_000u32.to_be_bytes());
    let (status, output) = parse_response_call("interface_status", &data);

    assert_eq!(status, NetaudioStatus::Ok);
    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["link_speed_mbps"], 1_000);
    assert_eq!(json["interfaces"], serde_json::json!([]));
    assert_eq!(json["reboot_required"], false);
    assert_eq!(json["pending_config"], serde_json::Value::Null);
}

#[test]
fn encoding_status_response_kind_serializes_expected_schema() {
    let data = [
        0xFF, 0xFF, 0x00, 0x3C, 0x21, 0x02, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x10, 0x73, 0x32, 0x00,
        0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x07, 0x24, 0x00, 0x82, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x18, 0x00, 0x03, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x20,
    ];
    let (status, output) = parse_response_call("encoding_status", &data);

    assert_eq!(status, NetaudioStatus::Ok);
    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["current_encoding"], 24);
    assert_eq!(json["supported_encodings"], serde_json::json!([24, 16, 32]));
}

#[test]
fn sample_rate_pullup_status_response_kind_serializes_authentic_a32_schema() {
    let encoded = b"ffff005c001e00000200000000010000417564696e6174650724008400000000003000050000000000000000000200000000000100000000000000000000000000000000000000000000000000000001000000020000000300000004";
    let data = encoded
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).unwrap();
            let low = (pair[1] as char).to_digit(16).unwrap();
            ((high << 4) | low) as u8
        })
        .collect::<Vec<_>>();
    let (status, output) = parse_response_call("sample_rate_pullup_status", &data);

    assert_eq!(status, NetaudioStatus::Ok);
    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["applied_value"]["raw_value"], 0);
    assert_eq!(json["requested_value"]["raw_value"], 0);
    assert_eq!(json["mode_code"], 2);
    assert_eq!(json["unmapped_word_at_body_offset_20"], 1);
    assert_eq!(json["supported_values"][1]["raw_value"], 1);
    assert_eq!(
        json["supported_values"][1]["meaning"],
        "positive_four_point_one_six_six_seven_percent"
    );
    assert_eq!(json["supported_values"][1]["rate_multiplier_numerator"], 25);
    assert_eq!(
        json["supported_values"][1]["rate_multiplier_denominator"],
        24
    );
}

#[test]
fn unmapped_0086_status_response_kind_serializes_authentic_a32_words() {
    let encoded =
        b"ffff0028001100000200000000010000417564696e61746507240086000000001000000129ad36f0";
    let data = encoded
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).unwrap();
            let low = (pair[1] as char).to_digit(16).unwrap();
            ((high << 4) | low) as u8
        })
        .collect::<Vec<_>>();
    let (status, output) = parse_response_call("unmapped_0086_status", &data);
    assert_eq!(status, NetaudioStatus::Ok);
    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["unmapped_word_at_body_offset_0"], 0);
    assert_eq!(json["unmapped_word_at_body_offset_4"], 0x1000_0001u32);
    assert_eq!(json["unmapped_word_at_body_offset_8"], 0x29AD_36F0u32);
}

#[test]
fn unmapped_0024_status_response_kind_serializes_authentic_a32_words() {
    let encoded = b"ffff0030001a00000200000000010000417564696e617465072400240000000000010008001000000000000000030000";
    let data = encoded
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).unwrap();
            let low = (pair[1] as char).to_digit(16).unwrap();
            ((high << 4) | low) as u8
        })
        .collect::<Vec<_>>();
    let (status, output) = parse_response_call("unmapped_0024_status", &data);
    assert_eq!(status, NetaudioStatus::Ok);
    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["unmapped_word_at_body_offset_0"], 0);
    assert_eq!(json["unmapped_word_at_body_offset_4"], 0x0001_0008u32);
    assert_eq!(json["unmapped_word_at_body_offset_8"], 0x0010_0000u32);
    assert_eq!(json["unmapped_word_at_body_offset_12"], 0);
    assert_eq!(json["unmapped_word_at_body_offset_16"], 0x0003_0000u32);
}

#[test]
fn metering_response_kind_serializes_expected_schema() {
    let data = [
        0xFF, 0xFF, 0x00, 0x21, 0x1F, 0x81, 0x00, 0x00, 0x00, 0x1D, 0xC1, 0x19, 0x24, 0x5C, 0x00,
        0x00, 0x41, 0x75, 0x64, 0x69, 0x6E, 0x61, 0x74, 0x65, 0x02, 0x03, 0x02, 0xFE, 0xFE, 0x7D,
        0xA0, 0x88, 0x00,
    ];
    let (status, output) = parse_response_call("metering", &data);

    assert_eq!(status, NetaudioStatus::Ok);
    let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(json["sequence"], 0x1F81);
    assert_eq!(json["source_eui64"], "001dc119245c0000");
    assert_eq!(json["tx_count"], 3);
    assert_eq!(json["rx_count"], 2);
    assert_eq!(json["tx_levels"], serde_json::json!([0xFE, 0x7D, 0xA0]));
    assert_eq!(json["rx_levels"], serde_json::json!([0x88, 0x00]));
}

#[test]
fn dante_brooklyn_control_protocol_flow_setup_response_kinds_serialize_authentic_exchange() {
    let request = decode_hexadecimal(
        "1102005000000100000000380000bb8000000018000100040048000100000000\
             000000240a000002001000430000000000000000000000004133322d30303030\
             3031003100000000080238010afe4e0b",
    );
    let response = decode_hexadecimal("1102001800000100000100017fef911d0001000100000000");

    let (request_status, request_output) = parse_response_call(
        "dante_brooklyn_control_protocol_flow_setup_request",
        &request,
    );
    assert_eq!(request_status, NetaudioStatus::Ok);
    let request_json: serde_json::Value = serde_json::from_slice(&request_output).unwrap();
    assert_eq!(request_json["receiver_device_name"], "A32-000001");
    assert_eq!(request_json["receiver_channel_name"], "1");
    assert_eq!(request_json["sample_rate"], 48_000);
    assert_eq!(request_json["encoding"], 24);
    assert_eq!(request_json["receiver_address"], "10.254.78.11");

    let (response_status, response_output) = parse_response_call(
        "dante_brooklyn_control_protocol_flow_setup_response",
        &response,
    );
    assert_eq!(response_status, NetaudioStatus::Ok);
    let response_json: serde_json::Value = serde_json::from_slice(&response_output).unwrap();
    assert_eq!(response_json["transaction_identifier_hex"], "00000100");
    assert_eq!(response_json["flow_identifier"], 0x7FEF911Du32);
    assert_eq!(response_json["field_at_offset_20_hex"], "00000000");
}

#[test]
fn invalid_page_is_reported_across_ffi_without_panicking() {
    assert_eq!(
        build_command_status(r#"{"command":"receivers","page":4096}"#),
        NetaudioStatus::InvalidPage
    );
    assert_eq!(
        build_command_status(r#"{"command":"transmitters","page":2048}"#),
        NetaudioStatus::InvalidPage
    );
}

#[test]
fn invalid_subscription_channel_is_reported_across_ffi() {
    assert_eq!(
        build_command_status(
            r#"{"command":"add_subscriptions","subscriptions":[{"rx_channel":257,"tx_channel":"tx","tx_device":"device"}]}"#,
        ),
        NetaudioStatus::InvalidSubscriptionChannel
    );
}

#[test]
fn invalid_gain_device_type_is_reported_across_ffi() {
    assert_eq!(
        build_command_status(
            r#"{"command":"set_gain_level","channel_number":1,"gain_level":2,"device_type":"outputs","host_mac":"001122334455"}"#,
        ),
        NetaudioStatus::InvalidDeviceType
    );
}

#[test]
fn invalid_wire_values_are_reported_across_ffi() {
    for (json, expected) in [
        (
            r#"{"command":"set_latency","latency":-1}"#,
            NetaudioStatus::InvalidLatency,
        ),
        (
            r#"{"command":"set_sample_rate","sample_rate":0}"#,
            NetaudioStatus::InvalidSampleRate,
        ),
        (
            r#"{"command":"set_encoding","encoding":0}"#,
            NetaudioStatus::InvalidEncoding,
        ),
        (
            r#"{"command":"set_gain_level","channel_number":0,"gain_level":1,"device_type":"input","host_mac":"001122334455"}"#,
            NetaudioStatus::InvalidChannel,
        ),
        (
            r#"{"command":"set_gain_level","channel_number":1,"gain_level":6,"device_type":"input","host_mac":"001122334455"}"#,
            NetaudioStatus::InvalidGainLevel,
        ),
        (
            r#"{"command":"create_tx_flow","flow_protocol_id":10025,"flow_slot":0,"channels":[1]}"#,
            NetaudioStatus::InvalidFlowSlot,
        ),
        (
            r#"{"command":"query_tx_flows","flow_protocol_id":4660}"#,
            NetaudioStatus::InvalidFlowProtocol,
        ),
        (
            r#"{"command":"reboot","host_mac":"001122334455","sequence":0}"#,
            NetaudioStatus::InvalidSequence,
        ),
        (
            r#"{"command":"factory_reset","host_mac":"001122334455","sequence":0}"#,
            NetaudioStatus::InvalidSequence,
        ),
        (
            r#"{"command":"clear_all_configuration","host_mac":"001122334455","sequence":0}"#,
            NetaudioStatus::InvalidSequence,
        ),
        (
            r#"{"command":"clear_all_configuration_preserving_internet_protocol_settings","host_mac":"001122334455","sequence":0}"#,
            NetaudioStatus::InvalidSequence,
        ),
    ] {
        assert_eq!(build_command_status(json), expected, "{json}");
    }
}

#[test]
fn lock_token_ffi_preserves_valid_token_parity() {
    let nonce = [0x11; crate::lock::NONCE_LENGTH];
    let key = [0x22; crate::lock::KEY_LENGTH];
    let expected = crate::lock::compute_token("1234", &nonce, &key).unwrap();

    let (status, token) = lock_token_call(&nonce, &key);

    assert_eq!(status, NetaudioStatus::Ok);
    assert_eq!(token, expected);
}

#[test]
fn lock_token_ffi_rejects_incorrect_buffer_lengths() {
    let valid_nonce = [0u8; crate::lock::NONCE_LENGTH];
    let valid_key = [0u8; crate::lock::KEY_LENGTH];

    for length in [0, 1, 23, 25, 64] {
        let nonce = vec![0u8; length];
        assert_eq!(
            lock_token_call(&nonce, &valid_key).0,
            NetaudioStatus::CryptoError
        );
    }
    for length in [0, 1, 31, 33, 64] {
        let key = vec![0u8; length];
        assert_eq!(
            lock_token_call(&valid_nonce, &key).0,
            NetaudioStatus::InvalidKey
        );
    }
}

#[test]
fn lock_token_ffi_rejects_invalid_pins() {
    let nonce = [0u8; crate::lock::NONCE_LENGTH];
    let key = [0u8; crate::lock::KEY_LENGTH];
    for pin in ["", "123", "12345", "abcd", "１２３４"] {
        assert_eq!(
            lock_token_call_with_pin(pin, &nonce, &key).0,
            NetaudioStatus::InvalidPin
        );
    }
}

#[test]
fn client_lock_ffi_rejects_incorrect_key_lengths_before_network_io() {
    for length in [0, 1, 31, 33, 64] {
        let key = vec![0u8; length];
        assert_eq!(client_lock_call(&key, true), NetaudioStatus::InvalidKey);
        assert_eq!(client_lock_call(&key, false), NetaudioStatus::InvalidKey);
    }
}
