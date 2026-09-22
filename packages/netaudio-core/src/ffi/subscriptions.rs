use super::*;

/// Validate an external subscription specification and compare optional complete receiver-flow readback.
/// Returns requested/observed identities and separate ARC and SDP confirmation facts as JSON.
#[no_mangle]
pub unsafe extern "C" fn netaudio_external_subscription_readback(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let readback = crate::spec::external_subscription_readback(json)?;

            Ok(readback)
        })
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_subscription_status(
    code: u16,
    receiver_status_code: u16,
    has_receiver_status: bool,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let status = crate::subscription_status::decode(
                code,
                has_receiver_status.then_some(receiver_status_code),
            );
            Ok(status)
        })
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_subscription_classification_for_identifier(
    identifier: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let identifier = c_string(identifier)?;
            let classification =
                crate::subscription_status::classification_for_identifier(identifier);
            Ok(classification)
        })
    }
}

/// Validate {protocol_id, channels, records} and return ordered subscription commands.
/// Channels contain number and media_type_code; records use the subscription command schema.
/// All pages are validated before any commands are returned.
#[no_mangle]
pub unsafe extern "C" fn netaudio_plan_subscription_commands(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let input = c_string(json)?;
            let pages = crate::spec::plan_subscription_commands(input)?;

            Ok(pages)
        })
    }
}

/// Evaluate {channels, subscriptions, expected} from a fresh receiver inventory.
/// Channels are receiver numbers. Subscriptions contain number, optional tx_channel,
/// tx_device, status_code, receiver_status_code, and managed_status. Expected entries
/// contain number and source: null for removal or [channel, device] for a subscription.
/// Returns aggregate matched/settled and per-channel source, matched, settled, and
/// connection_state. Unknown status never establishes connection completion.
#[no_mangle]
pub unsafe extern "C" fn netaudio_subscription_readback(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let input = c_string(json)?;
            let result = crate::subscription_readback::evaluate(input)?;

            Ok(result)
        })
    }
}

/// Plan subscription reconciliation from a fresh receiver inventory and desired
/// sources. Returns unchanged entries and ordered clear/set batches. All direct
/// protocol commands are validated before any mutation batch is returned.
#[no_mangle]
pub unsafe extern "C" fn netaudio_plan_subscription_reconciliation(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let input = c_string(json)?;
            let request = decode_json(input)?;
            let plan = crate::subscription_reconciliation::plan(request)?;

            Ok(plan)
        })
    }
}
