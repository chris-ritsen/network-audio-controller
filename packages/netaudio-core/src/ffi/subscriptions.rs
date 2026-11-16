use super::*;

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
