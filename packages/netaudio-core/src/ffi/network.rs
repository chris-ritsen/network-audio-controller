use super::*;

/// Resolve network configuration choices, inventory completeness, and control transport availability.
#[no_mangle]
pub unsafe extern "C" fn netaudio_network_control_state(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            Ok(crate::network::network_control_state(decode_json(
                c_string(json)?,
            )?))
        })
    }
}

/// Merge fresh interface flags with prior switch-choice evidence without upgrading its freshness.
#[no_mangle]
pub unsafe extern "C" fn netaudio_interface_redundancy_status(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let observation = decode_json(c_string(json)?)?;
            Ok(crate::network::interface_redundancy_status(observation))
        })
    }
}

/// Validate interface configuration and return normalized configured-state fields.
/// Input is {mode: "dhcp"} or {mode: "static", ip_address, netmask, dns_server?, gateway?}.
#[no_mangle]
pub unsafe extern "C" fn netaudio_interface_configuration(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::network::InterfaceConfigurationRequest = decode_json(json)?;
            let configuration = request.normalize()?;

            Ok(configuration)
        })
    }
}

/// Verify requested interface configuration and preservation of other network state.
/// Input contains configuration, interface, before/after inventories and before/after_redundancy.
#[no_mangle]
pub unsafe extern "C" fn netaudio_verify_interface_configuration(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::network::InterfaceReadbackRequest = decode_json(json)?;
            crate::network::verify_interface_configuration(request)?;

            Ok(())
        })
    }
}

/// Resolve redundancy state availability and the serializer value for an optional requested mode.
/// Input is {state, mode}; output contains reasons, serializer_cohort, and switch_configuration_choice.
#[no_mangle]
pub unsafe extern "C" fn netaudio_redundancy_control(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::network::RedundancyControlRequest = decode_json(json)?;
            let control = crate::network::redundancy_control(request);

            Ok(control)
        })
    }
}
