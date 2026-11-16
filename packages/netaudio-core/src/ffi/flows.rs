use super::*;

/// Require complete inventory and a supported, representable flow before deletion.
#[no_mangle]
pub unsafe extern "C" fn netaudio_flow_delete_preflight(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::flow_readback::delete_preflight(request))
        })
    }
}

/// Check fresh transmitter inventory before allocating a flow slot.
#[no_mangle]
pub unsafe extern "C" fn netaudio_flow_create_preflight(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::flow_readback::create_preflight(request))
        })
    }
}

/// Select and validate a transmit-flow deletion command without sending it.
#[no_mangle]
pub unsafe extern "C" fn netaudio_plan_transmit_flow_delete(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::flow_plan::FlowDeleteRequest = decode_json(json)?;
            let plan = crate::flow_plan::plan_delete(&request);
            Ok(plan)
        })
    }
}

/// Select and validate a transmit-flow creation command without sending it.
#[no_mangle]
pub unsafe extern "C" fn netaudio_plan_transmit_flow_create(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::flow_plan::FlowCreateRequest = decode_json(json)?;
            let plan = crate::flow_plan::plan_create(&request);
            Ok(plan)
        })
    }
}

#[no_mangle]
pub unsafe extern "C" fn netaudio_flow_authoring_capabilities(
    capability_word: u16,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let capabilities = crate::parser::flow_authoring_capabilities(capability_word);
            Ok(capabilities)
        })
    }
}

/// Plan transmitter inventory queries from advertised and observed revisions.
#[no_mangle]
pub unsafe extern "C" fn netaudio_transmit_flow_inventory_protocols(
    advertised_protocol: u16,
    observed_protocol: u16,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let protocols = crate::protocol::transmit_flow_inventory_protocols(
                advertised_protocol,
                observed_protocol,
            )
            .map_err(|message| {
                FfiError::new(NetaudioStatus::UnsupportedProtocolOperation, message)
            })?;

            Ok(protocols)
        })
    }
}
