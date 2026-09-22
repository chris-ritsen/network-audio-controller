use super::*;

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

/// Validate fresh sample-rate or encoding readback before creating a flow.
#[no_mangle]
pub unsafe extern "C" fn netaudio_flow_format_readback(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::flow_readback::format_precondition(request))
        })
    }
}

/// Classify valid, correlated readback without treating missing evidence as contradiction.
#[no_mangle]
pub unsafe extern "C" fn netaudio_flow_verification(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::flow_readback::verification(request))
        })
    }
}

/// Compare stable configuration of flows other than the operation's target.
#[no_mangle]
pub unsafe extern "C" fn netaudio_flow_topology_change(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::flow_readback::topology_change(request)
                .map_err(crate::spec::SpecError::InvalidJson)?)
        })
    }
}

/// Check whether inventory evidence is complete and has unique, usable flow identities.
#[no_mangle]
pub unsafe extern "C" fn netaudio_flow_inventory_complete(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let value = decode_json(c_string(json)?)?;
            Ok(crate::flow_readback::inventory_complete(value))
        })
    }
}

/// Correlate creation readback using complete, unambiguous before/after inventories.
#[no_mangle]
pub unsafe extern "C" fn netaudio_flow_creation_candidate(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let request = decode_json(c_string(json)?)?;
            Ok(crate::flow_readback::creation_candidate(request)
                .map_err(crate::spec::SpecError::InvalidJson)?)
        })
    }
}

/// Compare requested and effective transmit-flow specifications, preserving unavailable readback fields.
#[no_mangle]
pub unsafe extern "C" fn netaudio_compare_transmit_flows(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::flow_readback::FlowComparisonRequest = decode_json(json)?;
            let comparison = crate::flow_readback::compare_transmit_flows(&request);

            Ok(comparison)
        })
    }
}

/// Verify sample-rate readback against the prior topology and target capacity.
#[no_mangle]
pub unsafe extern "C" fn netaudio_verify_sample_rate_topology(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::sample_rate_topology::TopologyReadbackRequest = decode_json(json)?;
            crate::sample_rate_topology::verify_topology(request)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidPage, message))?;

            Ok(())
        })
    }
}

/// Classify sample-rate topology impact from {snapshot, target_capacity}.
#[no_mangle]
pub unsafe extern "C" fn netaudio_sample_rate_topology_impact(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::sample_rate_topology::TopologyImpactRequest = decode_json(json)?;
            let impact = crate::sample_rate_topology::topology_impact(request)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidPage, message))?;

            Ok(impact)
        })
    }
}

/// Interpret transmitter-flow membership for sample-rate readback.
#[no_mangle]
pub unsafe extern "C" fn netaudio_transmit_flow_topology(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::flow_readback::FlowReadbackRequest = decode_json(json)?;
            let topology = crate::flow_readback::transmit_flow_topology(&request)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidPage, message))?;

            Ok(topology)
        })
    }
}

/// Validate the canonical transmit-flow specification, including cross-field constraints.
#[no_mangle]
pub unsafe extern "C" fn netaudio_validate_transmit_flow_specification(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let specification: crate::flow_specification::TransmitFlowSpecification =
                decode_json(json)?;
            specification
                .validate()
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidJson, message))?;

            Ok(())
        })
    }
}

/// Convert an observed transmitter record into the canonical flow specification.
#[no_mangle]
pub unsafe extern "C" fn netaudio_transmit_flow_specification(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let json = c_string(json)?;
            let request: crate::flow_readback::FlowReadbackRequest = decode_json(json)?;
            let specification = crate::flow_readback::transmit_flow_specification(&request)
                .map_err(|message| FfiError::new(NetaudioStatus::InvalidPage, message))?;

            Ok(specification)
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
