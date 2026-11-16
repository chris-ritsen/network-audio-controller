use super::*;

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
