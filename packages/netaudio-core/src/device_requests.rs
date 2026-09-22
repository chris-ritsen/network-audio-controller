use crate::{bytes::read_u16, commands, protocol};

#[derive(serde::Serialize)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum DeviceRequest {
    Control {
        #[serde(flatten)]
        header: protocol::ControlRequestHeader,
    },
    Settings {
        #[serde(flatten)]
        request: SettingsRequest,
    },
}

#[derive(serde::Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum SettingsRequest {
    BoardInfo,
    ProductInfo,
    ClockStatus,
    InterfaceStatus,
    Audio {
        #[serde(flatten)]
        request: commands::AudioConfigurationRequest,
    },
}

pub fn parse_device_request(data: &[u8]) -> Option<DeviceRequest> {
    if read_u16(data, 0)? != commands::PROTOCOL_SETTINGS {
        return protocol::control_request_header(data)
            .map(|header| DeviceRequest::Control { header });
    }

    if let Some(request) = commands::parse_audio_configuration_request(data) {
        return Some(DeviceRequest::Settings {
            request: SettingsRequest::Audio { request },
        });
    }

    let message_id = read_u16(data, 4)?;
    let mac = data.get(8..14)?.try_into().ok()?;

    // Compare complete envelopes against the same builders used by clients.
    // Identity queries have a fixed builder ID, but callers may choose another ID.
    for (mut packet, request) in [
        (
            commands::build_dante_model(mac).ok()?,
            SettingsRequest::BoardInfo,
        ),
        (
            commands::build_make_model(mac).ok()?,
            SettingsRequest::ProductInfo,
        ),
    ] {
        packet[4..6].copy_from_slice(&message_id.to_be_bytes());

        if packet == data {
            return Some(DeviceRequest::Settings { request });
        }
    }

    if commands::build_probe_interface_status(mac, message_id)
        .ok()?
        .as_slice()
        == data
    {
        return Some(DeviceRequest::Settings {
            request: SettingsRequest::InterfaceStatus,
        });
    }

    let revision = read_u16(data, 24)?;

    if commands::build_refresh_clock_status(revision, mac, message_id)
        .ok()?
        .as_slice()
        == data
    {
        return Some(DeviceRequest::Settings {
            request: SettingsRequest::ClockStatus,
        });
    }

    None
}
