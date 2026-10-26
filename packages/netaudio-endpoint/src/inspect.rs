use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
use netaudio_core::native_audio::properties::AudioProperties;
use netaudio_core::{commands, protocol, responses};
use serde::Serialize;
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout_at};

const AUDIO_PROPERTIES: [u16; 8] = [0x0210, 0x0211, 0x0213, 0x0214, 0x0310, 0x8205, 0x8301, 0x8021];

#[derive(Serialize)]
pub struct Readback {
    peer: SocketAddrV4,
    interface_address: Ipv4Addr,
    audio: AudioProperties,
    directory_observed_at: String,
    properties_observed_at: String,
    directory_response_revision: u16,
    properties_response_revision: u16,
    atomic_snapshot: bool,
    directory: responses::PropertyDirectory,
    properties: responses::DeviceSettings,
}

pub async fn read(
    peer: SocketAddrV4,
    interface_address: Ipv4Addr,
    timeout: Duration,
) -> Result<Readback> {
    if peer.port() == 0
        || peer.ip().is_unspecified()
        || peer.ip().is_multicast()
        || peer.ip().is_broadcast()
    {
        bail!("peer must be the current device ARC address and nonzero UDP port");
    }

    if interface_address.is_unspecified()
        || interface_address.is_multicast()
        || interface_address.is_broadcast()
    {
        bail!("interface address must identify the selected local IPv4 interface");
    }

    let socket =
        UdpSocket::bind((interface_address, 0)).await.context("bind the selected interface")?;
    socket.connect(peer).await.context("select the ARC peer")?;
    let directory_request =
        commands::build_property_directory_for_protocol(protocol::PROTOCOL_ID, 1)
            .map_err(anyhow::Error::msg)
            .context("construct the directory request")?;
    let directory_reply = exchange(&socket, &directory_request, timeout).await?;
    let directory_observed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
    let directory_response_revision =
        protocol::response_envelope(&directory_reply).context("directory envelope")?.protocol_id;
    let directory = responses::parse_property_directory(&directory_reply)
        .context("malformed property directory")?;
    let properties_request =
        commands::build_query_performance_settings(protocol::PROTOCOL_ID, &AUDIO_PROPERTIES, 2)
            .map_err(anyhow::Error::msg)
            .context("construct the audio-property request")?;
    let properties_reply = exchange(&socket, &properties_request, timeout).await?;
    let properties_observed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
    let properties_response_revision =
        protocol::response_envelope(&properties_reply).context("property envelope")?.protocol_id;
    let properties = responses::parse_device_settings(&properties_reply)
        .context("malformed or ambiguous audio properties")?;
    let audio = AudioProperties::from_settings(&properties);
    Ok(Readback {
        peer,
        interface_address,
        audio,
        directory_observed_at,
        properties_observed_at,
        directory_response_revision,
        properties_response_revision,
        atomic_snapshot: false,
        directory,
        properties,
    })
}

async fn exchange(socket: &UdpSocket, request: &[u8], timeout: Duration) -> Result<Vec<u8>> {
    let owner = protocol::response_envelope(request).context("invalid authored request")?;
    let deadline = Instant::now().checked_add(timeout).context("request deadline overflow")?;
    let sent =
        timeout_at(deadline, socket.send(request)).await.context("ARC submission deadline")??;
    if sent != request.len() {
        bail!("ARC request was not submitted completely");
    }

    let mut buffer = vec![0u8; 65_536];
    loop {
        let received = timeout_at(deadline, socket.recv(&mut buffer))
            .await
            .context("ARC reply deadline")??;
        let Some(reply) = protocol::response_envelope(&buffer[..received]) else {
            continue;
        };

        if reply.transaction_id != owner.transaction_id || reply.opcode != owner.opcode {
            continue;
        }

        if !protocol::DEVICE_SETTINGS_ARC_PROTOCOL_IDS.contains(&reply.protocol_id) {
            bail!("unsupported ARC reply revision 0x{:04x}", reply.protocol_id);
        }

        if reply.result_code != 1 {
            bail!(
                "ARC operation 0x{:04x} returned result 0x{:04x}",
                reply.opcode,
                reply.result_code
            );
        }
        return Ok(buffer[..received].to_vec());
    }
}
