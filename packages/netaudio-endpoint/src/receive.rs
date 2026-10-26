use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::Format;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::time::Instant;

use crate::recording::{Recording, RecordingContext, Summary};
use crate::recording_intake::Intake;

pub struct Configuration {
    pub destination: SocketAddrV4,
    pub source: SocketAddrV4,
    pub interface_address: Ipv4Addr,
    pub format: Format,
    pub duration_seconds: u64,
    pub output: PathBuf,
}

pub async fn run(configuration: Configuration) -> Result<Summary> {
    if !configuration.destination.ip().is_multicast() || configuration.destination.port() == 0 {
        bail!("destination must be an IPv4 multicast group and nonzero UDP port");
    }

    if configuration.source.ip().is_multicast()
        || configuration.source.ip().is_unspecified()
        || configuration.source.ip().is_broadcast()
        || configuration.source.port() == 0
    {
        bail!("source must be the established unicast sender address and nonzero media port");
    }

    if configuration.interface_address.is_unspecified()
        || configuration.interface_address.is_multicast()
        || configuration.interface_address.is_broadcast()
    {
        bail!("interface address must identify the selected local IPv4 interface");
    }
    u16::try_from(configuration.format.frame_bytes())
        .context("format exceeds the WAVE frame-size limit")?;
    configuration
        .format
        .sample_rate()
        .checked_mul(configuration.format.frame_bytes() as u32)
        .context("format exceeds the WAVE byte-rate limit")?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(configuration.duration_seconds))
        .context("duration exceeds the monotonic clock range")?;
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    #[cfg(target_os = "linux")]
    socket
        .set_multicast_all_v4(false)
        .context("restrict multicast delivery to this socket's interface memberships")?;
    socket
        .bind(&SocketAddr::V4(configuration.destination).into())
        .context("bind the selected multicast destination")?;
    socket
        .join_multicast_v4(configuration.destination.ip(), &configuration.interface_address)
        .context("join the selected multicast group")?;
    socket.set_nonblocking(true)?;
    let mut receiver = Intake::start(socket.into()).await?;
    let mut recording = Recording::begin(&RecordingContext {
        source: Some(configuration.source),
        destination: configuration.destination,
        interface_address: configuration.interface_address,
        format: configuration.format,
        output: configuration.output.clone(),
    })?;
    let result = collect(&mut receiver, &configuration, &mut recording, deadline).await;
    let intake_closed = receiver.close().await;
    let result = match (result, intake_closed) {
        (Ok(status), Ok(())) => Ok(status),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(intake_error)) => {
            Err(error.context(format!("recording intake also failed: {intake_error:#}")))
        }
    };
    let summary = recording.finish(&result)?;
    result?;
    Ok(summary)
}

async fn collect(
    receiver: &mut Intake,
    configuration: &Configuration,
    recording: &mut Recording,
    deadline: Instant,
) -> Result<&'static str> {
    let interruption = tokio::signal::ctrl_c();
    let expiration = tokio::time::sleep_until(deadline);
    tokio::pin!(interruption);
    tokio::pin!(expiration);

    loop {
        tokio::select! {
            biased;
            _ = &mut expiration => return Ok("complete"),
            signal = &mut interruption => {
                signal.context("wait for interruption")?;
                return Ok("interrupted");
            },
            result = receiver.receive() => {
                let datagram = result.context("receive native audio datagram")?;

                if datagram.peer != SocketAddr::V4(configuration.source) {
                    recording.summary.other_source_packets += 1;
                    continue;
                }

                recording.admit(
                    &datagram.bytes,
                    &datagram.received_at,
                    configuration.source,
                    configuration.format,
                )?;
            }
        }
    }
}
