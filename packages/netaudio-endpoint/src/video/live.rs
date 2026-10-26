use std::collections::BTreeMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{oneshot, watch};

use super::configuration::{Authority, Configuration, LatePolicy, Limits, Playout};
use super::presentation;
use super::transmit::{self, Description, capture, configuration as source};

pub const DEFAULT_DURATION_SECONDS: u64 = 28_800;
const PROTOCOL: &str = "netaudio-video/1";
const SETUP_TIMEOUT: Duration = Duration::from_secs(15);
const MAXIMUM_MESSAGE_BYTES: usize = 65_536;
const PAYLOAD_TYPE: u8 = 96;
const MAXIMUM_DATAGRAM_BYTES: usize = 1200;
const MAXIMUM_ENCODED_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_PACKETS: usize = 16_384;
const MAXIMUM_PENDING_FRAMES: usize = 8;
const MAXIMUM_REFERENCE_FRAMES: u32 = 16;
const PROVIDER_TIMEOUT_MILLISECONDS: u32 = 5000;
const RETURN_TIMEOUT_MILLISECONDS: u32 = 3000;
const SEND_LATENESS_MILLISECONDS: u32 = 1000;
const PLAYOUT_DELAY_MILLISECONDS: u32 = 80;
const PLAYOUT_LATENESS_NANOSECONDS: u64 = 100_000_000;
const ENCODER_LEVEL: u8 = 40;
const ENCODER_BITRATE: u32 = 8_000_000;
const ENCODER_KEYFRAME_INTERVAL: u32 = 30;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Hello {
    protocol: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Welcome {
    protocol: String,
    destination: SocketAddrV4,
    generation: uuid::Uuid,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    ready: bool,
}

pub struct Listen {
    pub bind: SocketAddrV4,
    pub source: Ipv4Addr,
    pub output: PathBuf,
    pub presentation: presentation::Options,
    pub duration: Duration,
}

pub struct Share {
    pub receiver: SocketAddrV4,
    pub input: capture::Input,
    pub output: PathBuf,
    pub duration: Duration,
}

pub async fn listen(options: Listen) -> Result<super::Summary> {
    maximum_frames(options.duration)?;
    ensure!(
        options.presentation.display
            || options.presentation.fullscreen
            || options.presentation.camera.is_some(),
        "select a display or virtual camera output"
    );
    let interface = interface(*options.bind.ip())?;
    let listener =
        TcpListener::bind(options.bind).await.context("listen for a NetAudio video source")?;
    eprintln!("Listening on {} for video from {}", options.bind, options.source);
    let accepted = async {
        loop {
            let (mut stream, peer) = listener.accept().await?;

            if peer.ip() != options.source {
                continue;
            }

            stream.set_nodelay(true)?;

            match read_message::<Hello>(&mut stream).await {
                Ok(hello) if hello.protocol == PROTOCOL => {
                    break Ok::<_, anyhow::Error>(stream);
                }
                Ok(_) => eprintln!("Rejected unsupported video peer protocol from {peer}"),
                Err(error) => eprintln!("Rejected invalid video handshake from {peer}: {error:#}"),
            }
        }
    };
    let mut stream = tokio::select! {
        result = tokio::time::timeout(options.duration, accepted) => result.context("no video source connected before the deadline")??,
        result = super::interruption() => { result?; anyhow::bail!("video listener interrupted before a source connected"); }
    };
    let socket = media_socket(*options.bind.ip()).await?;
    let destination = ipv4(socket.local_addr()?)?;
    let generation = uuid::Uuid::new_v4();
    write_message(
        &mut stream,
        &Welcome {
            protocol: PROTOCOL.to_owned(),
            destination,
            generation,
        },
    )
    .await?;
    let description: Description = read_message(&mut stream).await?;
    ensure!(
        *description.source.ip() == options.source && description.destination == destination,
        "source offer differs from the connected peer endpoints"
    );
    ensure!(
        description.width == source::WIDTH
            && description.height == source::HEIGHT
            && description.cadence_numerator == source::FRAME_RATE
            && description.cadence_denominator == 1
            && description.colorimetry == "bt709"
            && description.profile_identifier == source::BASELINE_PROFILE
            && description.level_identifier == ENCODER_LEVEL
            && description.payload_type == PAYLOAD_TYPE
            && description.rtp_clock_rate == source::RTP_CLOCK_RATE,
        "unsupported video peer profile"
    );
    let configuration = Configuration {
        owner: format!("netaudio-video:{generation}"),
        authority: authority(generation, options.duration)?,
        interface,
        interface_address: *options.bind.ip(),
        source: description.source,
        destination,
        payload_type: description.payload_type,
        synchronization_source: description.synchronization_source,
        width: description.width,
        height: description.height,
        cadence_numerator: u32::try_from(description.cadence_numerator)?,
        cadence_denominator: u32::try_from(description.cadence_denominator)?,
        colorimetry: description.colorimetry,
        profile_identifier: description.profile_identifier,
        level_identifier: description.level_identifier,
        sequence_parameters_hex: description.sequence_parameters_hex,
        picture_parameters_hex: description.picture_parameters_hex,
        limits: Limits {
            maximum_datagram_bytes: MAXIMUM_DATAGRAM_BYTES,
            maximum_access_unit_bytes: MAXIMUM_ENCODED_BYTES,
            maximum_packets_per_access_unit: MAXIMUM_PACKETS,
            maximum_assembly_milliseconds: PROVIDER_TIMEOUT_MILLISECONDS,
            maximum_frame_bytes: source::WIDTH as usize * source::HEIGHT as usize * 3 / 2,
            maximum_pending_frames: MAXIMUM_PENDING_FRAMES,
            maximum_recorded_frames: usize::try_from(maximum_frames(options.duration)?)?,
            maximum_reference_frames: MAXIMUM_REFERENCE_FRAMES,
            provider_timeout_milliseconds: PROVIDER_TIMEOUT_MILLISECONDS,
        },
        playout: Playout {
            delay_milliseconds: PLAYOUT_DELAY_MILLISECONDS,
            allowed_lateness_nanoseconds: PLAYOUT_LATENESS_NANOSECONDS,
            late_policy: LatePolicy::Drop,
        },
    };
    let (ready_sender, ready) = oneshot::channel();
    let (cancel, cancellation) = watch::channel(false);
    let work = super::run_session(super::Session {
        configuration,
        output: options.output,
        duration: options.duration,
        socket,
        presentation: options.presentation,
        record_frames: false,
        ready: Some(ready_sender),
        cancellation: Some(cancellation),
        idle_timeout: Some(Duration::from_millis(u64::from(PROVIDER_TIMEOUT_MILLISECONDS))),
    });
    let control = async {
        tokio::time::timeout(SETUP_TIMEOUT, ready)
            .await
            .context("receiver setup deadline expired")??;
        write_message(
            &mut stream,
            &Ready {
                ready: true,
            },
        )
        .await?;
        eprintln!("Receiving video from {}", description.source);
        peer_closed(&mut stream).await
    };
    paired(work, control, cancel).await
}

pub async fn share(options: Share) -> Result<transmit::Summary> {
    maximum_frames(options.duration)?;
    gstreamer::init().context("initialize video providers")?;
    let mut stream = tokio::time::timeout(SETUP_TIMEOUT, TcpStream::connect(options.receiver))
        .await
        .context("receiver connection deadline expired")?
        .context("connect to video receiver")?;
    stream.set_nodelay(true)?;
    let address = *ipv4(stream.local_addr()?)?.ip();
    let interface = interface(address)?;
    write_message(
        &mut stream,
        &Hello {
            protocol: PROTOCOL.to_owned(),
        },
    )
    .await?;
    let welcome: Welcome = read_message(&mut stream).await?;
    ensure!(
        welcome.protocol == PROTOCOL
            && *welcome.destination.ip() == *options.receiver.ip()
            && welcome.destination.port() != 0,
        "receiver returned an invalid video endpoint"
    );
    let socket = media_socket(address).await?;
    let source_address = ipv4(socket.local_addr()?)?;
    let identity = uuid::Uuid::new_v4();
    let bytes = identity.as_bytes();
    let configuration = source::Configuration {
        owner: format!("netaudio-video:{}", welcome.generation),
        authority: authority(welcome.generation, options.duration)?,
        interface,
        interface_address: address,
        source: source_address,
        destination: welcome.destination,
        payload_type: PAYLOAD_TYPE,
        synchronization_source: u32::from_be_bytes(bytes[..4].try_into()?) & (u32::MAX >> 1),
        initial_sequence: u16::from_be_bytes(bytes[4..6].try_into()?),
        initial_timestamp: u32::from_be_bytes(bytes[6..10].try_into()?) & (u32::MAX >> 1),
        rtp_clock_rate: source::RTP_CLOCK_RATE,
        input: source::Input {
            width: source::WIDTH,
            height: source::HEIGHT,
            frames_per_second: source::FRAME_RATE as u32,
            pixel_format: source::PixelFormat::Bgra,
            orientation: source::Orientation::TopDown,
            alpha: source::Alpha::Discard,
            colorimetry: "sRGB".to_owned(),
        },
        encoder: source::Encoder {
            provider: source::Provider::Openh264,
            profile: source::Profile::ConstrainedBaseline,
            level_identifier: ENCODER_LEVEL,
            bitrate: ENCODER_BITRATE,
            keyframe_interval: ENCODER_KEYFRAME_INTERVAL,
            colorimetry: "bt709".to_owned(),
        },
        aggregation: source::Aggregation::ZeroLatency,
        packetization: source::Packetization::NonInterleaved,
        parameter_sets: source::ParameterSets::EveryIdr,
        limits: source::Limits {
            maximum_frames: maximum_frames(options.duration)?,
            maximum_datagram_bytes: MAXIMUM_DATAGRAM_BYTES,
            maximum_encoded_frame_bytes: MAXIMUM_ENCODED_BYTES,
            maximum_packets_per_frame: MAXIMUM_PACKETS,
            operation_timeout_milliseconds: PROVIDER_TIMEOUT_MILLISECONDS,
            return_timeout_milliseconds: RETURN_TIMEOUT_MILLISECONDS,
            allowed_lateness_milliseconds: SEND_LATENESS_MILLISECONDS,
        },
    };
    configuration.validate()?;
    let capture =
        capture::Capture::start(&options.input, configuration.operation_timeout()).await?;
    let (prepared_sender, prepared) = oneshot::channel();
    let (ready_sender, ready) = oneshot::channel();
    let (cancel, cancellation) = watch::channel(false);
    let work = transmit::run_session(transmit::Session {
        frames: configuration.limits.maximum_frames,
        configuration,
        input: transmit::SourceInput::Capture(capture),
        directory: options.output,
        duration: options.duration,
        prepare_only: false,
        socket: Some(socket),
        peer: Some(transmit::Peer {
            prepared: prepared_sender,
            ready,
        }),
        cancellation: Some(cancellation),
    });
    let control = async {
        let description = tokio::time::timeout(SETUP_TIMEOUT, prepared)
            .await
            .context("source preparation deadline expired")??;
        write_message(&mut stream, &description).await?;
        let response: Ready = read_message(&mut stream).await?;
        ensure!(response.ready, "receiver rejected video setup");
        ready_sender
            .send(())
            .map_err(|_| anyhow::anyhow!("source stopped before receiver became ready"))?;
        eprintln!("Sending video to {}", welcome.destination);
        peer_closed(&mut stream).await
    };
    paired(work, control, cancel).await
}

async fn paired<T>(
    work: impl Future<Output = Result<T>>,
    control: impl Future<Output = Result<()>>,
    cancel: watch::Sender<bool>,
) -> Result<T> {
    tokio::pin!(work, control);

    tokio::select! {
        result = &mut work => result,
        result = &mut control => {
            cancel.send_replace(true);
            let retired = work.await;
            result?;
            retired
        }
    }
}

async fn peer_closed(stream: &mut TcpStream) -> Result<()> {
    let mut byte = [0];
    ensure!(
        stream.read(&mut byte).await.context("read peer lifecycle")? == 0,
        "unexpected data after video setup"
    );
    Ok(())
}

async fn write_message(stream: &mut TcpStream, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= MAXIMUM_MESSAGE_BYTES, "video setup message is too large");
    tokio::time::timeout(SETUP_TIMEOUT, async {
        stream.write_u32(u32::try_from(bytes.len())?).await?;
        stream.write_all(&bytes).await?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("video setup write deadline expired")?
}

async fn read_message<T: DeserializeOwned>(stream: &mut TcpStream) -> Result<T> {
    tokio::time::timeout(SETUP_TIMEOUT, async {
        let length = stream.read_u32().await? as usize;
        ensure!(
            length > 0 && length <= MAXIMUM_MESSAGE_BYTES,
            "invalid video setup message length"
        );
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await?;
        serde_json::from_slice(&bytes).context("invalid video setup message")
    })
    .await
    .context("video setup read deadline expired")?
}

async fn media_socket(address: Ipv4Addr) -> Result<UdpSocket> {
    UdpSocket::bind(SocketAddrV4::new(address, 0)).await.context("bind owned video endpoint")
}

fn ipv4(address: SocketAddr) -> Result<SocketAddrV4> {
    match address {
        SocketAddr::V4(address) => Ok(address),
        SocketAddr::V6(_) => anyhow::bail!("this video profile requires IPv4"),
    }
}

fn interface(address: Ipv4Addr) -> Result<String> {
    if_addrs::get_if_addrs()?
        .into_iter()
        .find(|interface| interface.ip() == address)
        .map(|interface| interface.name)
        .context("video address does not belong to this host")
}

fn maximum_frames(duration: Duration) -> Result<u64> {
    ensure!(
        duration.as_secs() > 0 && duration.as_secs() <= DEFAULT_DURATION_SECONDS,
        "video duration must be between one second and eight hours"
    );
    duration.as_secs().checked_mul(source::FRAME_RATE).context("video frame limit overflow")
}

fn authority(generation: uuid::Uuid, duration: Duration) -> Result<Authority> {
    let now = Utc::now();
    let identity = format!("netaudio-video:{generation}");
    Ok(Authority {
        identity: identity.clone(), issued_at: now,
        expires_at: now + chrono::Duration::from_std(duration)?,
        source_epoch: identity,
        fresh_source_evidence: "Owned ephemeral source socket and randomized RTP identity selected for this peer connection".to_owned(),
        value_sources: ["endpoints", "source_identity", "codec", "parameters", "image", "limits", "provider"]
            .into_iter().map(|name| (name.to_owned(), "NetAudio peer setup and current encoder output".to_owned())).collect::<BTreeMap<_, _>>(),
    })
}
