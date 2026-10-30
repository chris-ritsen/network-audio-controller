mod inspect;
mod receive;
mod recording;
mod recording_intake;
mod unicast;
#[cfg(all(feature = "video", target_os = "linux"))]
mod video;
use netaudio_endpoint::wave;

use std::io::Read;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use netaudio_core::native_audio::{Format, MAXIMUM_UDP_PAYLOAD_BYTES, Packet, SampleWidth};

#[derive(Parser)]
#[command(version, about = "Independent network media transport")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[cfg(target_os = "linux")]
    #[command(about = "Own shared host PTP transport for independent endpoint consumers")]
    ClockServe {
        #[arg(long)]
        socket: PathBuf,
    },
    #[command(
        about = "List compiled audio backends or devices and formats for an explicit backend"
    )]
    AudioDevices {
        #[arg(long)]
        backend: Option<String>,
    },
    #[command(
        about = "Receive live video from another NetAudio host for display or a virtual camera"
    )]
    VideoListen {
        #[arg(long, help = "Local IPv4 address and control port, for example 192.168.1.20:47770")]
        bind: SocketAddrV4,
        #[arg(long, help = "Only accept the selected sender IPv4 address")]
        from: Ipv4Addr,
        #[arg(long)]
        display: bool,
        #[arg(long)]
        fullscreen: bool,
        #[arg(long, help = "Existing V4L2 loopback output device, for example /dev/video20")]
        camera: Option<PathBuf>,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "28800")]
        duration: NonZeroU64,
    },
    #[command(about = "Send desktop, window, camera or file video to a NetAudio receiver")]
    VideoShare {
        #[arg(long)]
        receiver: SocketAddrV4,
        #[arg(long, conflicts_with_all = ["file", "camera"])]
        desktop: bool,
        #[arg(long, requires = "desktop", default_value = "0")]
        window: u64,
        #[arg(long, conflicts_with_all = ["desktop", "camera"])]
        file: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["desktop", "file"])]
        camera: Option<PathBuf>,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "28800")]
        duration: NonZeroU64,
    },
    #[command(about = "Print the configured H.264 receiver session JSON schema")]
    VideoSessionSchema,
    #[command(about = "Print the H.264 multicast endpoint JSON schema")]
    VideoMulticastSchema,
    #[command(about = "Run a configured H.264 multicast source or receiver endpoint")]
    VideoServeMulticast {
        #[arg(long)]
        configuration: PathBuf,
    },
    #[command(about = "Print the configured H.264 source session JSON schema")]
    VideoTransmitSchema,
    #[command(
        about = "Transmit a finite, explicitly configured H.264 video source",
        long_about = "Encode tightly packed 1920x1080 BGRA frames at 30 fps with OpenH264 and send complete RTP datagrams to one explicitly configured unicast receiver. Requires current source authority, explicit encoder and RTP settings, and finite limits. Use --prepare-only to encode the first frame and write actual parameters without opening a media socket. Output contains parameters.json, frame timing and a final manifest. This command does not discover, allocate, route or stop a third-party stream. Receiver returns do not prove remote decoding or display."
    )]
    VideoTransmit {
        #[arg(long)]
        configuration: PathBuf,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        duration: NonZeroU64,
        #[arg(long)]
        prepare_only: bool,
    },
    #[command(
        about = "Receive an explicitly configured H.264 stream into NV12 frames",
        long_about = "Receive an explicitly configured H.264 stream into one append-only NV12 payload artifact with a paired identity, timing and byte-offset index, plus a completion manifest. Recording requires explicit bounded pending-frame and pending-byte limits in the session JSON. Requires a video-enabled build, GStreamer with h264parse, avdec_h264 and videoconvert, current session JSON, exact peer/PT/SSRC and compatible SPS/PPS. Supports progressive eight-bit 4:2:0 I/P pictures, single NAL, STAP-A and FU-A. This command neither discovers nor allocates a Dante stream. Publication uses local arrival time; it does not display video or establish synchronized presentation."
    )]
    VideoReceive {
        #[arg(long)]
        configuration: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        duration: NonZeroU64,
    },
    #[cfg(target_os = "linux")]
    #[command(
        about = "Run a virtual native-v2 audio source and routed receiver",
        long_about = "Publish configurable signed PCM transmit and receive channels with PTPv1 timing. Network sample rates range from 8000 through 192000 Hz, with 16-, 24- or 32-bit samples. Configuration JSON requires name, interface, address, identity, status_publication, local_clock_identity, parent_clock_identity, state_directory and audio. Select audio.type=native with explicit backend, client_name, input_channels, output_channels, sample_rate, buffer_frames, buffered_frames and clock_relationship (independent or network_synchronized). Optional input_device and output_device select stable device IDs from audio-devices; omission explicitly selects that backend's default device. The optional profile object supplies sample_rate, sample_bits, transmit_channels, receive_channels, flow_slots, transmit_flows, receive_flows, frames_per_packet, receive_latency_nanoseconds and maximum_media_payload_bytes; omission selects the 48000 Hz, 24-bit stereo profile. Packet geometry must fit the payload and sender spacing budgets. Format changes require restarting the endpoint. Native float audio cannot preserve every 32-bit integer exactly. Flow capacities are configured independently in each direction and default to eight. Receive groups follow source identity and local channel blocks bounded by flow_slots; source slot capacity can split them further. Capture is shared across transmit flows, and receive flows share one host sample timeline. Channel mappings contain one entry per configured channel; [1, 1] duplicates a mono input. Output channels must be distinct; unused device outputs receive silence. JACK mappings enumerate each direction in order from one. JACK creates send_N input ports and receive_N output ports without connecting them. No audio server is started. Select audio.type=wave with input and optional output for file-based recording work. The selected clock parent must be established independently. Route replacement retires its owned source flow. Uncertain allocation and STOP requests are not retried. CTRL-C, SIGTERM or duration withdraws discovery and closes owned audio resources."
    )]
    Serve {
        #[arg(long)]
        configuration: PathBuf,
        #[arg(long, help = "Stop after this many seconds; otherwise run until interrupted")]
        duration: Option<NonZeroU64>,
    },
    #[command(
        about = "Allocate one admitted native-v2 unicast source flow, record PCM and attempt one owned STOP",
        long_about = "Allocate one admitted native-v2 unicast source flow, record PCM and attempt one owned STOP. This changes source flow state. Session JSON requires profile (native_v2_reference1102), control_endpoint, source_address, interface_address, receiver_hostname, receiver_flow_name, source_channel_identifiers (ordered integer array), source_slot_capacity, sample_rate, sample_bits (16/24/32), requested_frames_per_packet, maximum_media_datagram_bytes and reply_profile (reference_handle or detailed_interface_outcomes). Context and capacities must be independently established. No source discovery, PTP placement or playback is performed. Lost allocation/STOP replies are never retried."
    )]
    ReceiveUnicast {
        #[arg(long)]
        session: PathBuf,
        #[arg(long)]
        duration: NonZeroU64,
        #[arg(long, default_value = "2")]
        timeout: NonZeroU64,
        #[arg(long)]
        output: PathBuf,
    },
    #[command(
        about = "Read current audio packetization, latency and clock-domain properties from an ARC endpoint"
    )]
    Inspect {
        #[arg(long)]
        peer: SocketAddrV4,
        #[arg(long)]
        interface_address: Ipv4Addr,
        #[arg(long, default_value = "2")]
        timeout: NonZeroU64,
    },
    #[command(
        about = "Record an existing multicast native-v2 PCM flow without changing device configuration"
    )]
    Receive {
        #[arg(long)]
        destination: SocketAddrV4,
        #[arg(long)]
        source: SocketAddrV4,
        #[arg(long)]
        interface_address: Ipv4Addr,
        #[command(flatten)]
        format: AudioFormat,
        #[arg(long)]
        duration: NonZeroU64,
        #[arg(long)]
        output: PathBuf,
    },
    #[command(about = "Decode one native-v2 datagram under an explicitly configured format")]
    Decode {
        #[arg(long)]
        input: PathBuf,
        #[command(flatten)]
        format: AudioFormat,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Encoding {
    #[value(name = "16")]
    Signed16,
    #[value(name = "24")]
    Signed24,
    #[value(name = "32")]
    Signed32,
}

#[derive(clap::Args)]
struct AudioFormat {
    #[arg(long)]
    sample_rate: NonZeroU32,
    #[arg(long)]
    sample_bits: Encoding,
    #[arg(long)]
    slots: NonZeroU16,
}

impl AudioFormat {
    fn configured(&self) -> Result<Format> {
        Ok(Format::new(
            self.sample_rate.get(),
            self.slots.get(),
            match self.sample_bits {
                Encoding::Signed16 => SampleWidth::Signed16,
                Encoding::Signed24 => SampleWidth::Signed24,
                Encoding::Signed32 => SampleWidth::Signed32,
            },
        )?)
    }
}

fn main() -> std::process::ExitCode {
    let arguments = Arguments::parse();
    let bounded_shutdown = matches!(
        &arguments.command,
        Command::VideoReceive { .. }
            | Command::VideoTransmit { .. }
            | Command::VideoListen { .. }
            | Command::VideoShare { .. }
            | Command::VideoServeMulticast { .. }
    );
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Error: initialize media runtime: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(execute(arguments));

    if bounded_shutdown {
        runtime.shutdown_timeout(std::time::Duration::ZERO);
    } else {
        drop(runtime);
    }

    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn execute(arguments: Arguments) -> Result<()> {
    match arguments.command {
        #[cfg(target_os = "linux")]
        Command::ClockServe {
            socket,
        } => {
            use tokio::signal::unix::{SignalKind, signal};
            let mut interrupt = signal(SignalKind::interrupt())?;
            let mut termination = signal(SignalKind::terminate())?;
            netaudio_endpoint::virtual_endpoint::clock_service::run(&socket, async {
                tokio::select! { _ = interrupt.recv() => {}, _ = termination.recv() => {} }
            })
            .await
        }
        Command::AudioDevices {
            backend,
        } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&netaudio_endpoint::audio::inventory(
                    backend.as_deref()
                )?)?
            );
            Ok(())
        }
        Command::VideoListen {
            bind,
            from,
            display,
            fullscreen,
            camera,
            output,
            duration,
        } => {
            #[cfg(all(feature = "video", target_os = "linux"))]
            {
                let summary = video::live::listen(video::live::Listen {
                    bind,
                    source: from,
                    output,
                    presentation: video::presentation::Options {
                        display,
                        fullscreen,
                        camera,
                    },
                    duration: std::time::Duration::from_secs(duration.get()),
                })
                .await?;
                serde_json::to_writer_pretty(std::io::stdout().lock(), &summary)?;
                println!();
                Ok(())
            }
            #[cfg(not(all(feature = "video", target_os = "linux")))]
            {
                let _ = (bind, from, display, fullscreen, camera, output, duration);
                bail!("live video requires a Linux video-enabled build");
            }
        }
        Command::VideoShare {
            receiver,
            desktop,
            window,
            file,
            camera,
            output,
            duration,
        } => {
            #[cfg(all(feature = "video", target_os = "linux"))]
            {
                use video::transmit::capture::Input;
                let input = match (desktop, file, camera) {
                    (true, None, None) => Input::Desktop {
                        window,
                    },
                    (false, Some(path), None) => Input::File(path),
                    (false, None, Some(path)) => Input::Camera(path),
                    _ => bail!("select exactly one input: --desktop, --file or --camera"),
                };
                let summary = video::live::share(video::live::Share {
                    receiver,
                    input,
                    output,
                    duration: std::time::Duration::from_secs(duration.get()),
                })
                .await?;
                serde_json::to_writer_pretty(std::io::stdout().lock(), &summary)?;
                println!();
                Ok(())
            }
            #[cfg(not(all(feature = "video", target_os = "linux")))]
            {
                let _ = (receiver, desktop, window, file, camera, output, duration);
                bail!("live video requires a Linux video-enabled build");
            }
        }
        Command::VideoTransmitSchema => {
            #[cfg(all(feature = "video", target_os = "linux"))]
            {
                serde_json::to_writer_pretty(
                    std::io::stdout().lock(),
                    &video::transmit::session_schema(),
                )?;
                println!();
                Ok(())
            }

            #[cfg(not(all(feature = "video", target_os = "linux")))]
            bail!("video source schema requires a Linux build with NETAUDIO_VIDEO=1");
        }
        Command::VideoTransmit {
            configuration,
            input,
            output,
            duration,
            prepare_only,
        } => {
            #[cfg(all(feature = "video", target_os = "linux"))]
            {
                let summary = video::transmit::run(
                    &configuration,
                    &input,
                    output,
                    std::time::Duration::from_secs(duration.get()),
                    prepare_only,
                )
                .await?;
                serde_json::to_writer_pretty(std::io::stdout().lock(), &summary)?;
                println!();
                Ok(())
            }

            #[cfg(not(all(feature = "video", target_os = "linux")))]
            {
                let _ = (configuration, input, output, duration, prepare_only);
                bail!(
                    "H.264 transmission requires Linux and a video-enabled NetAudio build; rebuild with NETAUDIO_VIDEO=1"
                );
            }
        }
        Command::VideoMulticastSchema => {
            #[cfg(all(feature = "video", target_os = "linux"))]
            {
                serde_json::to_writer_pretty(
                    std::io::stdout().lock(),
                    &video::owned_multicast::Runtime::schema(),
                )?;
                println!();
                Ok(())
            }
            #[cfg(not(all(feature = "video", target_os = "linux")))]
            bail!("owned H.264 multicast endpoint requires a Linux video-enabled build");
        }
        Command::VideoServeMulticast {
            configuration,
        } => {
            #[cfg(all(feature = "video", target_os = "linux"))]
            {
                video::owned_multicast::run(&configuration).await
            }
            #[cfg(not(all(feature = "video", target_os = "linux")))]
            {
                let _ = configuration;
                bail!("owned H.264 multicast endpoint requires a Linux video-enabled build");
            }
        }
        Command::VideoSessionSchema => {
            #[cfg(all(feature = "video", target_os = "linux"))]
            {
                serde_json::to_writer_pretty(std::io::stdout().lock(), &video::session_schema())?;
                println!();
                Ok(())
            }

            #[cfg(not(all(feature = "video", target_os = "linux")))]
            bail!("video session schema requires a Linux build with NETAUDIO_VIDEO=1");
        }
        Command::VideoReceive {
            configuration,
            output,
            duration,
        } => {
            #[cfg(all(feature = "video", target_os = "linux"))]
            {
                let summary = video::run(
                    &configuration,
                    output,
                    std::time::Duration::from_secs(duration.get()),
                )
                .await?;
                serde_json::to_writer_pretty(std::io::stdout().lock(), &summary)?;
                println!();
                Ok(())
            }

            #[cfg(not(all(feature = "video", target_os = "linux")))]
            {
                let _ = (configuration, output, duration);
                bail!(
                    "H.264 reception currently requires Linux and a video-enabled NetAudio build; install GStreamer development libraries and rebuild with NETAUDIO_VIDEO=1"
                );
            }
        }
        #[cfg(target_os = "linux")]
        Command::Serve {
            configuration,
            duration,
        } => {
            use netaudio_endpoint::virtual_endpoint::runtime;
            use tokio::signal::unix::{SignalKind, signal};

            let configuration = runtime::Configuration::read(&configuration)?;
            let mut interrupt = signal(SignalKind::interrupt())?;
            let mut termination = signal(SignalKind::terminate())?;
            let stopped = async {
                let elapsed = async {
                    match duration {
                        Some(duration) => {
                            tokio::time::sleep(std::time::Duration::from_secs(duration.get()))
                                .await;
                        }
                        None => std::future::pending().await,
                    }
                };

                tokio::select! {
                    _ = interrupt.recv() => {}
                    _ = termination.recv() => {}
                    _ = elapsed => {}
                }
            };
            let summary = runtime::run(configuration, stopped).await?;
            serde_json::to_writer_pretty(std::io::stdout().lock(), &summary)?;
            println!();
            Ok(())
        }
        Command::ReceiveUnicast {
            session,
            duration,
            timeout,
            output,
        } => {
            let summary = unicast::run(
                unicast::Session::read(&session)?,
                std::time::Duration::from_secs(duration.get()),
                std::time::Duration::from_secs(timeout.get()),
                output,
            )
            .await?;
            serde_json::to_writer_pretty(std::io::stdout().lock(), &summary)?;
            println!();
            if summary.valid_packets == 0 {
                bail!(
                    "owned unicast session received no supported PCM; inspect its recording and control manifests"
                );
            }

            Ok(())
        }
        Command::Inspect {
            peer,
            interface_address,
            timeout,
        } => {
            let readback = inspect::read(
                peer,
                interface_address,
                std::time::Duration::from_secs(timeout.get()),
            )
            .await?;
            serde_json::to_writer_pretty(std::io::stdout().lock(), &readback)?;
            println!();
            Ok(())
        }
        Command::Receive {
            destination,
            source,
            interface_address,
            format,
            duration,
            output,
        } => {
            let summary = receive::run(receive::Configuration {
                destination,
                source,
                interface_address,
                format: format.configured()?,
                duration_seconds: duration.get(),
                output,
            })
            .await?;
            serde_json::to_writer_pretty(std::io::stdout().lock(), &summary)?;
            println!();
            if summary.valid_packets == 0 {
                bail!(
                    "no supported native audio packets were received; inspect the recording manifest and packet archive"
                );
            }

            Ok(())
        }
        Command::Decode {
            input,
            format,
        } => {
            let mut bytes = Vec::new();
            std::fs::File::open(&input)
                .with_context(|| format!("read {}", input.display()))?
                .take((MAXIMUM_UDP_PAYLOAD_BYTES + 1) as u64)
                .read_to_end(&mut bytes)?;
            let configured = format.configured()?;
            let packet = Packet::decode(&bytes, configured)?;
            serde_json::to_writer_pretty(
                std::io::stdout().lock(),
                &serde_json::json!({
                    "seconds": packet.time.seconds(),
                    "sample_offset": packet.time.sample_offset(),
                    "sample_rate": configured.sample_rate(),
                    "slot_count": configured.slot_count(),
                    "sample_bits": configured.sample_width().bytes() * 8,
                    "frames": packet.frame_count(),
                    "samples": packet.samples().collect::<Vec<_>>(),
                }),
            )?;
            println!();
            Ok(())
        }
    }
}
