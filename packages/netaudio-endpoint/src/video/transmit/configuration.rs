use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::super::configuration::Authority;

pub const WIDTH: u32 = 1920;
pub const HEIGHT: u32 = 1080;
pub const FRAME_RATE: u64 = 30;
pub const FRAME_BYTES: usize = WIDTH as usize * HEIGHT as usize * 4;
pub const RTP_CLOCK_RATE: u32 = 90_000;
pub const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;
pub const BASELINE_PROFILE: u8 = 66;
const MAXIMUM_CONFIGURATION_BYTES: u64 = 65_536;
const MINIMUM_PROVIDER_DATAGRAM_BYTES: usize = 28;

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub owner: String,
    pub authority: Authority,
    pub interface: String,
    pub interface_address: Ipv4Addr,
    pub source: SocketAddrV4,
    pub destination: SocketAddrV4,
    pub payload_type: u8,
    pub synchronization_source: u32,
    pub initial_sequence: u16,
    pub initial_timestamp: u32,
    pub rtp_clock_rate: u32,
    pub input: Input,
    pub encoder: Encoder,
    pub aggregation: Aggregation,
    pub packetization: Packetization,
    pub parameter_sets: ParameterSets,
    pub limits: Limits,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub width: u32,
    pub height: u32,
    pub frames_per_second: u32,
    pub pixel_format: PixelFormat,
    pub orientation: Orientation,
    pub alpha: Alpha,
    pub colorimetry: String,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Encoder {
    pub provider: Provider,
    pub profile: Profile,
    pub level_identifier: u8,
    pub bitrate: u32,
    pub keyframe_interval: u32,
    pub colorimetry: String,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub maximum_frames: u64,
    pub maximum_datagram_bytes: usize,
    pub maximum_encoded_frame_bytes: usize,
    pub maximum_packets_per_frame: usize,
    pub operation_timeout_milliseconds: u32,
    pub return_timeout_milliseconds: u32,
    pub allowed_lateness_milliseconds: u32,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PixelFormat {
    Bgra,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Orientation {
    TopDown,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Alpha {
    Discard,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Openh264,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    ConstrainedBaseline,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    None,
    ZeroLatency,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Packetization {
    NonInterleaved,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParameterSets {
    EveryIdr,
}

impl Configuration {
    pub fn read(path: &Path) -> Result<Self> {
        use std::io::Read;

        let mut bytes = Vec::new();
        std::fs::File::open(path)?.take(MAXIMUM_CONFIGURATION_BYTES + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAXIMUM_CONFIGURATION_BYTES,
            "source configuration is too large"
        );
        let configuration: Self = serde_json::from_slice(&bytes)?;
        configuration.validate()?;
        Ok(configuration)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(!self.owner.trim().is_empty(), "source owner is required");
        ensure!(
            !self.authority.identity.trim().is_empty()
                && !self.authority.source_epoch.trim().is_empty()
                && !self.authority.fresh_source_evidence.trim().is_empty(),
            "current authority identity, source epoch and freshness evidence are required"
        );
        self.authority_remaining()?;

        for name in
            ["endpoints", "source_identity", "codec", "parameters", "image", "limits", "provider"]
        {
            ensure!(
                self.authority
                    .value_sources
                    .get(name)
                    .is_some_and(|value| !value.trim().is_empty()),
                "missing authority value source: {name}"
            );
        }

        ensure!(valid_unicast(self.interface_address), "source interface must be unicast IPv4");
        ensure!(
            *self.source.ip() == self.interface_address && self.source.port() != 0,
            "source must use the selected interface address and a nonzero port"
        );
        ensure!(
            valid_unicast(*self.destination.ip()) && self.destination.port() != 0,
            "this source supports an exact unicast destination only"
        );
        ensure!(self.source != self.destination, "source and destination must differ");
        ensure!(
            (96..=127).contains(&self.payload_type),
            "selected H.264 provider requires a dynamic RTP payload type"
        );
        ensure!(
            self.synchronization_source != u32::MAX && self.initial_timestamp != u32::MAX,
            "selected packetizer reserves u32::MAX for random source/timestamp selection"
        );
        ensure!(self.rtp_clock_rate == RTP_CLOCK_RATE, "H.264 RTP clock rate must be 90000");
        ensure!(
            self.input.width == WIDTH
                && self.input.height == HEIGHT
                && u64::from(self.input.frames_per_second) == FRAME_RATE,
            "source profile requires 1920x1080 BGRA at 30 frames per second"
        );
        let _: gstreamer_video::VideoColorimetry =
            self.input.colorimetry.parse().context("invalid source colorimetry")?;
        let _: gstreamer_video::VideoColorimetry =
            self.encoder.colorimetry.parse().context("invalid encoded colorimetry")?;
        ensure!(
            self.encoder.bitrate > 0 && self.encoder.keyframe_interval > 0,
            "bitrate and keyframe interval must be positive"
        );
        ensure!(self.encoder.level_identifier > 0, "explicit H.264 level is required");
        ensure!(
            (MINIMUM_PROVIDER_DATAGRAM_BYTES..=netaudio_core::video::rtp::MAXIMUM_DATAGRAM_BYTES)
                .contains(&self.limits.maximum_datagram_bytes),
            "invalid packetizer datagram limit"
        );
        ensure!(
            self.limits.maximum_frames > 0
                && self.limits.maximum_encoded_frame_bytes > 0
                && self.limits.maximum_packets_per_frame > 0,
            "frame and packet bounds must be positive"
        );
        ensure!(
            self.limits.operation_timeout_milliseconds > 0
                && self.limits.return_timeout_milliseconds > 0,
            "operation and armed return deadlines must be positive"
        );
        self.limits
            .maximum_datagram_bytes
            .checked_mul(self.limits.maximum_packets_per_frame)
            .context("encoded packet queue bound overflow")?;
        ensure!(
            self.limits.maximum_packets_per_frame <= tokio::sync::Semaphore::MAX_PERMITS,
            "packet queue exceeds the runtime representation"
        );
        i64::try_from(frame_timestamp(self.limits.maximum_frames)?)
            .context("source timeline exceeds the signed clock range")?;
        let interfaces = if_addrs::get_if_addrs()?;
        ensure!(
            interfaces.iter().any(|interface| interface.name == self.interface
                && interface.ip() == self.interface_address),
            "source address does not belong to the named interface"
        );
        Ok(())
    }

    pub fn authority_remaining(&self) -> Result<Duration> {
        let now = Utc::now();
        ensure!(
            self.authority.issued_at <= now && self.authority.expires_at > now,
            "source authority is not currently valid"
        );
        (self.authority.expires_at - now).to_std().context("invalid source authority lifetime")
    }

    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.limits.operation_timeout_milliseconds))
    }
}

pub fn frame_timestamp(index: u64) -> Result<u64> {
    index
        .checked_mul(NANOSECONDS_PER_SECOND)
        .map(|value| value.div_ceil(FRAME_RATE))
        .context("source frame time overflow")
}

fn valid_unicast(address: Ipv4Addr) -> bool {
    !address.is_unspecified() && !address.is_multicast() && !address.is_broadcast()
}
