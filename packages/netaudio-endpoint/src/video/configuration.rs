use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const MAXIMUM_CONFIGURATION_BYTES: u64 = 65_536;
const MAXIMUM_PARAMETER_SET_BYTES: usize = 4096;

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
    pub width: u32,
    pub height: u32,
    pub cadence_numerator: u32,
    pub cadence_denominator: u32,
    pub colorimetry: String,
    pub profile_identifier: u8,
    pub level_identifier: u8,
    pub sequence_parameters_hex: String,
    pub picture_parameters_hex: String,
    pub limits: Limits,
    pub playout: Playout,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    pub identity: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub source_epoch: String,
    pub fresh_source_evidence: String,
    pub value_sources: BTreeMap<String, String>,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub maximum_datagram_bytes: usize,
    pub maximum_access_unit_bytes: usize,
    pub maximum_packets_per_access_unit: usize,
    pub maximum_assembly_milliseconds: u32,
    pub maximum_frame_bytes: usize,
    pub maximum_pending_frames: usize,
    pub maximum_recorded_frames: usize,
    pub maximum_reference_frames: u32,
    pub provider_timeout_milliseconds: u32,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Playout {
    pub delay_milliseconds: u32,
    pub allowed_lateness_nanoseconds: u64,
    pub late_policy: LatePolicy,
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LatePolicy {
    Drop,
    Publish,
}

impl Configuration {
    pub fn read(path: &Path) -> Result<Self> {
        use std::io::Read;

        let mut bytes = Vec::new();
        std::fs::File::open(path)?.take(MAXIMUM_CONFIGURATION_BYTES + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAXIMUM_CONFIGURATION_BYTES,
            "video configuration exceeds the input size limit"
        );
        let configuration: Self = serde_json::from_slice(&bytes)?;
        configuration.validate()?;
        Ok(configuration)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(!self.owner.trim().is_empty(), "video owner is required");
        ensure!(
            !self.authority.identity.trim().is_empty(),
            "configuration authority identity is required"
        );
        ensure!(
            !self.authority.source_epoch.trim().is_empty()
                && !self.authority.fresh_source_evidence.trim().is_empty(),
            "current source epoch and evidence excluding old indistinguishable traffic are required"
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

        ensure!(
            valid_unicast(self.interface_address),
            "interface address must be explicit unicast IPv4"
        );
        ensure!(
            valid_unicast(*self.source.ip()) && self.source.port() != 0,
            "source must be an exact unicast media endpoint"
        );
        ensure!(self.destination.port() != 0, "destination port must be nonzero");
        ensure!(
            *self.destination.ip() == self.interface_address
                || self.destination.ip().is_multicast(),
            "destination must use the configured interface address or a multicast group"
        );
        ensure!(self.source != self.destination, "source and receiving endpoint must differ");
        ensure!(self.payload_type <= 127, "RTP payload type must fit seven bits");
        ensure!(
            self.width > 0
                && self.height > 0
                && self.width.is_multiple_of(2)
                && self.height.is_multiple_of(2),
            "NV12 requires positive even dimensions"
        );
        ensure!(
            self.cadence_numerator > 0 && self.cadence_denominator > 0,
            "cadence must be a positive rational"
        );
        ensure!(
            self.cadence_numerator <= i32::MAX as u32
                && self.cadence_denominator <= i32::MAX as u32,
            "cadence exceeds provider representation"
        );
        ensure!(!self.colorimetry.trim().is_empty(), "explicit colorimetry is required");
        let limits = &self.limits;
        ensure!(
            (14..=netaudio_core::video::rtp::MAXIMUM_DATAGRAM_BYTES)
                .contains(&limits.maximum_datagram_bytes),
            "invalid datagram size limit"
        );
        ensure!(
            limits.maximum_access_unit_bytes > 0
                && limits.maximum_packets_per_access_unit > 0
                && limits.maximum_assembly_milliseconds > 0,
            "assembly bounds must be positive"
        );
        ensure!(
            limits.maximum_pending_frames > 0
                && limits.maximum_recorded_frames > 0
                && limits.provider_timeout_milliseconds > 0,
            "frame and provider limits must be positive"
        );
        ensure!(
            limits.maximum_reference_frames > 0 && limits.maximum_reference_frames <= 16,
            "decoder reference-frame bound must be within 1..16"
        );
        ensure!(
            self.frame_bytes()? <= limits.maximum_frame_bytes,
            "image exceeds the configured frame byte limit"
        );
        limits
            .maximum_access_unit_bytes
            .checked_mul(limits.maximum_pending_frames)
            .context("encoded queue byte bound overflow")?;
        limits
            .maximum_frame_bytes
            .checked_mul(limits.maximum_pending_frames)
            .context("decoded queue byte bound overflow")?;
        self.frame_bytes()?
            .checked_mul(limits.maximum_recorded_frames)
            .context("recording byte bound overflow")?;
        i64::try_from(self.playout.allowed_lateness_nanoseconds)
            .context("allowed lateness exceeds the signed clock range")?;
        decode_hex(&self.sequence_parameters_hex)?;
        decode_hex(&self.picture_parameters_hex)?;

        let interfaces = if_addrs::get_if_addrs().context("enumerate receiving interfaces")?;
        ensure!(
            interfaces.iter().any(|interface| interface.name == self.interface
                && interface.ip() == self.interface_address),
            "configured address does not belong to the named interface"
        );
        Ok(())
    }

    pub fn authority_remaining(&self) -> Result<Duration> {
        let now = Utc::now();
        ensure!(
            self.authority.issued_at <= now && self.authority.expires_at > now,
            "configuration authority is not currently valid"
        );
        (self.authority.expires_at - now).to_std().context("invalid authority lifetime")
    }

    pub fn frame_bytes(&self) -> Result<usize> {
        let pixels = (self.width as usize)
            .checked_mul(self.height as usize)
            .context("video image size overflow")?;
        pixels.checked_add(pixels / 2).context("NV12 image size overflow")
    }
}

pub fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if value.is_empty()
        || !value.len().is_multiple_of(2)
        || value.len() > MAXIMUM_PARAMETER_SET_BYTES * 2
    {
        bail!("parameter set must be a bounded nonempty even-length hexadecimal NAL");
    }

    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let text =
                std::str::from_utf8(pair).context("parameter set is not hexadecimal ASCII")?;
            u8::from_str_radix(text, 16).context("invalid hexadecimal parameter set")
        })
        .collect()
}

fn valid_unicast(address: Ipv4Addr) -> bool {
    !address.is_unspecified() && !address.is_multicast() && !address.is_broadcast()
}
