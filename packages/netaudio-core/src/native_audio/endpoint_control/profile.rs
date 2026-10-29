use serde::{Deserialize, Serialize};

use crate::native_audio::source_control::{Error, MAXIMUM_CONTROL_BYTES};
use crate::native_audio::{Format, SampleWidth, HEADER_BYTES, MAXIMUM_UDP_PAYLOAD_BYTES};

use super::{
    CHANNEL_COUNT, FRAMES_PER_PACKET, MAXIMUM_TRANSMIT_FRAMES_PER_PACKET,
    RECEIVE_LATENCY_NANOSECONDS, SAMPLE_BITS, SAMPLE_RATE,
};

pub const MINIMUM_PACKET_SPACING_NANOSECONDS: u64 = 62_500;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u32,
    #[serde(default = "default_sample_bits")]
    pub sample_bits: u16,
    pub transmit_channels: u16,
    pub receive_channels: u16,
    pub flow_slots: u16,
    #[serde(default = "default_flow_capacity")]
    pub transmit_flows: u16,
    #[serde(default = "default_flow_capacity")]
    pub receive_flows: u16,
    #[serde(default = "default_frames_per_packet")]
    pub frames_per_packet: u16,
    pub receive_latency_nanoseconds: u32,
    pub maximum_media_payload_bytes: u16,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            sample_rate: SAMPLE_RATE,
            sample_bits: SAMPLE_BITS,
            transmit_channels: CHANNEL_COUNT,
            receive_channels: CHANNEL_COUNT,
            flow_slots: CHANNEL_COUNT,
            transmit_flows: default_flow_capacity(),
            receive_flows: default_flow_capacity(),
            frames_per_packet: FRAMES_PER_PACKET,
            receive_latency_nanoseconds: RECEIVE_LATENCY_NANOSECONDS,
            maximum_media_payload_bytes: 1472,
        }
    }
}

impl Profile {
    pub fn validate(self) -> Result<(), Error> {
        if !(8_000..=192_000).contains(&self.sample_rate) {
            return Err(Error("endpoint sample rate must be between 8000 and 192000 Hz"));
        }
        self.sample_width()?;
        if self.transmit_channels == 0 || self.receive_channels == 0 || self.flow_slots == 0 {
            return Err(Error("endpoint channel counts and flow slot capacity must be positive"));
        }

        if usize::from(self.maximum_media_payload_bytes) > MAXIMUM_UDP_PAYLOAD_BYTES
            || !(self.minimum_frames_per_packet()..=self.maximum_frames_per_packet())
                .contains(&self.frames_per_packet)
        {
            return Err(Error("flow slots cannot fit the minimum packetization in the configured UDP payload budget"));
        }

        if self.transmit_flows == 0
            || self.receive_flows == 0
            || u32::from(self.transmit_flows) + u32::from(self.receive_flows) > 4096
        {
            return Err(Error("flow capacities require positive counts within the 8192 reserved media port budget"));
        }

        let bitmap_bytes = usize::from(self.receive_channels).div_ceil(16) * 2;
        let record_end =
            (36 + usize::from(self.receive_flows.min(16)) * 2 + usize::from(self.flow_slots) * 2)
                .next_multiple_of(4);
        let receive_record_bytes =
            (record_end + 8 + usize::from(self.flow_slots) * bitmap_bytes).next_multiple_of(4) + 16;
        if receive_record_bytes > MAXIMUM_CONTROL_BYTES {
            return Err(Error(
                "receive flow slot bitmaps exceed the selected control packet budget",
            ));
        }

        if usize::from(self.receive_channels) * 8192 * 5 > 64 * 1024 * 1024 {
            return Err(Error(
                "configured receiver channels exceed the 64 MiB playout sample budget",
            ));
        }

        let latency_frames = self.receive_latency_frames();
        let priming_frames =
            2 * crate::native_audio::playout::quantum_frames(self.sample_rate) as u64;
        if latency_frames < priming_frames || latency_frames + priming_frames >= 8192 {
            return Err(Error("receive latency must leave priming and delivery space in the 8192-frame playout buffer"));
        }

        Ok(())
    }

    pub fn maximum_frames_per_packet(self) -> u16 {
        let Ok(width) = self.sample_width() else {
            return 0;
        };
        if self.flow_slots == 0 {
            return 0;
        }

        let frames = usize::from(self.maximum_media_payload_bytes).saturating_sub(HEADER_BYTES)
            / (usize::from(self.flow_slots) * width.bytes());
        frames.min(usize::from(MAXIMUM_TRANSMIT_FRAMES_PER_PACKET)) as u16
    }

    pub fn receive_latency_frames(self) -> u64 {
        (u64::from(self.receive_latency_nanoseconds) * u64::from(self.sample_rate))
            .div_ceil(1_000_000_000)
    }

    pub fn sample_width(self) -> Result<SampleWidth, Error> {
        SampleWidth::try_from(self.sample_bits)
            .map_err(|_| Error("endpoint PCM width must be 16, 24 or 32 bits"))
    }

    pub fn format(self, slots: u16) -> Result<Format, Error> {
        Format::new(self.sample_rate, slots, self.sample_width()?)
            .map_err(|_| Error("endpoint media format requires a positive rate and slot count"))
    }

    pub fn minimum_frames_per_packet(self) -> u16 {
        u16::try_from(
            (u64::from(self.sample_rate) * MINIMUM_PACKET_SPACING_NANOSECONDS)
                .div_ceil(1_000_000_000),
        )
        .unwrap_or(u16::MAX)
        .max(FRAMES_PER_PACKET)
    }
}

fn default_sample_rate() -> u32 {
    SAMPLE_RATE
}

fn default_sample_bits() -> u16 {
    SAMPLE_BITS
}

fn default_frames_per_packet() -> u16 {
    FRAMES_PER_PACKET
}

fn default_flow_capacity() -> u16 {
    8
}
