use serde::{Deserialize, Serialize};

use crate::native_audio::source_control::{Error, MAXIMUM_CONTROL_BYTES};
use crate::native_audio::{HEADER_BYTES, MAXIMUM_UDP_PAYLOAD_BYTES};

use super::{
    CHANNEL_COUNT, FRAMES_PER_PACKET, MAXIMUM_TRANSMIT_FRAMES_PER_PACKET,
    RECEIVE_LATENCY_NANOSECONDS, SAMPLE_BITS, SAMPLE_RATE,
};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub transmit_channels: u16,
    pub receive_channels: u16,
    pub flow_slots: u16,
    #[serde(default = "default_frames_per_packet")]
    pub frames_per_packet: u16,
    pub receive_latency_nanoseconds: u32,
    pub maximum_media_payload_bytes: u16,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            transmit_channels: CHANNEL_COUNT,
            receive_channels: CHANNEL_COUNT,
            flow_slots: CHANNEL_COUNT,
            frames_per_packet: FRAMES_PER_PACKET,
            receive_latency_nanoseconds: RECEIVE_LATENCY_NANOSECONDS,
            maximum_media_payload_bytes: 1472,
        }
    }
}

impl Profile {
    pub fn validate(self) -> Result<(), Error> {
        if self.transmit_channels == 0 || self.receive_channels == 0 || self.flow_slots == 0 {
            return Err(Error(
                "endpoint channel counts and flow slot capacity must be positive",
            ));
        }

        if usize::from(self.maximum_media_payload_bytes) > MAXIMUM_UDP_PAYLOAD_BYTES
            || !(FRAMES_PER_PACKET..=self.maximum_frames_per_packet())
                .contains(&self.frames_per_packet)
        {
            return Err(Error("flow slots cannot fit the minimum packetization in the configured UDP payload budget"));
        }

        let bitmap_bytes = usize::from(self.receive_channels).div_ceil(16) * 2;
        let record_end = (38 + usize::from(self.flow_slots) * 2).next_multiple_of(4);
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
        if latency_frames < 96 || latency_frames + 96 >= 8192 {
            return Err(Error("receive latency must leave priming and delivery space in the 8192-frame playout buffer"));
        }

        Ok(())
    }

    pub fn maximum_frames_per_packet(self) -> u16 {
        if self.flow_slots == 0 {
            return 0;
        }

        let frames = usize::from(self.maximum_media_payload_bytes).saturating_sub(HEADER_BYTES)
            / (usize::from(self.flow_slots) * usize::from(SAMPLE_BITS / 8));
        frames.min(usize::from(MAXIMUM_TRANSMIT_FRAMES_PER_PACKET)) as u16
    }

    pub fn receive_latency_frames(self) -> u64 {
        (u64::from(self.receive_latency_nanoseconds) * u64::from(SAMPLE_RATE))
            .div_ceil(1_000_000_000)
    }
}

fn default_frames_per_packet() -> u16 {
    FRAMES_PER_PACKET
}
