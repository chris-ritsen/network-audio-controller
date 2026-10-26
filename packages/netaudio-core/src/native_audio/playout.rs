use std::fmt;

use serde::Serialize;

use super::endpoint_control::{CHANNEL_COUNT, SAMPLE_RATE};
use super::{Format, Packet};

pub const QUANTUM_FRAMES: usize = 48;
pub const PRIMING_FRAMES: usize = 96;
pub const RING_FRAMES: usize = 8192;
pub const RECEIVE_LATENCY_FRAMES: u64 = 960;
pub const MAXIMUM_MISSING_FRAMES: usize = 2400;

pub type Samples = [i32; CHANNEL_COUNT as usize];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Owner {
    pub endpoint_generation: u64,
    pub stream_generation: u64,
    pub clock_generation: u64,
}

impl Owner {
    pub fn validate(self) -> Result<(), Error> {
        if self.endpoint_generation == 0
            || self.stream_generation == 0
            || self.clock_generation == 0
        {
            return Err(Error::InvalidOwner);
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Frame {
    pub samples: Samples,
    pub received: [bool; CHANNEL_COUNT as usize],
}

#[derive(Debug, Serialize)]
pub struct Delivery {
    pub owner: Owner,
    pub first_frame_index: u64,
    pub frames: Vec<Frame>,
    pub missing_limit_reached: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidOwner,
    InvalidFormat,
    InvalidMapping,
    WrongFrameCount,
    InvalidTime,
    OutsideWindow,
    Overlap,
    NotPrimed,
    EarlyDelivery,
    MissedDeadline,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOwner => "playout owner is missing or retired",
            Self::InvalidFormat => "playout format is outside the selected local PCM profile",
            Self::InvalidMapping => "playout slots do not have an unambiguous local channel map",
            Self::WrongFrameCount => "media packet differs from its accepted packetization",
            Self::InvalidTime => "playout sample coordinate is outside its supported era",
            Self::OutsideWindow => "media interval is outside the unconsumed playout window",
            Self::Overlap => "media interval overlaps already admitted samples",
            Self::NotPrimed => "playout does not have a complete initial sample interval",
            Self::EarlyDelivery => "playout delivery was requested before its sample deadline",
            Self::MissedDeadline => "playout missed a complete local sample quantum",
        })
    }
}

impl std::error::Error for Error {}

pub struct Buffer {
    owner: Owner,
    format: Format,
    frames_per_packet: usize,
    slots: Vec<Vec<usize>>,
    selected: [bool; CHANNEL_COUNT as usize],
    frames: Vec<Frame>,
    first_received: Option<u64>,
    cursor: Option<u64>,
    primed: bool,
    consecutive_missing: usize,
}

impl Buffer {
    pub fn new(
        owner: Owner,
        format: Format,
        frames_per_packet: u16,
        slots: &[Vec<u16>],
    ) -> Result<Self, Error> {
        owner.validate()?;

        if format.sample_rate() != SAMPLE_RATE
            || format.sample_width() != super::SampleWidth::Signed24
            || frames_per_packet == 0
            || usize::from(frames_per_packet) > RING_FRAMES
        {
            return Err(Error::InvalidFormat);
        }

        if slots.len() != usize::from(format.slot_count()) {
            return Err(Error::InvalidMapping);
        }

        let mut selected = [false; CHANNEL_COUNT as usize];
        let mut mapping = Vec::with_capacity(slots.len());

        for slot in slots {
            let mut channels = Vec::with_capacity(slot.len());

            for channel in slot {
                if !(1..=CHANNEL_COUNT).contains(channel) {
                    return Err(Error::InvalidMapping);
                }

                let index = usize::from(*channel - 1);

                if selected[index] {
                    return Err(Error::InvalidMapping);
                }

                selected[index] = true;
                channels.push(index);
            }

            mapping.push(channels);
        }

        if !selected.iter().any(|selected| *selected) {
            return Err(Error::InvalidMapping);
        }

        Ok(Self {
            owner,
            format,
            frames_per_packet: usize::from(frames_per_packet),
            slots: mapping,
            selected,
            frames: vec![Frame::default(); RING_FRAMES],
            first_received: None,
            cursor: None,
            primed: false,
            consecutive_missing: 0,
        })
    }

    pub fn owner(&self) -> Owner {
        self.owner
    }

    pub fn reset(&mut self, owner: Owner) -> Result<(), Error> {
        owner.validate()?;
        self.owner = owner;
        self.frames.fill(Frame::default());
        self.first_received = None;
        self.cursor = None;
        self.primed = false;
        self.consecutive_missing = 0;
        Ok(())
    }

    pub fn admit(&mut self, owner: Owner, bytes: &[u8], current_tick: u64) -> Result<usize, Error> {
        if owner != self.owner {
            return Err(Error::InvalidOwner);
        }

        let packet = Packet::decode(bytes, self.format).map_err(|_| Error::InvalidFormat)?;

        if packet.frame_count() != self.frames_per_packet {
            return Err(Error::WrongFrameCount);
        }

        let first = packet
            .time
            .ticks(SAMPLE_RATE)
            .map_err(|_| Error::InvalidTime)?
            .checked_add(RECEIVE_LATENCY_FRAMES)
            .ok_or(Error::InvalidTime)?;
        let end = first
            .checked_add(self.frames_per_packet as u64)
            .ok_or(Error::InvalidTime)?;
        let maximum_tick = (u64::from(u32::MAX) + 1) * u64::from(SAMPLE_RATE);

        if end > maximum_tick {
            return Err(Error::InvalidTime);
        }

        if first > current_tick.saturating_add(RING_FRAMES as u64) {
            return Err(Error::OutsideWindow);
        }

        let aligned = first / QUANTUM_FRAMES as u64 * QUANTUM_FRAMES as u64;
        let expired_priming =
            !self.primed && self.cursor.is_some_and(|cursor| cursor <= current_tick);

        if self.cursor.is_none() || expired_priming {
            if aligned <= current_tick {
                return Err(Error::OutsideWindow);
            }

            self.reset(owner)?;
            self.first_received = Some(first);
            self.cursor = Some(aligned);
        }

        let cursor = self.cursor.ok_or(Error::NotPrimed)?;
        let window_end = cursor
            .checked_add(RING_FRAMES as u64)
            .ok_or(Error::InvalidTime)?;

        if first < cursor || end > window_end {
            return Err(Error::OutsideWindow);
        }

        for tick in first..end {
            let frame = &self.frames[(tick % RING_FRAMES as u64) as usize];

            if frame.received.iter().any(|received| *received) {
                return Err(Error::Overlap);
            }
        }

        let mut samples = packet.samples();

        for tick in first..end {
            let frame = &mut self.frames[(tick % RING_FRAMES as u64) as usize];

            for slot in &self.slots {
                let sample = samples.next().ok_or(Error::InvalidFormat)?;

                for channel in slot {
                    frame.samples[*channel] = sample;
                    frame.received[*channel] = true;
                }
            }
        }

        Ok(self.frames_per_packet)
    }

    pub fn next_frame_index(&mut self, current_tick: u64) -> Option<u64> {
        let cursor = self.cursor?;

        if !self.primed {
            if cursor <= current_tick {
                return None;
            }

            let first = self.first_received?;
            let end = first.checked_add(PRIMING_FRAMES as u64)?;

            for tick in first..end {
                let frame = &self.frames[(tick % RING_FRAMES as u64) as usize];

                if self
                    .selected
                    .iter()
                    .zip(frame.received)
                    .any(|(selected, received)| *selected && !received)
                {
                    return None;
                }
            }

            self.primed = true;
        }

        Some(cursor)
    }

    pub fn deliver(&mut self, owner: Owner, current_tick: u64) -> Result<Delivery, Error> {
        if owner != self.owner {
            return Err(Error::InvalidOwner);
        }

        if !self.primed {
            return Err(Error::NotPrimed);
        }

        let first = self.cursor.ok_or(Error::NotPrimed)?;
        let end = first
            .checked_add(QUANTUM_FRAMES as u64)
            .ok_or(Error::InvalidTime)?;

        if current_tick < first {
            return Err(Error::EarlyDelivery);
        }

        if current_tick >= end {
            return Err(Error::MissedDeadline);
        }

        let mut frames = Vec::with_capacity(QUANTUM_FRAMES);
        let mut missing_limit_reached = false;

        for tick in first..end {
            let frame = std::mem::take(&mut self.frames[(tick % RING_FRAMES as u64) as usize]);
            let missing = self
                .selected
                .iter()
                .zip(frame.received)
                .any(|(selected, received)| *selected && !received);

            if self
                .first_received
                .is_some_and(|first_received| tick >= first_received)
            {
                self.consecutive_missing = if missing {
                    self.consecutive_missing.saturating_add(1)
                } else {
                    0
                };
                missing_limit_reached |= self.consecutive_missing >= MAXIMUM_MISSING_FRAMES;
            }

            frames.push(frame);
        }

        self.cursor = Some(end);
        Ok(Delivery {
            owner,
            first_frame_index: first,
            frames,
            missing_limit_reached,
        })
    }
}
