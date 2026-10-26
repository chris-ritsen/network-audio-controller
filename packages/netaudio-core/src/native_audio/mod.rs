pub mod bootstrap;
pub mod clock;
pub mod domain;
pub mod eligibility;
pub mod endpoint_control;
pub mod flow_inventory;
pub mod playout;
pub mod properties;
pub mod source_control;
pub mod source_discovery;

use std::fmt;

pub const HEADER_BYTES: usize = 9;
pub const REVERSE_KEEPALIVE: &[u8; 10] = b"KEEPALIVE\0";
pub const MAXIMUM_UDP_PAYLOAD_BYTES: usize = 65_507;

pub fn is_reverse_keepalive(bytes: &[u8]) -> bool {
    bytes.is_empty() || bytes == REVERSE_KEEPALIVE
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SampleWidth {
    Signed16,
    Signed24,
    Signed32,
}

impl SampleWidth {
    pub fn bytes(self) -> usize {
        match self {
            Self::Signed16 => 2,
            Self::Signed24 => 3,
            Self::Signed32 => 4,
        }
    }

    fn contains(self, sample: i32) -> bool {
        match self {
            Self::Signed16 => i16::try_from(sample).is_ok(),
            Self::Signed24 => (-8_388_608..=8_388_607).contains(&sample),
            Self::Signed32 => true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Format {
    sample_rate: u32,
    slot_count: u16,
    sample_width: SampleWidth,
}

impl Format {
    pub fn new(
        sample_rate: u32,
        slot_count: u16,
        sample_width: SampleWidth,
    ) -> Result<Self, Error> {
        if sample_rate == 0 || slot_count == 0 {
            return Err(Error::InvalidFormat);
        }

        Ok(Self {
            sample_rate,
            slot_count,
            sample_width,
        })
    }

    pub fn sample_rate(self) -> u32 {
        self.sample_rate
    }

    pub fn slot_count(self) -> u16 {
        self.slot_count
    }

    pub fn sample_width(self) -> SampleWidth {
        self.sample_width
    }

    pub fn frame_bytes(self) -> usize {
        usize::from(self.slot_count) * self.sample_width.bytes()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleTime {
    seconds: u32,
    sample_offset: u32,
}

impl SampleTime {
    pub fn new(seconds: u32, sample_offset: u32, sample_rate: u32) -> Result<Self, Error> {
        if sample_rate == 0 || sample_offset >= sample_rate {
            return Err(Error::InvalidTimestamp);
        }

        Ok(Self {
            seconds,
            sample_offset,
        })
    }

    pub fn seconds(self) -> u32 {
        self.seconds
    }

    pub fn sample_offset(self) -> u32 {
        self.sample_offset
    }

    pub fn ticks(self, sample_rate: u32) -> Result<u64, Error> {
        Self::new(self.seconds, self.sample_offset, sample_rate)?;
        Ok(u64::from(self.seconds) * u64::from(sample_rate) + u64::from(self.sample_offset))
    }

    pub fn advance(self, frames: u64, sample_rate: u32) -> Result<Self, Error> {
        let ticks = self
            .ticks(sample_rate)?
            .checked_add(frames)
            .ok_or(Error::TimestampOverflow)?;
        let seconds =
            u32::try_from(ticks / u64::from(sample_rate)).map_err(|_| Error::TimestampOverflow)?;
        Self::new(
            seconds,
            (ticks % u64::from(sample_rate)) as u32,
            sample_rate,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidFormat,
    InvalidTimestamp,
    TimestampOverflow,
    ShortHeader,
    UnsupportedDiscriminator(u8),
    EmptyPayload,
    IncompleteFrame,
    PayloadTooLarge,
    SampleOutOfRange,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat => {
                formatter.write_str("sample rate and slot count must be positive")
            }
            Self::InvalidTimestamp => {
                formatter.write_str("sample offset must be within the configured sample second")
            }
            Self::TimestampOverflow => {
                formatter.write_str("sample time exceeds the supported seconds era")
            }
            Self::ShortHeader => formatter.write_str("native audio header is incomplete"),
            Self::UnsupportedDiscriminator(value) => {
                write!(formatter, "unsupported native audio discriminator {value}")
            }
            Self::EmptyPayload => {
                formatter.write_str("native audio packet contains no complete frames")
            }
            Self::IncompleteFrame => {
                formatter.write_str("native audio payload contains an incomplete frame")
            }
            Self::PayloadTooLarge => {
                formatter.write_str("native audio packet exceeds the IPv4 UDP payload limit")
            }
            Self::SampleOutOfRange => {
                formatter.write_str("sample does not fit the configured signed PCM width")
            }
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
pub struct Packet<'a> {
    pub time: SampleTime,
    format: Format,
    payload: &'a [u8],
}

impl<'a> Packet<'a> {
    pub fn decode(datagram: &'a [u8], format: Format) -> Result<Self, Error> {
        if datagram.len() < HEADER_BYTES {
            return Err(Error::ShortHeader);
        }

        if datagram[0] != 2 {
            return Err(Error::UnsupportedDiscriminator(datagram[0]));
        }

        if datagram.len() > MAXIMUM_UDP_PAYLOAD_BYTES {
            return Err(Error::PayloadTooLarge);
        }

        let payload = &datagram[HEADER_BYTES..];
        if payload.is_empty() {
            return Err(Error::EmptyPayload);
        }

        if payload.len() % format.frame_bytes() != 0 {
            return Err(Error::IncompleteFrame);
        }

        let seconds =
            u32::from_be_bytes(datagram[1..5].try_into().map_err(|_| Error::ShortHeader)?);
        let sample_offset =
            u32::from_be_bytes(datagram[5..9].try_into().map_err(|_| Error::ShortHeader)?);
        Ok(Self {
            time: SampleTime::new(seconds, sample_offset, format.sample_rate)?,
            format,
            payload,
        })
    }

    pub fn frame_count(&self) -> usize {
        self.payload.len() / self.format.frame_bytes()
    }

    pub fn samples(&self) -> impl ExactSizeIterator<Item = i32> + '_ {
        self.payload
            .chunks_exact(self.format.sample_width.bytes())
            .map(|bytes| match self.format.sample_width {
                SampleWidth::Signed16 => i32::from(i16::from_be_bytes([bytes[0], bytes[1]])),
                SampleWidth::Signed24 => i32::from_be_bytes([
                    if bytes[0] & 0x80 == 0 { 0 } else { 0xff },
                    bytes[0],
                    bytes[1],
                    bytes[2],
                ]),
                SampleWidth::Signed32 => {
                    i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
                }
            })
    }

    pub fn next_time(&self) -> Result<SampleTime, Error> {
        self.time
            .advance(self.frame_count() as u64, self.format.sample_rate)
    }
}

pub fn encode(format: Format, time: SampleTime, samples: &[i32]) -> Result<Vec<u8>, Error> {
    time.ticks(format.sample_rate)?;
    if samples.is_empty() {
        return Err(Error::EmptyPayload);
    }

    if samples.len() % usize::from(format.slot_count) != 0 {
        return Err(Error::IncompleteFrame);
    }

    let length = samples
        .len()
        .checked_mul(format.sample_width.bytes())
        .and_then(|length| length.checked_add(HEADER_BYTES))
        .ok_or(Error::PayloadTooLarge)?;
    if length > MAXIMUM_UDP_PAYLOAD_BYTES {
        return Err(Error::PayloadTooLarge);
    }

    if samples
        .iter()
        .any(|sample| !format.sample_width.contains(*sample))
    {
        return Err(Error::SampleOutOfRange);
    }

    let mut packet = Vec::with_capacity(length);
    packet.push(2);
    packet.extend_from_slice(&time.seconds.to_be_bytes());
    packet.extend_from_slice(&time.sample_offset.to_be_bytes());
    for sample in samples {
        packet.extend_from_slice(&sample.to_be_bytes()[4 - format.sample_width.bytes()..]);
    }

    Ok(packet)
}
