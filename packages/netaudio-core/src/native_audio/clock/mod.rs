pub mod discipline;
pub mod version_one;

use std::fmt;

use serde::{Deserialize, Serialize};

use super::SampleTime;

pub const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct Timestamp {
    seconds: u32,
    nanoseconds: u32,
}

impl Timestamp {
    pub fn new(seconds: u32, nanoseconds: u32) -> Result<Self, Error> {
        if nanoseconds >= NANOSECONDS_PER_SECOND as u32 {
            return Err(Error::InvalidTimestamp);
        }

        Ok(Self {
            seconds,
            nanoseconds,
        })
    }

    pub fn from_total_nanoseconds(value: u64) -> Result<Self, Error> {
        Self::new(
            u32::try_from(value / NANOSECONDS_PER_SECOND).map_err(|_| Error::TimeOutsideEra)?,
            (value % NANOSECONDS_PER_SECOND) as u32,
        )
    }

    pub fn seconds(self) -> u32 {
        self.seconds
    }

    pub fn nanoseconds(self) -> u32 {
        self.nanoseconds
    }

    pub fn total_nanoseconds(self) -> Result<u64, Error> {
        Self::new(self.seconds, self.nanoseconds)?;
        Ok(u64::from(self.seconds) * NANOSECONDS_PER_SECOND + u64::from(self.nanoseconds))
    }

    pub fn sample_time(self, sample_rate: u32) -> Result<SampleTime, Error> {
        self.total_nanoseconds()?;
        if sample_rate == 0 {
            return Err(Error::InvalidSampleRate);
        }

        let offset = u64::from(self.nanoseconds) * u64::from(sample_rate) / NANOSECONDS_PER_SECOND;
        SampleTime::new(self.seconds, offset as u32, sample_rate)
            .map_err(|_| Error::InvalidSampleRate)
    }
}

pub fn split_sample_relation(
    buffer_time: Timestamp,
    offset_nanoseconds: i64,
    sample_rate: u32,
) -> Result<SampleTime, Error> {
    buffer_time.total_nanoseconds()?;
    if sample_rate == 0 {
        return Err(Error::InvalidSampleRate);
    }

    let rate = i128::from(sample_rate);
    let second = i128::from(NANOSECONDS_PER_SECOND);
    let base = i128::from(buffer_time.seconds) * rate
        + i128::from(buffer_time.nanoseconds) * rate / second;
    let offset = i128::from(offset_nanoseconds);
    let adjustment = (offset / second) * rate + (offset % second) * rate / second;
    let ticks = base + adjustment;
    if ticks < 0 {
        return Err(Error::TimeOutsideEra);
    }

    let seconds = u32::try_from(ticks / rate).map_err(|_| Error::TimeOutsideEra)?;
    SampleTime::new(seconds, (ticks % rate) as u32, sample_rate)
        .map_err(|_| Error::InvalidTimestamp)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidTimestamp,
    TimeOutsideEra,
    InvalidSampleRate,
    ShortPacket,
    UnsupportedVersion,
    UnsupportedTechnology,
    UnsupportedMessage(u8),
    InvalidDomain,
    InvalidConfiguration,
    StaleOwner,
    Unavailable,
    Arithmetic,
    GenerationExhausted,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimestamp => {
                formatter.write_str("clock nanoseconds are outside the normalized second")
            }
            Self::TimeOutsideEra => {
                formatter.write_str("clock time is outside the configured nonwrapping relative era")
            }
            Self::InvalidSampleRate => formatter.write_str("clock sample rate must be positive"),
            Self::ShortPacket => formatter.write_str("clock packet is incomplete"),
            Self::UnsupportedVersion => {
                formatter.write_str("unsupported clock protocol or network version")
            }
            Self::UnsupportedTechnology => {
                formatter.write_str("unsupported clock communication technology")
            }
            Self::UnsupportedMessage(value) => {
                write!(formatter, "unsupported clock message {value}")
            }
            Self::InvalidDomain => formatter.write_str("clock domain is empty or invalid"),
            Self::InvalidConfiguration => {
                formatter.write_str("clock context or timestamp provider is invalid")
            }
            Self::StaleOwner => {
                formatter.write_str("clock observation does not belong to the current exchange")
            }
            Self::Unavailable => formatter.write_str("current network sample time is unavailable"),
            Self::Arithmetic => {
                formatter.write_str("clock arithmetic is outside the supported coordinate")
            }
            Self::GenerationExhausted => {
                formatter.write_str("clock ownership generation is exhausted")
            }
        }
    }
}

impl std::error::Error for Error {}
