use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

const FAULT_BITS: u32 = 8;
const FAULT_MASK: u64 = (1 << FAULT_BITS) - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Fault {
    Xrun = 1,
    CaptureOverflow,
    PlaybackUnderflow,
    ReceiptOverflow,
    DeviceUnavailable,
    HostUnavailable,
    SampleRateChanged,
    BufferSizeChanged,
    StreamInvalidated,
    PermissionDenied,
    BackendFailure,
    InvalidSample,
    DeviceChanged,
    RealtimeDenied,
    GenerationExhausted,
}

impl Fault {
    pub fn recoverable(self) -> bool {
        matches!(self, Self::Xrun | Self::CaptureOverflow | Self::PlaybackUnderflow)
    }

    fn decode(value: u64) -> Self {
        match value & FAULT_MASK {
            code if code == Self::Xrun as u64 => Self::Xrun,
            code if code == Self::CaptureOverflow as u64 => Self::CaptureOverflow,
            code if code == Self::PlaybackUnderflow as u64 => Self::PlaybackUnderflow,
            code if code == Self::ReceiptOverflow as u64 => Self::ReceiptOverflow,
            code if code == Self::DeviceUnavailable as u64 => Self::DeviceUnavailable,
            code if code == Self::HostUnavailable as u64 => Self::HostUnavailable,
            code if code == Self::SampleRateChanged as u64 => Self::SampleRateChanged,
            code if code == Self::BufferSizeChanged as u64 => Self::BufferSizeChanged,
            code if code == Self::StreamInvalidated as u64 => Self::StreamInvalidated,
            code if code == Self::PermissionDenied as u64 => Self::PermissionDenied,
            code if code == Self::BackendFailure as u64 => Self::BackendFailure,
            code if code == Self::InvalidSample as u64 => Self::InvalidSample,
            code if code == Self::DeviceChanged as u64 => Self::DeviceChanged,
            code if code == Self::RealtimeDenied as u64 => Self::RealtimeDenied,
            _ => Self::GenerationExhausted,
        }
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Xrun => "native audio backend reported an xrun",
            Self::CaptureOverflow => "native audio capture queue overflowed",
            Self::PlaybackUnderflow => "native audio playback queue underrun",
            Self::ReceiptOverflow => "native audio consumption receipt queue overflowed",
            Self::DeviceUnavailable => "native audio device disconnected or stopped",
            Self::HostUnavailable => "native audio server disconnected or shut down",
            Self::SampleRateChanged => "native audio sample rate changed from its configured value",
            Self::BufferSizeChanged => "native audio buffer size changed from its configured value",
            Self::StreamInvalidated => "native audio stream configuration was invalidated",
            Self::PermissionDenied => "native audio device access was denied",
            Self::BackendFailure => "native audio backend reported an unclassified failure",
            Self::InvalidSample => "native audio supplied a non-finite sample",
            Self::DeviceChanged => "native audio device identity changed",
            Self::RealtimeDenied => "native audio real-time scheduling was denied",
            Self::GenerationExhausted => "native audio interruption generation exhausted",
        })
    }
}

impl std::error::Error for Fault {}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Event {
    revision: u64,
    pub fault: Fault,
}

impl Event {
    pub fn generation(self) -> u64 {
        self.revision >> FAULT_BITS
    }
}

#[derive(Default)]
pub(super) struct Lifecycle {
    current: AtomicU64,
    acknowledged: AtomicU64,
}

impl Lifecycle {
    pub fn report(&self, fault: Fault) {
        let _ = self.current.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            if current != 0 && !Fault::decode(current).recoverable() {
                return None;
            }

            Some(match (current >> FAULT_BITS).checked_add(1) {
                Some(generation) if generation <= u64::MAX >> FAULT_BITS => {
                    (generation << FAULT_BITS) | u64::from(fault as u8)
                }
                _ => (current & !FAULT_MASK) | u64::from(Fault::GenerationExhausted as u8),
            })
        });
    }

    pub fn pending(&self) -> Option<Event> {
        let acknowledged = self.acknowledged.load(Ordering::Acquire);
        let current = self.current.load(Ordering::Acquire);
        (current != acknowledged).then(|| Event {
            revision: current,
            fault: Fault::decode(current),
        })
    }

    pub fn acknowledge(&self, event: Event) -> anyhow::Result<()> {
        if !event.fault.recoverable() {
            return Err(event.fault.into());
        }

        self.acknowledged.store(event.revision, Ordering::Release);
        Ok(())
    }
}
