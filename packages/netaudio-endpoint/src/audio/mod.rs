mod bridge;
mod device;
#[cfg(feature = "jack")]
mod jack;
mod lifecycle;
mod resampling;

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

pub use bridge::{Capture, CaptureFactory, Playback, Receipt, Statistics};
pub use device::inventory;
pub use lifecycle::{Event, Fault};

const RECOVERY_WINDOW: Duration = Duration::from_secs(10);
const MAXIMUM_RECOVERIES: usize = 8;

pub const CHANNELS: usize = 2;
pub const SAMPLE_RATE: u32 = 48_000;
pub const QUANTUM_FRAMES: usize = 48;
pub const INTEGER_SCALE: f32 = 8_388_608.0;
pub const MAXIMUM_SAMPLE: i32 = 8_388_607;
pub const MINIMUM_SAMPLE: i32 = -8_388_608;
pub type Samples = [i32; CHANNELS];
pub type FloatSamples = [f32; CHANNELS];

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClockRelationship {
    Independent,
    NetworkSynchronized,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub backend: String,
    pub client_name: String,
    pub input_device: Option<String>,
    pub output_device: Option<String>,
    pub input_channels: [u16; CHANNELS],
    pub output_channels: [u16; CHANNELS],
    pub sample_rate: u32,
    pub buffer_frames: u32,
    pub buffered_frames: usize,
    pub clock_relationship: ClockRelationship,
}

impl Configuration {
    pub fn validate(&self) -> Result<()> {
        if self.backend.is_empty() || self.client_name.is_empty() {
            bail!("audio backend and client name must be explicit");
        }

        if self.input_channels.contains(&0) || self.output_channels.contains(&0) {
            bail!("native audio channel numbers start at one");
        }

        if self.output_channels[0] == self.output_channels[1] {
            bail!("native audio output channels must be distinct");
        }

        if !(8_000..=192_000).contains(&self.sample_rate)
            || self.buffer_frames == 0
            || self.buffered_frames < self.buffer_frames as usize * 3
            || self.buffered_frames > self.sample_rate as usize
        {
            bail!(
                "audio requires 8000–192000 Hz and buffering of at least three callback periods and at most one second"
            );
        }

        if matches!(self.clock_relationship, ClockRelationship::NetworkSynchronized)
            && self.sample_rate != SAMPLE_RATE
        {
            bail!(
                "network-synchronized audio requires a 48000 Hz device sharing the network clock"
            );
        }

        Ok(())
    }
}

pub struct Engine {
    streams: Streams,
    state: std::sync::Arc<bridge::State>,
    recoveries: VecDeque<Instant>,
    recovered_generation: u64,
}

enum Streams {
    Device(device::Streams),
    #[cfg(feature = "jack")]
    Jack(jack::Stream),
}

impl Engine {
    pub fn open(configuration: &Configuration) -> Result<(Self, CaptureFactory, Playback)> {
        configuration.validate()?;
        let bridge = bridge::Bridge::new(configuration)?;
        let streams = if configuration.backend == "jack" {
            #[cfg(feature = "jack")]
            {
                Streams::Jack(jack::Stream::open(configuration, bridge.input, bridge.output)?)
            }
            #[cfg(not(feature = "jack"))]
            {
                bail!("this build does not include the JACK backend");
            }
        } else {
            Streams::Device(device::Streams::open(configuration, bridge.input, bridge.output)?)
        };

        Ok((
            Self {
                streams,
                state: bridge.state,
                recoveries: VecDeque::new(),
                recovered_generation: 0,
            },
            bridge.capture,
            bridge.playback,
        ))
    }

    pub fn pending_event(&self) -> Option<Event> {
        self.state.event()
    }

    pub async fn next_event(&self) -> Event {
        std::future::poll_fn(|context| {
            self.state.failure_waker.register(context.waker());

            match self.pending_event() {
                None => std::task::Poll::Pending,
                Some(event) => std::task::Poll::Ready(event),
            }
        })
        .await
    }

    pub fn recover(&mut self, event: Event) -> Result<()> {
        if !event.fault.recoverable() {
            return Err(event.fault.into());
        }

        let now = Instant::now();

        while self
            .recoveries
            .front()
            .is_some_and(|previous| now.duration_since(*previous) >= RECOVERY_WINDOW)
        {
            self.recoveries.pop_front();
        }

        let interruptions = event.generation().saturating_sub(self.recovered_generation);

        if interruptions > MAXIMUM_RECOVERIES.saturating_sub(self.recoveries.len()) as u64 {
            bail!(
                "native audio recovery limit exceeded: more than {MAXIMUM_RECOVERIES} interruptions in {} seconds; last cause: {}",
                RECOVERY_WINDOW.as_secs(),
                event.fault
            );
        }

        self.recoveries.extend(std::iter::repeat_n(now, interruptions as usize));
        self.state.recover(event)?;
        self.recovered_generation = event.generation();
        Ok(())
    }

    pub fn statistics(&self) -> Statistics {
        self.state.statistics()
    }

    pub fn close(self) -> Result<Statistics> {
        match self.streams {
            Streams::Device(streams) => streams.close(),
            #[cfg(feature = "jack")]
            Streams::Jack(stream) => stream.close()?,
        }

        Ok(self.state.statistics())
    }
}
