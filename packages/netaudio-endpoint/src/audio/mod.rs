mod bridge;
mod device;
#[cfg(feature = "jack")]
mod jack;
mod lifecycle;
mod resampling;

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use netaudio_core::native_audio::SampleFormat;
use serde::{Deserialize, Serialize};

pub use bridge::{Capture, CaptureFactory, Playback, Receipt, Statistics};
pub use device::inventory;
pub use lifecycle::{Event, Fault};

const RECOVERY_WINDOW: Duration = Duration::from_secs(10);
const MAXIMUM_RECOVERIES: usize = 8;

pub const CHANNELS: usize = 2;
pub type Samples = Vec<i32>;
pub type FloatSamples = Vec<f32>;

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
    pub input_channels: Vec<u16>,
    pub output_channels: Vec<u16>,
    pub sample_rate: u32,
    pub buffer_frames: u32,
    pub buffered_frames: usize,
    pub clock_relationship: ClockRelationship,
}

impl Configuration {
    pub fn validate(&self, network_format: SampleFormat) -> Result<()> {
        if !(8_000..=192_000).contains(&network_format.sample_rate()) {
            bail!("native audio network sample rate must be between 8000 and 192000 Hz");
        }
        if self.backend.is_empty() || self.client_name.is_empty() {
            bail!("audio backend and client name must be explicit");
        }

        if self.input_channels.is_empty()
            || self.output_channels.is_empty()
            || self.input_channels.len() > usize::from(u16::MAX)
            || self.output_channels.len() > usize::from(u16::MAX)
        {
            bail!("native audio requires positive representable channel counts");
        }

        if self.input_channels.contains(&0) || self.output_channels.contains(&0) {
            bail!("native audio channel numbers start at one");
        }

        if self.output_channels.iter().copied().collect::<std::collections::BTreeSet<_>>().len()
            != self.output_channels.len()
        {
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
            && self.sample_rate != network_format.sample_rate()
        {
            bail!(
                "network-synchronized audio requires the configured network sample rate and a device sharing the network clock"
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
    pub fn open(
        configuration: &Configuration,
        network_format: SampleFormat,
    ) -> Result<(Self, CaptureFactory, Playback)> {
        configuration.validate(network_format)?;
        let bridge = bridge::Bridge::new(configuration, network_format)?;
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
