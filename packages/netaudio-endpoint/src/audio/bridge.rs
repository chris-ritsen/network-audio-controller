use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use anyhow::{Context, Result, bail};
use futures_util::task::AtomicWaker;
use netaudio_core::native_audio::playout::{Delivery, Owner};
use rtrb::{Consumer, Producer, RingBuffer};
use serde::Serialize;

use super::lifecycle::{Event, Fault, Lifecycle};

use super::{
    CHANNELS, Configuration, FloatSamples, INTEGER_SCALE, MAXIMUM_SAMPLE, MINIMUM_SAMPLE,
    QUANTUM_FRAMES, Samples, resampling,
};

const QUEUE_HEADROOM: usize = 4;
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Receipt {
    pub owner: Owner,
    pub first_frame_index: u64,
    pub frames: usize,
    pub output_accepted: bool,
    pub received_samples: usize,
    pub local_silence_samples: usize,
}

#[derive(Debug, Serialize)]
pub struct Statistics {
    pub captured_frames: u64,
    pub played_frames: u64,
    pub capture_overflows: u64,
    pub playback_underflows: u64,
    pub xruns: u64,
    pub recoveries: u64,
    pub failure: Option<String>,
}

pub(super) struct State {
    capture_generation: AtomicU64,
    next_capture_generation: AtomicU64,
    playback_generation: AtomicU64,
    lifecycle: Lifecycle,
    capture_waker: AtomicWaker,
    receipt_waker: AtomicWaker,
    pub failure_waker: AtomicWaker,
    captured_frames: AtomicU64,
    played_frames: AtomicU64,
    capture_overflows: AtomicU64,
    playback_underflows: AtomicU64,
    xruns: AtomicU64,
    recoveries: AtomicU64,
}

impl State {
    pub fn report(&self, fault: Fault) {
        if fault == Fault::Xrun {
            self.xruns.fetch_add(1, Ordering::Relaxed);
        }

        self.lifecycle.report(fault);
        self.wake();
    }

    fn wake(&self) {
        self.capture_waker.wake();
        self.receipt_waker.wake();
        self.failure_waker.wake();
    }

    pub fn event(&self) -> Option<Event> {
        self.lifecycle.pending()
    }

    pub fn recover(&self, event: Event) -> Result<()> {
        self.lifecycle.acknowledge(event)?;
        self.recoveries.fetch_add(1, Ordering::Relaxed);
        self.wake();
        Ok(())
    }

    pub fn check(&self) -> Result<()> {
        match self.event() {
            None => Ok(()),
            Some(event) => Err(event.fault.into()),
        }
    }

    fn ready(&self) -> Result<bool> {
        match self.event() {
            Some(event) if event.fault.recoverable() => Ok(false),
            Some(event) => Err(event.fault.into()),
            None => Ok(true),
        }
    }

    pub fn statistics(&self) -> Statistics {
        Statistics {
            captured_frames: self.captured_frames.load(Ordering::Relaxed),
            played_frames: self.played_frames.load(Ordering::Relaxed),
            capture_overflows: self.capture_overflows.load(Ordering::Relaxed),
            playback_underflows: self.playback_underflows.load(Ordering::Relaxed),
            xruns: self.xruns.load(Ordering::Relaxed),
            recoveries: self.recoveries.load(Ordering::Relaxed),
            failure: self.check().err().map(|error| error.to_string()),
        }
    }
}

#[derive(Clone, Copy)]
struct Captured {
    generation: u64,
    samples: FloatSamples,
}

struct OutputFrame {
    generation: u64,
    samples: FloatSamples,
    receipt: Option<Receipt>,
}

pub(super) struct InputCallback {
    pub state: Arc<State>,
    frames: Producer<Captured>,
}

impl InputCallback {
    pub fn process(&mut self, frames: impl Iterator<Item = FloatSamples>) {
        let generation = self.state.capture_generation.load(Ordering::Acquire);

        if generation == 0 || self.state.event().is_some() {
            return;
        }

        let mut captured = 0;

        for samples in frames {
            if samples.iter().any(|sample| !sample.is_finite()) {
                self.state.report(Fault::InvalidSample);
                break;
            }

            if self
                .frames
                .push(Captured {
                    generation,
                    samples,
                })
                .is_err()
            {
                self.state.capture_overflows.fetch_add(1, Ordering::Relaxed);
                self.state.report(Fault::CaptureOverflow);
                break;
            }

            captured += 1;
        }

        self.state.captured_frames.fetch_add(captured, Ordering::Relaxed);
        self.state.capture_waker.wake();
    }
}

pub(super) struct OutputCallback {
    pub state: Arc<State>,
    frames: Consumer<OutputFrame>,
    receipts: Producer<Receipt>,
    generation: u64,
    primed: bool,
    priming_frames: usize,
    completed_receipts: Vec<Receipt>,
}

impl OutputCallback {
    pub fn next(&mut self) -> FloatSamples {
        let generation = self.state.playback_generation.load(Ordering::Acquire);

        if generation != self.generation {
            self.generation = generation;
            self.primed = false;
        }

        while self.frames.peek().is_ok_and(|frame| frame.generation != generation) {
            let _ = self.frames.pop();
        }

        if generation == 0 || self.state.event().is_some() {
            return [0.0; CHANNELS];
        }

        if !self.primed {
            if self.frames.slots() < self.priming_frames {
                return [0.0; CHANNELS];
            }

            self.primed = true;
        }

        let Ok(frame) = self.frames.pop() else {
            self.state.playback_underflows.fetch_add(1, Ordering::Relaxed);
            self.state.report(Fault::PlaybackUnderflow);
            return [0.0; CHANNELS];
        };

        if let Some(receipt) = frame.receipt {
            if self.completed_receipts.len() == self.completed_receipts.capacity() {
                self.state.report(Fault::ReceiptOverflow);
                return [0.0; CHANNELS];
            }

            self.completed_receipts.push(receipt);
        }

        self.state.played_frames.fetch_add(1, Ordering::Relaxed);
        frame.samples
    }

    pub fn completed(&mut self) {
        if self.state.event().is_some() {
            self.completed_receipts.clear();
            return;
        }

        for receipt in self.completed_receipts.drain(..) {
            if self.receipts.push(receipt).is_err() {
                self.state.report(Fault::ReceiptOverflow);
                break;
            }
        }

        self.state.receipt_waker.wake();
    }
}

#[derive(Clone)]
pub struct CaptureFactory {
    frames: Arc<Mutex<Consumer<Captured>>>,
    state: Arc<State>,
    configuration: Configuration,
}

impl CaptureFactory {
    pub fn open(&self) -> Result<Capture> {
        let converter = resampling::Capture::new(&self.configuration)?;
        let mut capture = Capture {
            factory: self.clone(),
            generation: 0,
            converter,
            primed: false,
        };
        capture.resume()?;
        Ok(capture)
    }
}

pub struct Capture {
    factory: CaptureFactory,
    generation: u64,
    converter: resampling::Capture,
    primed: bool,
}

impl Capture {
    pub fn pause(&mut self) {
        let _ = self.factory.state.capture_generation.compare_exchange(
            self.generation,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn resume(&mut self) -> Result<()> {
        self.factory.state.check()?;

        if self.factory.state.capture_generation.load(Ordering::Acquire) != 0 {
            bail!("native capture already has an active source flow");
        }

        self.converter.reset()?;
        self.primed = false;
        let mut frames = self
            .factory
            .frames
            .lock()
            .map_err(|_| anyhow::anyhow!("native capture ownership lock failed"))?;

        if self.factory.state.capture_generation.load(Ordering::Acquire) != 0 {
            bail!("native capture ownership changed before its queue could be acquired");
        }

        while frames.pop().is_ok() {}

        self.generation = self
            .factory
            .state
            .next_capture_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .map_err(|_| anyhow::anyhow!("native capture generation exhausted"))?;
        self.factory
            .state
            .capture_generation
            .compare_exchange(0, self.generation, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| anyhow::anyhow!("native capture ownership changed"))?;
        Ok(())
    }

    pub async fn next_quantum(&mut self) -> Result<Vec<Samples>> {
        std::future::poll_fn(|context| {
            self.factory.state.capture_waker.register(context.waker());
            if !self.factory.state.ready()? {
                return Poll::Pending;
            }

            let mut frames = self
                .factory
                .frames
                .lock()
                .map_err(|_| anyhow::anyhow!("native capture ownership lock failed"))?;

            while frames.peek().is_ok_and(|frame| frame.generation != self.generation) {
                let _ = frames.pop();
            }

            let needed = self.converter.needed();
            let available = frames.slots();
            let threshold = if self.primed {
                needed
            } else {
                needed + self.factory.configuration.buffered_frames
            };

            if available < threshold {
                return Poll::Pending;
            }

            self.primed = true;
            let converted = self.converter.convert(
                (0..needed)
                    .map(|_| frames.pop().expect("capture queue availability checked").samples),
                available - needed,
            )?;
            Poll::Ready(Ok(converted
                .into_iter()
                .map(|frame| {
                    frame.map(|sample| {
                        (sample * INTEGER_SCALE)
                            .round()
                            .clamp(MINIMUM_SAMPLE as f32, MAXIMUM_SAMPLE as f32)
                            as i32
                    })
                })
                .collect()))
        })
        .await
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.pause();
    }
}

pub struct Playback {
    state: Arc<State>,
    frames: Producer<OutputFrame>,
    receipts: Consumer<Receipt>,
    converter: resampling::Playback,
    owner: Option<Owner>,
    next_frame: Option<u64>,
    capacity: usize,
    pending_receipts: VecDeque<Receipt>,
}

impl Playback {
    pub fn select(&self, generation: u64) {
        self.state.playback_generation.store(generation, Ordering::Release);
    }

    pub fn submit(&mut self, delivery: Delivery) -> Result<()> {
        self.state.check()?;
        delivery.owner.validate()?;

        if delivery.frames.len() != QUANTUM_FRAMES {
            bail!("native playback requires a complete PCM quantum");
        }

        if delivery.owner.stream_generation
            != self.state.playback_generation.load(Ordering::Acquire)
        {
            bail!("native playback delivery belongs to a retired route");
        }

        if self.owner != Some(delivery.owner) {
            self.converter.reset();
            self.owner = Some(delivery.owner);
            self.next_frame = None;
            self.pending_receipts.clear();
        }

        if self.next_frame.is_some_and(|next| next != delivery.first_frame_index) {
            bail!("native playback delivery has a discontinuous sample coordinate");
        }

        let received_samples = delivery
            .frames
            .iter()
            .flat_map(|frame| frame.received)
            .filter(|received| *received)
            .count();
        let converted = self.converter.convert(
            delivery
                .frames
                .iter()
                .map(|frame| frame.samples.map(|sample| sample as f32 / INTEGER_SCALE)),
            self.capacity - self.frames.slots(),
        )?;

        if self.frames.slots() < converted.len() {
            bail!("native playback cannot queue a complete converted quantum");
        }

        let receipt = Receipt {
            owner: delivery.owner,
            first_frame_index: delivery.first_frame_index,
            frames: QUANTUM_FRAMES,
            output_accepted: true,
            received_samples,
            local_silence_samples: QUANTUM_FRAMES * CHANNELS - received_samples,
        };
        self.pending_receipts.push_back(receipt);
        let completed = if !converted.is_empty()
            && self.pending_receipts.len() > self.converter.receipt_delay_quanta()
        {
            self.pending_receipts.pop_front()
        } else {
            None
        };
        if self.pending_receipts.len() > self.converter.receipt_delay_quanta() {
            bail!("native resampler did not advance its output consumption boundary");
        }

        let last = converted.len().saturating_sub(1);

        for (index, samples) in converted.into_iter().enumerate() {
            self.frames
                .push(OutputFrame {
                    generation: delivery.owner.stream_generation,
                    samples,
                    receipt: if index == last {
                        completed
                    } else {
                        None
                    },
                })
                .map_err(|_| anyhow::anyhow!("native playback queue overflowed"))?;
        }

        self.next_frame = Some(
            delivery
                .first_frame_index
                .checked_add(QUANTUM_FRAMES as u64)
                .context("native playback sample coordinate overflow")?,
        );
        Ok(())
    }

    pub async fn receipt(&mut self) -> Result<Receipt> {
        std::future::poll_fn(|context| {
            self.state.receipt_waker.register(context.waker());
            if !self.state.ready()? {
                return Poll::Pending;
            }

            match self.receipts.pop() {
                Ok(receipt) => Poll::Ready(Ok(receipt)),
                Err(_) => Poll::Pending,
            }
        })
        .await
    }
}

pub(super) struct Bridge {
    pub state: Arc<State>,
    pub input: InputCallback,
    pub output: OutputCallback,
    pub capture: CaptureFactory,
    pub playback: Playback,
}

impl Bridge {
    pub fn new(configuration: &Configuration) -> Result<Self> {
        let capacity = configuration.buffered_frames * QUEUE_HEADROOM;
        let capture_converter = resampling::Capture::new(configuration)?;
        let playback_converter = resampling::Playback::new(configuration)?;

        if configuration.buffered_frames + capture_converter.maximum_input_frames() > capacity
            || playback_converter.maximum_output_frames() > capacity
        {
            bail!("native audio buffering cannot hold the selected resampler's complete quantum");
        }

        let (input_sender, input_receiver) = RingBuffer::new(capacity);
        let (output_sender, output_receiver) = RingBuffer::new(capacity);
        let (receipt_sender, receipt_receiver) = RingBuffer::new(capacity);
        let state = Arc::new(State {
            capture_generation: AtomicU64::new(0),
            next_capture_generation: AtomicU64::new(1),
            playback_generation: AtomicU64::new(0),
            lifecycle: Lifecycle::default(),
            capture_waker: AtomicWaker::new(),
            receipt_waker: AtomicWaker::new(),
            failure_waker: AtomicWaker::new(),
            captured_frames: AtomicU64::new(0),
            played_frames: AtomicU64::new(0),
            capture_overflows: AtomicU64::new(0),
            playback_underflows: AtomicU64::new(0),
            xruns: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
        });

        Ok(Self {
            state: state.clone(),
            input: InputCallback {
                state: state.clone(),
                frames: input_sender,
            },
            output: OutputCallback {
                state: state.clone(),
                frames: output_receiver,
                receipts: receipt_sender,
                generation: 0,
                primed: false,
                priming_frames: configuration.buffered_frames,
                completed_receipts: Vec::with_capacity(configuration.buffered_frames),
            },
            capture: CaptureFactory {
                frames: Arc::new(Mutex::new(input_receiver)),
                state: state.clone(),
                configuration: configuration.clone(),
            },
            playback: Playback {
                state,
                frames: output_sender,
                receipts: receipt_receiver,
                converter: playback_converter,
                owner: None,
                next_frame: None,
                capacity,
                pending_receipts: VecDeque::new(),
            },
        })
    }
}
