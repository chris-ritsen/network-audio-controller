use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::endpoint_control::{CHANNEL_COUNT, SAMPLE_RATE};
use netaudio_core::native_audio::playout::{Delivery, Owner, QUANTUM_FRAMES};
use netaudio_core::native_audio::{Format, SampleWidth};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::wave::Segment;

const QUEUED_DELIVERIES: usize = 256;
const PENDING_FINALIZATIONS: usize = 4;

pub use crate::audio::Receipt;

struct Writer {
    directory: PathBuf,
    current: Option<(Owner, u64, Segment)>,
    receipts: BufWriter<File>,
    finalizations: mpsc::Sender<Segment>,
}

impl Writer {
    fn open(directory: &Path, finalizations: mpsc::Sender<Segment>) -> Result<Self> {
        std::fs::create_dir(directory)
            .with_context(|| format!("create new playback directory {}", directory.display()))?;
        let receipts = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("consumption.jsonl"))?;

        Ok(Self {
            directory: directory.to_owned(),
            current: None,
            receipts: BufWriter::new(receipts),
            finalizations,
        })
    }

    fn finish_segment(&mut self) -> Result<()> {
        if let Some((_, _, segment)) = self.current.take() {
            self.finalizations.try_send(segment).map_err(|error| {
                anyhow::anyhow!("playback file finalization queue failed: {error}")
            })?;
        }

        Ok(())
    }

    fn consume(&mut self, delivery: Delivery, current_generation: &AtomicU64) -> Result<Receipt> {
        delivery.owner.validate()?;

        if delivery.frames.len() != QUANTUM_FRAMES {
            bail!("playback requires one complete local PCM quantum");
        }

        let mut receipt = Receipt {
            owner: delivery.owner,
            first_frame_index: delivery.first_frame_index,
            frames: delivery.frames.len(),
            output_accepted: false,
            received_samples: 0,
            local_silence_samples: 0,
        };

        if current_generation.load(Ordering::Acquire) == delivery.owner.stream_generation {
            let replace =
                self.current.as_ref().is_none_or(|(owner, _, _)| *owner != delivery.owner);

            if replace {
                self.finish_segment()?;
                let format = Format::new(SAMPLE_RATE, CHANNEL_COUNT, SampleWidth::Signed24)?;
                let segment = Segment::create(
                    &self.directory,
                    delivery.owner.stream_generation,
                    format,
                    delivery.first_frame_index,
                )?;
                self.current = Some((delivery.owner, delivery.first_frame_index, segment));
            }

            let (_, next_frame_index, segment) =
                self.current.as_mut().context("playback segment is unavailable")?;

            if *next_frame_index != delivery.first_frame_index {
                bail!("playback delivery is repeated or has a discontinuous sample coordinate");
            }

            if current_generation.load(Ordering::Acquire) == delivery.owner.stream_generation {
                segment.append_samples(
                    delivery.frames.iter().flat_map(|frame| frame.samples),
                    delivery.frames.len(),
                )?;
                segment.flush()?;
                *next_frame_index = next_frame_index
                    .checked_add(delivery.frames.len() as u64)
                    .context("playback sample coordinate overflow")?;
                receipt.output_accepted = true;
                receipt.received_samples = delivery
                    .frames
                    .iter()
                    .flat_map(|frame| frame.received)
                    .filter(|received| *received)
                    .count();
                receipt.local_silence_samples =
                    delivery.frames.len() * CHANNEL_COUNT as usize - receipt.received_samples;
            }
        }

        serde_json::to_writer(&mut self.receipts, &receipt)?;
        self.receipts.write_all(b"\n")?;
        self.receipts.flush()?;
        Ok(receipt)
    }

    fn finish(mut self) -> Result<()> {
        self.finish_segment()?;
        self.receipts.flush()?;
        self.receipts.get_ref().sync_all()?;
        Ok(())
    }
}

pub struct WavePlayback {
    sender: Option<mpsc::Sender<Delivery>>,
    receipts: mpsc::Receiver<Receipt>,
    current_generation: Arc<AtomicU64>,
    worker: JoinHandle<Result<()>>,
    finalizer: JoinHandle<Result<()>>,
}

impl WavePlayback {
    pub fn open(directory: &Path) -> Result<Self> {
        let (finalizations, mut finalized) = mpsc::channel::<Segment>(PENDING_FINALIZATIONS);
        let mut writer = Writer::open(directory, finalizations)?;
        let finalizer = tokio::task::spawn_blocking(move || {
            while let Some(segment) = finalized.blocking_recv() {
                segment.finish()?;
            }

            Ok(())
        });
        let (sender, mut receiver) = mpsc::channel(QUEUED_DELIVERIES);
        let (receipt_sender, receipts) = mpsc::channel(QUEUED_DELIVERIES);
        let current_generation = Arc::new(AtomicU64::new(0));
        let worker_generation = current_generation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            while let Some(delivery) = receiver.blocking_recv() {
                let receipt = writer.consume(delivery, &worker_generation)?;

                if receipt_sender.blocking_send(receipt).is_err() {
                    break;
                }
            }

            writer.finish()
        });

        Ok(Self {
            sender: Some(sender),
            receipts,
            current_generation,
            worker,
            finalizer,
        })
    }

    pub fn select(&self, stream_generation: u64) {
        self.current_generation.store(stream_generation, Ordering::Release);
    }

    pub fn submit(&self, delivery: Delivery) -> Result<()> {
        self.sender.as_ref().context("playback is closing")?.try_send(delivery)?;
        Ok(())
    }

    pub async fn receipt(&mut self) -> Result<Receipt> {
        self.receipts
            .recv()
            .await
            .context("playback worker has stopped; file output is unavailable")
    }

    pub async fn close(mut self) -> Result<()> {
        self.select(0);
        self.sender.take();
        self.receipts.close();
        let worker = self.worker.await;
        let finalizer = self.finalizer.await;
        worker??;
        finalizer??;
        Ok(())
    }
}

pub enum Playback {
    Wave(WavePlayback),
    Native(Box<crate::audio::Playback>),
}

impl Playback {
    pub fn open(directory: &Path) -> Result<Self> {
        Ok(Self::Wave(WavePlayback::open(directory)?))
    }

    pub fn select(&self, generation: u64) {
        match self {
            Self::Wave(playback) => playback.select(generation),
            Self::Native(playback) => playback.select(generation),
        }
    }

    pub fn submit(&mut self, delivery: Delivery) -> Result<()> {
        match self {
            Self::Wave(playback) => playback.submit(delivery),
            Self::Native(playback) => playback.submit(delivery),
        }
    }

    pub async fn receipt(&mut self) -> Result<Receipt> {
        match self {
            Self::Wave(playback) => playback.receipt().await,
            Self::Native(playback) => playback.receipt().await,
        }
    }

    pub async fn close(self) -> Result<()> {
        match self {
            Self::Wave(playback) => playback.close().await,
            Self::Native(playback) => {
                playback.select(0);
                Ok(())
            }
        }
    }
}
