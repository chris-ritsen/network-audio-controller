use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::Format;
use netaudio_core::native_audio::playout::{Delivery, Owner, quantum_frames};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::wave::Segment;

const QUEUED_DELIVERIES: usize = 256;
const PENDING_FINALIZATIONS: usize = 4;

pub use crate::audio::Receipt;

struct Submission {
    delivery: Delivery,
    channels: Vec<u64>,
}

struct Writer {
    directory: PathBuf,
    current: Option<(Owner, u64, Segment)>,
    receipts: BufWriter<File>,
    finalizations: mpsc::Sender<Segment>,
    format: Format,
}

impl Writer {
    fn open(
        directory: &Path,
        finalizations: mpsc::Sender<Segment>,
        format: Format,
    ) -> Result<Self> {
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
            format,
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

    fn consume(
        &mut self,
        mut delivery: Delivery,
        channels: &[u64],
        current_generation: &AtomicU64,
        channel_generations: &[AtomicU64],
    ) -> Result<Receipt> {
        delivery.owner.validate()?;

        if delivery.frames.len() != quantum_frames(self.format.sample_rate()) {
            bail!("playback requires one complete local PCM quantum");
        }

        let mut receipt = Receipt {
            owner: delivery.owner,
            first_frame_index: delivery.first_frame_index,
            frames: delivery.frames.len(),
            output_accepted: false,
            received_samples: 0,
            local_silence_samples: 0,
            channel_generations: channels.to_vec(),
            received_by_channel: vec![0; channels.len()],
        };

        if current_generation.load(Ordering::Acquire) == delivery.owner.stream_generation {
            let replace =
                self.current.as_ref().is_none_or(|(owner, _, _)| *owner != delivery.owner);

            if replace {
                self.finish_segment()?;
                let segment = Segment::create(
                    &self.directory,
                    delivery.owner.stream_generation,
                    self.format,
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
                for frame in &mut delivery.frames {
                    for (index, generation) in channels.iter().enumerate() {
                        if *generation == 0
                            || channel_generations[index].load(Ordering::Acquire) != *generation
                        {
                            frame.samples[index] = 0;
                            frame.received[index] = false;
                        }
                    }
                }
                segment.append_samples(
                    delivery.frames.iter().flat_map(|frame| frame.samples.iter().copied()),
                    delivery.frames.len(),
                )?;
                segment.flush()?;
                *next_frame_index = next_frame_index
                    .checked_add(delivery.frames.len() as u64)
                    .context("playback sample coordinate overflow")?;
                receipt.output_accepted = true;
                for (index, received) in receipt.received_by_channel.iter_mut().enumerate() {
                    *received =
                        delivery.frames.iter().filter(|frame| frame.received[index]).count();
                }
                receipt.received_samples = delivery
                    .frames
                    .iter()
                    .flat_map(|frame| frame.received.iter().copied())
                    .filter(|received| *received)
                    .count();
                receipt.local_silence_samples = delivery.frames.len()
                    * usize::from(self.format.slot_count())
                    - receipt.received_samples;
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
    sender: Option<mpsc::Sender<Submission>>,
    receipts: mpsc::Receiver<Receipt>,
    current_generation: Arc<AtomicU64>,
    channel_generations: Arc<Vec<AtomicU64>>,
    worker: JoinHandle<Result<()>>,
    finalizer: JoinHandle<Result<()>>,
}

impl WavePlayback {
    pub fn open(directory: &Path, format: Format) -> Result<Self> {
        let (finalizations, mut finalized) = mpsc::channel::<Segment>(PENDING_FINALIZATIONS);
        let mut writer = Writer::open(directory, finalizations, format)?;
        let finalizer = super::platform::spawn_background(move || {
            while let Some(segment) = finalized.blocking_recv() {
                segment.finish()?;
            }

            Ok(())
        });
        let (sender, mut receiver) = mpsc::channel::<Submission>(QUEUED_DELIVERIES);
        let (receipt_sender, receipts) = mpsc::channel(QUEUED_DELIVERIES);
        let current_generation = Arc::new(AtomicU64::new(0));
        let worker_generation = current_generation.clone();
        let channel_generations =
            Arc::new((0..format.slot_count()).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
        let worker_channels = channel_generations.clone();
        let worker = super::platform::spawn_background(move || {
            while let Some(delivery) = receiver.blocking_recv() {
                let receipt = writer.consume(
                    delivery.delivery,
                    &delivery.channels,
                    &worker_generation,
                    &worker_channels,
                )?;

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
            channel_generations,
            worker,
            finalizer,
        })
    }

    pub fn select(&self, stream_generation: u64) {
        self.current_generation.store(stream_generation, Ordering::Release);
    }

    pub fn select_channels(&self, generations: &[u64]) -> Result<()> {
        if generations.len() != self.channel_generations.len() {
            bail!("playback channel ownership geometry differs from its adapter");
        }
        for (channel, generation) in self.channel_generations.iter().zip(generations) {
            channel.store(*generation, Ordering::Release);
        }
        Ok(())
    }

    pub fn submit(&self, delivery: Delivery, channels: Vec<u64>) -> Result<()> {
        self.sender.as_ref().context("playback is closing")?.try_send(Submission {
            delivery,
            channels,
        })?;
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
    pub fn open(directory: &Path, format: Format) -> Result<Self> {
        Ok(Self::Wave(WavePlayback::open(directory, format)?))
    }

    pub fn select(&self, generation: u64) {
        match self {
            Self::Wave(playback) => playback.select(generation),
            Self::Native(playback) => playback.select(generation),
        }
    }

    pub fn select_channels(&self, generations: &[u64]) -> Result<()> {
        match self {
            Self::Wave(playback) => playback.select_channels(generations),
            Self::Native(playback) => playback.select_channels(generations),
        }
    }

    pub fn submit(&mut self, delivery: Delivery, channels: Vec<u64>) -> Result<()> {
        match self {
            Self::Wave(playback) => playback.submit(delivery, channels),
            Self::Native(playback) => playback.submit(delivery, channels),
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
