use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::endpoint_control::{CHANNEL_COUNT, SAMPLE_BITS, SAMPLE_RATE};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub const QUANTUM_FRAMES: usize = 48;
pub const PRIMING_FRAMES: usize = 96;
pub const TRANSMIT_PIPELINE_FRAMES: u64 = 96;

const CAPTURE_QUEUE_BLOCKS: usize = 4;
const RIFF_HEADER_BYTES: usize = 12;
const CHUNK_HEADER_BYTES: usize = 8;
const PCM_FORMAT_BYTES: usize = 16;
const PCM_FORMAT_TAG: u16 = 1;
const BITS_PER_BYTE: u16 = 8;
const SAMPLE_BYTES: usize = (SAMPLE_BITS / BITS_PER_BYTE) as usize;
const FRAME_BYTES: usize = CHANNEL_COUNT as usize * SAMPLE_BYTES;
const SIGN_EXTENSION_SHIFT: u32 = i32::BITS - SAMPLE_BITS as u32;

pub type Frame = [i32; CHANNEL_COUNT as usize];

struct WaveSource {
    file: File,
    remaining_bytes: u64,
}

impl WaveSource {
    fn open(path: &Path) -> Result<Self> {
        let mut file =
            File::open(path).with_context(|| format!("open capture file {}", path.display()))?;
        let metadata = file.metadata()?;

        if !metadata.is_file() {
            bail!("capture source must be a regular PCM WAVE file");
        }

        let mut header = [0; RIFF_HEADER_BYTES];
        file.read_exact(&mut header)?;

        if &header[..4] != b"RIFF" || &header[8..] != b"WAVE" {
            bail!("capture source must use the RIFF WAVE container");
        }

        let container_bytes =
            u64::from(u32::from_le_bytes(header[4..8].try_into()?)) + CHUNK_HEADER_BYTES as u64;

        if container_bytes != metadata.len() {
            bail!("capture WAVE declared size differs from its complete file size");
        }

        let mut offset = RIFF_HEADER_BYTES as u64;
        let mut format_seen = false;
        let mut data = None;

        while offset < container_bytes {
            if container_bytes - offset < CHUNK_HEADER_BYTES as u64 {
                bail!("capture WAVE has an incomplete chunk header");
            }

            let mut chunk = [0; CHUNK_HEADER_BYTES];
            file.read_exact(&mut chunk)?;
            let length = u64::from(u32::from_le_bytes(chunk[4..].try_into()?));
            let start = offset + CHUNK_HEADER_BYTES as u64;
            let end = start.checked_add(length).context("capture WAVE chunk size overflow")?;
            let next = end.checked_add(length % 2).context("capture WAVE padding overflow")?;

            if next > container_bytes {
                bail!("capture WAVE chunk extends beyond its declared container");
            }

            match &chunk[..4] {
                b"fmt " => {
                    if format_seen || length < PCM_FORMAT_BYTES as u64 {
                        bail!("capture WAVE format is duplicated or incomplete");
                    }

                    let mut format = [0; PCM_FORMAT_BYTES];
                    file.read_exact(&mut format)?;
                    let tag = u16::from_le_bytes(format[..2].try_into()?);
                    let channels = u16::from_le_bytes(format[2..4].try_into()?);
                    let sample_rate = u32::from_le_bytes(format[4..8].try_into()?);
                    let bytes_per_second = u32::from_le_bytes(format[8..12].try_into()?);
                    let block_alignment = u16::from_le_bytes(format[12..14].try_into()?);
                    let sample_bits = u16::from_le_bytes(format[14..].try_into()?);

                    if tag != PCM_FORMAT_TAG
                        || channels != CHANNEL_COUNT
                        || sample_rate != SAMPLE_RATE
                        || sample_bits != SAMPLE_BITS
                        || block_alignment as usize != FRAME_BYTES
                        || bytes_per_second != SAMPLE_RATE * FRAME_BYTES as u32
                    {
                        bail!(
                            "capture requires stereo signed PCM24 at 48000 Hz with consistent WAVE framing"
                        );
                    }

                    format_seen = true;
                }
                b"data" if data.replace((start, length)).is_some() => {
                    bail!("capture WAVE contains more than one data chunk");
                }
                _ => {}
            }

            file.seek(SeekFrom::Start(next))?;
            offset = next;
        }

        let (start, length) = data.context("capture WAVE has no PCM data chunk")?;

        if !format_seen || length == 0 || length % FRAME_BYTES as u64 != 0 {
            bail!("capture WAVE needs its format and a positive whole number of PCM frames");
        }

        let frames = length / FRAME_BYTES as u64;

        if frames < PRIMING_FRAMES as u64 || !frames.is_multiple_of(QUANTUM_FRAMES as u64) {
            bail!(
                "capture WAVE needs at least {PRIMING_FRAMES} frames and a whole number of {QUANTUM_FRAMES}-frame quanta"
            );
        }

        file.seek(SeekFrom::Start(start))?;

        Ok(Self {
            file,
            remaining_bytes: length,
        })
    }

    fn read_quantum(&mut self) -> Result<Vec<Frame>> {
        let frames =
            (self.remaining_bytes / FRAME_BYTES as u64).min(QUANTUM_FRAMES as u64) as usize;
        let mut bytes = vec![0; frames * FRAME_BYTES];
        self.file.read_exact(&mut bytes)?;
        self.remaining_bytes -= bytes.len() as u64;
        let mut output = Vec::with_capacity(frames);

        for frame in bytes.as_chunks::<FRAME_BYTES>().0 {
            let mut samples = [0; CHANNEL_COUNT as usize];

            for (channel, sample) in frame.as_chunks::<SAMPLE_BYTES>().0.iter().enumerate() {
                let value = i32::from_le_bytes([sample[0], sample[1], sample[2], 0]);
                samples[channel] = (value << SIGN_EXTENSION_SHIFT) >> SIGN_EXTENSION_SHIFT;
            }

            output.push(samples);
        }

        Ok(output)
    }
}

pub struct WaveCapture {
    receiver: mpsc::Receiver<Vec<Frame>>,
    worker: Option<JoinHandle<Result<()>>>,
}

impl WaveCapture {
    pub fn open(path: &Path) -> Result<Self> {
        let mut source = WaveSource::open(path)?;
        let (sender, receiver) = mpsc::channel(CAPTURE_QUEUE_BLOCKS);
        let worker = tokio::task::spawn_blocking(move || {
            while source.remaining_bytes != 0 {
                let frames = source.read_quantum()?;

                if sender.blocking_send(frames).is_err() {
                    return Ok(());
                }
            }

            Ok(())
        });

        Ok(Self {
            receiver,
            worker: Some(worker),
        })
    }

    pub async fn next_quantum(&mut self) -> Result<Option<Vec<Frame>>> {
        match self.receiver.recv().await {
            Some(frames) => Ok(Some(frames)),
            None => {
                self.finish().await?;
                Ok(None)
            }
        }
    }

    pub async fn close(mut self) -> Result<()> {
        self.receiver.close();
        self.finish().await?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        if let Some(worker) = self.worker.as_mut() {
            let result = worker.await?;
            self.worker = None;
            result?;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub enum CaptureFactory {
    Wave(std::path::PathBuf),
    Native(crate::audio::CaptureFactory),
}

impl CaptureFactory {
    pub fn open(&self) -> Result<Capture> {
        match self {
            Self::Wave(path) => Ok(Capture::Wave(WaveCapture::open(path)?)),
            Self::Native(factory) => Ok(Capture::Native(Box::new(factory.open()?))),
        }
    }
}

pub enum Capture {
    Wave(WaveCapture),
    Native(Box<crate::audio::Capture>),
}

impl Capture {
    pub async fn next_quantum(&mut self) -> Result<Option<Vec<Frame>>> {
        match self {
            Self::Wave(capture) => capture.next_quantum().await,
            Self::Native(capture) => Ok(Some(capture.next_quantum().await?)),
        }
    }

    pub fn pause(&mut self) {
        if let Self::Native(capture) = self {
            capture.pause();
        }
    }

    pub fn resume(&mut self) -> Result<()> {
        if let Self::Native(capture) = self {
            capture.resume()?;
        }

        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        match self {
            Self::Wave(capture) => capture.close().await,
            Self::Native(capture) => {
                drop(capture);
                Ok(())
            }
        }
    }
}
