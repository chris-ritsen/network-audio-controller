use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::{Format, Packet};

pub struct Segment {
    writer: BufWriter<File>,
    partial_path: PathBuf,
    finished_path: PathBuf,
    format: Format,
    data_bytes: u32,
    pub first_sample_tick: u64,
    pub frames: u64,
}

impl Segment {
    pub fn create(
        directory: &Path,
        index: u64,
        format: Format,
        first_sample_tick: u64,
    ) -> Result<Self> {
        let finished_path = directory.join(format!("audio-{index:06}.wav"));
        let partial_path = directory.join(format!("audio-{index:06}.partial.wav"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial_path)
            .with_context(|| format!("create {}", partial_path.display()))?;
        let mut segment = Self {
            writer: BufWriter::new(file),
            partial_path,
            finished_path,
            format,
            data_bytes: 0,
            first_sample_tick,
            frames: 0,
        };
        segment.write_header()?;
        Ok(segment)
    }

    fn write_header(&mut self) -> Result<()> {
        let frame_bytes = u16::try_from(self.format.frame_bytes())
            .context("WAVE frame size is unrepresentable")?;
        let byte_rate = self
            .format
            .sample_rate()
            .checked_mul(u32::from(frame_bytes))
            .context("WAVE byte rate is unrepresentable")?;
        let payload_size = self
            .data_bytes
            .checked_add(self.data_bytes % 2)
            .context("WAVE data padding overflow")?;
        let container_size =
            payload_size.checked_add(36).context("WAVE file size is unrepresentable")?;
        self.writer.seek(SeekFrom::Start(0))?;
        self.writer.write_all(b"RIFF")?;
        self.writer.write_all(&container_size.to_le_bytes())?;
        self.writer.write_all(b"WAVEfmt ")?;
        self.writer.write_all(&16u32.to_le_bytes())?;
        self.writer.write_all(&1u16.to_le_bytes())?;
        self.writer.write_all(&self.format.slot_count().to_le_bytes())?;
        self.writer.write_all(&self.format.sample_rate().to_le_bytes())?;
        self.writer.write_all(&byte_rate.to_le_bytes())?;
        self.writer.write_all(&frame_bytes.to_le_bytes())?;
        self.writer.write_all(&((self.format.sample_width().bytes() * 8) as u16).to_le_bytes())?;
        self.writer.write_all(b"data")?;
        self.writer.write_all(&self.data_bytes.to_le_bytes())?;
        Ok(())
    }

    pub fn append(&mut self, packet: &Packet<'_>) -> Result<()> {
        self.append_samples(packet.samples(), packet.frame_count())
    }

    pub fn append_samples(
        &mut self,
        samples: impl Iterator<Item = i32>,
        frames: usize,
    ) -> Result<()> {
        let bytes = frames
            .checked_mul(self.format.frame_bytes())
            .and_then(|length| u32::try_from(length).ok())
            .context("WAVE packet size overflow")?;
        let length = self.data_bytes.checked_add(bytes).context("WAVE data size overflow")?;
        if length > u32::MAX - 37 {
            bail!("WAVE segment reached its representable size; recording remains partial");
        }

        let mut encoded = Vec::with_capacity(bytes as usize);
        let sample_bits = self.format.sample_width().bytes() * 8;
        let minimum = -(1i64 << (sample_bits - 1));
        let maximum = (1i64 << (sample_bits - 1)) - 1;

        for sample in samples {
            if !(minimum..=maximum).contains(&i64::from(sample)) {
                bail!("WAVE sample exceeds its declared PCM width");
            }

            if encoded.len() >= bytes as usize {
                bail!("WAVE sample sequence exceeds its declared frame count");
            }

            encoded.extend_from_slice(&sample.to_le_bytes()[..self.format.sample_width().bytes()]);
        }

        if encoded.len() != bytes as usize {
            bail!("WAVE sample sequence does not contain its declared complete frames");
        }

        self.writer.write_all(&encoded)?;

        self.data_bytes = length;
        self.frames =
            self.frames.checked_add(frames as u64).context("WAVE frame count overflow")?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<PathBuf> {
        if !self.data_bytes.is_multiple_of(2) {
            self.writer.write_all(&[0])?;
        }

        self.write_header()?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        drop(self.writer);
        std::fs::rename(&self.partial_path, &self.finished_path)?;
        Ok(self.finished_path)
    }
}
