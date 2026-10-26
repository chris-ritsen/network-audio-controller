use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use netaudio_core::native_audio::{Format, Packet};
use serde::Serialize;
use serde_json::json;

use crate::wave::Segment;

pub struct RecordingContext {
    pub source: Option<SocketAddrV4>,
    pub destination: SocketAddrV4,
    pub interface_address: Ipv4Addr,
    pub format: Format,
    pub output: PathBuf,
}

pub struct SampleInterval {
    pub first_tick: u64,
    pub next_tick: u64,
}

#[derive(Serialize)]
pub struct Summary {
    pub source_control: Option<crate::unicast::ControlSummary>,
    observed_frames_per_packet: std::collections::BTreeMap<usize, u64>,
    status: String,
    started_at: String,
    finished_at: Option<String>,
    source: Option<SocketAddrV4>,
    destination: SocketAddrV4,
    interface_address: Ipv4Addr,
    sample_rate: u32,
    sample_bits: usize,
    slot_count: u16,
    pub valid_packets: u64,
    rejected_packets: u64,
    pub other_source_packets: u64,
    received_frames: u64,
    nonzero_samples: u64,
    peak_sample_magnitude: Vec<u32>,
    discontinuities: u64,
    segments: u64,
    clock_validation: &'static str,
    physical_output: &'static str,
    output_directory: PathBuf,
    failure: Option<String>,
}

pub struct Recording {
    directory: PathBuf,
    packets: BufWriter<File>,
    segments: BufWriter<File>,
    segment: Option<Segment>,
    expected_sample_tick: Option<u64>,
    pub summary: Summary,
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn create_file(directory: &std::path::Path, name: &str) -> Result<BufWriter<File>> {
    Ok(BufWriter::new(OpenOptions::new().write(true).create_new(true).open(directory.join(name))?))
}

fn write_record(writer: &mut BufWriter<File>, record: &impl Serialize) -> Result<()> {
    serde_json::to_writer(&mut *writer, record)?;
    writer.write_all(b"\n")?;
    Ok(())
}

impl Recording {
    pub fn begin(configuration: &RecordingContext) -> Result<Self> {
        std::fs::create_dir(&configuration.output).with_context(|| {
            format!("create new recording directory {}", configuration.output.display())
        })?;
        let directory = std::fs::canonicalize(&configuration.output)?;
        let recording = Self {
            packets: create_file(&directory, "packets.partial.jsonl")?,
            segments: create_file(&directory, "segments.partial.jsonl")?,
            segment: None,
            expected_sample_tick: None,
            summary: Summary {
                source_control: None,
                observed_frames_per_packet: std::collections::BTreeMap::new(),
                status: "receiving".into(),
                started_at: now(),
                finished_at: None,
                source: configuration.source,
                destination: configuration.destination,
                interface_address: configuration.interface_address,
                sample_rate: configuration.format.sample_rate(),
                sample_bits: configuration.format.sample_width().bytes() * 8,
                slot_count: configuration.format.slot_count(),
                valid_packets: 0,
                rejected_packets: 0,
                other_source_packets: 0,
                received_frames: 0,
                nonzero_samples: 0,
                peak_sample_magnitude: vec![0; usize::from(configuration.format.slot_count())],
                discontinuities: 0,
                segments: 0,
                clock_validation: "not_performed",
                physical_output: "not_performed",
                output_directory: directory.clone(),
                failure: None,
            },
            directory,
        };
        recording.manifest()?;
        Ok(recording)
    }

    pub fn manifest(&self) -> Result<()> {
        write_manifest(&self.directory, &self.summary)
    }

    fn finish_segment(&mut self) -> Result<()> {
        if let Some(segment) = self.segment.take() {
            let first_sample_tick = segment.first_sample_tick;
            let frames = segment.frames;
            let path = segment.finish()?;
            write_record(
                &mut self.segments,
                &json!({
                    "path": path.file_name().and_then(|name| name.to_str()).context("recording segment filename is not UTF-8")?,
                    "first_sample_tick": first_sample_tick,
                    "frames": frames,
                    "sample_rate": self.summary.sample_rate,
                }),
            )?;
        }

        Ok(())
    }

    pub fn admit(
        &mut self,
        data: &[u8],
        received_at: &str,
        source: SocketAddrV4,
        format: Format,
    ) -> Result<Option<SampleInterval>> {
        let packet = match Packet::decode(data, format) {
            Ok(packet) => packet,
            Err(error) => {
                self.reject(data, received_at, source, &error.to_string())?;
                return Ok(None);
            }
        };

        let tick = packet.time.ticks(format.sample_rate())?;
        let next_tick =
            tick.checked_add(packet.frame_count() as u64).context("sample interval overflow")?;
        let discontinuity = self.expected_sample_tick.is_some_and(|expected| expected != tick);
        write_record(
            &mut self.packets,
            &json!({
                "received_at": received_at,
                "source": source,
                "datagram": data,
                "sample_tick": tick,
                "frames": packet.frame_count(),
                "expected_sample_tick": self.expected_sample_tick,
                "discontinuity": discontinuity,
            }),
        )?;
        if discontinuity {
            self.summary.discontinuities += 1;
            self.finish_segment()?;
        }

        if self.segment.is_none() {
            self.summary.segments += 1;
            self.segment =
                Some(Segment::create(&self.directory, self.summary.segments, format, tick)?);
        }

        self.segment.as_mut().context("recording segment is unavailable")?.append(&packet)?;
        self.expected_sample_tick = Some(next_tick);
        for (index, sample) in packet.samples().enumerate() {
            if sample != 0 {
                self.summary.nonzero_samples += 1;
            }

            let channel = index % usize::from(format.slot_count());
            self.summary.peak_sample_magnitude[channel] =
                self.summary.peak_sample_magnitude[channel].max(sample.unsigned_abs());
        }

        self.summary.valid_packets += 1;
        *self.summary.observed_frames_per_packet.entry(packet.frame_count()).or_default() += 1;
        self.summary.received_frames = self
            .summary
            .received_frames
            .checked_add(packet.frame_count() as u64)
            .context("recording frame count overflow")?;
        Ok(Some(SampleInterval {
            first_tick: tick,
            next_tick,
        }))
    }

    pub fn pin_source(&mut self, source: SocketAddrV4) {
        self.summary.source = Some(source);
    }

    pub fn reject(
        &mut self,
        data: &[u8],
        received_at: &str,
        source: SocketAddrV4,
        reason: &str,
    ) -> Result<()> {
        self.summary.rejected_packets += 1;
        write_record(
            &mut self.packets,
            &json!({
                "received_at": received_at, "source": source, "datagram": data, "rejection": reason,
            }),
        )
    }

    pub fn finish(mut self, result: &Result<&str>) -> Result<Summary> {
        self.finish_segment()?;
        for writer in [&mut self.packets, &mut self.segments] {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        self.summary.finished_at = Some(now());
        match result {
            Ok(_) if self.summary.valid_packets == 0 => {
                self.summary.status = "no_supported_audio".into()
            }

            Ok(status) => self.summary.status = (*status).into(),
            Err(error) => {
                self.summary.status = "failed".into();
                self.summary.failure = Some(format!("{error:#}"));
            }
        }
        drop(self.packets);
        drop(self.segments);
        for name in ["packets", "segments"] {
            std::fs::rename(
                self.directory.join(format!("{name}.partial.jsonl")),
                self.directory.join(format!("{name}.jsonl")),
            )?;
        }
        write_manifest(&self.directory, &self.summary)?;
        Ok(self.summary)
    }
}

fn write_manifest(directory: &std::path::Path, summary: &Summary) -> Result<()> {
    let path = directory.join("manifest.partial.json");
    let mut file = File::create(&path)?;
    serde_json::to_writer_pretty(&mut file, summary)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(path, directory.join("manifest.json"))?;
    Ok(())
}
