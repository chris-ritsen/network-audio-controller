use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::time::Instant;

use super::configuration::{Configuration, LatePolicy};
use super::{Ready, clock_sample};

pub(super) struct Recording {
    pub(super) directory: PathBuf,
    frames: Option<BufWriter<File>>,
}

impl Recording {
    pub(super) fn begin(
        directory: PathBuf,
        configuration: &Configuration,
        record_frames: bool,
    ) -> Result<Self> {
        std::fs::create_dir(&directory).context("create a new video output directory")?;
        write_json(&directory.join("configuration.json"), configuration)?;
        let frames = if record_frames {
            Some(BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(directory.join("frames.jsonl"))?,
            ))
        } else {
            None
        };
        Ok(Self {
            directory,
            frames,
        })
    }

    pub(super) fn publish(
        &mut self,
        frame: Ready,
        origin: Instant,
        configuration: &Configuration,
    ) -> Result<bool> {
        let Some(frames) = &mut self.frames else {
            return Ok(true);
        };
        let name = format!("frame-{:012}.nv12", frame.identity.access_unit);
        let mut output =
            OpenOptions::new().write(true).create_new(true).open(self.directory.join(&name))?;
        output.write_all(&frame.bytes).context("write complete NV12 frame")?;
        output.flush()?;
        let now_nanoseconds = clock_sample(origin)?;
        let last_allowed = frame
            .identity
            .deadline_nanoseconds
            .checked_add(i64::try_from(configuration.playout.allowed_lateness_nanoseconds)?)
            .context("late-frame deadline overflow")?;

        if matches!(configuration.playout.late_policy, LatePolicy::Drop)
            && now_nanoseconds > last_allowed
        {
            drop(output);
            std::fs::remove_file(self.directory.join(&name))?;
            return Ok(false);
        }
        serde_json::to_writer(
            &mut *frames,
            &serde_json::json!({
                "identity": frame.identity,
                "file": name,
                "bytes": frame.bytes.len(),
                "width": configuration.width,
                "height": configuration.height,
                "pixel_format": "NV12",
                "colorimetry": configuration.colorimetry,
                "published_nanoseconds": now_nanoseconds,
                "lateness_nanoseconds": now_nanoseconds.saturating_sub(frame.identity.deadline_nanoseconds).max(0)
            }),
        )?;
        frames.write_all(b"\n")?;
        frames.flush()?;
        Ok(true)
    }
}

pub(super) fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}
