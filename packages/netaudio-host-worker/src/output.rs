use anyhow::{Context, Result, ensure};
use netaudio_core::host_audio::protocol::{
    Event, MAXIMUM_FRAME_BYTES, Message, Provider, Purpose, VERSION,
};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

pub struct Output {
    sequence: u64,
    writer: std::io::BufWriter<std::io::Stdout>,
}

impl Output {
    pub fn new(provider: Provider, purpose: Purpose) -> Result<Self> {
        let path = std::env::current_exe().context("locate host audio worker executable")?;
        let mut file = std::fs::File::open(path).context("open host audio worker executable")?;
        let mut digest = Sha256::new();
        let mut bytes = [0; 65536];
        loop {
            let length = file.read(&mut bytes)?;
            if length == 0 {
                break;
            }
            digest.update(&bytes[..length]);
        }
        let mut output = Self {
            sequence: 0,
            writer: std::io::BufWriter::new(std::io::stdout()),
        };
        output.send(Event::Hello {
            protocol_version: VERSION,
            provider,
            purpose,
            executable_sha256: format!("{:x}", digest.finalize()),
            package_version: env!("CARGO_PKG_VERSION").into(),
            process_id: std::process::id(),
        })?;
        Ok(output)
    }

    pub fn send(&mut self, event: Event) -> Result<()> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .context("worker sequence exhausted")?;
        let bytes = serde_json::to_vec(&Message {
            sequence: self.sequence,
            event,
        })?;
        ensure!(
            bytes.len() <= MAXIMUM_FRAME_BYTES,
            "worker response exceeds frame budget"
        );
        self.writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer
            .flush()
            .context("write host audio worker response")
    }
}
