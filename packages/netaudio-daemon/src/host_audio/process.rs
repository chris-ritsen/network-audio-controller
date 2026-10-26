use super::provider::{Observation, Status};
use anyhow::{Context, Result, bail, ensure};
use netaudio_core::host_audio::{
    protocol::{
        Event, MAXIMUM_FRAME_BYTES, Message, Provider, Purpose, Request, RequestAction, VERSION,
    },
    snapshot::Snapshot,
};
use sha2::{Digest, Sha256};
use std::{io::Read, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin},
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub(crate) struct Process {
    child: Child,
    input: ChildStdin,
    messages: mpsc::Receiver<Result<Message>>,
    reader: JoinHandle<()>,
    provider: Provider,
    purpose: Purpose,
    digest: String,
    process_id: u32,
    message_sequence: u64,
    request_sequence: u64,
    observation_revision: u64,
    hello: bool,
    pub ready: bool,
    pub started: tokio::time::Instant,
}

impl Process {
    pub async fn spawn(
        path: &Path,
        provider: Provider,
        purpose: Purpose,
        host: &str,
    ) -> Result<Self> {
        let path = path.to_owned();
        let source = path.clone();
        let digest = tokio::task::spawn_blocking(move || {
            let mut options = std::fs::OpenOptions::new();
            options.read(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.custom_flags(nix::libc::O_NONBLOCK);
            }
            let mut file = options
                .open(&source)
                .with_context(|| format!("open host audio worker {}", source.display()))?;
            ensure!(
                file.metadata()?.is_file(),
                "host audio worker is not a regular file"
            );
            let mut digest = Sha256::new();
            let mut bytes = [0; 65536];
            loop {
                let count = file.read(&mut bytes)?;
                if count == 0 {
                    break;
                }
                digest.update(&bytes[..count]);
            }
            Ok::<_, anyhow::Error>(format!("{:x}", digest.finalize()))
        })
        .await
        .context("host audio worker digest task failed")??;
        let mut command = tokio::process::Command::new(&path);
        if purpose == Purpose::Measure {
            command.arg("--measure");
        }
        if purpose == Purpose::Record {
            command.arg("--record");
        }
        let mut child = command
            .env("NETAUDIO_HOST_NAME", host)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("start host audio worker {}", path.display()))?;
        let process_id = child
            .id()
            .context("host audio worker has no process identity")?;
        tracing::debug!(?provider, process_id, "host audio worker started");
        let input = child
            .stdin
            .take()
            .context("host audio worker input is unavailable")?;
        let mut output = child
            .stdout
            .take()
            .context("host audio worker output is unavailable")?;
        let (messages, received) = mpsc::channel(32);
        let reader = tokio::spawn(async move {
            loop {
                let message = async {
                    let length = output
                        .read_u32()
                        .await
                        .context("host audio worker output closed")?
                        as usize;
                    ensure!(
                        length > 0 && length <= MAXIMUM_FRAME_BYTES,
                        "host audio worker frame exceeds the transport budget"
                    );
                    let mut bytes = vec![0; length];
                    output
                        .read_exact(&mut bytes)
                        .await
                        .context("host audio worker output ended inside a frame")?;
                    serde_json::from_slice::<Message>(&bytes)
                        .context("decode host audio worker response")
                }
                .await;
                let failed = message.is_err();
                if messages.send(message).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            input,
            messages: received,
            reader,
            provider,
            purpose,
            digest,
            process_id,
            message_sequence: 0,
            request_sequence: 0,
            observation_revision: 0,
            hello: false,
            ready: false,
            started: tokio::time::Instant::now(),
        })
    }

    pub fn next_sequence(&mut self) -> u64 {
        self.request_sequence += 1;
        self.request_sequence
    }

    pub async fn send(&mut self, sequence: u64, action: RequestAction) -> Result<()> {
        let bytes = serde_json::to_vec(&Request { sequence, action })?;
        ensure!(
            bytes.len() <= MAXIMUM_FRAME_BYTES,
            "host audio worker request exceeds the transport budget"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            self.input.write_u32(bytes.len() as u32).await?;
            self.input.write_all(&bytes).await?;
            self.input.flush().await
        })
        .await
        .context("host audio worker input deadline expired")?
        .context("write host audio worker request")
    }

    pub async fn receive(&mut self) -> Result<Message> {
        let message = self
            .messages
            .recv()
            .await
            .context("host audio worker response task stopped")??;
        ensure!(
            message.sequence == self.message_sequence + 1,
            "host audio worker response sequence has a gap"
        );
        self.message_sequence = message.sequence;
        tracing::debug!(provider = ?self.provider, process_id = self.process_id, sequence = message.sequence, "host audio worker response received");
        match &message.event {
            Event::Hello {
                protocol_version,
                provider,
                purpose,
                executable_sha256,
                process_id,
                package_version,
            } => {
                ensure!(
                    !self.hello && message.sequence == 1,
                    "host audio worker repeated its handshake"
                );
                ensure!(
                    *protocol_version == VERSION
                        && *provider == self.provider
                        && *purpose == self.purpose
                        && *process_id == self.process_id,
                    "host audio worker identity does not match its owner"
                );
                ensure!(
                    executable_sha256 == &self.digest
                        && package_version == env!("CARGO_PKG_VERSION"),
                    "host audio worker build does not match its installed artifact"
                );
                self.hello = true;
                self.ready = self.purpose != Purpose::Observe;
            }
            Event::Observation {
                revision,
                observed_at,
                snapshot,
            } => {
                ensure!(
                    self.hello && self.purpose == Purpose::Observe,
                    "host audio worker observed state before its handshake"
                );
                ensure!(
                    *revision == self.observation_revision + 1,
                    "host audio provider observation revision has a gap"
                );
                chrono::DateTime::parse_from_rfc3339(observed_at)
                    .context("host audio worker timestamp is invalid")?;
                ensure!(
                    matches!(
                        (self.provider, snapshot.as_ref()),
                        (Provider::Jack, Snapshot::Jack(_)) | (Provider::Pulse, Snapshot::Pulse(_))
                    ),
                    "host audio worker returned the wrong provider state"
                );
                self.observation_revision = *revision;
                self.ready = true;
            }
            Event::Unavailable { .. } => {
                ensure!(self.hello, "host audio worker failed before its handshake")
            }
            Event::Measurement { .. } => ensure!(
                self.ready && self.purpose == Purpose::Measure,
                "host audio worker returned an unexpected measurement"
            ),
            Event::Recording { .. } => ensure!(
                self.ready && self.purpose == Purpose::Record,
                "host audio worker returned an unexpected recording"
            ),
            Event::Completed { status, body, .. } => ensure!(
                self.ready
                    && self.purpose == Purpose::Observe
                    && (200..=599).contains(status)
                    && body.is_object(),
                "host audio worker completion is malformed"
            ),
            Event::Dispatching { .. } => ensure!(
                self.ready && self.purpose == Purpose::Observe,
                "host audio worker dispatched before its initial observation"
            ),
        }
        Ok(message)
    }

    pub async fn initialize(
        &mut self,
        observations: &watch::Sender<Observation>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let deadline = self.started + Duration::from_secs(10);
        while !self.ready {
            let message = tokio::select! {
                _ = cancellation.cancelled() => bail!("host audio initialization canceled"),
                _ = tokio::time::sleep_until(deadline) => bail!("host audio initialization deadline expired"),
                message = self.receive() => message?,
            };
            match message.event {
                Event::Hello {
                    executable_sha256,
                    process_id,
                    ..
                } => observations.send_modify(|state| {
                    state.executable_sha256 = Some(executable_sha256);
                    state.process_id = Some(process_id);
                }),
                Event::Observation {
                    revision,
                    observed_at,
                    snapshot,
                } => observations.send_modify(|state| {
                    state.revision = revision;
                    state.observed_at = Some(observed_at);
                    state.snapshot = Some(*snapshot);
                    state.status = Status::Ready;
                    state.error = None;
                }),
                Event::Unavailable { reason } => bail!("{reason}"),
                _ => bail!("unexpected host audio initialization response"),
            }
        }
        Ok(())
    }

    pub async fn stop(mut self) -> Result<()> {
        let sequence = self.next_sequence();
        let _ = self.send(sequence, RequestAction::Shutdown).await;
        let _ = self.input.shutdown().await;
        let exit = async {
            let mut output_open = true;
            loop {
                tokio::select! {
                    biased;
                    result = self.child.wait() => return result,
                    message = self.messages.recv(), if output_open => {
                        output_open = message.is_some();
                    },
                }
            }
        };
        match tokio::time::timeout(Duration::from_secs(3), exit).await {
            Ok(result) => {
                result.context("wait for host audio worker exit")?;
            }
            Err(_) => {
                self.child
                    .start_kill()
                    .context("terminate unresponsive host audio worker")?;
                tokio::time::timeout(Duration::from_secs(3), self.child.wait())
                    .await
                    .context("host audio worker did not exit after termination")??;
            }
        }
        self.reader.abort();
        let _ = (&mut self.reader).await;
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.reader.abort();
    }
}
