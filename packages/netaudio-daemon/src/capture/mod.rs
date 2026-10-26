mod store;

use crate::configuration::Configuration;
use crate::state::ReadModel;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const QUEUE_CAPACITY: usize = 256;
const BATCH_CAPACITY: usize = 64;

#[derive(Clone)]
pub struct Observer {
    sender: mpsc::Sender<Packet>,
    failure: watch::Sender<Option<String>>,
}

pub struct Packet {
    pub observed_at: DateTime<Utc>,
    pub source: SocketAddr,
    pub destination: SocketAddr,
    pub direction: &'static str,
    pub payload: Arc<[u8]>,
}

pub struct Worker {
    receiver: mpsc::Receiver<Packet>,
    failure: watch::Receiver<Option<String>>,
    recorder: store::Recorder,
    model: Arc<ReadModel>,
}

pub async fn open(
    configuration: &Configuration,
    capture: bool,
    dissect: bool,
    model: Arc<ReadModel>,
) -> Result<Option<(Observer, Worker)>> {
    if !capture && !dissect {
        return Ok(None);
    }
    let path = if capture {
        let configured = configuration
            .profile
            .get("paths")
            .and_then(|value| value.get("db"))
            .or_else(|| {
                configuration
                    .profile
                    .get("capture")
                    .and_then(|value| value.get("db"))
            });
        Some(match configured {
            Some(value) => crate::configuration::resolve_path(std::path::Path::new(
                value
                    .as_str()
                    .context("Capture database path must be a string")?,
            ))?,
            None => dirs::home_dir()
                .context("Home directory unavailable")?
                .join(".local/share/netaudio/packet_capture.sqlite"),
        })
    } else {
        None
    };
    let recorder =
        tokio::task::spawn_blocking(move || store::Recorder::open(path.as_deref(), dissect))
            .await??;
    let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
    let (failure, observed) = watch::channel(None);
    model.observe("packet_capture", json!({"state":"ready","recording":capture,"dissection":dissect,"session_id":recorder.session_id()}), json!({"event":"packet_capture_status","state":"ready"}))?;
    Ok(Some((
        Observer { sender, failure },
        Worker {
            receiver,
            failure: observed,
            recorder,
            model,
        },
    )))
}

impl Observer {
    pub fn record(&self, packet: Packet) {
        if self.failure.borrow().is_some() {
            return;
        }
        if let Err(error) = self.sender.try_send(packet) {
            let reason = match error {
                mpsc::error::TrySendError::Full(_) => {
                    "Packet capture queue filled; capture stopped because complete retention is unavailable"
                }
                mpsc::error::TrySendError::Closed(_) => "Packet capture writer stopped",
            };
            self.failure.send_if_modified(|failure| {
                if failure.is_some() {
                    return false;
                }
                *failure = Some(reason.to_owned());
                true
            });
        }
    }
}

impl Worker {
    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        let mut stopping = false;
        let mut failure = None;
        loop {
            let packet = if stopping {
                self.receiver.recv().await
            } else {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        self.receiver.close();
                        stopping = true;
                        continue;
                    }
                    changed = self.failure.changed() => {
                        failure = Some(if changed.is_ok() {
                            self.failure.borrow_and_update().clone().unwrap_or_else(|| "Packet capture observer closed".into())
                        } else { "Packet capture observers stopped".into() });
                        self.receiver.close();
                        stopping = true;
                        continue;
                    }
                    packet = self.receiver.recv() => packet,
                }
            };
            let Some(packet) = packet else {
                break;
            };
            let mut batch = vec![packet];
            while batch.len() < BATCH_CAPACITY {
                let Ok(packet) = self.receiver.try_recv() else {
                    break;
                };
                batch.push(packet);
            }
            let mut recorder = self.recorder;
            let (recorder, result) = tokio::task::spawn_blocking(move || {
                let result = recorder.write(batch);
                (recorder, result)
            })
            .await
            .context("Packet capture writer failed")?;
            self.recorder = recorder;
            if let Err(error) = result {
                failure = Some(error.to_string());
                self.receiver.close();
                break;
            }
        }
        if let Some(error) = failure {
            tracing::error!(%error, "Packet capture stopped");
            let value = json!({"state":"failed","error":error});
            self.model.observe(
                "packet_capture",
                value.clone(),
                json!({"event":"packet_capture_status","packet_capture":value}),
            )?;
            cancellation.cancelled().await;
        }
        Ok(())
    }
}
