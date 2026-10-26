use anyhow::{Context, Result, ensure};
use netaudio_core::device_controls::{PanelStatus, allocate_panel_sequence};
use rand::RngCore;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Instant, timeout_at};

const MAXIMUM_PENDING_REQUESTS: usize = 256;

struct PendingRequest {
    device: String,
    registered: Instant,
    sender: watch::Sender<Option<PanelStatus>>,
}

#[derive(Default)]
struct PendingState {
    offset: Option<u32>,
    requests: BTreeMap<u32, PendingRequest>,
}

#[derive(Clone, Default)]
pub struct PanelRequests {
    pending: Arc<Mutex<PendingState>>,
}

pub struct PanelObservation {
    pub sequence: u32,
    receiver: watch::Receiver<Option<PanelStatus>>,
    pending: Arc<Mutex<PendingState>>,
}

impl Drop for PanelObservation {
    fn drop(&mut self) {
        match self.pending.lock() {
            Ok(mut pending) => {
                pending.requests.remove(&self.sequence);
            }
            Err(error) => tracing::error!(%error, "could not release panel observation"),
        }
    }
}

impl PanelObservation {
    pub fn received(&self) -> bool {
        self.receiver.borrow().is_some()
    }

    pub async fn wait(
        &mut self,
        category: &str,
        duration: Duration,
        expected: Option<&serde_json::Value>,
    ) -> Result<PanelStatus> {
        let deadline = Instant::now() + duration;
        let mut latest = None;
        loop {
            if let Some(status) = self.receiver.borrow_and_update().clone() {
                ensure!(
                    status.diagnostic_error.is_none(),
                    "panel returned a malformed application status"
                );
                for observation in &status.observations {
                    let observation = serde_json::to_value(observation)?;
                    if observation["category"] == category {
                        if expected.is_none_or(|expected| {
                            netaudio_core::device_controls::planning::matches(
                                &observation["value"],
                                expected,
                            )
                        }) {
                            return Ok(status);
                        }
                        latest = Some(status.clone());
                    }
                }
            }
            match timeout_at(deadline, self.receiver.changed()).await {
                Ok(result) => result.context("panel observation canceled")?,
                Err(_) => {
                    return latest.with_context(|| format!("fresh {category} status timed out"));
                }
            }
        }
    }
}

impl PanelRequests {
    pub fn subscribe(&self, device: &str) -> Result<PanelObservation> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("panel request lock poisoned"))?;
        ensure!(
            pending.requests.len() < MAXIMUM_PENDING_REQUESTS,
            "too many pending panel requests"
        );
        let offset = match pending.offset {
            Some(offset) => offset,
            None => {
                let mut bytes = [0; 4];
                rand::rngs::OsRng
                    .try_fill_bytes(&mut bytes)
                    .context("initialize panel request sequence")?;
                let offset = u32::from_ne_bytes(bytes);
                pending.offset = Some(offset);
                offset
            }
        };
        let sequence = loop {
            let sequence = allocate_panel_sequence().wrapping_add(offset);
            if sequence != 0 && !pending.requests.contains_key(&sequence) {
                break sequence;
            }
        };
        let (sender, receiver) = watch::channel(None);
        pending.requests.insert(
            sequence,
            PendingRequest {
                device: device.to_owned(),
                registered: Instant::now(),
                sender,
            },
        );
        Ok(PanelObservation {
            sequence,
            receiver,
            pending: Arc::clone(&self.pending),
        })
    }

    pub fn observe(&self, device: &str, received: Instant, status: &PanelStatus) -> Result<bool> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("panel request lock poisoned"))?;
        let Some(request) = pending.requests.get(&status.sequence).filter(|request| {
            request.device == device && status.requester == 0 && received >= request.registered
        }) else {
            return Ok(false);
        };
        request.sender.send_replace(Some(status.clone()));
        Ok(true)
    }
}
