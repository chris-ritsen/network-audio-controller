use super::{audit, process};

use crate::monitoring::journal::service::Journal;
use anyhow::{Context, Result};
use netaudio_core::host_audio::{
    control::Command,
    protocol::{Event, Provider, Purpose, RequestAction},
    snapshot::Snapshot,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Initializing,
    Connecting,
    Ready,
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Observation {
    pub provider: Provider,
    pub generation: u64,
    pub revision: u64,
    pub status: Status,
    pub error: Option<String>,
    pub observed_at: Option<String>,
    pub snapshot: Option<Snapshot>,
    pub executable_sha256: Option<String>,
    pub process_id: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct Outcome {
    pub status: u16,
    pub body: Value,
}

enum Request {
    Refresh(oneshot::Sender<Result<Outcome>>),
    Execute(Command, Option<Value>, oneshot::Sender<Result<Outcome>>),
}

#[derive(Clone)]
pub struct Handle {
    requests: mpsc::Sender<Request>,
    observations: watch::Receiver<Observation>,
    reconnect: Arc<Notify>,
}

pub struct Worker {
    provider: Provider,
    path: PathBuf,
    host: String,
    journal: Arc<Journal>,
    requests: mpsc::Receiver<Request>,
    observations: watch::Sender<Observation>,
    reconnect: Arc<Notify>,
}

struct Pending {
    sequence: u64,
    reply: oneshot::Sender<Result<Outcome>>,
    audit: Option<audit::Audit>,
    deadline: tokio::time::Instant,
    command: bool,
}

impl Handle {
    pub fn subscribe(&self) -> watch::Receiver<Observation> {
        self.observations.clone()
    }

    pub fn observation(&self) -> Observation {
        self.observations.borrow().clone()
    }

    pub fn server_changed(&self) {
        self.reconnect.notify_one();
    }

    pub async fn refresh(&self) -> Result<Outcome> {
        let (reply, result) = oneshot::channel();
        self.requests
            .try_send(Request::Refresh(reply))
            .context("host audio provider request queue is unavailable")?;
        result
            .await
            .context("host audio provider ended without a refresh result")?
    }

    pub async fn execute(&self, command: Command, precondition: Option<Value>) -> Result<Outcome> {
        let (reply, result) = oneshot::channel();
        self.requests
            .try_send(Request::Execute(command, precondition, reply))
            .context("host audio provider request queue is unavailable")?;
        result
            .await
            .context("host audio provider ended without a command result")?
    }
}

impl Worker {
    pub fn new(
        provider: Provider,
        path: PathBuf,
        host: String,
        journal: Arc<Journal>,
    ) -> (Handle, Self) {
        let (requests, receiver) = mpsc::channel(16);
        let (observations, observed) = watch::channel(Observation {
            provider,
            generation: 0,
            revision: 0,
            status: Status::Initializing,
            error: None,
            observed_at: None,
            snapshot: None,
            executable_sha256: None,
            process_id: None,
        });
        let reconnect = Arc::new(Notify::new());
        (
            Handle {
                requests,
                observations: observed,
                reconnect: reconnect.clone(),
            },
            Self {
                provider,
                path,
                host,
                journal,
                requests: receiver,
                observations,
                reconnect,
            },
        )
    }

    fn unavailable(&self, reason: String) {
        self.observations.send_modify(|state| {
            state.status = Status::Unavailable;
            state.error = Some(reason);
            state.process_id = None;
        });
    }

    async fn start(&self, cancellation: &CancellationToken) -> Option<process::Process> {
        self.observations.send_modify(|state| {
            state.generation += 1;
            state.revision = 0;
            state.status = Status::Connecting;
            state.error = None;
            state.executable_sha256 = None;
            state.process_id = None;
        });
        let result = async {
            let mut child =
                process::Process::spawn(&self.path, self.provider, Purpose::Observe, &self.host)
                    .await?;
            if let Err(error) = child.initialize(&self.observations, cancellation).await {
                child.stop().await?;
                return Err(error);
            }
            Ok::<_, anyhow::Error>(child)
        }
        .await;
        match result {
            Ok(child) => Some(child),
            Err(error) => {
                self.unavailable(format!("{error:#}"));
                None
            }
        }
    }

    async fn finish(&self, pending: Pending, mut outcome: Outcome) {
        if let Some(audit) = pending.audit {
            outcome.body["operation_id"] = json!(audit.identifier());
            if let Err(error) = audit.finish(&self.journal, &outcome).await {
                outcome.status = 500;
                outcome.body["journal_error"] = json!(format!("{error:#}"));
            }
        }
        let _ = pending.reply.send(Ok(outcome));
    }

    async fn retire(
        &self,
        child: &mut Option<process::Process>,
        pending: &mut Option<Pending>,
        reason: String,
    ) -> Result<()> {
        if let Some(pending) = pending.take() {
            let change_sent = if pending.command {
                Value::Null
            } else {
                json!(false)
            };
            self.finish(
                pending,
                Outcome {
                    status: 502,
                    body: json!({"error":reason,"change_sent":change_sent,"state_verified":null}),
                },
            )
            .await;
        }
        if let Some(child) = child.take() {
            child.stop().await?;
        }
        self.unavailable(reason);
        Ok(())
    }

    async fn message(
        &self,
        child: &mut Option<process::Process>,
        pending: &mut Option<Pending>,
        message: Result<netaudio_core::host_audio::protocol::Message>,
    ) -> Result<()> {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                return self.retire(child, pending, format!("{error:#}")).await;
            }
        };
        match message.event {
            Event::Observation {
                revision,
                observed_at,
                snapshot,
            } => {
                self.observations.send_modify(|state| {
                    state.revision = revision;
                    state.observed_at = Some(observed_at);
                    state.snapshot = Some(*snapshot);
                    state.status = Status::Ready;
                    state.error = None;
                });
            }
            Event::Dispatching {
                request_sequence,
                operation_id,
                ..
            } => {
                let expected = pending.as_ref().is_some_and(|pending| {
                    pending.command
                        && pending.sequence == request_sequence
                        && pending
                            .audit
                            .as_ref()
                            .is_some_and(|audit| audit.identifier() == operation_id)
                });
                if !expected {
                    self.retire(
                        child,
                        pending,
                        "host audio worker dispatched an unexpected command".into(),
                    )
                    .await?;
                }
            }
            Event::Completed {
                request_sequence,
                status,
                body,
            } => {
                if pending
                    .as_ref()
                    .is_some_and(|pending| pending.sequence == request_sequence)
                {
                    self.finish(
                        pending.take().expect("matched worker request"),
                        Outcome { status, body },
                    )
                    .await;
                } else {
                    self.retire(
                        child,
                        pending,
                        "host audio worker completed an unexpected request".into(),
                    )
                    .await?;
                }
            }
            Event::Unavailable { reason } => self.retire(child, pending, reason).await?,
            Event::Hello { .. } | Event::Measurement { .. } | Event::Recording { .. } => {
                self.retire(
                    child,
                    pending,
                    "host audio worker returned an unexpected event".into(),
                )
                .await?
            }
        }
        Ok(())
    }

    async fn request(
        &self,
        request: Request,
        child: &mut Option<process::Process>,
        pending: &mut Option<Pending>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if child.is_none() && matches!(request, Request::Refresh(_)) {
            *child = self.start(cancellation).await;
        }
        let Some(process) = child.as_mut() else {
            let reply = match request {
                Request::Refresh(reply) | Request::Execute(_, _, reply) => reply,
            };
            let _ = reply.send(Ok(Outcome {
                status: 503,
                body: json!({"error":self.observations.borrow().error,"change_sent":false}),
            }));
            return Ok(());
        };
        let (action, reply, audit, command) = match request {
            Request::Refresh(reply) => (RequestAction::Snapshot, reply, None, false),
            Request::Execute(command, precondition, reply) => {
                let audit =
                    match audit::Audit::begin(&self.journal, &self.host, self.provider, &command)
                        .await
                    {
                        Ok(audit) => audit,
                        Err(error) => {
                            let _ = reply.send(Err(error));
                            return Ok(());
                        }
                    };
                let action = RequestAction::Execute {
                    operation_id: audit.identifier().into(),
                    command,
                    precondition,
                };
                (action, reply, Some(audit), true)
            }
        };
        let sequence = process.next_sequence();
        *pending = Some(Pending {
            sequence,
            reply,
            audit,
            deadline: tokio::time::Instant::now() + Duration::from_secs(15),
            command,
        });
        if let Err(error) = process.send(sequence, action).await {
            self.retire(child, pending, format!("{error:#}")).await?;
        }
        Ok(())
    }

    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        let mut child = self.start(&cancellation).await;
        let mut pending: Option<Pending> = None;
        loop {
            let deadline = pending.as_ref().map(|pending| pending.deadline);
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.retire(&mut child, &mut pending, "host audio provider exceeded its response deadline".into()).await?;
                }
                message = async {
                    match child.as_mut() {
                        Some(child) => child.receive().await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.message(&mut child, &mut pending, message).await?;
                }
                _ = self.reconnect.notified() => {
                    if child.is_none() {
                        child = self.start(&cancellation).await;
                    }
                }
                request = self.requests.recv(), if pending.is_none() => {
                    let Some(request) = request else {
                        break;
                    };
                    self.request(request, &mut child, &mut pending, &cancellation).await?;
                }
            }
        }
        self.retire(
            &mut child,
            &mut pending,
            "host audio provider stopped".into(),
        )
        .await?;
        self.requests.close();
        while let Some(request) = self.requests.recv().await {
            let reply = match request {
                Request::Refresh(reply) | Request::Execute(_, _, reply) => reply,
            };
            let _ = reply.send(Ok(Outcome {
                status: 503,
                body: json!({"error":"host audio provider stopped","change_sent":false}),
            }));
        }
        Ok(())
    }
}
