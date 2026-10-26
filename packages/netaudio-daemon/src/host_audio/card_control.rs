use super::{audit::Audit, manager::Manager, provider::Outcome};
use crate::monitoring::journal::service::Journal;
use anyhow::{Context, Result};
use netaudio_core::host_audio::card_links::Command;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

struct Request {
    command: Command,
    precondition: Option<Value>,
    reply: oneshot::Sender<Result<Outcome>>,
}

pub(super) struct Handle(mpsc::Sender<Request>);

pub(super) struct Worker(mpsc::Receiver<Request>);

pub(super) fn channel() -> (Handle, Worker) {
    let (sender, receiver) = mpsc::channel(16);
    (Handle(sender), Worker(receiver))
}

impl Handle {
    pub async fn execute(&self, command: Command, precondition: Option<Value>) -> Result<Outcome> {
        let (reply, result) = oneshot::channel();
        self.0
            .try_send(Request {
                command,
                precondition,
                reply,
            })
            .context("Sound card configuration request queue is unavailable")?;
        result
            .await
            .context("Sound card configuration owner ended without a result")?
    }
}

impl Worker {
    pub async fn run(
        mut self,
        manager: Arc<Manager>,
        journal: Arc<Journal>,
        cancellation: CancellationToken,
    ) -> Result<()> {
        loop {
            let request = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                request = self.0.recv() => request.context("Sound card command channel closed")?,
            };
            let result = execute(&manager, &journal, request.command, request.precondition).await;
            let _ = request.reply.send(result);
        }
    }
}

async fn execute(
    manager: &Manager,
    journal: &Journal,
    command: Command,
    precondition: Option<Value>,
) -> Result<Outcome> {
    let audit = Audit::begin_request(
        journal,
        &manager.host,
        "cards",
        "link_audio_card",
        serde_json::to_value(&command)?,
    )
    .await?;
    let identifier = audit.identifier().to_owned();
    let mut outcome = match apply(manager, &command, precondition.as_ref()).await {
        Ok(outcome) => outcome,
        Err(error) => Outcome {
            status: 500,
            body: json!({"error":format!("{error:#}"),"change_sent":null,"state_verified":null}),
        },
    };
    outcome.body["operation_id"] = json!(identifier);
    if let Err(error) = audit.finish(journal, &outcome).await {
        outcome.status = 500;
        outcome.body["journal_error"] = json!(format!("{error:#}"));
    }
    Ok(outcome)
}

async fn apply(
    manager: &Manager,
    command: &Command,
    precondition: Option<&Value>,
) -> Result<Outcome> {
    let plan = match manager.prepare_card(command).await {
        Ok(plan) => plan,
        Err(error) => {
            return Ok(Outcome {
                status: 400,
                body: json!({"error":format!("{error:#}"),"accepted":false,"change_sent":false,"state_verified":false}),
            });
        }
    };
    if precondition.is_some_and(|expected| expected != &plan.precondition) {
        return Ok(Outcome {
            status: 409,
            body: json!({"error":"The sound card, Dante device, or configuration changed after the preview; read it again before confirming","accepted":false,"change_sent":false,"state_verified":false}),
        });
    }
    let mut outcome = Outcome {
        status: 200,
        body: plan.description,
    };
    outcome.body["accepted"] = json!(true);
    outcome.body["saved_in"] = json!(manager.links.path());
    if plan.unchanged {
        outcome.body["change_sent"] = json!(false);
        outcome.body["changed"] = json!(false);
        outcome.body["state_verified"] = json!(true);
        outcome.body["now"] = json!(plan.command.device);
        outcome.body["result"] = json!("already so; nothing changed");
        return Ok(outcome);
    }
    let revision = plan.precondition["configuration_revision"]
        .as_str()
        .context("Sound card plan has no configuration revision")?
        .to_owned();
    let configuration = manager.links.clone();
    let command = plan.command;
    let observed = tokio::task::spawn_blocking(move || {
        configuration.update(&revision, &command.card, command.device.as_deref())
    })
    .await?;
    match observed {
        Ok(snapshot) => {
            outcome.body["change_sent"] = json!(true);
            outcome.body["changed"] = json!(true);
            outcome.body["state_verified"] = json!(true);
            outcome.body["now"] = outcome.body["device"].clone();
            outcome.body["card_links"] = json!(snapshot.mappings);
            outcome.body["result"] = json!("applied");
        }
        Err(error) => {
            outcome.status = if error.conflict { 409 } else { 500 };
            outcome.body["change_sent"] = json!(error.installed);
            outcome.body["state_verified"] = json!(false);
            outcome.body["accepted"] = json!(error.installed);
            outcome.body["error"] = json!(error.message);
        }
    }
    Ok(outcome)
}
