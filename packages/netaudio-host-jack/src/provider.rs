mod callbacks;
mod control;
mod snapshot;

use anyhow::{Context, Result, ensure};
use callbacks::{Notifications, Processing, State};
use netaudio_core::host_audio::{
    protocol::{Event, RequestAction},
    snapshot::Snapshot,
};
use netaudio_host_worker::{
    input::{Event as InputEvent, Input},
    output::Output,
    timestamp,
};
use std::sync::Arc;

struct Provider {
    client: jack::AsyncClient<Notifications, Processing>,
    state: Arc<State>,
    connected_at: String,
    server: String,
    revision: u64,
    observed_revision: u64,
    current: Option<Snapshot>,
}

impl Provider {
    fn open(input: &Input) -> Result<Self> {
        let wake = input.wake();
        let state = Arc::new(State::new(wake.clone()));
        let (mut client, _) = jack::Client::new("netaudio", jack::ClientOptions::NO_START_SERVER)
            .context("connect to the existing JACK server")?;
        client
            .register_property_change_handler(jack::ClosurePropertyChangeHandler::new(move |_| {
                wake.notify()
            }))
            .context("subscribe to JACK metadata changes")?;
        let client = client
            .activate_async(Notifications(state.clone()), Processing(state.clone()))
            .context("activate the JACK observer")?;
        Ok(Self {
            client,
            state,
            connected_at: timestamp(),
            server: std::env::var("JACK_DEFAULT_SERVER")
                .ok()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "default".into()),
            revision: 0,
            observed_revision: 0,
            current: None,
        })
    }

    fn refresh(&mut self, output: &mut Output) -> Result<Snapshot> {
        ensure!(!self.state.stopped(), "the JACK server stopped");
        let (revision, snapshot) = snapshot::read(self)?;
        self.observed_revision = revision;
        let snapshot = Snapshot::Jack(snapshot);
        if self.current.as_ref() != Some(&snapshot) {
            self.revision = self
                .revision
                .checked_add(1)
                .context("JACK observation revision exhausted")?;
            output.send(Event::Observation {
                revision: self.revision,
                observed_at: timestamp(),
                snapshot: Box::new(snapshot.clone()),
            })?;
            self.current = Some(snapshot.clone());
        }
        Ok(snapshot)
    }
}

pub fn run(output: &mut Output) -> Result<()> {
    let mut input = Input::new()?;
    let mut provider = Provider::open(&input)?;
    provider.refresh(output)?;
    loop {
        if provider.state.wake.revision() != provider.observed_revision {
            provider.refresh(output)?;
        }
        match input.receive(None)? {
            InputEvent::Changed => {}
            InputEvent::Deadline => unreachable!(),
            InputEvent::Closed => break,
            InputEvent::Request(request) => match request.action {
                RequestAction::Shutdown => break,
                RequestAction::Measure { .. } | RequestAction::Record { .. } => {
                    anyhow::bail!("Measurement requests require a measurement worker")
                }
                RequestAction::Snapshot => {
                    let snapshot = provider.refresh(output)?;
                    output.send(Event::Completed {
                        request_sequence: request.sequence,
                        status: 200,
                        body: serde_json::to_value(snapshot)?,
                    })?;
                }
                RequestAction::Execute {
                    operation_id,
                    command,
                    precondition,
                } => {
                    if !control::execute(
                        &mut provider,
                        &mut input,
                        output,
                        request.sequence,
                        operation_id,
                        command,
                        precondition,
                    )? {
                        break;
                    }
                }
            },
        }
    }
    provider
        .client
        .deactivate()
        .context("deactivate JACK observer")?;
    Ok(())
}
