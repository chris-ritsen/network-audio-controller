mod control;
mod projection;
mod snapshot;

use anyhow::{Context as ErrorContext, Result, bail};
use libpulse_binding::{
    context::{Context, FlagSet, State, subscribe::InterestMaskSet},
    mainloop::threaded::Mainloop,
    operation::Operation,
};
use netaudio_core::host_audio::{
    protocol::{Event, RequestAction},
    snapshot::Snapshot,
};
use netaudio_host_worker::{
    input::{Event as InputEvent, Input, Wake},
    output::Output,
    timestamp,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

struct Provider {
    context: Context,
    mainloop: Mainloop,
    wake: Arc<Wake>,
    source_revision: Arc<AtomicU64>,
    observed_revision: u64,
    revision: u64,
    current: Option<Snapshot>,
    connected_at: String,
}

impl Provider {
    fn open(input: &mut Input) -> Result<Self> {
        let mainloop = Mainloop::new().context("create PulseAudio main loop")?;
        let mut context =
            Context::new(&mainloop, "netaudio").context("create PulseAudio context")?;
        let wake = input.wake();
        let source_revision = Arc::new(AtomicU64::new(0));
        let callback_wake = wake.clone();
        let callback_revision = source_revision.clone();
        context.set_state_callback(Some(Box::new(move || {
            callback_revision.fetch_add(1, Ordering::Release);
            callback_wake.notify();
        })));
        let callback_wake = wake.clone();
        let callback_revision = source_revision.clone();
        context.set_subscribe_callback(Some(Box::new(move |_, _, _| {
            callback_revision.fetch_add(1, Ordering::Release);
            callback_wake.notify();
        })));
        context
            .connect(None, FlagSet::NOAUTOSPAWN, None)
            .map_err(|error| anyhow::anyhow!("connect to PulseAudio: {error}"))?;
        let mut provider = Self {
            context,
            mainloop,
            wake,
            source_revision,
            observed_revision: 0,
            revision: 0,
            current: None,
            connected_at: timestamp(),
        };
        provider
            .mainloop
            .start()
            .map_err(|error| anyhow::anyhow!("start PulseAudio main loop: {error}"))?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            provider.mainloop.lock();
            let state = provider.context.get_state();
            let error = provider.context.errno();
            provider.mainloop.unlock();
            match state {
                State::Ready => break,
                State::Failed | State::Terminated => bail!("PulseAudio connection failed: {error}"),
                _ => wait(input, deadline)?,
            }
        }
        let result = Arc::new(Mutex::new(None));
        let callback_result = result.clone();
        let callback_wake = provider.wake.clone();
        provider.mainloop.lock();
        let operation = provider.context.subscribe(
            InterestMaskSet::SINK
                | InterestMaskSet::SOURCE
                | InterestMaskSet::SINK_INPUT
                | InterestMaskSet::SOURCE_OUTPUT
                | InterestMaskSet::SERVER,
            move |success| {
                *callback_result
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = Some(success);
                callback_wake.notify();
            },
        );
        provider.mainloop.unlock();
        if !provider.complete(operation, result, input)? {
            bail!("PulseAudio rejected the event subscription");
        }
        Ok(provider)
    }

    fn check(&mut self) -> Result<()> {
        self.mainloop.lock();
        let state = self.context.get_state();
        let error = self.context.errno();
        self.mainloop.unlock();
        if state != State::Ready {
            bail!("PulseAudio is unavailable: {state:?} ({error})");
        }
        Ok(())
    }

    fn complete(
        &mut self,
        mut operation: Operation<dyn FnMut(bool)>,
        result: Arc<Mutex<Option<bool>>>,
        input: &mut Input,
    ) -> Result<bool> {
        let deadline = Instant::now() + Duration::from_secs(3);
        let completion = (|| {
            loop {
                if let Some(success) = *result.lock().unwrap_or_else(|error| error.into_inner()) {
                    return Ok(success);
                }
                self.check()?;
                wait(input, deadline)?;
            }
        })();
        self.mainloop.lock();
        if operation.get_state() == libpulse_binding::operation::State::Running {
            operation.cancel();
        }
        drop(operation);
        self.mainloop.unlock();
        completion
    }

    fn refresh(&mut self, input: &mut Input, output: &mut Output) -> Result<Snapshot> {
        self.check()?;
        let (revision, snapshot) = snapshot::read(self, input)?;
        self.observed_revision = revision;
        let snapshot = Snapshot::Pulse(snapshot);
        if self.current.as_ref() != Some(&snapshot) {
            self.revision = self
                .revision
                .checked_add(1)
                .context("PulseAudio observation revision exhausted")?;
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

impl Drop for Provider {
    fn drop(&mut self) {
        self.mainloop.lock();
        self.context.set_state_callback(None);
        self.context.set_subscribe_callback(None);
        self.context.disconnect();
        self.mainloop.unlock();
        self.mainloop.stop();
    }
}

fn wait(input: &mut Input, deadline: Instant) -> Result<()> {
    match input.receive_source(Some(deadline))? {
        InputEvent::Changed => Ok(()),
        InputEvent::Deadline => bail!("PulseAudio operation deadline expired"),
        InputEvent::Closed => bail!("host audio worker input closed"),
        InputEvent::Request(request) => {
            if matches!(request.action, RequestAction::Shutdown) {
                bail!("host audio worker shutdown requested");
            }
            input.defer(request)
        }
    }
}

pub fn run(output: &mut Output) -> Result<()> {
    let mut input = Input::new()?;
    let mut provider = Provider::open(&mut input)?;
    provider.refresh(&mut input, output)?;
    loop {
        if provider.source_revision.load(Ordering::Acquire) != provider.observed_revision {
            provider.refresh(&mut input, output)?;
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
                    let snapshot = provider.refresh(&mut input, output)?;
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
                    control::execute(
                        &mut provider,
                        &mut input,
                        output,
                        request.sequence,
                        operation_id,
                        command,
                        precondition,
                    )?;
                }
            },
        }
    }
    Ok(())
}
