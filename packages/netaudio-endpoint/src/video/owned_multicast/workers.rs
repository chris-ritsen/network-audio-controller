use std::num::NonZeroU16;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::sync::{mpsc, watch};

use super::Receiver;
use super::discovery::{self, DiscoveryState};
use crate::video::recording;
use crate::video::transmit::{self, Observation as TransmitObservation};
use crate::video::{self, Observation as ReceiveObservation};

pub(super) enum Event {
    Source(u64, TransmitObservation),
    Receiver(NonZeroU16, u64, ReceiveObservation),
}

pub(super) enum Completion {
    Source {
        generation: u64,
        observation: TransmitObservation,
        failure: Option<String>,
    },
    Receiver {
        identifier: NonZeroU16,
        generation: u64,
        observation: ReceiveObservation,
        failure: Option<String>,
        failure_subscription_status: u16,
    },
    Recording {
        identifier: NonZeroU16,
        generation: u64,
    },
}

pub(super) async fn source(
    mut session: transmit::Session,
    generation: u64,
    events: mpsc::Sender<Event>,
) -> Completion {
    let (observation, observations) = watch::channel(TransmitObservation::default());
    session.observation = Some(observation);
    let session = transmit::run_session(session);
    tokio::pin!(session);
    let forwarding =
        forward(observations.clone(), events, move |snapshot| Event::Source(generation, snapshot));
    tokio::pin!(forwarding);
    let mut observing = true;
    let result = loop {
        tokio::select! {
            result = &mut session => break result,
            _ = &mut forwarding, if observing => { observing = false; }
        }
    };
    let mut observation = *observations.borrow();
    if observation.terminal.is_none() {
        observation.terminal = Some(transmit::Terminal::QuiescenceUnknown);
    }
    Completion::Source {
        generation,
        observation,
        failure: result.err().map(|error| format!("{error:#}")),
    }
}

pub(super) async fn receiver(
    receiver: Receiver,
    generation: u64,
    events: mpsc::Sender<Event>,
    mut cancellation: watch::Receiver<bool>,
    duration: Duration,
    recording: recording::Session,
) -> Completion {
    let identifier = receiver.identifier;
    let (discovery_cancel, discovery_cancellation) = watch::channel(false);
    let (discovery_observer, mut discovered) = watch::channel(DiscoveryState::Waiting);
    let discovery_task = tokio::spawn(discovery::watch_source(
        receiver.clone(),
        discovery_cancellation,
        discovery_observer,
    ));
    let (media_cancel, media_cancellation) = watch::channel(false);
    let (observation, observations) = watch::channel(ReceiveObservation::default());
    let mut failure_subscription_status = 3;
    let mut media_started = false;
    let operation = async {
        let acquired = loop {
            let snapshot = discovered.borrow_and_update().clone();
            match snapshot {
                DiscoveryState::Ready => break true,
                DiscoveryState::Lost(reason) => return Err(anyhow!(reason)),
                DiscoveryState::Waiting => {}
            }
            tokio::select! {
                _ = cancelled(&mut cancellation) => break false,
                changed = discovered.changed() => changed.context("source discovery ended before acquisition")?,
            }
        };
        if !acquired { return Ok(()); }
        if *cancellation.borrow() { return Ok(()); }
        failure_subscription_status = 19;
        receiver.configuration.validate()?;
        let socket = video::bind(&receiver.configuration)?;
        media_started = true;
        let session = video::run_session_with_recording(video::Session {
            configuration: receiver.configuration.clone(),
            output: receiver.output.clone(),
            duration,
            socket,
            presentation: video::presentation::Options::default(),
            record_frames: receiver.record_frames,
            ready: None,
            cancellation: Some(media_cancellation),
            idle_timeout: None,
            observation: Some(observation),
        }, recording);
        tokio::pin!(session);
        let forwarding = forward(observations.clone(), events, move |snapshot| Event::Receiver(identifier, generation, snapshot));
        tokio::pin!(forwarding);
        let mut observing = true;
        let mut stopping = false;
        let mut source_loss = None;
        let result = loop {
            tokio::select! {
                result = &mut session => break result.map(|_| ()),
                forwarded = &mut forwarding, if observing => {
                    observing = false;
                    if forwarded.is_err() {
                        stopping = true;
                        media_cancel.send_replace(true);
                    }
                }
                _ = cancelled(&mut cancellation), if !stopping => {
                    stopping = true;
                    media_cancel.send_replace(true);
                }
                changed = discovered.changed(), if !stopping => {
                    let snapshot = discovered.borrow_and_update().clone();
                    let failure = match snapshot {
                        DiscoveryState::Ready if changed.is_ok() => None,
                        DiscoveryState::Lost(reason) => Some(reason),
                        _ => Some("qualified source discovery ended or became unavailable".to_owned()),
                    };
                    if let Some(failure) = failure {
                        failure_subscription_status = 3;
                        source_loss = Some(failure);
                        stopping = true;
                        media_cancel.send_replace(true);
                    }
                }
            }
        };
        if let Some(source_loss) = source_loss {
            match result {
                Ok(()) => Err(anyhow!(source_loss)),
                Err(error) => Err(error.context(source_loss)),
            }
        } else { result }
    }.await;
    discovery_cancel.send_replace(true);
    let discovery_retirement =
        discovery_task.await.context("source discovery worker panicked").and_then(|result| result);
    let mut observation = *observations.borrow();
    if observation.terminal.is_none() {
        observation.terminal = Some(if media_started {
            video::Terminal::QuiescenceUnknown
        } else {
            video::Terminal::Retired
        });
    }
    let failure = match (operation, discovery_retirement) {
        (Ok(()), Ok(())) => None,
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Some(format!("{error:#}")),
        (Err(error), Err(retirement)) => {
            Some(format!("{error:#}; source discovery retirement: {retirement:#}"))
        }
    };
    Completion::Receiver {
        identifier,
        generation,
        observation,
        failure,
        failure_subscription_status,
    }
}

async fn cancelled(cancellation: &mut watch::Receiver<bool>) {
    let _ = cancellation.wait_for(|value| *value).await;
}

async fn forward<Observation: Copy>(
    mut observations: watch::Receiver<Observation>,
    events: mpsc::Sender<Event>,
    event: impl Fn(Observation) -> Event,
) -> Result<()> {
    while observations.changed().await.is_ok() {
        let snapshot = *observations.borrow_and_update();
        events.send(event(snapshot)).await.context("endpoint observation receiver ended")?;
    }
    Ok(())
}

pub(super) async fn recording(
    identifier: NonZeroU16,
    generation: u64,
    terminal: recording::TerminalWatcher,
) -> Completion {
    let _ = terminal.wait().await;
    Completion::Recording {
        identifier,
        generation,
    }
}
