use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::clock::discipline::{
    Configuration, DelayOwner, Follower, Outcome, Publication,
};
use netaudio_core::native_audio::clock::version_one::{self, Body, Packet};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

use super::clock_socket::{ClockMapping, ClockSocket, Event};
use super::deadline::Deadline;
use super::sockets::Monotonic;

const TRANSMIT_NOTIFICATION_DEADLINE_NANOSECONDS: u64 = 2_000_000_000;
const DELAY_REQUEST_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Default)]
pub struct Summary {
    pub maximum_observation_nanoseconds: u64,
}

pub struct Worker {
    publications: watch::Receiver<Arc<Publication>>,
    stop: Option<oneshot::Sender<()>>,
    worker: JoinHandle<Result<Summary>>,
}

impl Worker {
    pub async fn start(
        address: Ipv4Addr,
        configuration: Configuration,
        time: Monotonic,
    ) -> Result<Self> {
        let (startup, ready) = oneshot::channel();
        let (stop, stopped) = oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let runtime =
                tokio::runtime::Builder::new_current_thread().enable_io().enable_time().build()?;
            runtime.block_on(async move {
                let mut driver = Driver::start(address, configuration, time.clone())?;
                let initial = driver.follower.publication(time.now())?;
                let (publications, receiver) = watch::channel(Arc::new(initial));

                if startup.send(receiver).is_err() {
                    return Ok(Summary::default());
                }

                driver.run(publications, stopped).await
            })
        });

        let publications = match ready.await {
            Ok(receiver) => receiver,
            Err(_) => {
                worker.await.context("clock worker failed during startup")??;
                bail!("clock worker stopped before publishing its initial state");
            }
        };

        Ok(Self {
            publications,
            stop: Some(stop),
            worker,
        })
    }

    pub fn current(&self) -> Arc<Publication> {
        self.publications.borrow().clone()
    }

    pub async fn changed(&mut self) -> Result<()> {
        self.publications.changed().await.context("clock publication provider stopped")
    }

    pub async fn close(mut self) -> Result<Summary> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }

        self.worker.await.context("clock worker failed during shutdown")?
    }
}

pub struct Observed {
    event: Event,
    event_port: bool,
}

struct PendingTransmit {
    identifier: u32,
    owner: DelayOwner,
    deadline: u64,
}

pub struct Driver {
    pub follower: Follower,
    time: Monotonic,
    mapping: ClockMapping,
    event: ClockSocket,
    general: ClockSocket,
    destination: SocketAddrV4,
    pending_transmit: Option<PendingTransmit>,
}

impl Driver {
    async fn run(
        &mut self,
        publications: watch::Sender<Arc<Publication>>,
        mut stopped: oneshot::Receiver<()>,
    ) -> Result<Summary> {
        let mut deadline = Deadline::new(self.time.clone())?;
        let mut delay_request = tokio::time::interval(DELAY_REQUEST_INTERVAL);
        delay_request.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut summary = Summary::default();
        let mut reported_state = None;

        loop {
            let due = self.follower.next_deadline();
            deadline.arm(due)?;

            tokio::select! {
                _ = &mut stopped => return Ok(summary),
                observed = self.next_event() => {
                    let started = self.time.now();
                    let result = observed.and_then(|observed| self.observe(observed));
                    summary.maximum_observation_nanoseconds =
                        summary.maximum_observation_nanoseconds.max(self.time.now() - started);

                    if let Err(error) = result {
                        eprintln!("clock observation failed: {error:#}");
                        self.follower.provider_lost(self.time.now())?;
                    }
                }
                result = deadline.expired(), if due.is_some() => {
                    result?;
                    self.follower.evaluate(self.time.now())?;
                }
                _ = delay_request.tick() => {
                    if let Err(error) = self.request_delay() {
                        eprintln!("clock delay request failed: {error:#}");
                    }
                }
            }

            let publication = self.follower.publication(self.time.now())?;
            let (_, snapshot) = publication.observation();
            let state = (
                snapshot.generation,
                snapshot.usable,
                snapshot.reason.clone(),
                snapshot.last_fit_rejection.as_ref().map(std::mem::discriminant),
            );

            if reported_state.as_ref() != Some(&state) {
                eprintln!("clock state changed: {}", serde_json::to_string(&snapshot)?);
                reported_state = Some(state);
            }

            if publications.send(Arc::new(publication)).is_err() {
                return Ok(summary);
            }
        }
    }

    pub fn start(address: Ipv4Addr, configuration: Configuration, time: Monotonic) -> Result<Self> {
        let group = version_one::multicast_group(configuration.domain)?;
        let follower = Follower::new(configuration, time.now())?;
        let mapping = ClockMapping::new(time.clone())?;
        let event = ClockSocket::bind(address, group, version_one::EVENT_PORT)?;
        let general = ClockSocket::bind(address, group, version_one::GENERAL_PORT)?;

        Ok(Self {
            follower,
            time,
            mapping,
            event,
            general,
            destination: SocketAddrV4::new(group, version_one::EVENT_PORT),
            pending_transmit: None,
        })
    }

    pub async fn next_event(&self) -> Result<Observed> {
        tokio::select! {
            result = self.event.receive() => Ok(Observed { event: result?, event_port: true }),
            result = self.general.receive() => Ok(Observed { event: result?, event_port: false }),
        }
    }

    pub fn observe(&mut self, observed: Observed) -> Result<Outcome> {
        let now = self.time.now();
        self.follower.evaluate(now)?;

        match observed.event {
            Event::Received {
                bytes,
                peer: _,
                interface_index,
                realtime_nanoseconds,
            } => {
                let packet = match Packet::decode(&bytes) {
                    Ok(packet) => packet,
                    Err(_) => return Ok(Outcome::Ignored),
                };
                let expected_event_port =
                    matches!(packet.body, Body::Sync { .. } | Body::DelayRequest { .. });

                if observed.event_port != expected_event_port
                    || packet.domain != self.follower.configuration().domain
                    || packet.source != self.follower.configuration().parent_identity
                    || interface_index != self.follower.configuration().interface_index
                {
                    return Ok(Outcome::Ignored);
                }

                let timestamp = match self.mapping.project(realtime_nanoseconds) {
                    Ok(timestamp) => timestamp,
                    Err(error) => {
                        self.follower.provider_lost(now)?;
                        return Err(error);
                    }
                };

                Ok(self.follower.receive(&packet, interface_index, timestamp, self.time.now())?)
            }
            Event::Transmitted {
                identifier,
                realtime_nanoseconds,
            } => {
                let Some(pending) = self.pending_transmit.as_ref() else {
                    return Ok(Outcome::Ignored);
                };

                if !observed.event_port
                    || pending.identifier != identifier
                    || now >= pending.deadline
                {
                    return Ok(Outcome::Ignored);
                }

                let owner = pending.owner;
                self.pending_transmit = None;
                let timestamp = match self.mapping.project(realtime_nanoseconds) {
                    Ok(timestamp) => timestamp,
                    Err(error) => {
                        self.follower.provider_lost(now)?;
                        return Err(error);
                    }
                };

                match self.follower.request_transmitted(owner, timestamp, self.time.now()) {
                    Ok(outcome) => Ok(outcome),
                    Err(netaudio_core::native_audio::clock::Error::StaleOwner) => {
                        Ok(Outcome::Ignored)
                    }
                    Err(error) => Err(error.into()),
                }
            }
        }
    }

    pub fn request_delay(&mut self) -> Result<()> {
        let now = self.time.now();

        if self.pending_transmit.as_ref().is_some_and(|pending| now >= pending.deadline) {
            self.pending_transmit = None;
        }

        let Some(request) = self.follower.prepare_delay_request(now)? else {
            return Ok(());
        };
        let identifier = match self.event.try_send(&request.bytes, self.destination) {
            Ok(identifier) => identifier,
            Err(error) => {
                self.follower.request_failed(request.owner, self.time.now())?;
                return Err(error);
            }
        };

        self.pending_transmit = Some(PendingTransmit {
            identifier,
            owner: request.owner,
            deadline: now + TRANSMIT_NOTIFICATION_DEADLINE_NANOSECONDS,
        });
        Ok(())
    }
}
