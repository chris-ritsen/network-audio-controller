use std::net::Ipv4Addr;

use anyhow::{Context, Result};
use netaudio_core::native_audio::endpoint_control::Endpoint;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

use super::resolution::{Event, Resolver};
use super::selection::{Candidate, Request, Selection};
use super::sockets::Monotonic;

#[derive(Clone, Default)]
struct Observation {
    route_generation: u64,
    candidate: Option<Candidate>,
    unavailable: Option<String>,
}

impl Observation {
    fn refresh(
        &mut self,
        selection: &Selection,
        resolver: &Resolver,
        generation: &mut u64,
    ) -> Result<()> {
        match selection.resolve(|name| resolver.resolve(name)) {
            Ok(mut candidate) => {
                if let Some(previous) = &self.candidate
                    && previous.context == candidate.context
                {
                    candidate.context = previous.context.clone();
                    candidate.discovery_generation = previous.discovery_generation;
                } else {
                    *generation = generation
                        .checked_add(1)
                        .context("source discovery generation space is exhausted")?;
                    candidate.discovery_generation = *generation;
                }
                self.candidate = Some(candidate);
                self.unavailable = None;
            }
            Err(error) => {
                self.candidate = None;
                let reason = format!("{error:#}");

                if self.unavailable.as_deref() != Some(reason.as_str()) {
                    eprintln!("receiver source unavailable: {reason}");
                }

                self.unavailable = Some(reason);
            }
        }
        Ok(())
    }
}

pub struct Discovery {
    time: Monotonic,
    current: Observation,
    requests: watch::Sender<Option<Request>>,
    observations: watch::Receiver<Observation>,
    stop: Option<oneshot::Sender<()>>,
    worker: JoinHandle<Result<()>>,
}

impl Discovery {
    pub fn new(address: Ipv4Addr, interface_index: u32, time: Monotonic) -> Self {
        let (requests, selected) = watch::channel(None);
        let (publications, observations) = watch::channel(Observation::default());
        let (stop, stopped) = oneshot::channel();
        let worker_time = time.clone();
        let worker = super::platform::spawn_background(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()?
                .block_on(resolve(
                    address,
                    interface_index,
                    worker_time,
                    selected,
                    publications,
                    stopped,
                ));
            if let Err(error) = &result {
                eprintln!("source discovery worker failed: {error:#}");
            }
            result
        });

        Self {
            time,
            current: Observation::default(),
            requests,
            observations,
            stop: Some(stop),
            worker,
        }
    }

    pub fn select(&mut self, endpoint: &Endpoint) -> Result<()> {
        self.current = Observation {
            route_generation: endpoint.route_generation,
            ..Observation::default()
        };
        self.requests
            .send(Some(Request::from_endpoint(endpoint)))
            .context("source discovery worker stopped before route selection")?;
        Ok(())
    }

    pub fn candidate(&self) -> Option<&Candidate> {
        self.current
            .candidate
            .as_ref()
            .filter(|candidate| self.time.now() <= candidate.usable_through)
    }

    pub fn unavailable(&self) -> Option<&str> {
        self.current.unavailable.as_deref().or_else(|| {
            self.current
                .candidate
                .as_ref()
                .filter(|candidate| self.time.now() > candidate.usable_through)
                .map(|_| "selected source records have expired")
        })
    }

    pub async fn changed(&mut self) -> Result<()> {
        loop {
            self.observations.changed().await.context("source discovery worker stopped")?;
            let observed = self.observations.borrow_and_update();
            if observed.route_generation == self.current.route_generation {
                self.current = observed.clone();
                return Ok(());
            }
        }
    }

    pub async fn close(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.worker.await.context("source discovery worker failed during shutdown")?
    }
}

async fn resolve(
    address: Ipv4Addr,
    interface_index: u32,
    time: Monotonic,
    mut requests: watch::Receiver<Option<Request>>,
    publications: watch::Sender<Observation>,
    mut stopped: oneshot::Receiver<()>,
) -> Result<()> {
    let mut selected: Option<(Selection, Resolver)> = None;
    let mut current = Observation::default();
    let mut generation = 0;
    loop {
        tokio::select! {
            biased;
            _ = &mut stopped => return Ok(()),
            changed = requests.changed() => {
                changed.context("source discovery route owner stopped")?;
                let request = requests.borrow_and_update().clone()
                    .context("source discovery request has no route state")?;
                selected = None;
                current = Observation { route_generation: request.generation, ..Observation::default() };
                match Selection::from_request(&request) {
                    Ok(Some(selection)) => {
                        let resolver = Resolver::new(address, interface_index, selection.names(), time.clone())?;
                        current.refresh(&selection, &resolver, &mut generation)?;
                        selected = Some((selection, resolver));
                    }
                    Ok(None) => {}
                    Err(error) => current.unavailable = Some(format!("{error:#}")),
                }
                publications.send_replace(current.clone());
            }
            event = async {
                match &mut selected {
                    Some((_, resolver)) => resolver.next_event().await,
                    None => std::future::pending().await,
                }
            } => {
                match event? {
                    Event::RecordsChanged => {
                        let (selection, resolver) = selected.as_ref().context("source discovery selection disappeared")?;
                        current.refresh(selection, resolver, &mut generation)?;
                        publications.send_replace(current.clone());
                    }
                    Event::Rejected(reason) => eprintln!("source discovery datagram rejected: {reason}"),
                }
            }
        }
    }
}
