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
    candidates: Vec<Candidate>,
    unavailable: Vec<String>,
}

impl Observation {
    fn refresh(
        &mut self,
        selections: &[Selection],
        unavailable: &[String],
        resolver: &Resolver,
        generation: &mut u64,
    ) -> Result<()> {
        let previous = std::mem::take(&mut self.candidates);
        self.unavailable = unavailable.to_vec();
        for selection in selections {
            match selection.resolve_groups(|name| resolver.resolve(name)) {
                Ok(candidates) => {
                    for mut candidate in candidates {
                        if let Some(previous) =
                            previous.iter().find(|previous| previous.context == candidate.context)
                        {
                            candidate.context = previous.context.clone();
                            candidate.discovery_generation = previous.discovery_generation;
                        } else {
                            *generation = generation
                                .checked_add(1)
                                .context("source discovery generation space is exhausted")?;
                            candidate.discovery_generation = *generation;
                        }
                        self.candidates.push(candidate);
                    }
                }
                Err(error) => self.unavailable.push(format!("{error:#}")),
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
        self.current.route_generation = endpoint.route_generation;
        self.current.candidates.retain(|candidate| candidate.matches_routes(endpoint));
        self.current.unavailable.clear();
        self.requests
            .send(Some(Request::from_endpoint(endpoint)))
            .context("source discovery worker stopped before route selection")?;
        Ok(())
    }

    pub fn candidates(&self) -> Vec<Candidate> {
        self.current
            .candidates
            .iter()
            .filter(|candidate| self.time.now() <= candidate.usable_through)
            .cloned()
            .collect()
    }

    pub fn unavailable(&self) -> Vec<String> {
        let mut reasons = self.current.unavailable.clone();
        for candidate in &self.current.candidates {
            if self.time.now() > candidate.usable_through {
                reasons.push(format!(
                    "selected source {} records have expired",
                    candidate.context.device
                ));
            }
        }
        reasons
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
    let mut selections = Vec::new();
    let mut selection_unavailable = Vec::new();
    let mut resolver: Option<Resolver> = None;
    let mut current = Observation::default();
    let mut generation = 0;
    loop {
        tokio::select! {
            biased;
            _ = &mut stopped => return Ok(()),
            changed = requests.changed() => {
                changed.context("source discovery route owner stopped")?;
                let request = requests.borrow_and_update().clone().context("source discovery request has no route state")?;
                current.route_generation = request.generation;
                (selections, selection_unavailable) = Selection::from_request(&request);
                let names = selections.iter().flat_map(Selection::names).collect::<Vec<_>>();
                if let Some(resolver) = &mut resolver { resolver.select(names)?; }
                else if !names.is_empty() { resolver = Some(Resolver::new(address, interface_index, names, time.clone())?); }
                if let Some(resolver) = &resolver { current.refresh(&selections, &selection_unavailable, resolver, &mut generation)?; }
                if resolver.is_none() { current.candidates.clear(); current.unavailable = selection_unavailable.clone(); }
                publications.send_replace(current.clone());
            }
            event = async {
                match &mut resolver {
                    Some(resolver) => resolver.next_event().await,
                    None => std::future::pending().await,
                }
            } => {
                match event? {
                    Event::RecordsChanged => {
                        current.refresh(&selections, &selection_unavailable, resolver.as_ref().context("source resolver disappeared")?, &mut generation)?;
                        publications.send_replace(current.clone());
                    }
                    Event::Rejected(reason) => eprintln!("source discovery datagram rejected: {reason}"),
                }
            }
        }
    }
}
