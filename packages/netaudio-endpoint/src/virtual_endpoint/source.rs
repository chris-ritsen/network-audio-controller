use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use netaudio_core::native_audio::clock::discipline::SampleClock;
use netaudio_core::native_audio::endpoint_control::{Endpoint, TransmitFlow, source};
use netaudio_core::native_audio::is_reverse_keepalive;
use netaudio_core::native_audio::playout::quantum_frames;
use netaudio_core::native_audio::source_control::Header;
use serde::Serialize;
use tokio::task::JoinHandle;

use super::pcm::{Capture, CaptureFactory, Frame};
use super::platform::Generations;
use super::sockets::{Datagram, MediaPool, Monotonic};
use super::transmit::Flow;

struct Pending {
    peer: SocketAddrV4,
    header: Header,
    state: TransmitFlow,
    result: u16,
    identifier: u16,
    generation: u32,
    output_generation: u64,
    port_index: usize,
}

enum Event {
    Generation(Result<u32>),
    Opened(Result<Capture>),
    Capture(Result<Option<Vec<Frame>>>),
}

pub struct Reply {
    pub peer: SocketAddrV4,
    pub bytes: Vec<u8>,
}

#[derive(Default, Serialize)]
pub struct Summary {
    pub allocations: u64,
    pub releases: u64,
    pub peak_flows: usize,
    pub submitted_packets: u64,
    pub failures: u64,
    pub maximum_submission_lateness_nanoseconds: u64,
    pub flows: Vec<FlowSummary>,
}

#[derive(Serialize)]
pub struct FlowSummary {
    pub identifier: u16,
    pub generation: u32,
    pub media_port: u16,
    pub submitted_packets: u64,
    pub suspensions: u64,
    pub last_failure: Option<String>,
}

struct Owned {
    flow: Flow,
    summary: FlowSummary,
}

pub struct Runtime {
    pub pool: MediaPool,
    pub summary: Summary,
    flows: BTreeMap<u16, Owned>,
    generations: Generations,
    reserved_generations: VecDeque<u32>,
    output_generation: u64,
    input: CaptureFactory,
    capture: Option<Capture>,
    capture_opening: Option<JoinHandle<Result<Capture>>>,
    capture_next_tick: Option<u64>,
    capture_paused: bool,
    capture_ended: bool,
    pending: BTreeMap<u16, Pending>,
    replies: VecDeque<Reply>,
    capacity: usize,
    generation_failed: bool,
    scheduled: Option<u16>,
    last_sent: u16,
}

impl Runtime {
    pub async fn new(
        pool: MediaPool,
        mut generations: Generations,
        input: CaptureFactory,
        capacity: u16,
    ) -> Result<Self> {
        let capacity = usize::from(capacity);
        let mut reserved_generations = VecDeque::new();
        for _ in 0..capacity {
            reserved_generations.push_back(generations.next().await?);
        }
        Ok(Self {
            pool,
            summary: Summary::default(),
            flows: BTreeMap::new(),
            generations,
            reserved_generations,
            output_generation: 0,
            input,
            capture: None,
            capture_opening: None,
            capture_next_tick: None,
            capture_paused: false,
            capture_ended: false,
            pending: BTreeMap::new(),
            replies: VecDeque::new(),
            capacity,
            generation_failed: false,
            scheduled: None,
            last_sent: 0,
        })
    }

    fn next_output_generation(&mut self) -> Result<u64> {
        self.output_generation = self
            .output_generation
            .checked_add(1)
            .context("source output generation space is exhausted")?;
        Ok(self.output_generation)
    }

    fn capture_needed(&self) -> bool {
        !self.capture_ended
            && self.flows.values().any(|owned| !owned.flow.suspended())
            && self
                .flows
                .values()
                .filter(|owned| !owned.flow.suspended())
                .all(|owned| owned.flow.needs_capture())
    }

    pub async fn next_event(
        &mut self,
        endpoint: &mut Endpoint,
        time: &Monotonic,
        clock: &impl SampleClock,
    ) -> Result<Option<Reply>> {
        if let Some(reply) = self.replies.pop_front() {
            return Ok(Some(reply));
        }
        let needed = self.capture_needed();
        let event = tokio::select! {
            result = self.generations.next(), if self.reserved_generations.len() < self.capacity && !self.generation_failed => Event::Generation(result),
            result = async {
                match &mut self.capture_opening {
                    Some(opening) => opening.await?,
                    None => std::future::pending().await,
                }
            } => Event::Opened(result),
            result = async {
                match &mut self.capture {
                    Some(capture) if needed => capture.next_quantum().await,
                    _ => std::future::pending().await,
                }
            } => Event::Capture(result),
        };
        match event {
            Event::Generation(result) => {
                self.generation_failed = result.is_err();
                self.reserved_generations.push_back(result?);
            }
            Event::Opened(result) => {
                self.capture_opening.take();
                match result {
                    Ok(capture) => {
                        self.capture = Some(capture);
                        self.capture_paused = false;
                        self.capture_ended = false;
                        self.capture_next_tick = None;
                        for (_, pending) in std::mem::take(&mut self.pending) {
                            let reply = self.allocation_ready(endpoint, pending)?;
                            self.replies.push_back(reply);
                        }
                    }
                    Err(error) => {
                        eprintln!("source capture preparation failed: {error:#}");
                        for (_, pending) in std::mem::take(&mut self.pending) {
                            self.pool.release(pending.port_index, time.now())?;
                            self.replies.push_back(Reply {
                                peer: pending.peer,
                                bytes: source::unavailable(pending.header)?,
                            });
                        }
                    }
                }
                return Ok(self.replies.pop_front());
            }
            Event::Capture(result) => match result {
                Ok(Some(frames)) => {
                    let reading = clock
                        .reading(time.now())?
                        .context("source capture lost its network clock")?;
                    let quantum = quantum_frames(endpoint.profile.sample_rate) as u64;
                    let first = match self.capture_next_tick {
                        Some(tick) => tick,
                        None => {
                            (reading
                                .sample_tick(endpoint.profile.sample_rate)?
                                .checked_add(
                                    (u64::from(endpoint.profile.sample_rate) * 2_000_000)
                                        .div_ceil(1_000_000_000),
                                )
                                .context("capture sample coordinate overflow")?)
                            .div_ceil(quantum)
                                * quantum
                        }
                    };
                    self.capture_next_tick = Some(
                        first
                            .checked_add(frames.len() as u64)
                            .context("capture sample coordinate overflow")?,
                    );
                    let frames = frames.into_iter().map(Arc::new).collect::<Vec<_>>();
                    let mut failed = Vec::new();
                    for (&identifier, owned) in &mut self.flows {
                        if let Err(error) = owned.flow.capture(first, &frames) {
                            failed.push((identifier, error.to_string()));
                        }
                    }
                    for (identifier, reason) in failed {
                        self.suspend_flow(endpoint, identifier, Some(reason))?;
                    }
                }
                Ok(None) => {
                    self.capture_ended = true;
                    for owned in self.flows.values_mut() {
                        owned.flow.end_capture();
                    }
                }
                Err(error) => {
                    self.capture_ended = true;
                    for owned in self.flows.values_mut() {
                        owned.flow.capture_failed();
                    }
                    self.suspend(endpoint)?;
                    return Err(error);
                }
            },
        }
        Ok(None)
    }

    pub fn request(
        &mut self,
        endpoint: &mut Endpoint,
        datagram: &Datagram,
        now: u64,
    ) -> Result<Option<Reply>> {
        let Some(action) = source::admit(endpoint, &datagram.bytes)? else {
            return Ok(None);
        };
        let bytes = match action {
            source::Action::Reply(bytes) => bytes,
            source::Action::Allocate {
                header,
                flow,
                result,
            } => {
                if self.flows.len() + self.pending.len() >= self.capacity
                    || self.reserved_generations.is_empty()
                    || self.capture_ended
                {
                    source::unavailable(header)?
                } else {
                    let identifier = (1..=endpoint.profile.transmit_flows)
                        .find(|identifier| {
                            !self.flows.contains_key(identifier)
                                && !self.pending.contains_key(identifier)
                        })
                        .context("source advertised capacity disagrees with runtime ownership")?;
                    let output_generation = self.next_output_generation()?;
                    let Some(port_index) = self.pool.acquire(output_generation, now) else {
                        return Ok(Some(Reply {
                            peer: datagram.peer,
                            bytes: source::unavailable(header)?,
                        }));
                    };
                    let pending = Pending {
                        peer: datagram.peer,
                        header,
                        state: flow,
                        result,
                        identifier,
                        generation: self
                            .reserved_generations
                            .pop_front()
                            .context("source generation was not prepared")?,
                        output_generation,
                        port_index,
                    };
                    if self.capture.is_some() {
                        if let Err(error) = self.resume_capture() {
                            self.pool.release(pending.port_index, now)?;
                            self.summary.failures += 1;
                            eprintln!("source capture resume failed: {error:#}");
                            return Ok(Some(Reply {
                                peer: pending.peer,
                                bytes: source::unavailable(pending.header)?,
                            }));
                        }
                        return self.allocation_ready(endpoint, pending).map(Some);
                    }
                    if self.capture_opening.is_none() {
                        let input = self.input.clone();
                        self.capture_opening =
                            Some(super::platform::spawn_background(move || input.open()));
                    }
                    self.pending.insert(identifier, pending);
                    return Ok(None);
                }
            }
            source::Action::Remap {
                header,
                identifier,
                channels,
                result,
            } => {
                self.suspend_flow(endpoint, identifier, None)?;
                let response = source::remap_committed(
                    endpoint,
                    header,
                    identifier,
                    channels.clone(),
                    result,
                )?;
                if let Some(owned) = self.flows.get_mut(&identifier)
                    && !owned.flow.exhausted()
                {
                    owned.flow.resume(&channels, now)?;
                    self.resume_capture()?;
                }
                response
            }
            source::Action::Stop {
                header,
                identifier,
            } => {
                self.retire(endpoint, identifier, now)?;
                source::stop_committed(endpoint, header, identifier)?
            }
        };
        Ok(Some(Reply {
            peer: datagram.peer,
            bytes,
        }))
    }

    fn allocation_ready(&mut self, endpoint: &mut Endpoint, pending: Pending) -> Result<Reply> {
        let mut state = pending.state;
        state.generation = pending.generation;
        let flow =
            Flow::new(&state, pending.port_index, pending.output_generation, endpoint.profile)?;
        let bytes = source::allocation_committed(
            endpoint,
            pending.header,
            state,
            pending.identifier,
            pending.generation,
            pending.result,
        )?;
        let media_port = self.pool.slots[pending.port_index].socket.local_addr()?.port();
        self.flows.insert(
            pending.identifier,
            Owned {
                flow,
                summary: FlowSummary {
                    identifier: pending.identifier,
                    generation: pending.generation,
                    media_port,
                    submitted_packets: 0,
                    suspensions: 0,
                    last_failure: None,
                },
            },
        );
        self.summary.allocations += 1;
        self.summary.peak_flows = self.summary.peak_flows.max(self.flows.len());
        Ok(Reply {
            peer: pending.peer,
            bytes,
        })
    }

    fn pause_capture_if_idle(&mut self) {
        if self.flows.values().all(|owned| owned.flow.suspended()) {
            if let Some(capture) = &mut self.capture {
                capture.pause();
            }
            self.capture_paused = true;
            self.capture_next_tick = None;
        }
    }

    fn resume_capture(&mut self) -> Result<()> {
        if self.capture_paused {
            if let Some(capture) = &mut self.capture {
                capture.resume()?;
            }
            self.capture_paused = false;
            self.capture_next_tick = None;
        }
        Ok(())
    }

    fn suspend_flow(
        &mut self,
        endpoint: &mut Endpoint,
        identifier: u16,
        reason: Option<String>,
    ) -> Result<()> {
        let generation = self.next_output_generation()?;
        if let Some(owned) = self.flows.get_mut(&identifier) {
            owned.flow.suspend(generation)?;
            self.pool.slots[owned.flow.port_index].owner.store(generation, Ordering::Release);
            owned.summary.suspensions += 1;
            if let Some(reason) = reason {
                eprintln!("source flow {identifier} suspended: {reason}");
                owned.summary.last_failure = Some(reason);
                self.summary.failures += 1;
            }
        }
        if let Some(state) = endpoint.transmit_flows.get_mut(&identifier) {
            state.active = false;
        }
        self.pause_capture_if_idle();
        Ok(())
    }

    pub fn suspend(&mut self, endpoint: &mut Endpoint) -> Result<()> {
        for identifier in self.flows.keys().copied().collect::<Vec<_>>() {
            self.suspend_flow(endpoint, identifier, None)?;
        }
        Ok(())
    }

    pub fn failed_port(
        &mut self,
        endpoint: &mut Endpoint,
        index: usize,
        reason: &str,
    ) -> Result<()> {
        if let Some(identifier) = self
            .flows
            .iter()
            .find_map(|(&identifier, owned)| (owned.flow.port_index == index).then_some(identifier))
        {
            self.suspend_flow(endpoint, identifier, Some(reason.to_owned()))?;
        }
        self.pool.disable(index)
    }

    pub fn keepalive(
        &mut self,
        endpoint: &Endpoint,
        index: usize,
        datagram: &Datagram,
        clock: &impl SampleClock,
        now: u64,
    ) -> Result<()> {
        let Some((&identifier, owned)) =
            self.flows.iter_mut().find(|(_, owned)| owned.flow.port_index == index)
        else {
            return Ok(());
        };
        let state = endpoint
            .transmit_flows
            .get(&identifier)
            .context("source allocation state is missing")?;
        if owned.flow.output_generation != datagram.generation
            || state.destination != datagram.peer
            || !is_reverse_keepalive(&datagram.bytes)
        {
            return Ok(());
        }
        if owned.flow.suspended() && !owned.flow.exhausted() && clock.reading(now)?.is_some() {
            owned.flow.resume(&state.channels, now)?;
        }
        owned.flow.keepalive(datagram.generation, datagram.peer, &datagram.bytes, now);
        if !owned.flow.suspended() {
            self.resume_capture()?;
        }
        Ok(())
    }

    pub fn deadline(
        &mut self,
        endpoint: &mut Endpoint,
        clock: &impl SampleClock,
        now: u64,
    ) -> Result<Option<u64>> {
        let mut selected = None;
        let mut failures = Vec::new();
        for (&identifier, owned) in &mut self.flows {
            match owned.flow.deadline(clock, now) {
                Ok(Some(due)) => {
                    let rank = (due, identifier <= self.last_sent, identifier);
                    if selected.as_ref().is_none_or(|(previous, _)| rank < *previous) {
                        selected = Some((rank, identifier));
                    }
                }
                Ok(None) => {}
                Err(error) => failures.push((identifier, format!("{error:#}"))),
            }
        }
        for (identifier, reason) in failures {
            self.suspend_flow(endpoint, identifier, Some(reason))?;
        }
        self.scheduled = selected.map(|(_, identifier)| identifier);
        Ok(selected.map(|(rank, _)| rank.0))
    }

    pub fn send(
        &mut self,
        endpoint: &mut Endpoint,
        clock: &impl SampleClock,
        time: &Monotonic,
    ) -> Result<()> {
        let identifier = self.scheduled.take().context("source deadline has no flow owner")?;
        self.last_sent = identifier;
        let owned = self.flows.get_mut(&identifier).context("source flow has retired")?;
        match owned.flow.send(&self.pool.slots[owned.flow.port_index].socket, clock, time) {
            Ok(lateness) => {
                self.summary.submitted_packets += 1;
                owned.summary.submitted_packets += 1;
                self.summary.maximum_submission_lateness_nanoseconds =
                    self.summary.maximum_submission_lateness_nanoseconds.max(lateness);
                endpoint
                    .transmit_flows
                    .get_mut(&identifier)
                    .context("source allocation has retired")?
                    .active = owned.flow.active();
                if owned.flow.exhausted() {
                    self.suspend_flow(endpoint, identifier, None)?;
                }
            }
            Err(error) => self.suspend_flow(endpoint, identifier, Some(format!("{error:#}")))?,
        }
        Ok(())
    }

    fn retire(&mut self, endpoint: &mut Endpoint, identifier: u16, now: u64) -> Result<()> {
        if let Some(owned) = self.flows.remove(&identifier) {
            self.pool.release(owned.flow.port_index, now)?;
            self.summary.flows.push(owned.summary);
            self.summary.releases += 1;
        }
        endpoint.transmit_flows.remove(&identifier);
        self.pause_capture_if_idle();
        Ok(())
    }

    pub async fn close(mut self, endpoint: &mut Endpoint, now: u64) -> Result<Summary> {
        for identifier in self.flows.keys().copied().collect::<Vec<_>>() {
            self.retire(endpoint, identifier, now)?;
        }
        if let Some(opening) = self.capture_opening.take() {
            self.capture = Some(opening.await??);
        }
        let capture = match self.capture.take() {
            Some(capture) => capture.close().await,
            None => Ok(()),
        };
        let generations = self.generations.close().await;
        capture?;
        generations?;
        Ok(self.summary)
    }
}
