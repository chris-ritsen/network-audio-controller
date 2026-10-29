use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use anyhow::{Context, Result, bail};
use futures_util::future::select_all;
use netaudio_core::native_audio::SampleTime;
use netaudio_core::native_audio::clock::discipline::SampleClock;
use netaudio_core::native_audio::endpoint_control::{Endpoint, Profile};
use netaudio_core::native_audio::playout::{Delivery, Frame, Owner, quantum_frames};
use serde::Serialize;

use super::playback::{Playback, Receipt};
use super::receive_session::{self, Session};
use super::selection::Candidate;
use super::sockets::{Datagram, MediaPool, Monotonic, PoolSlot};

const PORTS_PER_FLOW: usize = 2;
const MAXIMUM_PENDING_OUTPUT_BLOCKS: usize = 256;

#[derive(Default, Serialize)]
pub struct Summary {
    #[serde(flatten)]
    pub totals: receive_session::Summary,
    pub peak_flows: usize,
    pub output_frames: u64,
    pub flows: Vec<FlowSummary>,
    pub unavailable: Vec<String>,
}

#[derive(Serialize)]
pub struct FlowSummary {
    pub identifier: u16,
    pub candidate: Option<Candidate>,
    pub acquisition: Option<super::acquisition::Progress>,
    #[serde(flatten)]
    pub counters: receive_session::Summary,
}

pub enum Event {
    Session(usize, Result<receive_session::Event>),
    Consumed(Receipt),
}

pub struct Runtime {
    pub summary: Summary,
    sessions: Vec<Session>,
    playback: Option<Playback>,
    time: Monotonic,
    profile: Profile,
    endpoint_generation: u64,
    output_generation: u64,
    output_owner: Option<Owner>,
    output_tick: Option<u64>,
    pending: BTreeMap<u64, Vec<(usize, Receipt)>>,
    last_event: usize,
}

impl Runtime {
    pub async fn new(
        address: Ipv4Addr,
        playback: Option<Playback>,
        time: Monotonic,
        incarnation: u64,
        profile: Profile,
    ) -> Result<Self> {
        let pool =
            MediaPool::reserve(address, time.now(), profile.receive_flows * PORTS_PER_FLOW as u16)
                .await?;
        let mut sessions = Vec::new();
        for (index, pool) in pool.partition(PORTS_PER_FLOW)?.into_iter().enumerate() {
            sessions.push(
                Session::new(
                    address,
                    playback.is_some(),
                    time.clone(),
                    incarnation,
                    (index + 1) as u16,
                    pool,
                )
                .await?,
            );
        }
        Ok(Self {
            summary: Summary::default(),
            sessions,
            playback,
            time,
            profile,
            endpoint_generation: incarnation,
            output_generation: 0,
            output_owner: None,
            output_tick: None,
            pending: BTreeMap::new(),
            last_event: 0,
        })
    }

    pub fn media_slots(&self) -> impl Iterator<Item = &PoolSlot> {
        self.sessions.iter().flat_map(|session| &session.pool.slots)
    }

    pub fn next_available_port(&self) -> Option<u64> {
        self.sessions.iter().filter_map(|session| session.pool.next_available()).min()
    }

    pub fn synchronize(
        &mut self,
        endpoint: &mut Endpoint,
        candidates: &[Candidate],
        clock: &impl SampleClock,
    ) -> Result<()> {
        let mut assigned = vec![false; candidates.len()];
        for session in &mut self.sessions {
            let current = session.candidate.as_ref().and_then(|previous| {
                candidates.iter().enumerate().find(|(_, candidate)| {
                    self.time.now() <= candidate.usable_through && candidate.same_context(previous)
                })
            });
            match current {
                Some((index, candidate)) => {
                    assigned[index] = true;
                    session.select(Some(candidate));
                }
                None => session.select(None),
            }
        }
        self.summary.unavailable.clear();
        for (index, candidate) in candidates.iter().enumerate() {
            if assigned[index] || self.time.now() > candidate.usable_through {
                continue;
            }
            if let Some(session) = self.sessions.iter_mut().find(|session| {
                session.candidate.is_none()
                    && session.idle()
                    && !session.summary.uncertain_remote_effect
            }) {
                session.select(Some(candidate));
            } else {
                self.summary.unavailable.push(format!(
                    "no receive flow resource is currently available for {}",
                    candidate.context.device
                ));
            }
        }
        for session in &mut self.sessions {
            if let Err(error) = session.synchronize(endpoint, clock) {
                session.summary.unavailable = Some(format!("{error:#}"));
                eprintln!("receiver flow {} reconciliation failed: {error:#}", session.identifier);
            }
        }
        self.summary.peak_flows = self
            .summary
            .peak_flows
            .max(self.sessions.iter().filter(|session| !session.idle()).count());
        self.update_output_selection()?;
        Ok(())
    }

    fn update_output_selection(&mut self) -> Result<()> {
        if !self.sessions.iter().any(|session| session.owner().is_some()) {
            self.stop_output();
        }
        if let Some(playback) = &self.playback {
            let mut generations = vec![0; usize::from(self.profile.receive_channels)];
            for session in &self.sessions {
                if let Some(owner) = session.owner() {
                    for identifier in &session.channels {
                        let channel = &mut generations[usize::from(*identifier - 1)];
                        if *channel != 0 {
                            bail!("multiple receive flows own the same host channel");
                        }
                        *channel = owner.stream_generation;
                    }
                }
            }
            playback.select_channels(&generations)?;
        }
        Ok(())
    }

    fn stop_output(&mut self) {
        if let Some(playback) = &self.playback {
            playback.select(0);
        }
        self.output_owner = None;
        self.output_tick = None;
        self.pending.clear();
    }

    pub fn media(
        &mut self,
        index: usize,
        datagram: &Datagram,
        clock: &impl SampleClock,
        endpoint: &mut Endpoint,
    ) -> Result<()> {
        self.sessions
            .get_mut(index / PORTS_PER_FLOW)
            .context("receive media port has no session")?
            .media(index % PORTS_PER_FLOW, datagram, clock, endpoint)
    }

    pub async fn next_event(&mut self) -> Result<Event> {
        let count = self.sessions.len();
        let offset = self.last_event;
        let session_events = self
            .sessions
            .iter_mut()
            .enumerate()
            .map(|(index, session)| {
                Box::pin(async move { (index, Event::Session(index, session.next_event().await)) })
            })
            .collect::<Vec<_>>();
        let mut session_events = session_events;
        session_events.rotate_left(offset % count);
        tokio::select! {
            (result, _, _) = select_all(session_events) => {
                self.last_event = (result.0 + 1) % count;
                Ok(result.1)
            }
            receipt = async {
                match &mut self.playback {
                    Some(playback) if self.output_owner.is_some() => playback.receipt().await,
                    _ => std::future::pending().await,
                }
            } => Ok(Event::Consumed(receipt?)),
        }
    }

    pub fn event(&mut self, event: Event, endpoint: &mut Endpoint) -> Result<()> {
        match event {
            Event::Session(index, result) => {
                let session = &mut self.sessions[index];
                match result.and_then(|event| session.event(event, endpoint)) {
                    Ok(()) => {}
                    Err(error) => {
                        session.summary.unavailable = Some(format!("{error:#}"));
                        eprintln!("receive flow {} event failed: {error:#}", session.identifier);
                    }
                }
            }
            Event::Consumed(receipt) => {
                if self.output_owner != Some(receipt.owner) {
                    return Ok(());
                }
                let receipts = self
                    .pending
                    .remove(&receipt.first_frame_index)
                    .context("host output receipt has no submitted block")?;
                if receipt.output_accepted {
                    self.summary.output_frames += receipt.frames as u64;
                    for (index, mut flow_receipt) in receipts {
                        let session = &mut self.sessions[index];
                        flow_receipt.received_samples = session
                            .channels
                            .iter()
                            .map(|channel| {
                                let channel = usize::from(*channel - 1);
                                if receipt.channel_generations[channel]
                                    == flow_receipt.owner.stream_generation
                                {
                                    receipt.received_by_channel[channel]
                                } else {
                                    0
                                }
                            })
                            .sum();
                        flow_receipt.local_silence_samples = flow_receipt.frames
                            * session.channels.len()
                            - flow_receipt.received_samples;
                        session.consumed(flow_receipt, endpoint)?;
                    }
                }
            }
        }
        self.update_output_selection()
    }

    pub fn deadline(&mut self, clock: &impl SampleClock) -> Result<Option<u64>> {
        let mut deadline = None;
        for session in &mut self.sessions {
            if let Some(due) = session.deadline(clock)? {
                deadline = Some(deadline.map_or(due, |current: u64| current.min(due)));
            }
        }
        let Some(reading) = clock.reading(self.time.now())? else {
            return Ok(deadline);
        };
        let tick = reading.sample_tick(self.profile.sample_rate)?;
        let ready = self.sessions.iter_mut().filter_map(|session| session.ready_tick(tick)).min();
        if self.output_tick.is_none()
            && let Some(first) = ready
        {
            self.output_generation = self
                .output_generation
                .checked_add(1)
                .context("host output generation exhausted")?;
            let owner = Owner {
                endpoint_generation: self.endpoint_generation,
                stream_generation: self.output_generation,
                clock_generation: reading.generation,
            };
            self.output_owner = Some(owner);
            self.output_tick = Some(first);
            self.playback
                .as_ref()
                .context("receive output disappeared")?
                .select(owner.stream_generation);
        }
        if let Some(first) = self.output_tick {
            let due = reading.sample_deadline(
                sample_time(first, self.profile.sample_rate)?,
                self.profile.sample_rate,
            )?;
            deadline = Some(deadline.map_or(due, |current| current.min(due)));
        }
        Ok(deadline)
    }

    pub fn dispatch(&mut self, endpoint: &mut Endpoint, clock: &impl SampleClock) -> Result<()> {
        for session in &mut self.sessions {
            if let Err(error) = session.dispatch(endpoint, clock) {
                session.retire(endpoint, &format!("{error:#}"))?;
            }
        }
        let Some(reading) = clock.reading(self.time.now())? else {
            return Ok(());
        };
        let tick = reading.sample_tick(self.profile.sample_rate)?;
        let Some(first) = self.output_tick.filter(|first| tick >= *first) else {
            return Ok(());
        };
        let quantum = quantum_frames(self.profile.sample_rate);
        if tick >= first + quantum as u64 {
            for session in &mut self.sessions {
                if session.owner().is_some() {
                    session.summary.missed_playout_deadlines += 1;
                }
            }
            return self.suspend_output(endpoint, "shared host output missed its sample deadline");
        }
        if self.pending.len() >= MAXIMUM_PENDING_OUTPUT_BLOCKS {
            bail!("host output consumption has exceeded its bounded receipt queue");
        }
        let channels = usize::from(self.profile.receive_channels);
        let mut frames = vec![
            Frame {
                samples: vec![0; channels],
                received: vec![false; channels]
            };
            quantum
        ];
        let mut owners = vec![0; channels];
        let mut receipts = Vec::new();
        for (index, session) in self.sessions.iter_mut().enumerate() {
            let delivery = match session.render(first, tick, endpoint, &reading) {
                Ok(delivery) => delivery,
                Err(error) => {
                    session.retire(endpoint, &format!("receive placement failed: {error:#}"))?;
                    None
                }
            };
            if let Some(delivery) = delivery {
                let mut received_samples = 0;
                for (local, identifier) in session.channels.iter().enumerate() {
                    let channel = usize::from(*identifier - 1);
                    if owners[channel] != 0 {
                        bail!("host playback channel has competing flow owners");
                    }
                    owners[channel] = delivery.owner.stream_generation;
                    for (output, input) in frames.iter_mut().zip(&delivery.frames) {
                        output.samples[channel] = input.samples[local];
                        output.received[channel] = input.received[local];
                        received_samples += usize::from(input.received[local]);
                    }
                }
                receipts.push((
                    index,
                    Receipt {
                        owner: delivery.owner,
                        first_frame_index: first,
                        frames: quantum,
                        output_accepted: true,
                        received_samples,
                        local_silence_samples: quantum * session.channels.len() - received_samples,
                        channel_generations: Vec::new(),
                        received_by_channel: Vec::new(),
                    },
                ));
            }
        }
        let delivery = Delivery {
            owner: self.output_owner.context("host output lost its clock owner")?,
            first_frame_index: first,
            frames,
            missing_limit_reached: false,
        };
        self.playback.as_mut().context("receive output disappeared")?.submit(delivery, owners)?;
        self.pending.insert(first, receipts);
        self.output_tick = Some(
            first.checked_add(quantum as u64).context("host output sample coordinate overflow")?,
        );
        self.update_output_selection()
    }

    pub fn retire(&mut self, endpoint: &mut Endpoint, reason: &str) -> Result<()> {
        for session in &mut self.sessions {
            session.retire(endpoint, reason)?;
        }
        self.stop_output();
        self.update_output_selection()
    }

    pub fn failed_port(
        &mut self,
        endpoint: &mut Endpoint,
        index: usize,
        reason: &str,
    ) -> Result<()> {
        let session = &mut self.sessions[index / PORTS_PER_FLOW];
        session.retire(endpoint, reason)?;
        session.pool.disable(index % PORTS_PER_FLOW)?;
        self.update_output_selection()
    }

    pub fn suspend_output(&mut self, endpoint: &mut Endpoint, reason: &str) -> Result<()> {
        for session in &mut self.sessions {
            session.suspend_output(endpoint, reason)?;
        }
        self.stop_output();
        self.update_output_selection()
    }

    pub async fn close(mut self, endpoint: &mut Endpoint) -> Result<Summary> {
        self.stop_output();
        for session in &mut self.sessions {
            session.retire(endpoint, "receive endpoint stopped")?;
        }
        for session in self.sessions {
            let identifier = session.identifier;
            let candidate = session.candidate.clone();
            let acquisition = session.acquisition.progress(candidate.as_ref()).cloned();
            let counters = session.close(endpoint).await?;
            self.summary.totals.add(&counters);
            self.summary.flows.push(FlowSummary {
                identifier,
                candidate,
                acquisition,
                counters,
            });
        }
        if let Some(playback) = self.playback.take() {
            playback.close().await?;
        }
        Ok(self.summary)
    }
}

fn sample_time(tick: u64, rate: u32) -> Result<SampleTime> {
    Ok(SampleTime::new(
        u32::try_from(tick / u64::from(rate))?,
        (tick % u64::from(rate)) as u32,
        rate,
    )?)
}
