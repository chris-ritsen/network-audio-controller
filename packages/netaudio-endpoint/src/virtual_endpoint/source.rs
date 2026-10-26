use std::net::SocketAddrV4;
use std::sync::atomic::Ordering;

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::clock::discipline::SampleClock;
use netaudio_core::native_audio::endpoint_control::{Endpoint, TransmitFlow, source};
use netaudio_core::native_audio::is_reverse_keepalive;
use netaudio_core::native_audio::source_control::Header;
use tokio::task::{JoinHandle, JoinSet};

use super::pcm::{Capture, CaptureFactory};
use super::platform::Generations;
use super::sockets::{Datagram, MediaPool, Monotonic};
use super::transmit::Flow;

struct Pending {
    peer: SocketAddrV4,
    header: Header,
    state: TransmitFlow,
    result: u16,
    generation: u32,
    output_generation: u64,
    port_index: usize,
    capture: JoinHandle<Result<Capture>>,
}

enum Event {
    Generation(Result<u32>),
    Allocation(Result<Capture>),
    Capture(Result<()>),
    Cleanup(Result<()>),
}

pub struct Reply {
    pub peer: SocketAddrV4,
    pub bytes: Vec<u8>,
}

pub struct Runtime {
    pub pool: MediaPool,
    pub flow: Option<Flow>,
    pub submitted_packets: u64,
    pub maximum_submission_lateness_nanoseconds: u64,
    generations: Generations,
    reserved_generation: Option<u32>,
    output_generation: u64,
    input: CaptureFactory,
    pending: Option<Pending>,
    cleanup: JoinSet<Result<()>>,
    generation_failed: bool,
}

impl Runtime {
    pub fn new(pool: MediaPool, generations: Generations, input: CaptureFactory) -> Self {
        Self {
            pool,
            flow: None,
            submitted_packets: 0,
            maximum_submission_lateness_nanoseconds: 0,
            generations,
            reserved_generation: None,
            output_generation: 0,
            input,
            pending: None,
            cleanup: JoinSet::new(),
            generation_failed: false,
        }
    }

    fn next_output_generation(&mut self) -> Result<u64> {
        self.output_generation = self
            .output_generation
            .checked_add(1)
            .context("source output generation space is exhausted")?;
        Ok(self.output_generation)
    }

    pub async fn next_event(
        &mut self,
        endpoint: &mut Endpoint,
        time: &Monotonic,
    ) -> Result<Option<Reply>> {
        let event = tokio::select! {
            result = self.generations.next(), if self.reserved_generation.is_none() && !self.generation_failed => Event::Generation(result),
            result = async {
                match &mut self.pending {
                    Some(pending) => (&mut pending.capture).await?,
                    None => std::future::pending().await,
                }
            } => Event::Allocation(result),
            result = async {
                match self.flow.as_mut().filter(|flow| flow.needs_capture()) {
                    Some(flow) => flow.receive_capture().await,
                    None => std::future::pending().await,
                }
            } => Event::Capture(result),
            result = self.cleanup.join_next(), if !self.cleanup.is_empty() => Event::Cleanup(match result {
                Some(result) => result?,
                None => unreachable!(),
            }),
        };

        match event {
            Event::Generation(result) => {
                self.generation_failed = result.is_err();
                self.reserved_generation = Some(result?);
            }
            Event::Allocation(result) => {
                return self.allocation_ready(endpoint, time, result).map(Some);
            }
            Event::Capture(result) => {
                if result.is_err()
                    && let Some(flow) = &mut self.flow
                {
                    flow.capture_failed();
                }

                if result.is_err() || self.flow.as_ref().is_some_and(Flow::exhausted) {
                    self.suspend(endpoint)?;
                }

                result?;
            }
            Event::Cleanup(result) => result?,
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
                if self.pending.is_some() || self.reserved_generation.is_none() {
                    source::unavailable(header)?
                } else {
                    let output_generation = self.next_output_generation()?;
                    let Some(port_index) = self.pool.acquire(output_generation, now) else {
                        return Ok(Some(Reply {
                            peer: datagram.peer,
                            bytes: source::unavailable(header)?,
                        }));
                    };
                    let generation = self
                        .reserved_generation
                        .take()
                        .context("source generation was not prepared")?;
                    let input = self.input.clone();
                    let capture = tokio::task::spawn_blocking(move || input.open());
                    self.pending = Some(Pending {
                        peer: datagram.peer,
                        header,
                        state: flow,
                        result,
                        generation,
                        output_generation,
                        port_index,
                        capture,
                    });
                    return Ok(None);
                }
            }
            source::Action::Remap {
                header,
                channels,
                result,
            } => {
                self.suspend(endpoint)?;
                let response = source::remap_committed(endpoint, header, channels.clone(), result)?;

                if let Some(flow) = &mut self.flow
                    && !flow.exhausted()
                {
                    flow.resume(&channels, now)?;
                }

                response
            }
            source::Action::Stop {
                header,
            } => {
                self.retire(endpoint, now)?;
                source::stop_committed(endpoint, header)?
            }
        };

        Ok(Some(Reply {
            peer: datagram.peer,
            bytes,
        }))
    }

    fn allocation_ready(
        &mut self,
        endpoint: &mut Endpoint,
        time: &Monotonic,
        prepared: Result<Capture>,
    ) -> Result<Reply> {
        let pending = self.pending.take().context("prepared source allocation disappeared")?;
        let capture = match prepared {
            Ok(capture) => capture,
            Err(error) => {
                self.pool.release(pending.port_index, time.now())?;
                eprintln!("source capture preparation failed: {error:#}");
                return Ok(Reply {
                    peer: pending.peer,
                    bytes: source::unavailable(pending.header)?,
                });
            }
        };

        let mut state = pending.state;
        state.generation = pending.generation;
        let flow = Flow::new(&state, pending.port_index, pending.output_generation, capture)?;
        let bytes = source::allocation_committed(
            endpoint,
            pending.header,
            state,
            pending.generation,
            pending.result,
        )?;
        self.flow = Some(flow);
        Ok(Reply {
            peer: pending.peer,
            bytes,
        })
    }

    pub fn suspend(&mut self, endpoint: &mut Endpoint) -> Result<()> {
        let generation = self.next_output_generation()?;

        if let Some(flow) = &mut self.flow {
            flow.suspend(generation)?;
            self.pool.slots[flow.port_index].owner.store(generation, Ordering::Release);
        }

        if let Some(state) = &mut endpoint.transmit_flow {
            state.active = false;
        }

        Ok(())
    }

    pub fn keepalive(
        &mut self,
        endpoint: &Endpoint,
        index: usize,
        datagram: &Datagram,
        clock: &impl SampleClock,
        now: u64,
    ) -> Result<()> {
        let Some(flow) = &mut self.flow else {
            return Ok(());
        };
        let state =
            endpoint.transmit_flow.as_ref().context("source allocation state is missing")?;

        if flow.port_index != index
            || flow.output_generation != datagram.generation
            || state.destination != datagram.peer
            || !is_reverse_keepalive(&datagram.bytes)
        {
            return Ok(());
        }

        if flow.suspended() && !flow.exhausted() && clock.reading(now)?.is_some() {
            flow.resume(&state.channels, now)?;
        }

        flow.keepalive(datagram.generation, datagram.peer, &datagram.bytes, now);
        Ok(())
    }

    pub fn deadline(&mut self, clock: &impl SampleClock, now: u64) -> Result<Option<u64>> {
        match &mut self.flow {
            Some(flow) => flow.deadline(clock, now),
            None => Ok(None),
        }
    }

    pub fn send(
        &mut self,
        endpoint: &mut Endpoint,
        clock: &impl SampleClock,
        time: &Monotonic,
    ) -> Result<()> {
        let flow = self.flow.as_mut().context("source flow has retired")?;
        let lateness = flow.send(&self.pool.slots[flow.port_index].socket, clock, time)?;
        self.submitted_packets += 1;
        self.maximum_submission_lateness_nanoseconds =
            self.maximum_submission_lateness_nanoseconds.max(lateness);
        endpoint.transmit_flow.as_mut().context("source allocation has retired")?.active =
            flow.active();

        if flow.exhausted() {
            self.suspend(endpoint)?;
        }

        Ok(())
    }

    fn retire(&mut self, endpoint: &mut Endpoint, now: u64) -> Result<()> {
        self.suspend(endpoint)?;

        if let Some(flow) = self.flow.take() {
            self.pool.release(flow.port_index, now)?;
            self.cleanup.spawn(flow.close());
        }

        Ok(())
    }

    pub async fn close(mut self, endpoint: &mut Endpoint, now: u64) -> Result<()> {
        self.retire(endpoint, now)?;
        let mut failures = Vec::new();

        if let Some(pending) = self.pending.take() {
            match pending.capture.await {
                Ok(Ok(capture)) => {
                    if let Err(error) = capture.close().await {
                        failures.push(format!("{error:#}"));
                    }
                }
                Ok(Err(error)) => failures.push(format!("{error:#}")),
                Err(error) => failures.push(error.to_string()),
            }
        }

        while let Some(result) = self.cleanup.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(format!("{error:#}")),
                Err(error) => failures.push(error.to_string()),
            }
        }

        if let Err(error) = self.generations.close().await {
            failures.push(format!("{error:#}"));
        }

        if !failures.is_empty() {
            bail!("source shutdown failed: {}", failures.join("; "));
        }

        Ok(())
    }
}
