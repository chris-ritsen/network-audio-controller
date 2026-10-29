use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::clock::discipline::{Reading, SampleClock};
use netaudio_core::native_audio::endpoint_control::{Endpoint, ReceiveFlow};
use netaudio_core::native_audio::flow_inventory::Inventory;
use netaudio_core::native_audio::playout::{self, Buffer, Owner};
use netaudio_core::native_audio::source_control::{self, Allocation, Handle, Header, ReplyProfile};
use netaudio_core::native_audio::{
    Format, MAXIMUM_UDP_PAYLOAD_BYTES, Packet, REVERSE_KEEPALIVE, SampleTime,
};
use netaudio_core::protocol::RESULT_CODE_SUCCESS;
use serde::Serialize;
use tokio::net::UdpSocket;

use super::playback::Receipt;
use super::receive_buffers::{Preparation, Prepared, Worker};
use super::selection::Candidate;
use super::sockets::{self, Datagram, MediaPool, Monotonic};
use netaudio_core::native_audio::eligibility::SourceFormat;

const CONTROL_DEADLINE_NANOSECONDS: u64 = 2_000_000_000;
const FIRST_MEDIA_DEADLINE_NANOSECONDS: u64 = 5_000_000_000;
const REVERSE_INTERVAL_NANOSECONDS: u64 = 250_000_000;
const SUBSCRIPTION_UNRESOLVED: u16 = 1;
const SUBSCRIPTION_UNICAST: u16 = 9;
const SELECTED_ASSOCIATION_MASK: u8 = 1;

#[derive(Clone, Default, Serialize)]
pub struct Summary {
    pub allocation_requests: u64,
    pub allocation_acceptances: u64,
    pub inventory_confirmations: u64,
    pub stop_requests: u64,
    pub stop_acceptances: u64,
    pub received_packets: u64,
    pub rejected_packets: u64,
    pub reverse_packets: u64,
    pub delivered_frames: u64,
    pub received_samples_consumed: u64,
    pub local_silence_samples_consumed: u64,
    pub media_suspensions: u64,
    pub missed_playout_deadlines: u64,
    pub missing_frame_suspensions: u64,
    pub uncertain_remote_effect: bool,
    pub unavailable: Option<String>,
}

impl Summary {
    pub fn add(&mut self, other: &Self) {
        self.allocation_requests += other.allocation_requests;
        self.allocation_acceptances += other.allocation_acceptances;
        self.inventory_confirmations += other.inventory_confirmations;
        self.stop_requests += other.stop_requests;
        self.stop_acceptances += other.stop_acceptances;
        self.received_packets += other.received_packets;
        self.rejected_packets += other.rejected_packets;
        self.reverse_packets += other.reverse_packets;
        self.delivered_frames += other.delivered_frames;
        self.received_samples_consumed += other.received_samples_consumed;
        self.local_silence_samples_consumed += other.local_silence_samples_consumed;
        self.media_suspensions += other.media_suspensions;
        self.missed_playout_deadlines += other.missed_playout_deadlines;
        self.missing_frame_suspensions += other.missing_frame_suspensions;
        self.uncertain_remote_effect |= other.uncertain_remote_effect;
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Allocate,
    Inventory,
    Stop,
}

struct Pending {
    operation: Operation,
    header: Header,
    peer: SocketAddrV4,
    deadline: u64,
}

struct Flow {
    candidate: Candidate,
    port_index: usize,
    socket_generation: u64,
    owner: Owner,
    format: Format,
    destination: SocketAddrV4,
    handle: Option<Handle>,
    inventory: Inventory,
    inventory_channels: Vec<u16>,
    confirmed: bool,
    retiring: bool,
    source: Option<SocketAddrV4>,
    buffer: Buffer,
    next_reverse_tick: Option<u64>,
    first_media_deadline: u64,
    last_receipt_end: Option<u64>,
}

impl Flow {
    fn slots(&self) -> Vec<Vec<u16>> {
        self.candidate.context.slots.iter().map(|slot| slot.receivers.clone()).collect()
    }
}

pub enum Event {
    Prepared(Result<Box<Prepared>>),
    Control(Vec<u8>, SocketAddr),
    Acquisition,
    Timeout,
}

pub struct Session {
    pub identifier: u16,
    pub acquisition: super::acquisition::Client,
    pub candidate: Option<Candidate>,
    pub channels: Vec<u16>,
    pub pool: MediaPool,
    pub summary: Summary,
    control: Arc<UdpSocket>,
    output_available: bool,
    time: Monotonic,
    endpoint_generation: u64,
    generation: u64,
    next_transaction: Option<u16>,
    attempted: Option<(Candidate, u64)>,
    waiting_for_port: bool,
    flow: Option<Flow>,
    pending: Option<Pending>,
    control_buffer: Vec<u8>,
    buffers: Worker,
    preparing: Option<(Candidate, u64, bool)>,
    prepared: Option<Box<Prepared>>,
}

fn sample_time(tick: u64, sample_rate: u32) -> Result<SampleTime> {
    Ok(SampleTime::new(
        u32::try_from(tick / u64::from(sample_rate))?,
        (tick % u64::from(sample_rate)) as u32,
        sample_rate,
    )?)
}

impl Session {
    pub async fn new(
        address: Ipv4Addr,
        output_available: bool,
        time: Monotonic,
        incarnation: u64,
        identifier: u16,
        pool: MediaPool,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            identifier,
            acquisition: super::acquisition::Client::new(address, time.clone()).await?,
            candidate: None,
            channels: Vec::new(),
            summary: Summary::default(),
            control: sockets::bind(address).await?,
            output_available,
            time,
            endpoint_generation: incarnation,
            generation: u64::from(identifier) << 48,
            next_transaction: Some(1),
            attempted: None,
            waiting_for_port: false,
            flow: None,
            pending: None,
            control_buffer: vec![0; MAXIMUM_UDP_PAYLOAD_BYTES],
            buffers: Worker::new(),
            preparing: None,
            prepared: None,
        })
    }

    fn generation(&mut self) -> Result<u64> {
        self.generation = self.generation.checked_add(1).context("receive generation exhausted")?;
        Ok(self.generation)
    }

    fn transaction(&mut self) -> Result<u16> {
        let transaction =
            self.next_transaction.context("receive control transaction space exhausted")?;
        self.next_transaction = transaction.checked_add(1);
        Ok(transaction)
    }

    fn submit(&mut self, operation: Operation, peer: SocketAddrV4, bytes: &[u8]) -> Result<()> {
        let header = Header::decode(bytes)?;
        self.pending = Some(Pending {
            operation,
            header,
            peer,
            deadline: self
                .time
                .now()
                .checked_add(CONTROL_DEADLINE_NANOSECONDS)
                .context("receive control deadline overflow")?,
        });
        let sent = self.control.try_send_to(bytes, peer.into())?;

        if sent != bytes.len() {
            bail!("receive control request was not submitted completely");
        }

        Ok(())
    }

    fn reconcile(
        &mut self,
        endpoint: &mut Endpoint,
        candidate: Option<&Candidate>,
        format: Option<&SourceFormat>,
        clock: &impl SampleClock,
    ) -> Result<()> {
        let reading = clock.reading(self.time.now())?;
        let current = self.flow.as_ref().is_none_or(|flow| {
            candidate.is_some_and(|candidate| candidate.same_context(&flow.candidate))
                && reading
                    .as_ref()
                    .is_some_and(|reading| reading.generation == flow.owner.clock_generation)
        });

        if !current {
            self.retire(endpoint, "receive route or clock context changed")?;
        }

        if let Some((previous, generation, valid)) = &mut self.preparing {
            *valid &= candidate.is_some_and(|candidate| candidate.same_context(previous))
                && reading.as_ref().is_some_and(|reading| reading.generation == *generation);
        }
        if self.prepared.as_ref().is_some_and(|prepared| {
            !candidate
                .is_some_and(|candidate| candidate.same_context(&prepared.preparation.candidate))
                || !reading.as_ref().is_some_and(|reading| {
                    reading.generation == prepared.preparation.owner.clock_generation
                })
        }) {
            self.buffers.dispose(self.prepared.take().unwrap().buffer)?;
        }

        if self.flow.is_some()
            || self.pending.is_some()
            || self.preparing.is_some()
            || self.summary.uncertain_remote_effect
        {
            return Ok(());
        }

        self.waiting_for_port = false;
        let Some(candidate) = candidate else {
            return Ok(());
        };
        let Some(reading) = reading else {
            self.summary.unavailable = Some("receive clock is unavailable".into());
            return Ok(());
        };
        let Some(format) = format else {
            return Ok(());
        };

        if !self.buffers.available() {
            self.summary.unavailable = Some("receive buffer worker is unavailable".into());
            return Ok(());
        }

        if !self.output_available {
            self.summary.unavailable = Some("receive output is not configured".into());
            return Ok(());
        }

        if self.prepared.is_none()
            && self.attempted.as_ref().is_some_and(|(previous, generation)| {
                previous.same_context(candidate) && *generation == reading.generation
            })
        {
            return Ok(());
        }

        if self.pool.next_available().is_none_or(|deadline| deadline > self.time.now()) {
            self.waiting_for_port = true;
            self.summary.unavailable = Some("receive ports are quarantined or unavailable".into());
            return Ok(());
        }

        if self.prepared.is_none() {
            let source_flow_capacity = format.flow_capacity;
            self.channels = candidate
                .context
                .slots
                .iter()
                .flat_map(|slot| slot.receivers.iter().copied())
                .collect();
            self.channels.sort_unstable();
            let inventory_channels =
                format.channels.iter().map(|channel| channel.routing_identifier).collect();
            let owner = Owner {
                endpoint_generation: self.endpoint_generation,
                stream_generation: self.generation()?,
                clock_generation: reading.generation,
            };
            let format =
                candidate.context.profile.format(u16::try_from(candidate.context.slots.len())?)?;
            self.buffers.prepare(Preparation {
                candidate: candidate.clone(),
                owner,
                format,
                inventory_channels,
                source_flow_capacity,
            })?;
            self.preparing = Some((candidate.clone(), reading.generation, true));
            self.attempted = Some((candidate.clone(), reading.generation));
            self.summary.unavailable = Some("preparing receive buffer".into());
            return Ok(());
        }
        let generation = self
            .prepared
            .as_ref()
            .context("receive buffer preparation disappeared")?
            .preparation
            .owner
            .stream_generation;
        let Some(port_index) = self.pool.acquire(generation, self.time.now()) else {
            self.waiting_for_port = true;
            self.summary.unavailable = Some("receive ports are quarantined or unavailable".into());
            return Ok(());
        };
        let prepared = self.prepared.take().context("receive buffer preparation disappeared")?;
        let Preparation {
            owner,
            format,
            inventory_channels,
            source_flow_capacity,
            ..
        } = prepared.preparation;
        let SocketAddr::V4(destination) = self.pool.slots[port_index].socket.local_addr()? else {
            bail!("receive media socket is not IPv4");
        };
        let channels =
            candidate.context.slots.iter().map(|slot| slot.source_identifier).collect::<Vec<_>>();
        let request = source_control::allocate(
            &Allocation {
                receiver_hostname: &endpoint.name,
                receiver_flow_name: &format!("rx-flow-{}", self.identifier),
                destination,
                format,
                source_channel_identifiers: &channels,
                requested_frames_per_packet: candidate.context.requested_frames_per_packet,
            },
            self.transaction()?,
        )?;
        self.flow = Some(Flow {
            candidate: candidate.clone(),
            port_index,
            socket_generation: generation,
            owner,
            format,
            destination,
            handle: None,
            inventory: Inventory::new(
                candidate.context.routing.declaration.current_revision,
                source_flow_capacity,
            )?,
            inventory_channels,
            confirmed: false,
            retiring: false,
            source: None,
            buffer: prepared.buffer,
            next_reverse_tick: None,
            first_media_deadline: self
                .time
                .now()
                .checked_add(FIRST_MEDIA_DEADLINE_NANOSECONDS)
                .context("first media deadline overflow")?,
            last_receipt_end: None,
        });
        self.attempted = Some((candidate.clone(), reading.generation));
        self.summary.unavailable = Some("waiting for source allocation".into());
        self.summary.allocation_requests += 1;
        self.submit(Operation::Allocate, candidate.context.control_endpoint, &request)
    }

    fn clear_status(&self, endpoint: &mut Endpoint) {
        endpoint.receive_flows.remove(&self.identifier);

        for identifier in &self.channels {
            let channel = &mut endpoint.receive_channels[usize::from(*identifier - 1)];
            channel.subscription_status = if channel.desired.is_some() {
                SUBSCRIPTION_UNRESOLVED
            } else {
                0
            };
            channel.available_interfaces = 0;
            channel.asserted_interfaces = 0;
        }
    }

    pub fn retire(&mut self, endpoint: &mut Endpoint, reason: &str) -> Result<()> {
        if let Some((_, _, valid)) = &mut self.preparing {
            *valid = false;
        }
        if let Some(prepared) = self.prepared.take() {
            self.buffers.dispose(prepared.buffer)?;
        }
        if self.summary.unavailable.as_deref() != Some(reason) {
            eprintln!("receiver retiring: {reason}");
        }
        if !self.summary.uncertain_remote_effect {
            self.summary.unavailable = Some(reason.to_owned());
        }
        self.clear_status(endpoint);

        let Some(flow) = &mut self.flow else {
            return Ok(());
        };
        flow.retiring = true;

        if self.pending.as_ref().is_some_and(|pending| {
            matches!(pending.operation, Operation::Allocate | Operation::Stop)
        }) {
            return Ok(());
        }

        self.pending = None;
        let handle = flow.handle.context("receive flow has no allocation owner to retire")?;
        let peer = flow.candidate.context.control_endpoint;
        let request = source_control::stop(handle, self.transaction()?)?;
        self.summary.stop_requests += 1;
        self.submit(Operation::Stop, peer, &request)
    }

    fn release(&mut self) -> Result<()> {
        if let Some(flow) = self.flow.take() {
            self.pool.release(flow.port_index, self.time.now())?;
            self.buffers.dispose(flow.buffer)?;
        }

        Ok(())
    }

    fn inventory(&mut self) -> Result<()> {
        let transaction = self.transaction()?;
        let flow = self.flow.as_ref().context("source inventory lost its allocation")?;
        let request = flow.inventory.request(transaction)?;
        self.submit(Operation::Inventory, flow.candidate.context.routing.endpoint, &request)
    }

    pub async fn next_event(&mut self) -> Result<Event> {
        let deadline =
            self.pending.as_ref().map(|pending| self.time.instant(pending.deadline)).transpose()?;

        tokio::select! {
            _ = self.acquisition.changed() => Ok(Event::Acquisition),
            prepared = self.buffers.next() => Ok(Event::Prepared(prepared?)),
            _ = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    _ => std::future::pending().await,
                }
            } => Ok(Event::Timeout),
            received = self.control.recv_from(&mut self.control_buffer), if self.pending.is_some() => {
                let (length, peer) = received?;
                Ok(Event::Control(self.control_buffer[..length].to_vec(), peer))
            }
        }
    }

    pub fn event(&mut self, event: Event, endpoint: &mut Endpoint) -> Result<()> {
        match event {
            Event::Acquisition => Ok(()),
            Event::Prepared(prepared) => {
                let (_, _, valid) =
                    self.preparing.take().context("receive buffer has no preparation owner")?;
                let prepared = prepared?;
                if valid {
                    self.prepared = Some(prepared);
                } else {
                    self.buffers.dispose(prepared.buffer)?;
                }
                Ok(())
            }
            Event::Timeout => {
                let pending = self.pending.take().context("receive deadline has no request")?;

                if matches!(pending.operation, Operation::Allocate | Operation::Stop) {
                    self.summary.uncertain_remote_effect = true;
                    self.release()?;
                    self.clear_status(endpoint);
                    bail!(
                        "receive mutation timed out; remote effect is uncertain and will not be retried"
                    );
                }

                self.retire(endpoint, "source flow inventory timed out")
            }
            Event::Control(bytes, peer) => self.control_reply(&bytes, peer, endpoint),
        }
    }

    fn control_reply(
        &mut self,
        bytes: &[u8],
        peer: SocketAddr,
        endpoint: &mut Endpoint,
    ) -> Result<()> {
        let Some(pending) = &self.pending else {
            return Ok(());
        };

        if self.time.now() >= pending.deadline {
            return self.event(Event::Timeout, endpoint);
        }

        if peer != SocketAddr::V4(pending.peer)
            || !Header::decode(bytes).is_ok_and(|header| pending.header.owns_reply(header))
        {
            self.summary.rejected_packets += 1;
            return Ok(());
        }

        match pending.operation {
            Operation::Allocate => {
                let outcome = source_control::allocation_reply(
                    bytes,
                    ReplyProfile::DetailedInterfaceOutcomes,
                )?;
                self.pending = None;
                let Some(handle) = outcome.handle else {
                    self.summary.unavailable =
                        Some(format!("source rejected allocation: {:#06x}", outcome.header.result));
                    self.release()?;
                    return Ok(());
                };
                let flow = self.flow.as_mut().context("source reply lost its receive owner")?;
                flow.handle = Some(handle);
                self.summary.allocation_acceptances += 1;

                if outcome.header.result != RESULT_CODE_SUCCESS
                    || outcome.interface_outcomes.as_deref() != Some(&[RESULT_CODE_SUCCESS])
                {
                    return self
                        .retire(endpoint, "source did not accept the complete receive interface");
                }

                if flow.retiring {
                    return self.retire(endpoint, "retiring superseded receive allocation");
                }

                self.inventory()
            }
            Operation::Inventory => {
                self.pending = None;
                let flow = self.flow.as_mut().context("inventory reply lost its allocation")?;
                let result = (|| -> Result<bool> {
                    if !flow.inventory.accept(bytes)? {
                        return Ok(false);
                    }

                    flow.inventory.confirm(
                        flow.handle.context("source handle is missing")?,
                        flow.destination,
                        &flow.inventory_channels,
                        flow.candidate.context.requested_frames_per_packet,
                        flow.format,
                    )?;
                    Ok(true)
                })();

                match result {
                    Ok(false) => self.inventory(),
                    Ok(true) => {
                        flow.confirmed = true;
                        flow.first_media_deadline = self
                            .time
                            .now()
                            .checked_add(FIRST_MEDIA_DEADLINE_NANOSECONDS)
                            .context("first media deadline overflow")?;
                        self.summary.inventory_confirmations += 1;
                        self.summary.unavailable =
                            Some("waiting for received audio and output consumption".into());
                        Ok(())
                    }
                    Err(error) => {
                        self.retire(endpoint, &format!("source flow readback failed: {error:#}"))
                    }
                }
            }
            Operation::Stop => {
                let reply = source_control::stop_reply(bytes)?;
                self.pending = None;

                if reply.result == RESULT_CODE_SUCCESS {
                    self.summary.stop_acceptances += 1;
                } else {
                    self.summary.uncertain_remote_effect = true;
                    self.summary.unavailable =
                        Some(format!("owned source STOP failed: {:#06x}", reply.result));
                }

                self.release()
            }
        }
    }

    pub fn media(
        &mut self,
        index: usize,
        datagram: &Datagram,
        clock: &impl SampleClock,
        endpoint: &mut Endpoint,
    ) -> Result<()> {
        let Some(flow) = self.flow.as_mut().filter(|flow| flow.confirmed && !flow.retiring) else {
            return Ok(());
        };
        let Some(reading) = clock.reading(self.time.now())? else {
            return Ok(());
        };

        if index != flow.port_index
            || datagram.generation != flow.socket_generation
            || reading.generation != flow.owner.clock_generation
            || datagram.peer.ip() != flow.candidate.context.control_endpoint.ip()
            || flow.source.is_some_and(|source| source != datagram.peer)
        {
            self.summary.rejected_packets += 1;
            return Ok(());
        }

        let packet = Packet::decode(&datagram.bytes, flow.format)?;

        if packet.frame_count() != usize::from(flow.candidate.context.requested_frames_per_packet) {
            bail!("received audio differs from confirmed source packetization");
        }

        let tick = reading.sample_tick(flow.format.sample_rate())?;
        flow.buffer.admit(flow.owner, &datagram.bytes, tick)?;
        flow.source = Some(datagram.peer);
        self.summary.received_packets += 1;

        if flow.next_reverse_tick.is_none() {
            flow.next_reverse_tick = Some(tick);
        }

        if let std::collections::btree_map::Entry::Vacant(entry) =
            endpoint.receive_flows.entry(self.identifier)
        {
            entry.insert(ReceiveFlow {
                source: datagram.peer,
                slots: flow.slots(),
                active: false,
                status: 0,
            });

            for slot in &flow.candidate.context.slots {
                for identifier in &slot.receivers {
                    let channel = &mut endpoint.receive_channels[usize::from(*identifier - 1)];
                    channel.subscription_status = SUBSCRIPTION_UNICAST;
                    channel.available_interfaces = SELECTED_ASSOCIATION_MASK;
                    channel.asserted_interfaces = 0;
                }
            }
        }

        Ok(())
    }

    pub fn deadline(&mut self, clock: &impl SampleClock) -> Result<Option<u64>> {
        let mut deadline = self.waiting_for_port.then(|| self.pool.next_available()).flatten();
        let Some(flow) = self.flow.as_mut().filter(|flow| flow.confirmed && !flow.retiring) else {
            return Ok(deadline);
        };
        let Some(reading) = clock.reading(self.time.now())? else {
            return Ok(deadline);
        };

        if flow.last_receipt_end.is_none() {
            deadline = Some(flow.first_media_deadline);
        }

        for tick in [flow.next_reverse_tick].into_iter().flatten() {
            let due = reading.sample_deadline(
                sample_time(tick, flow.format.sample_rate())?,
                flow.format.sample_rate(),
            )?;
            deadline = Some(deadline.map_or(due, |current| current.min(due)));
        }

        Ok(deadline)
    }

    pub fn dispatch(&mut self, endpoint: &mut Endpoint, clock: &impl SampleClock) -> Result<()> {
        let Some(reading) = clock.reading(self.time.now())? else {
            return Ok(());
        };
        let Some(flow) = self.flow.as_mut().filter(|flow| flow.confirmed && !flow.retiring) else {
            return Ok(());
        };

        if flow.last_receipt_end.is_none() && self.time.now() >= flow.first_media_deadline {
            return self
                .retire(endpoint, "received audio did not reach output before its deadline");
        }

        let tick = reading.sample_tick(flow.format.sample_rate())?;

        if flow.next_reverse_tick.is_some_and(|next| tick >= next) {
            let source = flow.source.context("reverse activity has no admitted source")?;
            let sent = self.pool.slots[flow.port_index]
                .socket
                .try_send_to(REVERSE_KEEPALIVE, source.into())?;

            if sent != REVERSE_KEEPALIVE.len() {
                bail!("receive reverse activity submission was incomplete");
            }

            flow.next_reverse_tick = Some(
                tick.checked_add(
                    (u64::from(flow.format.sample_rate()) * REVERSE_INTERVAL_NANOSECONDS)
                        .div_ceil(1_000_000_000),
                )
                .context("reverse sample deadline overflow")?,
            );
            self.summary.reverse_packets += 1;
        }

        Ok(())
    }

    pub fn suspend(
        &mut self,
        endpoint: &mut Endpoint,
        reading: &Reading<'_>,
        reason: &str,
    ) -> Result<()> {
        if let Some(flow) = &mut self.flow {
            flow.owner.clock_generation = reading.generation;
        }

        self.suspend_output(endpoint, reason)
    }

    pub fn suspend_output(&mut self, endpoint: &mut Endpoint, reason: &str) -> Result<()> {
        if self.flow.as_ref().is_none_or(|flow| flow.retiring) {
            return Ok(());
        }

        eprintln!("receive output suspended at {} ns: {reason}", self.time.now());
        let generation = self.generation()?;
        let flow = self.flow.as_mut().context("receive suspension has no allocation")?;
        flow.owner.stream_generation = generation;
        flow.buffer.reset(flow.owner)?;
        flow.last_receipt_end = None;
        flow.first_media_deadline = self
            .time
            .now()
            .checked_add(FIRST_MEDIA_DEADLINE_NANOSECONDS)
            .context("receive recovery deadline overflow")?;
        self.summary.media_suspensions += 1;
        self.summary.unavailable = Some(reason.to_owned());

        if let Some(flow) = endpoint.receive_flows.get_mut(&self.identifier) {
            flow.active = false;
        }

        for identifier in &self.channels {
            endpoint.receive_channels[usize::from(*identifier - 1)].asserted_interfaces = 0;
        }

        Ok(())
    }

    pub fn consumed(&mut self, receipt: Receipt, endpoint: &mut Endpoint) -> Result<()> {
        let Some(flow) =
            self.flow.as_mut().filter(|flow| !flow.retiring && flow.owner == receipt.owner)
        else {
            return Ok(());
        };

        if !receipt.output_accepted {
            return Ok(());
        }

        if flow.last_receipt_end.is_some_and(|end| end != receipt.first_frame_index) {
            bail!("receive output consumption is discontinuous");
        }

        flow.last_receipt_end = Some(
            receipt
                .first_frame_index
                .checked_add(receipt.frames as u64)
                .context("receive output coordinate overflow")?,
        );
        self.summary.delivered_frames += receipt.frames as u64;
        self.summary.received_samples_consumed += receipt.received_samples as u64;
        self.summary.local_silence_samples_consumed += receipt.local_silence_samples as u64;

        if receipt.received_samples > 0 {
            self.summary.unavailable = None;

            if let Some(flow) = endpoint.receive_flows.get_mut(&self.identifier) {
                flow.active = true;
            }

            for slot in &flow.candidate.context.slots {
                for identifier in &slot.receivers {
                    endpoint.receive_channels[usize::from(*identifier - 1)].asserted_interfaces =
                        SELECTED_ASSOCIATION_MASK;
                }
            }
        }

        Ok(())
    }

    pub fn select(&mut self, candidate: Option<&Candidate>) {
        if candidate.is_none() && self.idle() && !self.summary.uncertain_remote_effect {
            self.attempted = None;
        }
        self.acquisition.synchronize(candidate);
        self.candidate = candidate.cloned();
    }

    pub fn synchronize(&mut self, endpoint: &mut Endpoint, clock: &impl SampleClock) -> Result<()> {
        let candidate = self.candidate.clone();
        let format = self
            .acquisition
            .progress(candidate.as_ref())
            .and_then(|progress| progress.source_format.clone());
        self.reconcile(endpoint, candidate.as_ref(), format.as_ref(), clock)?;
        if self.flow.is_none()
            && self.pending.is_none()
            && self.preparing.is_none()
            && self.prepared.is_none()
        {
            self.channels = candidate
                .as_ref()
                .map(|candidate| {
                    candidate
                        .context
                        .slots
                        .iter()
                        .flat_map(|slot| slot.receivers.iter().copied())
                        .collect()
                })
                .unwrap_or_default();
        }
        Ok(())
    }

    pub fn idle(&self) -> bool {
        self.flow.is_none()
            && self.pending.is_none()
            && self.preparing.is_none()
            && self.prepared.is_none()
    }

    pub fn ready_tick(&mut self, tick: u64) -> Option<u64> {
        self.flow
            .as_mut()
            .filter(|flow| flow.confirmed && !flow.retiring)
            .and_then(|flow| flow.buffer.next_frame_index(tick))
    }

    pub fn owner(&self) -> Option<Owner> {
        self.flow.as_ref().filter(|flow| flow.confirmed && !flow.retiring).map(|flow| flow.owner)
    }

    pub fn render(
        &mut self,
        first: u64,
        tick: u64,
        endpoint: &mut Endpoint,
        reading: &Reading<'_>,
    ) -> Result<Option<playout::Delivery>> {
        let Some(flow) = self.flow.as_mut().filter(|flow| flow.confirmed && !flow.retiring) else {
            return Ok(None);
        };
        if flow.buffer.next_frame_index(tick).is_none() {
            return Ok(None);
        }
        flow.buffer.align(first)?;
        let delivery = match flow.buffer.deliver(flow.owner, tick) {
            Ok(delivery) => delivery,
            Err(playout::Error::MissedDeadline) => {
                self.summary.missed_playout_deadlines += 1;
                self.suspend(endpoint, reading, "receive output missed its sample deadline")?;
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        if delivery.missing_limit_reached {
            self.summary.missing_frame_suspensions += 1;
            self.suspend(endpoint, reading, "received audio exceeded the missing-frame limit")?;
        }
        Ok(Some(delivery))
    }

    pub async fn close(mut self, endpoint: &mut Endpoint) -> Result<Summary> {
        let retirement = self.retire(endpoint, "receive endpoint stopped");

        while self.pending.is_some() {
            let event = self.next_event().await?;

            if let Err(error) = self.event(event, endpoint) {
                self.summary.unavailable = Some(format!("{error:#}"));
            }
        }

        self.release()?;
        self.buffers.close().await?;

        retirement?;
        Ok(self.summary)
    }
}
