use std::collections::VecDeque;
use std::net::SocketAddrV4;

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::clock::discipline::{NetworkInstant, SampleClock};
use netaudio_core::native_audio::endpoint_control::{SAMPLE_RATE, TransmitFlow};
use netaudio_core::native_audio::{Format, SampleTime, SampleWidth, encode, is_reverse_keepalive};
use tokio::net::UdpSocket;

use super::pcm::{Capture, Frame, PRIMING_FRAMES, QUANTUM_FRAMES, TRANSMIT_PIPELINE_FRAMES};
use super::sockets::Monotonic;

const MAXIMUM_BUFFERED_FRAMES: usize = PRIMING_FRAMES + QUANTUM_FRAMES;
const INITIAL_KEEPALIVE_GRACE_NANOSECONDS: u64 = 5_000_000_000;
const KEEPALIVE_DEADLINE_NANOSECONDS: u64 = 2_000_000_000;
const MAXIMUM_PACKET_AGE_FRAMES: u64 = 192;
const MINIMUM_SEND_SPACING_NANOSECONDS: u64 = 62_500;

struct Schedule {
    clock_generation: u64,
    next_sample_tick: u64,
    keepalive_deadline: Option<u64>,
    last_submission: Option<NetworkInstant>,
}

pub struct Flow {
    pub generation: u32,
    pub output_generation: u64,
    pub port_index: usize,
    pub delayed_packets: u64,
    destination: SocketAddrV4,
    channels: Vec<u16>,
    frames_per_packet: u16,
    format: Format,
    capture: Capture,
    frames: VecDeque<Frame>,
    schedule: Option<Schedule>,
    ended: bool,
    suspended: bool,
    previously_submitted: bool,
    resumed_keepalive_deadline: Option<u64>,
}

impl Flow {
    pub fn new(
        state: &TransmitFlow,
        port_index: usize,
        output_generation: u64,
        capture: Capture,
    ) -> Result<Self> {
        let slots = u16::try_from(state.channels.len()).context("source slot count overflow")?;

        if state.generation == 0 || output_generation == 0 {
            bail!("source output requires current allocation and output generations");
        }

        Ok(Self {
            generation: state.generation,
            output_generation,
            port_index,
            delayed_packets: 0,
            destination: state.destination,
            channels: state.channels.clone(),
            frames_per_packet: state.frames_per_packet,
            format: Format::new(SAMPLE_RATE, slots, SampleWidth::Signed24)?,
            capture,
            frames: VecDeque::with_capacity(MAXIMUM_BUFFERED_FRAMES),
            schedule: None,
            ended: false,
            suspended: false,
            previously_submitted: false,
            resumed_keepalive_deadline: None,
        })
    }

    pub fn needs_capture(&self) -> bool {
        !self.suspended
            && !self.ended
            && self.frames.len() <= MAXIMUM_BUFFERED_FRAMES - QUANTUM_FRAMES
    }

    pub async fn receive_capture(&mut self) -> Result<()> {
        if !self.needs_capture() {
            bail!("capture requested without room for a complete quantum");
        }

        match self.capture.next_quantum().await? {
            Some(frames) => self.frames.extend(frames),
            None => self.ended = true,
        }

        Ok(())
    }

    pub fn keepalive(&mut self, generation: u64, peer: SocketAddrV4, bytes: &[u8], now: u64) {
        if generation != self.output_generation
            || peer != self.destination
            || !is_reverse_keepalive(bytes)
        {
            return;
        }

        if let Some(schedule) =
            self.schedule.as_mut().filter(|schedule| schedule.keepalive_deadline.is_some())
        {
            schedule.keepalive_deadline = Some(now.saturating_add(KEEPALIVE_DEADLINE_NANOSECONDS));
        }
    }

    pub fn active(&self) -> bool {
        self.schedule.as_ref().is_some_and(|schedule| schedule.last_submission.is_some())
    }

    pub fn suspend(&mut self, output_generation: u64) -> Result<()> {
        if output_generation == 0 || output_generation <= self.output_generation {
            bail!("source retirement requires a newer output generation");
        }

        self.output_generation = output_generation;
        self.capture.pause();
        self.suspended = true;
        self.schedule = None;
        self.frames.clear();
        self.resumed_keepalive_deadline = None;
        Ok(())
    }

    pub fn resume(&mut self, channels: &[u16], now: u64) -> Result<()> {
        if !self.suspended || channels.len() != self.channels.len() || self.exhausted() {
            bail!("source cannot resume without a retired output and available capture");
        }

        self.capture.resume()?;
        self.channels = channels.to_vec();
        self.suspended = false;
        self.resumed_keepalive_deadline = if self.previously_submitted {
            Some(
                now.checked_add(KEEPALIVE_DEADLINE_NANOSECONDS)
                    .context("source liveness deadline overflow")?,
            )
        } else {
            None
        };
        Ok(())
    }

    pub fn suspended(&self) -> bool {
        self.suspended
    }

    pub fn capture_failed(&mut self) {
        self.ended = true;
        self.frames.clear();
    }

    pub fn exhausted(&self) -> bool {
        self.ended && self.frames.len() < usize::from(self.frames_per_packet)
    }

    pub fn deadline(&mut self, clock: &impl SampleClock, now: u64) -> Result<Option<u64>> {
        if self.suspended || self.exhausted() {
            return Ok(None);
        }

        let Some(reading) = clock.reading(now)? else {
            if self.schedule.is_some() {
                bail!("source flow lost its admitted clock generation");
            }

            return Ok(None);
        };

        if let Some(schedule) = &self.schedule {
            if schedule.clock_generation != reading.generation {
                bail!("source flow lost its admitted clock generation");
            }

            if schedule.keepalive_deadline.is_some_and(|deadline| now >= deadline) {
                bail!("source receiver keepalive deadline expired");
            }
        } else {
            if self.frames.len() < PRIMING_FRAMES {
                return Ok(None);
            }

            let current_tick = reading.sample_tick(SAMPLE_RATE)?;
            let quantum_frames = QUANTUM_FRAMES as u64;
            let first_tick = current_tick
                .checked_add(TRANSMIT_PIPELINE_FRAMES)
                .context("source pipeline sample coordinate overflow")?
                .div_ceil(quantum_frames)
                * quantum_frames;
            self.schedule = Some(Schedule {
                clock_generation: reading.generation,
                next_sample_tick: first_tick,
                keepalive_deadline: self.resumed_keepalive_deadline,
                last_submission: None,
            });
        }

        let schedule = self.schedule.as_ref().context("source schedule is unavailable")?;
        let stale_tick = schedule
            .next_sample_tick
            .checked_add(MAXIMUM_PACKET_AGE_FRAMES)
            .context("source packet age coordinate overflow")?;

        if reading.instant()?.exceeds_sample(sample_time(stale_tick)?, SAMPLE_RATE)? {
            bail!(
                "source packet exceeded its declared capture age budget: current sample {}, packet sample {}, maximum age {MAXIMUM_PACKET_AGE_FRAMES} frames",
                reading.sample_tick(SAMPLE_RATE)?,
                schedule.next_sample_tick,
            );
        }

        let departure_tick = schedule
            .next_sample_tick
            .checked_add(TRANSMIT_PIPELINE_FRAMES)
            .context("source pipeline coordinate overflow")?;
        let due = reading.sample_deadline(sample_time(departure_tick)?, SAMPLE_RATE)?;
        let spacing = match &schedule.last_submission {
            Some(previous) => reading
                .instant_deadline(&previous.after_nanoseconds(MINIMUM_SEND_SPACING_NANOSECONDS)?)?,
            None => due,
        };

        Ok(Some(due.max(spacing)))
    }

    pub fn send(
        &mut self,
        socket: &UdpSocket,
        clock: &impl SampleClock,
        time: &Monotonic,
    ) -> Result<u64> {
        let now = time.now();
        let deadline =
            self.deadline(clock, now)?.context("source flow is not ready to transmit")?;

        if now < deadline {
            bail!("source media was requested before its clock deadline");
        }

        if self.frames.len() < usize::from(self.frames_per_packet) {
            bail!("capture provider did not supply a complete source packet");
        }

        let mut samples =
            Vec::with_capacity(usize::from(self.frames_per_packet) * self.channels.len());

        for frame in self.frames.iter().take(usize::from(self.frames_per_packet)) {
            for channel in &self.channels {
                samples.push(if *channel == 0 {
                    0
                } else {
                    *frame
                        .get(usize::from(*channel - 1))
                        .context("source slot refers to an unavailable capture channel")?
                });
            }
        }

        let schedule = self.schedule.as_mut().context("source schedule was retired")?;
        let bytes = encode(self.format, sample_time(schedule.next_sample_tick)?, &samples)?;
        let submitted = time.now();
        let reading =
            clock.reading(submitted)?.context("source clock was lost before submission")?;

        if reading.generation != schedule.clock_generation {
            bail!("source clock generation changed before submission");
        }

        let instant = reading.instant()?;
        let stale_tick = schedule
            .next_sample_tick
            .checked_add(MAXIMUM_PACKET_AGE_FRAMES)
            .context("source packet age coordinate overflow")?;

        if instant.exceeds_sample(sample_time(stale_tick)?, SAMPLE_RATE)? {
            bail!("source packet exceeded its declared capture age budget before submission");
        }

        if schedule.keepalive_deadline.is_some_and(|deadline| submitted >= deadline) {
            bail!("source receiver keepalive deadline expired before media submission");
        }

        let written = socket.try_send_to(&bytes, self.destination.into())?;

        if written != bytes.len() {
            bail!("source socket submitted an incomplete media datagram");
        }

        schedule.last_submission = Some(instant);
        self.previously_submitted = true;

        if schedule.keepalive_deadline.is_none() {
            schedule.keepalive_deadline = Some(
                submitted
                    .checked_add(INITIAL_KEEPALIVE_GRACE_NANOSECONDS)
                    .context("source keepalive deadline overflow")?,
            );
        }

        if submitted > deadline {
            self.delayed_packets = self.delayed_packets.saturating_add(1);
        }

        self.frames.drain(..usize::from(self.frames_per_packet));
        schedule.next_sample_tick = schedule
            .next_sample_tick
            .checked_add(u64::from(self.frames_per_packet))
            .context("source sample coordinate overflow")?;
        Ok(submitted.saturating_sub(deadline))
    }

    pub async fn close(self) -> Result<()> {
        self.capture.close().await
    }
}

fn sample_time(tick: u64) -> Result<SampleTime> {
    let rate = u64::from(SAMPLE_RATE);
    let seconds =
        u32::try_from(tick / rate).context("source sample time is outside its relative era")?;
    Ok(SampleTime::new(seconds, (tick % rate) as u32, SAMPLE_RATE)?)
}
