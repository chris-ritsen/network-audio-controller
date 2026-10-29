use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::clock::discipline::{NetworkInstant, SampleClock};
use netaudio_core::native_audio::endpoint_control::profile::MINIMUM_PACKET_SPACING_NANOSECONDS;
use netaudio_core::native_audio::endpoint_control::{
    Profile, TRANSMIT_LATENCY_NANOSECONDS, TransmitFlow,
};
use netaudio_core::native_audio::{Format, SampleTime, encode, is_reverse_keepalive};
use tokio::net::UdpSocket;

use super::pcm::Frame;
use super::sockets::Monotonic;
use netaudio_core::native_audio::playout::quantum_frames;

const INITIAL_KEEPALIVE_GRACE_NANOSECONDS: u64 = 5_000_000_000;
const KEEPALIVE_DEADLINE_NANOSECONDS: u64 = 2_000_000_000;
const TRANSMIT_PIPELINE_NANOSECONDS: u64 = 2_000_000;

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
    quantum_frames: usize,
    pipeline_frames: u64,
    maximum_packet_age_frames: u64,
    frames: VecDeque<(u64, Arc<Frame>)>,
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
        profile: Profile,
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
            format: profile.format(slots)?,
            quantum_frames: quantum_frames(profile.sample_rate),
            pipeline_frames: (u64::from(profile.sample_rate) * TRANSMIT_PIPELINE_NANOSECONDS)
                .div_ceil(1_000_000_000),
            maximum_packet_age_frames: (u64::from(profile.sample_rate)
                * u64::from(TRANSMIT_LATENCY_NANOSECONDS))
                / 1_000_000_000,
            frames: VecDeque::with_capacity(3 * quantum_frames(profile.sample_rate)),
            schedule: None,
            ended: false,
            suspended: false,
            previously_submitted: false,
            resumed_keepalive_deadline: None,
        })
    }

    pub fn needs_capture(&self) -> bool {
        !self.suspended && !self.ended && self.frames.len() <= 2 * self.quantum_frames
    }

    pub fn capture(&mut self, first_tick: u64, frames: &[Arc<Frame>]) -> Result<()> {
        if self.suspended || self.ended {
            return Ok(());
        }
        if !self.needs_capture() || frames.len() != self.quantum_frames {
            bail!("source flow cannot retain another complete capture quantum");
        }
        self.frames.extend(
            frames
                .iter()
                .enumerate()
                .map(|(index, frame)| (first_tick + index as u64, frame.clone())),
        );
        Ok(())
    }

    pub fn end_capture(&mut self) {
        self.ended = true;
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
            if self.frames.len() < 2 * self.quantum_frames {
                return Ok(None);
            }

            let first_tick = self.frames.front().context("source capture is unavailable")?.0;
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
            .checked_add(self.maximum_packet_age_frames)
            .context("source packet age coordinate overflow")?;

        if reading.instant()?.exceeds_sample(
            sample_time(stale_tick, self.format.sample_rate())?,
            self.format.sample_rate(),
        )? {
            bail!(
                "source packet exceeded its declared capture age budget: current sample {}, packet sample {}, maximum age {} frames",
                reading.sample_tick(self.format.sample_rate())?,
                schedule.next_sample_tick,
                self.maximum_packet_age_frames,
            );
        }

        let departure_tick = schedule
            .next_sample_tick
            .checked_add(self.pipeline_frames)
            .context("source pipeline coordinate overflow")?;
        let due = reading.sample_deadline(
            sample_time(departure_tick, self.format.sample_rate())?,
            self.format.sample_rate(),
        )?;
        let spacing = match &schedule.last_submission {
            Some(previous) => reading.instant_deadline(
                &previous.after_nanoseconds(MINIMUM_PACKET_SPACING_NANOSECONDS)?,
            )?,
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

        for (index, (tick, frame)) in
            self.frames.iter().take(usize::from(self.frames_per_packet)).enumerate()
        {
            if *tick
                != self.schedule.as_ref().context("source schedule unavailable")?.next_sample_tick
                    + index as u64
            {
                bail!("shared capture supplied a discontinuous sample coordinate");
            }
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
        let bytes = encode(
            self.format,
            sample_time(schedule.next_sample_tick, self.format.sample_rate())?,
            &samples,
        )?;
        let submitted = time.now();
        let reading =
            clock.reading(submitted)?.context("source clock was lost before submission")?;

        if reading.generation != schedule.clock_generation {
            bail!("source clock generation changed before submission");
        }

        let instant = reading.instant()?;
        let stale_tick = schedule
            .next_sample_tick
            .checked_add(self.maximum_packet_age_frames)
            .context("source packet age coordinate overflow")?;

        if instant.exceeds_sample(
            sample_time(stale_tick, self.format.sample_rate())?,
            self.format.sample_rate(),
        )? {
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
}

fn sample_time(tick: u64, sample_rate: u32) -> Result<SampleTime> {
    let rate = u64::from(sample_rate);
    let seconds =
        u32::try_from(tick / rate).context("source sample time is outside its relative era")?;
    Ok(SampleTime::new(seconds, (tick % rate) as u32, sample_rate)?)
}
