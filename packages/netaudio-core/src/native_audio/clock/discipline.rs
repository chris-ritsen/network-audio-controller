use std::collections::VecDeque;

use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::{One, Signed, ToPrimitive, Zero};
use serde::{Deserialize, Serialize};

use super::version_one::{self, Body, Dataset, EpochContext, Packet, PortIdentity};
use super::{Error, Timestamp, NANOSECONDS_PER_SECOND};
use crate::native_audio::SampleTime;

const EXCHANGE_DEADLINE: u64 = 2_000_000_000;
const SYNC_DEADLINE: u64 = 1_000_000_000;
const MAXIMUM_COMPLETE_AGE: u64 = 4_000_000_000;
const QUARANTINE_DURATION: u64 = 60_000_000_000;
const ACQUISITION_BUDGET: u64 = 30_000_000_000;

const MAXIMUM_FITTED_RATE_PARTS_PER_BILLION: i64 = 100_000;
const MAXIMUM_PATH_DELAY_NANOSECONDS: u64 = 5_000_000;
const MAXIMUM_TIMESTAMP_UNCERTAINTY_NANOSECONDS: u32 = 10_000;
const OBSERVATION_HISTORY_NANOSECONDS: u64 = 64_000_000_000;
const MAXIMUM_PHASE_SLEW_PARTS_PER_BILLION: i64 = 50_000;
const MAXIMUM_OUTPUT_RATE_PARTS_PER_BILLION: i64 = 200_000;
const MAXIMUM_OBSERVATIONS: usize = 32;
const MINIMUM_OBSERVATIONS: usize = 8;
const MINIMUM_OBSERVATION_SPAN_NANOSECONDS: u64 = 4_000_000_000;
const EXTRAPOLATION_ERROR_PARTS_PER_BILLION: u64 = 1_000;
const MAXIMUM_ESTIMATED_ERROR_NANOSECONDS: u64 = 50_000;
const MAXIMUM_FORWARD_SEQUENCE_DISTANCE: u16 = 32_767;
const FRACTIONAL_NANOSECONDS_PER_NANOSECOND: u128 = 1 << 32;
const FRACTIONAL_NANOSECONDS_PER_SECOND: u128 =
    FRACTIONAL_NANOSECONDS_PER_NANOSECOND * NANOSECONDS_PER_SECOND as u128;

fn integer(value: impl Into<BigInt>) -> BigRational {
    BigRational::from_integer(value.into())
}

fn ratio(numerator: impl Into<BigInt>, denominator: impl Into<BigInt>) -> BigRational {
    BigRational::new(numerator.into(), denominator.into())
}

fn bounded(value: BigRational, magnitude: i64) -> BigRational {
    value.max(integer(-magnitude)).min(integer(magnitude))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub domain: [u8; 16],
    pub local_identity: PortIdentity,
    pub parent_identity: PortIdentity,
    pub interface_index: u32,
    pub provider_incarnation: u64,
    pub receive_calibration_nanoseconds: i64,
    pub transmit_calibration_nanoseconds: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalTimestamp {
    pub nanoseconds: u64,
    pub uncertainty_nanoseconds: u32,
    pub provider_incarnation: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DelayOwner {
    pub generation: u64,
    pub serial: u64,
    pub sequence: u16,
}

#[derive(Clone, Debug, Serialize)]
pub struct PreparedDelayRequest {
    pub owner: DelayOwner,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub generation: u64,
    pub usable: bool,
    pub reason: String,
    pub complete_observations: usize,
    pub parent_identity: PortIdentity,
    pub fitted_rate_parts_per_billion: Option<String>,
    pub estimated_error_nanoseconds: Option<String>,
    pub network_time: Option<Timestamp>,
    pub last_dataset: Option<Dataset>,
    pub last_fit_rejection: Option<FitRejection>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum FitRejection {
    InsufficientObservations,
    NonadvancingObservationTimes,
    RateOutsideLimit { parts_per_billion: String },
    PathDelayOutsideLimit { nanoseconds: String },
    ErrorBudgetExceeded { nanoseconds: String },
}

#[derive(Clone, Debug, Serialize)]
pub struct SampleBasis {
    pub generation: u64,
    pub seconds: u32,
    pub sample_offset: u32,
    pub sample_rate: u32,
    pub estimated_error_nanoseconds: String,
}

#[derive(Clone, Debug)]
pub struct NetworkInstant {
    fractional_nanoseconds: u128,
}

impl NetworkInstant {
    pub fn after_nanoseconds(&self, interval: u64) -> Result<Self, Error> {
        Ok(Self {
            fractional_nanoseconds: self
                .fractional_nanoseconds
                .checked_add(u128::from(interval) * FRACTIONAL_NANOSECONDS_PER_NANOSECOND)
                .ok_or(Error::Arithmetic)?,
        })
    }

    pub fn exceeds_sample(&self, sample: SampleTime, sample_rate: u32) -> Result<bool, Error> {
        let ticks = sample
            .ticks(sample_rate)
            .map_err(|_| Error::InvalidTimestamp)?;
        let current = self
            .fractional_nanoseconds
            .checked_mul(u128::from(sample_rate))
            .ok_or(Error::Arithmetic)?;
        let boundary = u128::from(ticks)
            .checked_mul(FRACTIONAL_NANOSECONDS_PER_SECOND)
            .ok_or(Error::Arithmetic)?;
        Ok(current > boundary)
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ignored,
    AwaitingFollowUp,
    SyncCompleted,
    AwaitingDelayTimestamp,
    AwaitingDelayResponse,
    ObservationAccepted,
    ObservationRejected,
}

#[derive(Clone)]
struct SyncPair {
    sequence: u16,
    origin: Timestamp,
    received: u64,
    uncertainty: u32,
    completed_at: u64,
    epoch: EpochContext,
}

struct AssistedSync {
    sequence: u16,
    received: u64,
    uncertainty: u32,
    deadline: u64,
    epoch: EpochContext,
}

struct PendingDelay {
    owner: DelayOwner,
    synchronization: SyncPair,
    deadline: u64,
    transmitted: Option<(u64, u32)>,
    response: Option<Timestamp>,
}

#[derive(Clone)]
struct Observation {
    origin: u64,
    received: u64,
    transmitted: u64,
    response: u64,
    uncertainty: u64,
}

impl Observation {
    fn local_midpoint(&self) -> BigRational {
        ratio(
            BigInt::from(self.received) + BigInt::from(self.transmitted),
            2,
        )
    }

    fn network_midpoint(&self) -> BigRational {
        ratio(BigInt::from(self.origin) + BigInt::from(self.response), 2)
    }
}

#[derive(Clone)]
struct Fit {
    intercept: BigRational,
    slope: BigRational,
    residual: BigRational,
    variation: BigRational,
    uncertainty: BigRational,
}

impl Fit {
    fn time(&self, local: u64) -> BigRational {
        &self.intercept + &self.slope * integer(local)
    }

    fn rate(&self) -> BigRational {
        (&self.slope - BigRational::one()) * integer(NANOSECONDS_PER_SECOND)
    }

    fn calculate(observations: &VecDeque<Observation>) -> Result<Self, FitRejection> {
        if observations.len() < 2 {
            return Err(FitRejection::InsufficientObservations);
        }

        let count = integer(observations.len());
        let mut sum_local = BigRational::zero();
        let mut sum_network = BigRational::zero();
        let mut sum_product = BigRational::zero();
        let mut sum_square = BigRational::zero();
        for observation in observations {
            let local = observation.local_midpoint();
            let network = observation.network_midpoint();
            sum_product += &local * &network;
            sum_square += &local * &local;
            sum_local += local;
            sum_network += network;
        }

        let denominator = &count * sum_square - &sum_local * &sum_local;
        if denominator <= BigRational::zero() {
            return Err(FitRejection::NonadvancingObservationTimes);
        }

        let slope = (&count * sum_product - &sum_local * &sum_network) / denominator;
        let rate = (&slope - BigRational::one()) * integer(NANOSECONDS_PER_SECOND);

        if rate.abs() > integer(MAXIMUM_FITTED_RATE_PARTS_PER_BILLION) {
            return Err(FitRejection::RateOutsideLimit {
                parts_per_billion: rate.to_string(),
            });
        }

        let intercept = (sum_network - &slope * sum_local) / count;
        let mut residual = BigRational::zero();
        let mut minimum_delay: Option<BigRational> = None;
        let mut maximum_delay = BigRational::zero();
        let mut uncertainty = 0;
        for observation in observations {
            let local = observation.local_midpoint();
            let network = observation.network_midpoint();
            residual = residual.max((network - (&intercept + &slope * local)).abs());
            let delay = (integer(observation.response - observation.origin)
                - &slope * integer(observation.transmitted - observation.received))
                / integer(2);
            if delay < BigRational::zero() || delay > integer(MAXIMUM_PATH_DELAY_NANOSECONDS) {
                return Err(FitRejection::PathDelayOutsideLimit {
                    nanoseconds: delay.to_string(),
                });
            }
            minimum_delay = Some(match minimum_delay {
                Some(current) => current.min(delay.clone()),
                None => delay.clone(),
            });
            maximum_delay = maximum_delay.max(delay);
            uncertainty = uncertainty.max(observation.uncertainty);
        }

        let variation = (maximum_delay
            - minimum_delay.ok_or(FitRejection::InsufficientObservations)?)
            / integer(2);
        Ok(Self {
            intercept,
            slope,
            residual,
            variation,
            uncertainty: integer(uncertainty),
        })
    }
}

#[derive(Clone)]
struct Segment {
    local_anchor: u64,
    network_anchor: u128,
    slope: u64,
}

impl Segment {
    fn quantize(value: BigRational) -> Result<u128, Error> {
        (value * integer(FRACTIONAL_NANOSECONDS_PER_NANOSECOND))
            .round()
            .to_integer()
            .to_u128()
            .ok_or(Error::Arithmetic)
    }

    fn new(local_anchor: u64, network_anchor: u128, slope: BigRational) -> Result<Self, Error> {
        let slope = u64::try_from(Self::quantize(slope)?).map_err(|_| Error::Arithmetic)?;
        if slope == 0 {
            return Err(Error::Arithmetic);
        }

        Ok(Self {
            local_anchor,
            network_anchor,
            slope,
        })
    }

    fn time(&self, local: u64) -> Result<u128, Error> {
        let elapsed = local
            .checked_sub(self.local_anchor)
            .ok_or(Error::Arithmetic)?;
        let interval = u128::from(self.slope) * u128::from(elapsed);
        self.network_anchor
            .checked_add(interval)
            .ok_or(Error::Arithmetic)
    }

    fn invert(&self, network: u128) -> Result<u64, Error> {
        let elapsed = network
            .saturating_sub(self.network_anchor)
            .div_ceil(u128::from(self.slope));

        self.local_anchor
            .checked_add(u64::try_from(elapsed).map_err(|_| Error::Arithmetic)?)
            .ok_or(Error::Arithmetic)
    }
}

pub struct Reading<'a> {
    pub generation: u64,
    local_nanoseconds: u64,
    segment: &'a Segment,
}

pub trait SampleClock {
    fn reading(&self, now: u64) -> Result<Option<Reading<'_>>, Error>;
}

pub struct Publication {
    observed_at: u64,
    usable_through: Option<u64>,
    segment: Option<Segment>,
    fit: Option<Fit>,
    last_midpoint: Option<BigRational>,
    observation: Snapshot,
}

impl SampleClock for Publication {
    fn reading(&self, now: u64) -> Result<Option<Reading<'_>>, Error> {
        if now < self.observed_at {
            return Err(Error::InvalidTimestamp);
        }

        if self.usable_through.is_none_or(|deadline| now > deadline) {
            return Ok(None);
        }

        Ok(self.segment.as_ref().map(|segment| Reading {
            generation: self.observation.generation,
            local_nanoseconds: now,
            segment,
        }))
    }
}

impl Publication {
    pub fn observation(&self) -> (u64, &Snapshot) {
        (self.observed_at, &self.observation)
    }

    pub fn snapshot(&self, now: u64) -> Result<Snapshot, Error> {
        let mut snapshot = self.observation.clone();
        let Some(reading) = self.reading(now)? else {
            if snapshot.usable {
                snapshot.reason = "clock_freshness_or_estimated_error_lost".into();
            }

            snapshot.usable = false;
            snapshot.network_time = None;
            snapshot.estimated_error_nanoseconds = None;
            return Ok(snapshot);
        };

        let time = reading.segment.time(now)?;
        snapshot.network_time = Some(Timestamp::from_total_nanoseconds(
            u64::try_from(time / FRACTIONAL_NANOSECONDS_PER_NANOSECOND)
                .map_err(|_| Error::TimeOutsideEra)?,
        )?);
        let fit = self.fit.as_ref().ok_or(Error::Unavailable)?;
        let last = self.last_midpoint.as_ref().ok_or(Error::Unavailable)?;
        snapshot.estimated_error_nanoseconds =
            Some(estimate_error(fit, time, last, now).to_string());
        Ok(snapshot)
    }
}

fn estimate_error(fit: &Fit, time: u128, last: &BigRational, now: u64) -> BigRational {
    let extrapolation = (integer(now) - last).max(BigRational::zero())
        * ratio(
            EXTRAPOLATION_ERROR_PARTS_PER_BILLION,
            NANOSECONDS_PER_SECOND,
        );
    (ratio(time, FRACTIONAL_NANOSECONDS_PER_NANOSECOND) - fit.time(now)).abs()
        + &fit.residual
        + &fit.uncertainty
        + &fit.variation
        + extrapolation
}

impl Reading<'_> {
    pub fn instant(&self) -> Result<NetworkInstant, Error> {
        Ok(NetworkInstant {
            fractional_nanoseconds: self.segment.time(self.local_nanoseconds)?,
        })
    }

    pub fn sample_tick(&self, sample_rate: u32) -> Result<u64, Error> {
        if sample_rate == 0 {
            return Err(Error::InvalidSampleRate);
        }

        let time = self.segment.time(self.local_nanoseconds)?;
        let ticks = time
            .checked_mul(u128::from(sample_rate))
            .ok_or(Error::Arithmetic)?
            / FRACTIONAL_NANOSECONDS_PER_SECOND;
        u64::try_from(ticks).map_err(|_| Error::TimeOutsideEra)
    }

    pub fn sample_deadline(&self, sample: SampleTime, sample_rate: u32) -> Result<u64, Error> {
        let ticks = sample
            .ticks(sample_rate)
            .map_err(|_| Error::InvalidTimestamp)?;
        let network = u128::from(ticks)
            .checked_mul(FRACTIONAL_NANOSECONDS_PER_SECOND)
            .ok_or(Error::Arithmetic)?
            .div_ceil(u128::from(sample_rate));
        self.segment.invert(network)
    }

    pub fn instant_deadline(&self, instant: &NetworkInstant) -> Result<u64, Error> {
        self.segment.invert(instant.fractional_nanoseconds)
    }
}

pub struct Follower {
    configuration: Configuration,
    generation: u64,
    request_serial: u64,
    request_sequence: u16,
    quarantine: VecDeque<(u16, u64)>,
    last_evaluation: u64,
    acquisition_started: u64,
    last_sequence: Option<u16>,
    last_origin: Option<Timestamp>,
    epoch: Option<EpochContext>,
    assisted: Option<AssistedSync>,
    latest_sync: Option<SyncPair>,
    pending: Option<PendingDelay>,
    observations: VecDeque<Observation>,
    fit: Option<Fit>,
    output: Option<Segment>,
    usable_through: Option<u64>,
    reason: &'static str,
    blocked: bool,
    last_dataset: Option<Dataset>,
    last_fit_rejection: Option<FitRejection>,
}

impl SampleClock for Follower {
    fn reading(&self, now: u64) -> Result<Option<Reading<'_>>, Error> {
        if now < self.last_evaluation {
            return Err(Error::InvalidTimestamp);
        }

        if !self.readiness(now) {
            return Ok(None);
        }

        Ok(self.output.as_ref().map(|segment| Reading {
            generation: self.generation,
            local_nanoseconds: now,
            segment,
        }))
    }
}

impl Follower {
    pub fn new(configuration: Configuration, now: u64) -> Result<Self, Error> {
        configuration.local_identity.validate()?;
        configuration.parent_identity.validate()?;
        version_one::multicast_group(configuration.domain)?;
        if configuration.local_identity == configuration.parent_identity
            || configuration.interface_index == 0
            || configuration.provider_incarnation == 0
        {
            return Err(Error::InvalidConfiguration);
        }

        Ok(Self {
            configuration,
            generation: 1,
            request_serial: 0,
            request_sequence: 0,
            quarantine: VecDeque::new(),
            last_evaluation: now,
            acquisition_started: now,
            last_sequence: None,
            last_origin: None,
            epoch: None,
            assisted: None,
            latest_sync: None,
            pending: None,
            observations: VecDeque::new(),
            fit: None,
            output: None,
            usable_through: None,
            reason: "acquiring_complete_observations",
            blocked: false,
            last_dataset: None,
            last_fit_rejection: None,
        })
    }

    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    pub fn next_deadline(&self) -> Option<u64> {
        let readiness = if self.blocked {
            None
        } else if let Some(deadline) = self.usable_through {
            deadline.checked_add(1)
        } else {
            self.acquisition_started.checked_add(ACQUISITION_BUDGET)
        };

        [
            readiness,
            self.assisted.as_ref().map(|pending| pending.deadline),
            self.pending.as_ref().map(|pending| pending.deadline),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    pub fn publication(&mut self, now: u64) -> Result<Publication, Error> {
        let observation = self.snapshot(now)?;
        Ok(Publication {
            observed_at: now,
            usable_through: self.usable_through,
            segment: self.output.clone(),
            fit: self.fit.clone(),
            last_midpoint: self.observations.back().map(Observation::local_midpoint),
            observation,
        })
    }

    fn calibrate(&self, timestamp: LocalTimestamp, receiving: bool) -> Result<u64, Error> {
        if timestamp.provider_incarnation != self.configuration.provider_incarnation
            || timestamp.uncertainty_nanoseconds > MAXIMUM_TIMESTAMP_UNCERTAINTY_NANOSECONDS
        {
            return Err(Error::InvalidConfiguration);
        }

        let calibration = if receiving {
            -i128::from(self.configuration.receive_calibration_nanoseconds)
        } else {
            i128::from(self.configuration.transmit_calibration_nanoseconds)
        };
        u64::try_from(i128::from(timestamp.nanoseconds) + calibration)
            .map_err(|_| Error::Arithmetic)
    }

    fn retire_delay(&mut self, now: u64) -> Result<(), Error> {
        if let Some(pending) = self.pending.take() {
            self.quarantine.push_back((
                pending.owner.sequence,
                now.checked_add(QUARANTINE_DURATION)
                    .ok_or(Error::Arithmetic)?,
            ));
        }

        Ok(())
    }

    fn invalidate(&mut self, now: u64, reason: &'static str, blocked: bool) -> Result<(), Error> {
        self.retire_delay(now)?;
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(Error::GenerationExhausted)?;
        self.assisted = None;
        self.latest_sync = None;
        self.observations.clear();
        self.fit = None;
        self.output = None;
        self.usable_through = None;
        self.last_sequence = None;
        self.last_origin = None;
        self.last_dataset = None;
        self.last_fit_rejection = None;
        self.acquisition_started = now;
        self.reason = reason;
        self.blocked = blocked;
        Ok(())
    }

    pub fn provider_lost(&mut self, now: u64) -> Result<(), Error> {
        self.invalidate(now, "timestamp_provider_unavailable", true)
    }

    pub fn evaluate(&mut self, now: u64) -> Result<(), Error> {
        if now < self.last_evaluation {
            self.invalidate(now, "monotonic_provider_reset", true)?;
            return Err(Error::InvalidConfiguration);
        }

        self.last_evaluation = now;
        while self
            .quarantine
            .front()
            .is_some_and(|(_, deadline)| now >= *deadline)
        {
            self.quarantine.pop_front();
        }

        if self
            .assisted
            .as_ref()
            .is_some_and(|pending| now >= pending.deadline)
        {
            self.assisted = None;
        }

        if self
            .pending
            .as_ref()
            .is_some_and(|pending| now >= pending.deadline)
        {
            self.retire_delay(now)?;
        }

        if self.blocked {
            return Ok(());
        }

        if self.output.is_some() {
            if !self.readiness(now) {
                self.invalidate(now, "clock_freshness_or_estimated_error_lost", false)?;
            }
        } else if now.saturating_sub(self.acquisition_started) >= ACQUISITION_BUDGET {
            self.invalidate(now, "clock_acquisition_deadline", false)?;
        }

        Ok(())
    }

    fn sequence_forward(&self, incoming: u16) -> bool {
        self.last_sequence.is_none_or(|previous| {
            matches!(
                incoming.wrapping_sub(previous),
                1..=MAXIMUM_FORWARD_SEQUENCE_DISTANCE
            )
        })
    }

    fn complete_sync(&mut self, pair: SyncPair, now: u64) -> Result<Outcome, Error> {
        if !self.sequence_forward(pair.sequence) {
            return Ok(Outcome::Ignored);
        }

        if self
            .last_origin
            .is_some_and(|previous| pair.origin <= previous)
        {
            self.invalidate(now, "relative_origin_reversed_or_repeated", true)?;
            return Ok(Outcome::Ignored);
        }

        if self.epoch.is_some_and(|previous| previous != pair.epoch) {
            self.invalidate(now, "relative_epoch_context_changed", true)?;
            return Ok(Outcome::Ignored);
        }

        self.epoch = Some(pair.epoch);
        self.last_sequence = Some(pair.sequence);
        self.last_origin = Some(pair.origin);
        self.latest_sync = Some(pair);
        self.assisted = None;
        Ok(Outcome::SyncCompleted)
    }

    pub fn receive(
        &mut self,
        packet: &Packet,
        interface_index: u32,
        arrival: LocalTimestamp,
        now: u64,
    ) -> Result<Outcome, Error> {
        self.evaluate(now)?;
        if self.blocked
            || interface_index != self.configuration.interface_index
            || packet.domain != self.configuration.domain
            || packet.source != self.configuration.parent_identity
        {
            return Ok(Outcome::Ignored);
        }

        let received = match self.calibrate(arrival, true) {
            Ok(value) => value,
            Err(error) => {
                self.invalidate(now, "timestamp_provider_invalid", true)?;
                return Err(error);
            }
        };

        if arrival.nanoseconds > now {
            return Err(Error::InvalidTimestamp);
        }

        match &packet.body {
            Body::Sync {
                origin,
                epoch,
                dataset,
            } => {
                if packet.flags & version_one::UNSYNCHRONIZED_FLAG != 0
                    || !self.sequence_forward(packet.sequence)
                    || dataset.estimated_drift == 0x7fff_ffff
                {
                    return Ok(Outcome::Ignored);
                }

                if let Some(pending) = &self.assisted {
                    if !matches!(
                        packet.sequence.wrapping_sub(pending.sequence),
                        1..=MAXIMUM_FORWARD_SEQUENCE_DISTANCE
                    ) {
                        return Ok(Outcome::Ignored);
                    }
                }

                self.last_dataset = Some(dataset.clone());
                if packet.flags & version_one::ASSISTED_FLAG != 0 {
                    self.assisted = Some(AssistedSync {
                        sequence: packet.sequence,
                        received,
                        uncertainty: arrival.uncertainty_nanoseconds,
                        deadline: now.checked_add(SYNC_DEADLINE).ok_or(Error::Arithmetic)?,
                        epoch: *epoch,
                    });
                    Ok(Outcome::AwaitingFollowUp)
                } else {
                    self.complete_sync(
                        SyncPair {
                            sequence: packet.sequence,
                            origin: *origin,
                            received,
                            uncertainty: arrival.uncertainty_nanoseconds,
                            completed_at: now,
                            epoch: *epoch,
                        },
                        now,
                    )
                }
            }
            Body::FollowUp {
                associated_sequence,
                origin,
            } => {
                if self
                    .assisted
                    .as_ref()
                    .is_none_or(|pending| pending.sequence != *associated_sequence)
                {
                    return Ok(Outcome::Ignored);
                }

                let pending = self.assisted.take().ok_or(Error::StaleOwner)?;
                self.complete_sync(
                    SyncPair {
                        sequence: pending.sequence,
                        origin: *origin,
                        received: pending.received,
                        uncertainty: pending.uncertainty,
                        completed_at: now,
                        epoch: pending.epoch,
                    },
                    now,
                )
            }
            Body::DelayResponse {
                received,
                requester,
                request_sequence,
            } => {
                if *requester != self.configuration.local_identity {
                    return Ok(Outcome::Ignored);
                }

                let Some(pending) = self.pending.as_mut() else {
                    return Ok(Outcome::Ignored);
                };

                if *request_sequence != pending.owner.sequence || pending.response.is_some() {
                    return Ok(Outcome::Ignored);
                }
                pending.response = Some(*received);
                self.finish_observation(now)
            }
            Body::DelayRequest { .. } => Ok(Outcome::Ignored),
        }
    }

    pub fn prepare_delay_request(
        &mut self,
        now: u64,
    ) -> Result<Option<PreparedDelayRequest>, Error> {
        self.evaluate(now)?;
        if self.blocked || self.pending.is_some() {
            return Ok(None);
        }

        let Some(synchronization) = self.latest_sync.take() else {
            return Ok(None);
        };

        if now.saturating_sub(synchronization.completed_at) > SYNC_DEADLINE {
            return Ok(None);
        }

        let pre_send_time = if self.output.is_some() {
            self.network_timestamp(now)?
        } else {
            let elapsed = now
                .checked_sub(synchronization.received)
                .ok_or(Error::Arithmetic)?;
            let estimated = synchronization
                .origin
                .total_nanoseconds()?
                .checked_add(elapsed)
                .ok_or(Error::Arithmetic)?;
            Timestamp::from_total_nanoseconds(estimated)?
        };

        let sequence = self.request_sequence.wrapping_add(1);
        if self
            .quarantine
            .iter()
            .any(|(retired, _)| *retired == sequence)
        {
            self.latest_sync = Some(synchronization);
            return Ok(None);
        }

        self.request_serial = self
            .request_serial
            .checked_add(1)
            .ok_or(Error::GenerationExhausted)?;
        let owner = DelayOwner {
            generation: self.generation,
            serial: self.request_serial,
            sequence,
        };

        let bytes = version_one::encode(
            self.configuration.domain,
            self.configuration.local_identity,
            sequence,
            0,
            &Body::DelayRequest {
                origin: pre_send_time,
                epoch: synchronization.epoch,
            },
        )?;
        self.request_sequence = sequence;
        self.pending = Some(PendingDelay {
            owner,
            synchronization,
            deadline: now
                .checked_add(EXCHANGE_DEADLINE)
                .ok_or(Error::Arithmetic)?,
            transmitted: None,
            response: None,
        });
        Ok(Some(PreparedDelayRequest { owner, bytes }))
    }

    pub fn request_transmitted(
        &mut self,
        owner: DelayOwner,
        timestamp: LocalTimestamp,
        now: u64,
    ) -> Result<Outcome, Error> {
        self.evaluate(now)?;
        let transmitted = match self.calibrate(timestamp, false) {
            Ok(value) => value,
            Err(error) => {
                self.invalidate(now, "timestamp_provider_invalid", true)?;
                return Err(error);
            }
        };

        if timestamp.nanoseconds > now {
            self.invalidate(now, "timestamp_provider_invalid", true)?;
            return Err(Error::InvalidTimestamp);
        }

        let pending = self.pending.as_mut().ok_or(Error::StaleOwner)?;
        if pending.owner != owner || pending.transmitted.is_some() {
            return Err(Error::StaleOwner);
        }
        pending.transmitted = Some((transmitted, timestamp.uncertainty_nanoseconds));
        self.finish_observation(now)
    }

    pub fn request_failed(&mut self, owner: DelayOwner, now: u64) -> Result<(), Error> {
        if !self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.owner == owner)
        {
            return Err(Error::StaleOwner);
        }

        self.retire_delay(now)
    }

    fn finish_observation(&mut self, now: u64) -> Result<Outcome, Error> {
        let pending = self.pending.as_ref().ok_or(Error::StaleOwner)?;
        let Some((transmitted, transmit_uncertainty)) = pending.transmitted else {
            return Ok(Outcome::AwaitingDelayTimestamp);
        };

        let Some(response) = pending.response else {
            return Ok(Outcome::AwaitingDelayResponse);
        };

        let synchronization = pending.synchronization.clone();
        self.retire_delay(now)?;
        let origin = synchronization.origin.total_nanoseconds()?;
        let response = response.total_nanoseconds()?;
        if transmitted < synchronization.received
            || transmitted - synchronization.received > EXCHANGE_DEADLINE
            || response < origin
        {
            return Ok(Outcome::ObservationRejected);
        }

        let observation = Observation {
            origin,
            received: synchronization.received,
            transmitted,
            response,
            uncertainty: u64::from(synchronization.uncertainty) + u64::from(transmit_uncertainty),
        };

        if self.observations.back().is_some_and(|previous| {
            previous.local_midpoint() >= observation.local_midpoint() || previous.origin >= origin
        }) {
            return Ok(Outcome::ObservationRejected);
        }

        let mut candidate = self.observations.clone();
        let cutoff = integer(now.saturating_sub(OBSERVATION_HISTORY_NANOSECONDS));
        while candidate
            .front()
            .is_some_and(|old| old.local_midpoint() < cutoff)
        {
            candidate.pop_front();
        }
        candidate.push_back(observation);
        while candidate.len() > MAXIMUM_OBSERVATIONS {
            candidate.pop_front();
        }

        if candidate.len() == 1 {
            self.observations = candidate;
            self.reason = "acquiring_complete_observations";
            return Ok(Outcome::ObservationAccepted);
        }

        let fit = match Fit::calculate(&candidate) {
            Ok(value) => value,
            Err(rejection) => {
                self.last_fit_rejection = Some(rejection);
                return Ok(Outcome::ObservationRejected);
            }
        };

        if let Some(output) = &self.output {
            let current = output.time(now)?;
            let last = candidate
                .back()
                .ok_or(Error::Unavailable)?
                .local_midpoint();
            let error = estimate_error(&fit, current, &last, now);

            if error > integer(MAXIMUM_ESTIMATED_ERROR_NANOSECONDS) {
                self.last_fit_rejection = Some(FitRejection::ErrorBudgetExceeded {
                    nanoseconds: error.to_string(),
                });
                return Ok(Outcome::ObservationRejected);
            }
        }

        self.last_fit_rejection = None;
        self.observations = candidate;
        self.fit = Some(fit.clone());
        if self.output.is_some() {
            if !self.refresh_readiness(now)? {
                self.invalidate(now, "updated_fit_exceeds_clock_limits", false)?;
                return Ok(Outcome::ObservationRejected);
            }

            let current = self.output.as_ref().ok_or(Error::Unavailable)?.time(now)?;
            let phase = fit.time(now) - ratio(current, FRACTIONAL_NANOSECONDS_PER_NANOSECOND);
            let rate = bounded(
                fit.rate() + bounded(phase, MAXIMUM_PHASE_SLEW_PARTS_PER_BILLION),
                MAXIMUM_OUTPUT_RATE_PARTS_PER_BILLION,
            );
            self.output = Some(Segment::new(
                now,
                current,
                BigRational::one() + rate / integer(NANOSECONDS_PER_SECOND),
            )?);
            if !self.refresh_readiness(now)? {
                self.invalidate(now, "updated_output_exceeds_clock_limits", false)?;
                return Ok(Outcome::ObservationRejected);
            }
        } else if self.history_ready(now)? {
            self.output = Some(Segment::new(
                now,
                Segment::quantize(fit.time(now))?,
                fit.slope,
            )?);
            if !self.refresh_readiness(now)? {
                self.output = None;
                self.reason = "estimated_clock_error_exceeds_limit";
            } else {
                self.reason = "usable_estimated_clock";
            }
        }

        Ok(Outcome::ObservationAccepted)
    }

    fn history_ready(&self, now: u64) -> Result<bool, Error> {
        if self.observations.len() < MINIMUM_OBSERVATIONS || self.fit.is_none() {
            return Ok(false);
        }

        let first = self
            .observations
            .front()
            .ok_or(Error::Unavailable)?
            .local_midpoint();
        let last = self
            .observations
            .back()
            .ok_or(Error::Unavailable)?
            .local_midpoint();
        let age = integer(now) - &last;
        Ok(
            last - first >= integer(MINIMUM_OBSERVATION_SPAN_NANOSECONDS)
                && age >= BigRational::zero()
                && age <= integer(MAXIMUM_COMPLETE_AGE),
        )
    }

    fn estimated_error(&self, now: u64) -> Result<BigRational, Error> {
        let fit = self.fit.as_ref().ok_or(Error::Unavailable)?;
        let current = self.output.as_ref().ok_or(Error::Unavailable)?.time(now)?;
        let last = self
            .observations
            .back()
            .ok_or(Error::Unavailable)?
            .local_midpoint();
        Ok(estimate_error(fit, current, &last, now))
    }

    fn readiness(&self, now: u64) -> bool {
        !self.blocked && self.usable_through.is_some_and(|deadline| now <= deadline)
    }

    fn refresh_readiness(&mut self, now: u64) -> Result<bool, Error> {
        self.usable_through = None;
        if self.blocked || !self.history_ready(now)? || self.output.is_none() {
            return Ok(false);
        }

        let segment = self.output.as_ref().ok_or(Error::Unavailable)?;
        let time = segment.time(now)?;
        let maximum = u128::from(u32::MAX) * FRACTIONAL_NANOSECONDS_PER_SECOND
            + FRACTIONAL_NANOSECONDS_PER_SECOND
            - FRACTIONAL_NANOSECONDS_PER_NANOSECOND;

        if time > maximum
            || self.estimated_error(now)? > integer(MAXIMUM_ESTIMATED_ERROR_NANOSECONDS)
        {
            return Ok(false);
        }

        let last = self
            .observations
            .back()
            .ok_or(Error::Unavailable)?
            .local_midpoint();
        let fresh_through = (last.clone() + integer(MAXIMUM_COMPLETE_AGE))
            .floor()
            .to_integer()
            .to_u64()
            .ok_or(Error::Arithmetic)?;
        let era_remaining = (maximum - time) / u128::from(segment.slope);
        let mut remaining = fresh_through.checked_sub(now).ok_or(Error::Arithmetic)?;
        remaining = u64::try_from(era_remaining.min(u128::from(remaining)))
            .map_err(|_| Error::Arithmetic)?;

        let fit = self.fit.as_ref().ok_or(Error::Unavailable)?;
        let phase = ratio(time, FRACTIONAL_NANOSECONDS_PER_NANOSECOND) - fit.time(now);
        let drift = ratio(segment.slope, FRACTIONAL_NANOSECONDS_PER_NANOSECOND) - &fit.slope;
        let extrapolation_rate = ratio(
            EXTRAPOLATION_ERROR_PARTS_PER_BILLION,
            NANOSECONDS_PER_SECOND,
        );
        let fixed_error = &fit.residual
            + &fit.uncertainty
            + &fit.variation
            + (integer(now) - last) * &extrapolation_rate;

        for direction in [-1, 1] {
            let intercept = &fixed_error + &phase * integer(direction);
            let slope = &extrapolation_rate + &drift * integer(direction);

            if slope > BigRational::zero() {
                let allowed = (integer(MAXIMUM_ESTIMATED_ERROR_NANOSECONDS) - intercept) / slope;
                remaining = allowed
                    .min(integer(remaining))
                    .floor()
                    .to_integer()
                    .to_u64()
                    .ok_or(Error::Arithmetic)?;
            }
        }

        self.usable_through = Some(now.checked_add(remaining).ok_or(Error::Arithmetic)?);
        Ok(true)
    }

    pub fn snapshot(&mut self, now: u64) -> Result<Snapshot, Error> {
        self.evaluate(now)?;
        let usable = self.output.is_some() && !self.blocked;
        let network_time = if usable {
            Some(self.network_timestamp(now)?)
        } else {
            None
        };

        Ok(Snapshot {
            generation: self.generation,
            usable,
            reason: self.reason.to_owned(),
            complete_observations: self.observations.len(),
            parent_identity: self.configuration.parent_identity,
            fitted_rate_parts_per_billion: self.fit.as_ref().map(|fit| fit.rate().to_string()),
            estimated_error_nanoseconds: if usable {
                Some(self.estimated_error(now)?.to_string())
            } else {
                None
            },
            network_time,
            last_dataset: self.last_dataset.clone(),
            last_fit_rejection: self.last_fit_rejection.clone(),
        })
    }

    pub fn reading(&mut self, now: u64) -> Result<Option<Reading<'_>>, Error> {
        self.evaluate(now)?;

        Ok(self.output.as_ref().map(|segment| Reading {
            generation: self.generation,
            local_nanoseconds: now,
            segment,
        }))
    }

    fn network_timestamp(&self, now: u64) -> Result<Timestamp, Error> {
        let time = self.output.as_ref().ok_or(Error::Unavailable)?.time(now)?;
        Timestamp::from_total_nanoseconds(
            u64::try_from(time / FRACTIONAL_NANOSECONDS_PER_NANOSECOND)
                .map_err(|_| Error::TimeOutsideEra)?,
        )
    }

    pub fn sample_basis(&mut self, now: u64, sample_rate: u32) -> Result<SampleBasis, Error> {
        let ticks = self
            .reading(now)?
            .ok_or(Error::Unavailable)?
            .sample_tick(sample_rate)?;
        let seconds =
            u32::try_from(ticks / u64::from(sample_rate)).map_err(|_| Error::TimeOutsideEra)?;
        Ok(SampleBasis {
            generation: self.generation,
            seconds,
            sample_offset: (ticks % u64::from(sample_rate)) as u32,
            sample_rate,
            estimated_error_nanoseconds: self.estimated_error(now)?.to_string(),
        })
    }

    pub fn sample_deadline(
        &mut self,
        sample: SampleTime,
        sample_rate: u32,
        now: u64,
    ) -> Result<u64, Error> {
        self.reading(now)?
            .ok_or(Error::Unavailable)?
            .sample_deadline(sample, sample_rate)
    }

    pub fn instant(&mut self, now: u64) -> Result<NetworkInstant, Error> {
        self.reading(now)?.ok_or(Error::Unavailable)?.instant()
    }

    pub fn instant_deadline(&mut self, instant: &NetworkInstant, now: u64) -> Result<u64, Error> {
        self.reading(now)?
            .ok_or(Error::Unavailable)?
            .instant_deadline(instant)
    }
}
