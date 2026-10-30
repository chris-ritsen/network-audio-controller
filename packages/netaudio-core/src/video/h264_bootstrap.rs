use std::net::SocketAddrV4;

use anyhow::{anyhow, ensure, Result};

use super::h264::{self, AccessUnit, Receiver};
use super::h264_parameters::{Cadence, ColorInterpretation, Parameters, SequenceParameters};
use super::rtp::{Identity, StructuralPacket};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClockIdentity {
    pub domain: String,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    pub owner: String,
    pub generation: u64,
    pub clock: ClockIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceAuthority {
    OwnReservedMedia,
    QualifiedConfiguration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreshDestination {
    pub reservation: String,
    pub nonreuse_basis: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Authority {
    pub context: Context,
    pub accepted_allocation: String,
    pub source: SocketAddrV4,
    pub source_authority: SourceAuthority,
    pub destination: SocketAddrV4,
    pub fresh_destination: FreshDestination,
    pub interface: u32,
    pub valid_from_nanoseconds: i64,
    pub expires_nanoseconds: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Packetization {
    SingleNal,
    NonInterleaved,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    pub profile_identifier: u8,
    pub level_identifier: u8,
    pub packetization: Packetization,
    pub maximum_width: u32,
    pub maximum_height: u32,
    pub maximum_pixels: u64,
    pub maximum_reference_frames: u32,
    pub cadence: Cadence,
    pub color: ColorInterpretation,
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub maximum_bytes: usize,
    pub maximum_packets: usize,
    pub maximum_access_units: usize,
    pub maximum_age_nanoseconds: i64,
    pub access_unit: h264::Limits,
}

pub struct Observation<'a> {
    pub context: &'a Context,
    pub source: SocketAddrV4,
    pub destination: SocketAddrV4,
    pub interface: u32,
    pub arrival_nanoseconds: i64,
}

pub struct Candidate {
    authority: Authority,
    policy: Policy,
    limits: Limits,
    identity: Identity,
    parameters: Parameters,
    recovery: AccessUnit,
    started_nanoseconds: i64,
    expires_nanoseconds: i64,
}

impl Candidate {
    pub fn authority(&self) -> &Authority {
        &self.authority
    }
    pub fn policy(&self) -> Policy {
        self.policy
    }
    pub fn limits(&self) -> Limits {
        self.limits
    }
    pub fn identity(&self) -> Identity {
        self.identity
    }
    pub fn parameters(&self) -> &Parameters {
        &self.parameters
    }
    pub fn recovery(&self) -> &AccessUnit {
        &self.recovery
    }
    pub fn started_nanoseconds(&self) -> i64 {
        self.started_nanoseconds
    }
    pub fn expires_nanoseconds(&self) -> i64 {
        self.expires_nanoseconds
    }

    pub fn validate_current(&self, context: &Context, now_nanoseconds: i64) -> Result<()> {
        ensure!(
            context == &self.authority.context,
            "bootstrap candidate context changed"
        );
        ensure!(
            now_nanoseconds >= self.recovery.arrival_nanoseconds
                && now_nanoseconds < self.expires_nanoseconds,
            "bootstrap candidate expired or its clock moved backwards"
        );
        Ok(())
    }
}

pub enum Progress {
    Ignored,
    Pending,
    ReadyForProvider(Box<Candidate>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    AwaitingPacket,
    Quarantine,
    ReadyForProvider,
    Invalidated,
}

struct Quarantine {
    receiver: Receiver,
    identity: Option<Identity>,
    started_nanoseconds: Option<i64>,
    bytes: usize,
    packets: usize,
    access_units: usize,
    validated_nals: usize,
    sequence: Option<SequenceParameters>,
    parameters: Option<Parameters>,
}

pub struct Bootstrap {
    authority: Authority,
    policy: Policy,
    limits: Limits,
    last_observation_nanoseconds: i64,
    state: State,
    quarantine: Option<Quarantine>,
}

impl Bootstrap {
    pub fn new(
        authority: Authority,
        policy: Policy,
        limits: Limits,
        now_nanoseconds: i64,
    ) -> Result<Self> {
        ensure!(
            !authority.context.owner.trim().is_empty()
                && !authority.context.clock.domain.trim().is_empty()
                && !authority.accepted_allocation.trim().is_empty()
                && !authority.fresh_destination.reservation.trim().is_empty()
                && !authority.fresh_destination.nonreuse_basis.trim().is_empty(),
            "bootstrap requires explicit owner, clock, allocation and fresh destination authority"
        );
        for endpoint in [authority.source, authority.destination] {
            ensure!(
                endpoint.port() != 0
                    && !endpoint.ip().is_unspecified()
                    && !endpoint.ip().is_multicast()
                    && !endpoint.ip().is_broadcast(),
                "bootstrap requires exact unicast IPv4 media endpoints"
            );
        }
        ensure!(
            authority.interface != 0,
            "bootstrap requires an actual receiving interface"
        );
        ensure!(
            authority.valid_from_nanoseconds <= now_nanoseconds
                && now_nanoseconds < authority.expires_nanoseconds,
            "bootstrap authority is not current"
        );
        ensure!(
            policy.maximum_width > 0
                && policy.maximum_height > 0
                && policy.maximum_pixels > 0
                && policy.cadence.numerator > 0
                && policy.cadence.denominator > 0,
            "bootstrap requires finite geometry bounds and explicit positive cadence"
        );
        ensure!(
            matches!(policy.profile_identifier, 66 | 77 | 100),
            "bootstrap supports only baseline, main or high H.264 profiles"
        );
        ensure!(
            matches!(
                policy.level_identifier,
                10 | 11
                    | 12
                    | 13
                    | 20
                    | 21
                    | 22
                    | 30
                    | 31
                    | 32
                    | 40
                    | 41
                    | 42
                    | 50
                    | 51
                    | 52
                    | 60
                    | 61
                    | 62
            ),
            "bootstrap requires a supported explicit H.264 level"
        );
        ensure!(
            limits.maximum_bytes > 0
                && limits.maximum_packets > 0
                && limits.maximum_access_units > 0
                && limits.maximum_age_nanoseconds > 0,
            "bootstrap quarantine limits must be positive and finite"
        );
        let receiver = Receiver::bootstrap(limits.access_unit)?;
        Ok(Self {
            authority,
            policy,
            limits,
            last_observation_nanoseconds: now_nanoseconds,
            state: State::AwaitingPacket,
            quarantine: Some(Quarantine {
                receiver,
                identity: None,
                started_nanoseconds: None,
                bytes: 0,
                packets: 0,
                access_units: 0,
                validated_nals: 0,
                sequence: None,
                parameters: None,
            }),
        })
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn expiration_nanoseconds(&self) -> Option<i64> {
        let quarantine = self.quarantine.as_ref()?;
        let mut expiration = self.authority.expires_nanoseconds;
        if let Some(started) = quarantine.started_nanoseconds {
            expiration = expiration.min(started.checked_add(self.limits.maximum_age_nanoseconds)?);
        }
        if let Some(assembly) = quarantine.receiver.expiration_nanoseconds() {
            expiration = expiration.min(assembly);
        }
        Some(expiration)
    }

    pub fn invalidate(&mut self) {
        self.quarantine = None;
        self.state = State::Invalidated;
    }

    pub fn observe_context(&mut self, context: &Context, now_nanoseconds: i64) -> Result<()> {
        let result = self.check_current(context, now_nanoseconds);
        if result.is_err() && self.quarantine.is_some() {
            self.invalidate();
        }
        result
    }

    pub fn push(&mut self, bytes: &[u8], observation: Observation<'_>) -> Result<Progress> {
        let result = self.append(bytes, observation);
        if result.is_err() && self.quarantine.is_some() {
            self.invalidate();
        }
        result
    }

    fn check_current(&mut self, context: &Context, now_nanoseconds: i64) -> Result<()> {
        ensure!(
            self.quarantine.is_some(),
            "bootstrap attempt has already ended"
        );
        ensure!(
            context == &self.authority.context,
            "bootstrap owner or clock generation changed"
        );
        ensure!(
            now_nanoseconds >= self.last_observation_nanoseconds,
            "bootstrap clock moved backwards"
        );
        ensure!(
            self.expiration_nanoseconds()
                .is_some_and(|expiration| now_nanoseconds < expiration),
            "bootstrap authority or quarantine expired"
        );
        self.last_observation_nanoseconds = now_nanoseconds;
        Ok(())
    }

    fn append(&mut self, bytes: &[u8], observation: Observation<'_>) -> Result<Progress> {
        self.check_current(observation.context, observation.arrival_nanoseconds)?;
        if observation.source != self.authority.source
            || observation.destination != self.authority.destination
            || observation.interface != self.authority.interface
        {
            return Ok(Progress::Ignored);
        }
        let quarantine = self.quarantine.as_mut().unwrap();
        let started = *quarantine
            .started_nanoseconds
            .get_or_insert(observation.arrival_nanoseconds);
        let expires = started
            .checked_add(self.limits.maximum_age_nanoseconds)
            .ok_or_else(|| anyhow!("bootstrap quarantine deadline overflow"))?
            .min(self.authority.expires_nanoseconds);
        self.state = State::Quarantine;
        quarantine.bytes = quarantine
            .bytes
            .checked_add(bytes.len())
            .filter(|total| *total <= self.limits.maximum_bytes)
            .ok_or_else(|| anyhow!("bootstrap quarantine byte bound exceeded"))?;
        quarantine.packets = quarantine
            .packets
            .checked_add(1)
            .filter(|total| *total <= self.limits.maximum_packets)
            .ok_or_else(|| anyhow!("bootstrap quarantine packet bound exceeded"))?;
        let packet = StructuralPacket::parse(bytes)?;
        ensure!(
            quarantine
                .identity
                .is_none_or(|expected| expected == packet.identity),
            "bootstrap RTP payload type or synchronization source changed"
        );
        quarantine.identity = Some(packet.identity);
        if self.policy.packetization == Packetization::SingleNal {
            ensure!(
                (1..=23).contains(&(packet.packet.payload[0] & h264::NAL_TYPE_MASK)),
                "bootstrap packetization differs from the supplied policy"
            );
        }
        let completed = quarantine
            .receiver
            .push(packet.packet, observation.arrival_nanoseconds)?;
        if let Some(access_unit) = &completed {
            for range in access_unit
                .nal_ranges
                .iter()
                .skip(quarantine.validated_nals)
            {
                acquire_parameters(
                    &mut quarantine.sequence,
                    &mut quarantine.parameters,
                    &access_unit.bytes[range.clone()],
                    self.policy,
                )?;
            }
            quarantine.validated_nals = 0;
        } else {
            for unit in quarantine
                .receiver
                .pending_nals()
                .skip(quarantine.validated_nals)
            {
                acquire_parameters(
                    &mut quarantine.sequence,
                    &mut quarantine.parameters,
                    unit,
                    self.policy,
                )?;
                quarantine.validated_nals += 1;
            }
            return Ok(Progress::Pending);
        }
        let access_unit = completed.unwrap();
        quarantine.access_units = quarantine
            .access_units
            .checked_add(1)
            .filter(|total| *total <= self.limits.maximum_access_units)
            .ok_or_else(|| anyhow!("bootstrap quarantine access-unit bound exceeded"))?;
        let has_slice = access_unit.nal_ranges.iter().any(|range| {
            matches!(
                access_unit.bytes[range.start] & h264::NAL_TYPE_MASK,
                h264::SLICE | h264::RANDOM_ACCESS_SLICE
            )
        });
        if !has_slice {
            return Ok(Progress::Pending);
        }
        let parameters = quarantine.parameters.as_ref().unwrap();
        if !access_unit.random_access {
            parameters.validate(&access_unit)?;
            return Ok(Progress::Pending);
        }
        parameters.validate_recovery(&access_unit)?;
        let mut quarantine = self.quarantine.take().unwrap();
        self.state = State::ReadyForProvider;
        Ok(Progress::ReadyForProvider(Box::new(Candidate {
            authority: self.authority.clone(),
            policy: self.policy,
            limits: self.limits,
            identity: quarantine.identity.unwrap(),
            parameters: quarantine.parameters.take().unwrap(),
            recovery: access_unit,
            started_nanoseconds: started,
            expires_nanoseconds: expires,
        })))
    }
}

fn validate_sequence(sequence: &SequenceParameters, policy: Policy) -> Result<()> {
    let format = sequence.format();
    let interpretation = sequence.interpretation();
    let (coded_width, coded_height) = sequence.coded_dimensions();
    ensure!(
        u64::from(coded_width) <= u64::from(policy.maximum_width).div_ceil(16) * 16
            && u64::from(coded_height) <= u64::from(policy.maximum_height).div_ceil(16) * 16,
        "bootstrap coded geometry exceeds the supplied policy"
    );
    ensure!(
        format.profile_identifier == policy.profile_identifier
            && format.level_identifier == policy.level_identifier,
        "bootstrap SPS profile or level differs from the supplied policy"
    );
    ensure!(
        format.width > 0
            && format.height > 0
            && format.width <= policy.maximum_width
            && format.height <= policy.maximum_height
            && u64::from(format.width) * u64::from(format.height) <= policy.maximum_pixels,
        "bootstrap SPS geometry exceeds the supplied policy"
    );
    if let Some(timing) = interpretation.timing {
        ensure!(
            timing.units_in_tick > 0 && timing.time_scale > 0,
            "bootstrap SPS timing must be positive"
        );
        if timing.fixed_frame_rate {
            ensure!(
                u128::from(timing.time_scale) * u128::from(policy.cadence.denominator)
                    == u128::from(policy.cadence.numerator) * u128::from(timing.units_in_tick) * 2,
                "bootstrap SPS timing contradicts explicit cadence"
            );
        }
    }
    ensure!(
        interpretation
            .full_range
            .is_none_or(|value| value == policy.color.full_range)
            && interpretation
                .color
                .is_none_or(|value| value == policy.color.description),
        "bootstrap SPS color contradicts explicit interpretation"
    );
    Ok(())
}

fn acquire_parameters(
    sequence: &mut Option<SequenceParameters>,
    parameters: &mut Option<Parameters>,
    unit: &[u8],
    policy: Policy,
) -> Result<()> {
    if matches!(
        unit[0] & h264::NAL_TYPE_MASK,
        h264::SEQUENCE_PARAMETERS | h264::PICTURE_PARAMETERS | h264::RANDOM_ACCESS_SLICE
    ) {
        ensure!(
            unit[0] & 0x60 != 0,
            "bootstrap parameter or IDR NAL has zero reference priority"
        );
    }
    match unit[0] & h264::NAL_TYPE_MASK {
        h264::SEQUENCE_PARAMETERS => {
            if let Some(parameters) = parameters {
                ensure!(
                    unit == parameters.sequence(),
                    "bootstrap sequence parameters changed"
                );
            } else if let Some(sequence) = sequence {
                ensure!(
                    unit == sequence.bytes(),
                    "bootstrap sequence parameters changed"
                );
            } else {
                let parsed =
                    SequenceParameters::parse(unit.to_vec(), policy.maximum_reference_frames)?;
                validate_sequence(&parsed, policy)?;
                *sequence = Some(parsed);
            }
        }
        h264::PICTURE_PARAMETERS => {
            if let Some(parameters) = parameters {
                ensure!(
                    unit == parameters.picture(),
                    "bootstrap picture parameters changed"
                );
            } else {
                let parsed = sequence
                    .take()
                    .ok_or_else(|| anyhow!("bootstrap PPS has no validated SPS"))?;
                *parameters = Some(Parameters::from_sequence(parsed, unit.to_vec())?);
            }
        }
        h264::SLICE | h264::RANDOM_ACCESS_SLICE => {
            ensure!(
                parameters.is_some(),
                "bootstrap coded picture precedes its parameters"
            );
        }
        _ => {}
    }
    Ok(())
}
