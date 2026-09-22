use std::collections::{BTreeMap, HashSet};
use std::num::{NonZeroU16, NonZeroU32};

use serde::{Deserialize, Serialize};

use crate::flow_readback::FlowTopology;
use crate::flow_specification::FlowType;

#[derive(PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ChannelCapacity {
    pub sample_rate_hertz: NonZeroU32,
    pub receive_channel_count: u16,
    pub transmit_channel_count: u16,
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SampleRateStatus {
    pub current_value: NonZeroU32,
    pub available_values: Vec<NonZeroU32>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvidenceRequest {
    Status {
        status: SampleRateStatus,
    },
    Capacity {
        capacities: Option<Vec<ChannelCapacity>>,
        sample_rate_hertz: NonZeroU32,
    },
    ReceiverInventory {
        channel_numbers: Vec<NonZeroU16>,
        receive_channel_count: u16,
    },
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum Evidence {
    Status(SampleRateStatus),
    Capacity(Option<ChannelCapacity>),
    ReceiverInventory(()),
}

impl EvidenceRequest {
    pub fn validate(self) -> Result<Evidence, String> {
        match self {
            Self::Status { status } => {
                let unique: HashSet<_> = status.available_values.iter().copied().collect();

                if unique.len() != status.available_values.len() {
                    return Err("supported sample-rate readback contains duplicates".into());
                }

                if !unique.contains(&status.current_value) {
                    return Err("current sample rate is absent from the device's supported sample-rate list".into());
                }

                Ok(Evidence::Status(status))
            }
            Self::Capacity {
                capacities,
                sample_rate_hertz,
            } => {
                let mut table = BTreeMap::new();

                for capacity in capacities.unwrap_or_default() {
                    if let Some(previous) = table.get(&capacity.sample_rate_hertz) {
                        if previous != &capacity {
                            return Err(format!(
                                "device reports conflicting channel capacities for {} Hz",
                                capacity.sample_rate_hertz
                            ));
                        }
                    }

                    table.insert(capacity.sample_rate_hertz, capacity);
                }

                Ok(Evidence::Capacity(table.remove(&sample_rate_hertz)))
            }
            Self::ReceiverInventory {
                channel_numbers,
                receive_channel_count,
            } => {
                let observed: HashSet<_> =
                    channel_numbers.iter().map(|number| number.get()).collect();

                if channel_numbers.len() != usize::from(receive_channel_count)
                    || observed.len() != channel_numbers.len()
                    || !(1..=receive_channel_count).all(|number| observed.contains(&number))
                {
                    return Err("fresh receiver inventory does not match the reported active channel capacity".into());
                }

                Ok(Evidence::ReceiverInventory(()))
            }
        }
    }
}

#[derive(PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ReceiverSubscription {
    pub receiver_channel_number: NonZeroU16,
    pub receiver_channel_name: String,
    pub transmitter_channel_name: String,
    pub transmitter_device_name: String,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TopologySnapshot {
    pub capacity: ChannelCapacity,
    pub receiver_subscriptions: Vec<ReceiverSubscription>,
    pub transmitter_flows: Vec<FlowTopology>,
    pub flow_protocol_identifier: u16,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TopologyImpactRequest {
    pub snapshot: TopologySnapshot,
    pub target_capacity: Option<ChannelCapacity>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowMembershipLoss {
    flow_number: NonZeroU16,
    flow_type: FlowType,
    retained_channel_members: Vec<u16>,
    removed_channel_members: Vec<u16>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct UncharacterizedFlow {
    flow_number: NonZeroU16,
    flow_type: FlowType,
    channel_count: NonZeroU16,
    reason: &'static str,
}

#[derive(Serialize, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TopologyImpact {
    requires_destructive_confirmation: bool,
    reversible_receiver_clipping: Vec<ReceiverSubscription>,
    destructive_transmitter_membership_loss: Vec<FlowMembershipLoss>,
    uncharacterized_transmitter_flows: Vec<UncharacterizedFlow>,
}

pub fn topology_impact(request: TopologyImpactRequest) -> Result<TopologyImpact, String> {
    let snapshot = request.snapshot;
    snapshot.validate()?;

    let mut impact = TopologyImpact::default();
    let Some(target) = request.target_capacity else {
        impact.requires_destructive_confirmation = !snapshot.transmitter_flows.is_empty();
        return Ok(impact);
    };

    impact.reversible_receiver_clipping = snapshot
        .receiver_subscriptions
        .into_iter()
        .filter(|state| state.receiver_channel_number.get() > target.receive_channel_count)
        .collect();

    if target.transmit_channel_count >= snapshot.capacity.transmit_channel_count {
        return Ok(impact);
    }

    for flow in snapshot.transmitter_flows {
        let retained: Vec<_> = flow
            .channel_members
            .iter()
            .copied()
            .filter(|member| *member > 0 && *member <= target.transmit_channel_count)
            .collect();
        let removed: Vec<_> = flow
            .channel_members
            .iter()
            .copied()
            .filter(|member| *member > target.transmit_channel_count)
            .collect();
        let reason = if matches!(flow.flow_type, FlowType::Unicast) {
            Some("the proven unicast inventory does not expose transmitter channel members")
        } else if !removed.is_empty() && retained.is_empty() {
            Some("all active members fall outside the target capacity and that transition is unproven")
        } else {
            None
        };

        if let Some(reason) = reason {
            impact
                .uncharacterized_transmitter_flows
                .push(UncharacterizedFlow {
                    flow_number: flow.flow_number,
                    flow_type: flow.flow_type,
                    channel_count: flow.channel_count,
                    reason,
                });
        } else if !removed.is_empty() {
            impact
                .destructive_transmitter_membership_loss
                .push(FlowMembershipLoss {
                    flow_number: flow.flow_number,
                    flow_type: flow.flow_type,
                    retained_channel_members: retained,
                    removed_channel_members: removed,
                });
        }
    }

    impact.requires_destructive_confirmation =
        !impact.destructive_transmitter_membership_loss.is_empty();

    Ok(impact)
}

impl TopologySnapshot {
    fn validate(&self) -> Result<(), String> {
        let (_, modern) = crate::flow_readback::readback_protocol(self.flow_protocol_identifier)?;
        let mut seen = HashSet::new();

        for flow in &self.transmitter_flows {
            if flow.may_retire_after_sample_rate_change
                != flow.flow_type.may_retire_after_sample_rate_change(modern)
            {
                return Err(
                    "flow retirement capability does not match its protocol and type".into(),
                );
            }

            if !seen.insert(flow.flow_number) {
                return Err("duplicate transmitter flow identity in topology snapshot".into());
            }

            if flow.sample_rate_hertz != self.capacity.sample_rate_hertz {
                return Err(
                    "transmitter flow sample rate does not match the topology snapshot".into(),
                );
            }
        }

        seen.clear();

        for subscription in &self.receiver_subscriptions {
            if !seen.insert(subscription.receiver_channel_number) {
                return Err("duplicate receiver channel identity in topology snapshot".into());
            }
        }

        Ok(())
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TopologyReadbackRequest {
    pub before: TopologySnapshot,
    pub after: TopologySnapshot,
    pub target_capacity: Option<ChannelCapacity>,
    pub target_sample_rate_hertz: NonZeroU32,
}

pub fn verify_topology(request: TopologyReadbackRequest) -> Result<(), String> {
    let TopologyReadbackRequest {
        before,
        after,
        target_capacity,
        target_sample_rate_hertz,
    } = request;
    before.validate()?;
    after.validate()?;

    if after.flow_protocol_identifier != before.flow_protocol_identifier {
        return Err("transmitter-flow protocol changed during the sample-rate operation".into());
    }

    if after.capacity.sample_rate_hertz != target_sample_rate_hertz {
        return Err("topology readback did not adopt the target sample rate".into());
    }

    let Some(target) = target_capacity else {
        return Ok(());
    };

    if target.sample_rate_hertz != target_sample_rate_hertz || after.capacity != target {
        return Err("topology readback does not match the target channel capacity".into());
    }

    let expected_subscriptions: BTreeMap<_, _> = before
        .receiver_subscriptions
        .into_iter()
        .filter(|state| state.receiver_channel_number.get() <= target.receive_channel_count)
        .map(|state| (state.receiver_channel_number, state))
        .collect();
    let resulting_subscriptions: BTreeMap<_, _> = after
        .receiver_subscriptions
        .into_iter()
        .map(|state| (state.receiver_channel_number, state))
        .collect();

    if resulting_subscriptions != expected_subscriptions {
        return Err(
            "receiver subscriptions did not reach the exact expected in-capacity state".into(),
        );
    }

    let resulting_flows: BTreeMap<_, _> = after
        .transmitter_flows
        .into_iter()
        .map(|flow| (flow.flow_number, flow))
        .collect();
    let mut expected_flows = BTreeMap::new();

    for mut flow in before.transmitter_flows {
        // Automatic unicast flows can retire while receivers retain the old sample rate.
        if target.transmit_channel_count >= before.capacity.transmit_channel_count
            && flow.may_retire_after_sample_rate_change
            && !resulting_flows.contains_key(&flow.flow_number)
        {
            continue;
        }

        for member in &mut flow.channel_members {
            if *member > target.transmit_channel_count {
                *member = 0;
            }
        }

        flow.sample_rate_hertz = target_sample_rate_hertz;
        expected_flows.insert(flow.flow_number, flow);
    }

    if resulting_flows != expected_flows {
        return Err(
            "transmitter flows did not reach the exact expected membership and metadata state"
                .into(),
        );
    }

    Ok(())
}
