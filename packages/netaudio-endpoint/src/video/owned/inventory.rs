use std::num::NonZeroU16;

use anyhow::{Context, Result};
use netaudio_core::video::inventory::{
    self, Capacity, Context as WireContext, Resources, Row, Table,
};

use super::{Acquisition, ReceiverPhase, Runtime};

fn positive(value: u16) -> NonZeroU16 {
    NonZeroU16::new(value).unwrap()
}

fn capacity(value: u16) -> Capacity {
    Capacity {
        current: value,
        ceiling: positive(value),
    }
}

fn resources(channels: u16, flows: u16) -> Resources {
    Resources {
        channels: capacity(channels),
        flows: capacity(flows),
        slots_per_flow: capacity(1),
    }
}

fn selected(identifier: NonZeroU16, first: u16, last: u16, bound: u16) -> bool {
    inventory::selected_interval(first, last, bound)
        .is_some_and(|range| range.contains(&identifier.get()))
}

fn page(context: WireContext, table: Table, reserved: usize, rows: &[Row<'_>]) -> Result<Vec<u8>> {
    let maximum = rows.len().min(usize::from(u8::MAX));
    let mut count = maximum;
    loop {
        let capacity = reserved.min(usize::from(u8::MAX)).max(count) as u8;
        match inventory::page_rows(context, table, capacity, &rows[..count], count < rows.len()) {
            Ok(bytes) => return Ok(bytes),
            Err(error) if error.0 == "video inventory exceeds its declared extent" && count > 1 => {
                count /= 2
            }
            Err(error) => return Err(error.into()),
        }
    }
}

impl Runtime {
    pub(super) fn capability(&self, context: WireContext) -> Result<Vec<u8>> {
        let receiver_count = u16::try_from(self.configuration.receivers.len())?;
        inventory::Capability {
            capability_word: 0x1000,
            raw_status: 0,
            receiver_error_fields: [0; 2],
            raw_base_bytes: [0; 4],
            display_group_upper_identifier: 0,
            raw_compatibility_flags: 0,
            message_flags: 0,
            interfaces: capacity(1),
            transmitter: self.configuration.source.as_ref().map(|_| resources(1, 1)),
            receiver: (receiver_count > 0).then(|| resources(receiver_count, receiver_count)),
            raw_extension_before_scalar: [0; 2],
            raw_extension_scalar: 0,
            raw_extension_after_scalar: [0; 2],
            receiver_format_capacity: u8::from(receiver_count > 0),
        }
        .encode(context)
        .map_err(Into::into)
    }

    pub(super) fn transmitter_flow_page(
        &self,
        context: WireContext,
        first: u16,
        last: u16,
        reserved: usize,
    ) -> Result<Vec<u8>> {
        let Some(source) = &self.configuration.source else {
            return page(context, Table::TransmitterFlows, 0, &[]);
        };
        if !self.source.allocated || !selected(source.local_flow, first, last, 1) {
            return page(context, Table::TransmitterFlows, reserved, &[]);
        }
        let endpoints = [source.configuration.destination];
        let properties = [0];
        let row = Row::TransmitterFlow(inventory::TransmitterFlow {
            flow: inventory::Flow {
                identity: inventory::Identity {
                    global: source.global_flow,
                    media_local: source.local_flow,
                },
                effective_name: Some(source.flow_name.as_bytes()),
                codec_identifier: None,
                raw_flags: 0,
                raw_current_scalar: 0,
                raw_state: 0,
                raw_endpoint_state: 0,
                endpoints: &endpoints,
            },
            source_labels: [None, None],
            source_channel: source.local_channel,
            endpoint_properties: Some(inventory::EndpointProperties {
                raw_state: 0,
                values: &properties,
            }),
        });
        page(context, Table::TransmitterFlows, reserved, &[row])
    }

    pub(super) fn inventory(&self, query: inventory::Query) -> Result<Vec<u8>> {
        let receiver_count = u16::try_from(self.configuration.receivers.len())?;
        let bound = match query.table {
            Table::TransmitterChannels | Table::TransmitterFlows => {
                u16::from(self.configuration.source.is_some())
            }
            Table::ReceiverChannels | Table::ReceiverFlows => receiver_count,
        };
        let reserved = inventory::selected_interval(query.first, query.last, bound)
            .map(|range| usize::from(*range.end()) - usize::from(*range.start()) + 1)
            .unwrap_or(0);
        if query.table == Table::TransmitterFlows {
            return self.transmitter_flow_page(query.context, query.first, query.last, reserved);
        }
        if query.table == Table::TransmitterChannels {
            let Some(source) = &self.configuration.source else {
                return page(query.context, query.table, 0, &[]);
            };
            if !selected(source.local_channel, query.first, query.last, bound) {
                return page(query.context, query.table, reserved, &[]);
            }
            return page(
                query.context,
                query.table,
                reserved,
                &[Row::TransmitterChannel(inventory::Channel {
                    identity: inventory::Identity {
                        global: source.global_channel,
                        media_local: source.local_channel,
                    },
                    effective_name: source.channel_name.as_bytes(),
                    default_name: source.channel_name.as_bytes(),
                    codec_identifier: Some(source.channel_codec_identifier),
                    raw_state: 0,
                    external_readiness: 0,
                    raw_protection_scheme: 0,
                    display_group: 0,
                    raw_scalar: 0,
                })],
            );
        }
        let endpoints = self
            .configuration
            .receivers
            .iter()
            .map(|receiver| [receiver.configuration.destination])
            .collect::<Vec<_>>();
        let flow_names = self
            .configuration
            .receivers
            .iter()
            .map(|receiver| format!("RX-FLOW-{}", receiver.local_flow))
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for (index, definition) in self.configuration.receivers.iter().enumerate() {
            let state = self
                .receivers
                .get(&definition.identifier)
                .context("configured receiver state is missing")?;
            let active = state.worker_generation == Some(state.generation)
                && state.desired.is_some()
                && state.phase == ReceiverPhase::Receiving
                && state.observation.terminal.is_none();
            match query.table {
                Table::ReceiverChannels
                    if selected(definition.identifier, query.first, query.last, bound) =>
                {
                    let subscription_status = if state.desired.is_none() {
                        0
                    } else if active {
                        match definition.acquisition {
                            Acquisition::Multicast {
                                ..
                            } => 10,
                            Acquisition::Unicast {
                                ..
                            } => 9,
                        }
                    } else if state.phase == ReceiverPhase::Failed {
                        state.failure_subscription_status
                    } else if state
                        .source_control
                        .as_ref()
                        .is_some_and(super::unicast::State::outcome_unknown)
                        || state.phase == ReceiverPhase::Receiving
                        || state.phase == ReceiverPhase::Retired
                        || state.phase == ReceiverPhase::Retiring
                        || state.phase == ReceiverPhase::FreshAuthorityRequired
                    {
                        7
                    } else if matches!(
                        state.source_control,
                        Some(super::unicast::State::AllocationPending { .. })
                    ) {
                        8
                    } else if state.phase == ReceiverPhase::Preparing {
                        2
                    } else {
                        1
                    };
                    rows.push(Row::ReceiverChannel(inventory::ReceiverChannel {
                        channel: inventory::Channel {
                            identity: inventory::Identity {
                                global: definition.global_identifier,
                                media_local: definition.identifier,
                            },
                            effective_name: definition.channel_name.as_bytes(),
                            default_name: definition.channel_name.as_bytes(),
                            codec_identifier: Some(1),
                            raw_state: 0,
                            external_readiness: u16::from(
                                active && state.observation.output_queued,
                            ),
                            raw_protection_scheme: 0,
                            display_group: 0,
                            raw_scalar: 0,
                        },
                        receiver_processing: u16::from(active && state.observation.decoded_frame),
                        supported_codec_identifiers: &[1],
                        source_labels: state
                            .desired
                            .as_ref()
                            .map(|source| (source.channel.as_slice(), source.device.as_slice())),
                        raw_subscription_status: subscription_status,
                        raw_availability: 1,
                        raw_asserted_state: u8::from(active && state.observation.admitted_packet),
                    }));
                }
                Table::ReceiverFlows
                    if selected(definition.local_flow, query.first, query.last, bound)
                        && state.worker_generation.is_some()
                        && (state.observation.multicast_joined
                            || (state.observation.socket_owned
                                && state
                                    .source_control
                                    .as_ref()
                                    .is_some_and(super::unicast::State::is_allocated)))
                        && state.observation.terminal.is_none() =>
                {
                    rows.push(Row::ReceiverFlow(inventory::ReceiverFlow {
                        flow: inventory::Flow {
                            identity: inventory::Identity {
                                global: definition.global_flow,
                                media_local: definition.local_flow,
                            },
                            effective_name: Some(flow_names[index].as_bytes()),
                            codec_identifier: None,
                            raw_flags: 0,
                            raw_current_scalar: 0,
                            raw_state: 0,
                            raw_endpoint_state: 0,
                            endpoints: &endpoints[index],
                        },
                        transport_description_selector: 1,
                        receiver_channel: Some(definition.identifier),
                        slot_count: 1,
                    }));
                }
                _ => {}
            }
        }
        page(query.context, query.table, reserved, &rows)
    }
}
