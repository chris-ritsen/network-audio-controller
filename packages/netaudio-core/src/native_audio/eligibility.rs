use serde::Serialize;

use crate::bytes::hexadecimal;
use crate::parser::audio::{Field, TransmitChannel};
use crate::parser::ChannelCount;

use super::domain::NamedDomain;
use super::endpoint_control::Profile;
use super::properties::{AudioProperties, PropertyObservation};
use super::source_control::Error;
use super::source_discovery::Source;

#[derive(Clone, Debug, Serialize)]
pub struct SelectedChannel {
    pub name: String,
    pub routing_identifier: u16,
    pub source_identifier: u16,
}

#[derive(Clone, Debug, Serialize)]
pub struct SourceFormat {
    pub channels: Vec<SelectedChannel>,
    pub flow_capacity: u16,
    pub sample_rate: u32,
    pub sample_bits: u16,
    pub requested_frames_per_packet: u16,
    pub receive_latency_nanoseconds: u32,
}

pub fn qualify(
    capabilities: &ChannelCount,
    properties: &AudioProperties,
    channels: &[&TransmitChannel],
    selected: &[(&str, &Source)],
    requested_frames_per_packet: u16,
    receiver_domain: NamedDomain,
) -> Result<SourceFormat, Error> {
    qualify_with_profile(
        capabilities,
        properties,
        channels,
        selected,
        requested_frames_per_packet,
        receiver_domain,
        Profile::default(),
    )
}

pub fn qualify_with_profile(
    capabilities: &ChannelCount,
    properties: &AudioProperties,
    channels: &[&TransmitChannel],
    selected: &[(&str, &Source)],
    requested_frames_per_packet: u16,
    receiver_domain: NamedDomain,
    profile: Profile,
) -> Result<SourceFormat, Error> {
    profile.validate()?;
    if capabilities.uses_modern_transmit_flow_authoring {
        return Err(Error("selected source needs media-specific resources"));
    }

    if channels.len() != usize::from(capabilities.tx_count) {
        return Err(Error("source channel inventory does not match the acquired audio capacity"));
    }

    if selected.is_empty()
        || capabilities
            .maximum_transmit_flow_channel_slots
            .is_none_or(|capacity| usize::from(capacity) < selected.len())
        || capabilities.maximum_transmit_flows.is_none_or(|capacity| capacity == 0)
    {
        return Err(Error("source flow capacity is absent or insufficient for the selected slots"));
    }

    match &properties.named_clock_domain_hexadecimal {
        PropertyObservation::Reported {
            value,
        } if *value == hexadecimal(receiver_domain.bytes()) => {}
        PropertyObservation::Reported {
            ..
        } => return Err(Error("source named clock domain does not match the receiver")),
        _ => return Err(Error("source named clock domain has no unambiguous reported value")),
    }

    let mut result = SourceFormat {
        channels: Vec::with_capacity(selected.len()),
        flow_capacity: capabilities
            .maximum_transmit_flows
            .ok_or(Error("source flow capacity is absent"))?,
        sample_rate: profile.sample_rate,
        sample_bits: profile.sample_bits,
        requested_frames_per_packet,
        receive_latency_nanoseconds: profile.receive_latency_nanoseconds,
    };

    for (name, source) in selected {
        source.packetization.request(requested_frames_per_packet)?;
        if source.sample_rate != profile.sample_rate
            || !source.encodings.contains(&profile.sample_bits)
        {
            return Err(Error(
                "selected source discovery does not advertise the receiver audio format",
            ));
        }
        if source.named_domain != receiver_domain {
            return Err(Error("source named clock domain does not match the receiver"));
        }

        let mut matches = channels.iter().filter(
            |channel| matches!(&channel.name, Field::Present(value) if value == name.as_bytes()),
        );
        let channel = matches
            .next()
            .ok_or(Error("selected source channel name is absent from the acquired inventory"))?;

        if matches.next().is_some() {
            return Err(Error(
                "selected source channel name is ambiguous in the acquired inventory",
            ));
        }

        let Field::Present(availability) = &channel.availability else {
            return Err(Error("selected source channel format is absent or invalid"));
        };

        if availability.sample_rate != profile.sample_rate
            || availability.supports_encoding(profile.sample_bits) != Some(true)
        {
            return Err(Error(
                "selected source channel does not advertise the receiver audio format",
            ));
        }

        if requested_frames_per_packet == 0
            || requested_frames_per_packet < source.packetization.first
            || (source.packetization.third != 0
                && requested_frames_per_packet > source.packetization.third)
        {
            return Err(Error("requested packetization is outside selected source bounds"));
        }

        if source
            .latency_nanoseconds
            .is_some_and(|latency| latency > profile.receive_latency_nanoseconds)
        {
            return Err(Error("selected source latency exceeds the receiver capacity"));
        }

        result.channels.push(SelectedChannel {
            name: (*name).to_owned(),
            routing_identifier: channel.identifier,
            source_identifier: source.channel_identifier,
        });
    }

    Ok(result)
}
