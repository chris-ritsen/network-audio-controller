use std::net::SocketAddrV4;

use anyhow::{Context, Result, bail};
use hickory_proto::rr::Name;
use netaudio_core::native_audio::domain::NamedDomain;
use netaudio_core::native_audio::endpoint_control::{
    Endpoint, FRAMES_PER_PACKET, NAMED_CLOCK_DOMAIN,
};
use netaudio_core::native_audio::source_discovery::{Routing, Source};
use serde::Serialize;

use super::resolution::cache::ResolvedService;
use super::resolution::records::service_name;

const CHANNEL_SERVICE: &[u8] = b"_netaudio-chan";
const ROUTING_SERVICE: &[u8] = b"_netaudio-arc";

#[derive(Clone, Debug)]
struct Channel {
    name: Name,
    label: String,
    receivers: Vec<u16>,
}

#[derive(Clone, Debug)]
pub struct Selection {
    generation: u64,
    device: String,
    channels: Vec<Channel>,
    routing_name: Name,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SourceSlot {
    pub service: String,
    pub channel_name: String,
    pub source_identifier: u16,
    pub receivers: Vec<u16>,
    pub record_generations: [u64; 3],
    pub source: Source,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RoutingService {
    pub service: String,
    pub endpoint: SocketAddrV4,
    pub record_generations: [u64; 3],
    pub declaration: Routing,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Candidate {
    pub route_generation: u64,
    pub device: String,
    pub target: String,
    pub control_endpoint: SocketAddrV4,
    pub routing: RoutingService,
    pub slots: Vec<SourceSlot>,
    pub requested_frames_per_packet: u16,
    pub receiver_domain: NamedDomain,
    pub usable_through: u64,
}

impl Candidate {
    pub fn same_context(&self, other: &Self) -> bool {
        self.route_generation == other.route_generation
            && self.device == other.device
            && self.target == other.target
            && self.control_endpoint == other.control_endpoint
            && self.routing == other.routing
            && self.slots == other.slots
            && self.requested_frames_per_packet == other.requested_frames_per_packet
            && self.receiver_domain == other.receiver_domain
    }
}

impl Selection {
    pub fn from_endpoint(endpoint: &Endpoint) -> Result<Option<Self>> {
        let mut selection: Option<Self> = None;

        for (index, receiver) in endpoint.receive_channels.iter().enumerate() {
            let Some(route) = &receiver.desired else {
                continue;
            };

            let routing_name = service_name(route.device.as_bytes(), ROUTING_SERVICE)?;
            let selected = selection.get_or_insert_with(|| Self {
                generation: endpoint.route_generation,
                device: route.device.clone(),
                channels: Vec::new(),
                routing_name,
            });

            if route.device != selected.device {
                bail!("the single receive flow cannot combine routes from different devices");
            }

            let instance = format!("{}@{}", route.channel, route.device);
            let name = service_name(instance.as_bytes(), CHANNEL_SERVICE)?;
            let receiver_identifier = u16::try_from(index + 1)?;

            if let Some(channel) = selected.channels.iter_mut().find(|channel| channel.name == name)
            {
                channel.receivers.push(receiver_identifier);
            } else {
                selected.channels.push(Channel {
                    name,
                    label: route.channel.clone(),
                    receivers: vec![receiver_identifier],
                });
            }
        }

        Ok(selection)
    }

    pub fn names(&self) -> impl Iterator<Item = Name> + '_ {
        self.channels
            .iter()
            .map(|channel| channel.name.clone())
            .chain(std::iter::once(self.routing_name.clone()))
    }

    pub fn resolve(
        &self,
        mut resolve: impl FnMut(&Name) -> Result<ResolvedService>,
    ) -> Result<Candidate> {
        let mut services = Vec::with_capacity(self.channels.len());

        for channel in &self.channels {
            services.push(resolve(&channel.name)?);
        }

        let first = services.first().context("source selection has no channels")?;
        let routing = resolve(&self.routing_name)?;

        if routing.name != self.routing_name
            || routing.target != first.target
            || routing.endpoint.ip() != first.endpoint.ip()
        {
            bail!("selected source routing service does not share its channel target and address");
        }

        let mut candidate = Candidate {
            route_generation: self.generation,
            device: self.device.clone(),
            target: first.target.to_utf8(),
            control_endpoint: first.endpoint,
            routing: RoutingService {
                service: routing.name.to_utf8(),
                endpoint: routing.endpoint,
                record_generations: routing.record_generations,
                declaration: Routing::decode(&routing.text)?,
            },
            slots: Vec::with_capacity(services.len()),
            requested_frames_per_packet: FRAMES_PER_PACKET,
            receiver_domain: NAMED_CLOCK_DOMAIN,
            usable_through: first.usable_through.min(routing.usable_through),
        };
        let mut ceiling = u16::MAX;

        for (channel, service) in self.channels.iter().zip(&services) {
            if service.name != channel.name
                || service.target != first.target
                || service.endpoint != first.endpoint
            {
                bail!("selected source channels do not share one current target and endpoint");
            }

            let source = Source::decode(&service.text)?;

            if source.named_domain != candidate.receiver_domain {
                bail!("source named clock domain does not match the receiver");
            }

            if candidate
                .slots
                .iter()
                .any(|slot| slot.source_identifier == source.channel_identifier)
            {
                bail!("distinct source channel services claim the same source identifier");
            }

            if usize::from(source.slot_capacity) < self.channels.len() {
                bail!("selected channels exceed the source flow slot capacity");
            }

            candidate.requested_frames_per_packet =
                candidate.requested_frames_per_packet.max(source.packetization.first);

            if source.packetization.third != 0 {
                ceiling = ceiling.min(source.packetization.third);
            }

            candidate.usable_through = candidate.usable_through.min(service.usable_through);
            candidate.slots.push(SourceSlot {
                service: channel.name.to_utf8(),
                channel_name: channel.label.clone(),
                source_identifier: source.channel_identifier,
                receivers: channel.receivers.clone(),
                record_generations: service.record_generations,
                source,
            });
        }

        candidate.requested_frames_per_packet = candidate.requested_frames_per_packet.min(ceiling);

        if candidate
            .slots
            .iter()
            .any(|slot| slot.source.packetization.first > candidate.requested_frames_per_packet)
        {
            bail!("selected channels have no common source packetization range");
        }

        Ok(candidate)
    }
}
