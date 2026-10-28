use std::net::SocketAddrV4;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use hickory_proto::rr::Name;
use netaudio_core::native_audio::domain::NamedDomain;
use netaudio_core::native_audio::endpoint_control::{
    Endpoint, NAMED_CLOCK_DOMAIN, Profile, RouteIntent,
};
use netaudio_core::native_audio::source_discovery::{Routing, Source};
use serde::Serialize;

use super::resolution::cache::ResolvedService;
use super::resolution::records::service_name;

const CHANNEL_SERVICE: &[u8] = b"_netaudio-chan";
const ROUTING_SERVICE: &[u8] = b"_netaudio-arc";

#[derive(Clone)]
pub struct Request {
    pub generation: u64,
    profile: Profile,
    routes: Vec<Option<RouteIntent>>,
}

impl Request {
    pub fn from_endpoint(endpoint: &Endpoint) -> Self {
        Self {
            generation: endpoint.route_generation,
            profile: endpoint.profile,
            routes: endpoint
                .receive_channels
                .iter()
                .map(|receiver| receiver.desired.clone())
                .collect(),
        }
    }
}

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
    profile: Profile,
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
pub struct CandidateContext {
    pub route_generation: u64,
    pub device: String,
    pub target: String,
    pub control_endpoint: SocketAddrV4,
    pub routing: RoutingService,
    pub slots: Vec<SourceSlot>,
    pub requested_frames_per_packet: u16,
    pub receiver_domain: NamedDomain,
    pub profile: Profile,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Candidate {
    #[serde(flatten)]
    pub context: Arc<CandidateContext>,
    pub discovery_generation: u64,
    pub usable_through: u64,
}

impl Candidate {
    pub fn same_context(&self, other: &Self) -> bool {
        self.discovery_generation == other.discovery_generation
            && (Arc::ptr_eq(&self.context, &other.context) || self.context == other.context)
    }
}

impl Selection {
    pub fn from_request(request: &Request) -> Result<Option<Self>> {
        let mut selection: Option<Self> = None;

        for (index, route) in request.routes.iter().enumerate() {
            let Some(route) = route else {
                continue;
            };

            let routing_name = service_name(route.device.as_bytes(), ROUTING_SERVICE)?;
            let selected = selection.get_or_insert_with(|| Self {
                generation: request.generation,
                device: route.device.clone(),
                channels: Vec::new(),
                routing_name,
                profile: request.profile,
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

        if selection.as_ref().is_some_and(|selection| {
            selection.channels.len() > usize::from(request.profile.flow_slots)
        }) {
            bail!(
                "selected distinct source channels exceed the configured receive flow slot capacity"
            );
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

        let mut context = CandidateContext {
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
            requested_frames_per_packet: self.profile.frames_per_packet,
            receiver_domain: NAMED_CLOCK_DOMAIN,
            profile: self.profile,
        };
        let mut usable_through = first.usable_through.min(routing.usable_through);
        let mut ceiling = self.profile.maximum_frames_per_packet();

        for (channel, service) in self.channels.iter().zip(&services) {
            if service.name != channel.name
                || service.target != first.target
                || service.endpoint != first.endpoint
            {
                bail!("selected source channels do not share one current target and endpoint");
            }

            let source = Source::decode(&service.text)?;
            source.packetization.request(self.profile.frames_per_packet)?;

            if source.sample_rate != self.profile.sample_rate
                || !source.encodings.contains(&self.profile.sample_bits)
            {
                bail!("source discovery does not advertise the configured receiver audio format");
            }

            if source.named_domain != context.receiver_domain {
                bail!("source named clock domain does not match the receiver");
            }

            if context.slots.iter().any(|slot| slot.source_identifier == source.channel_identifier)
            {
                bail!("distinct source channel services claim the same source identifier");
            }

            if usize::from(source.slot_capacity) < self.channels.len() {
                bail!("selected channels exceed the source flow slot capacity");
            }

            context.requested_frames_per_packet =
                context.requested_frames_per_packet.max(source.packetization.first);

            if source.packetization.third != 0 {
                ceiling = ceiling.min(source.packetization.third);
            }

            usable_through = usable_through.min(service.usable_through);
            context.slots.push(SourceSlot {
                service: channel.name.to_utf8(),
                channel_name: channel.label.clone(),
                source_identifier: source.channel_identifier,
                receivers: channel.receivers.clone(),
                record_generations: service.record_generations,
                source,
            });
        }

        context.requested_frames_per_packet = context.requested_frames_per_packet.min(ceiling);

        if context
            .slots
            .iter()
            .any(|slot| slot.source.packetization.first > context.requested_frames_per_packet)
        {
            bail!("selected channels have no common source packetization range");
        }

        Ok(Candidate {
            context: Arc::new(context),
            discovery_generation: 0,
            usable_through,
        })
    }
}
