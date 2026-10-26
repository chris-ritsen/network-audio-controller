use anyhow::{Context, Result, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::StreamExt;
use netaudio_core::sap::{
    self, SapAnnouncement, SapSessionAnnouncement, SapTransition, SapTransitionRequest,
};
use netaudio_core::sdp::{self, SdpDocument};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::num::NonZeroU16;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::time::{DelayQueue, delay_queue::Key};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Identity {
    pub source_ipv4: Ipv4Addr,
    pub session_id: u64,
}

impl Identity {
    pub fn key(&self) -> String {
        format!("{}/{}", self.source_ipv4, self.session_id)
    }
}

pub struct Announcement {
    packet: SapAnnouncement,
    description: SdpDocument,
    identity: Identity,
}

impl Announcement {
    pub fn parse(payload: &[u8]) -> Result<Self> {
        let packet = sap::parse(payload).map_err(anyhow::Error::msg)?;
        let description = sdp::parse(&packet.raw_sdp)
            .map_err(anyhow::Error::msg)
            .context("invalid SAP SDP payload")?;
        let identity = Identity {
            source_ipv4: packet.origin_address.parse()?,
            session_id: description.session_id,
        };

        Ok(Self {
            packet,
            description,
            identity,
        })
    }
}

pub struct Observation {
    pub interface: String,
    pub source: SocketAddrV4,
    pub received_at: DateTime<Utc>,
    pub received_monotonic: Instant,
}

struct Flow {
    announcement: Announcement,
    observation: Observation,
    interfaces: BTreeSet<String>,
    discovered_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    expires_monotonic: Instant,
    content_sha256: String,
}

impl Flow {
    fn new(announcement: Announcement, observation: Observation, expiry: Duration) -> Result<Self> {
        let content_sha256 = hex::encode(Sha256::digest(announcement.packet.raw_sdp.as_bytes()));
        let mut flow = Self {
            interfaces: BTreeSet::from([observation.interface.clone()]),
            discovered_at: observation.received_at,
            expires_at: observation.received_at,
            expires_monotonic: observation.received_monotonic,
            announcement,
            observation,
            content_sha256,
        };
        flow.set_expiry(expiry)?;
        Ok(flow)
    }

    fn refresh(&mut self, observation: Observation, expiry: Duration) -> Result<()> {
        self.interfaces.insert(observation.interface.clone());
        self.observation = observation;
        self.set_expiry(expiry)
    }

    fn set_expiry(&mut self, expiry: Duration) -> Result<()> {
        self.expires_at = self
            .observation
            .received_at
            .checked_add_signed(chrono::Duration::from_std(expiry)?)
            .context("SAP wall-clock expiry is out of range")?;
        self.expires_monotonic = self
            .observation
            .received_monotonic
            .checked_add(expiry)
            .context("SAP monotonic expiry is out of range")?;
        Ok(())
    }

    fn value(&self) -> Value {
        let packet = &self.announcement.packet;
        let description = &self.announcement.description;
        let audio = description.routable_audio.as_ref();
        let primary = audio.map(|audio| {
            json!({
                "address": audio.primary_destination_address,
                "port": audio.destination_port,
            })
        });
        let secondary = audio.and_then(|audio| {
            audio.secondary_destination_address.as_ref().map(|address| {
                json!({
                    "address": address,
                    "port": audio.destination_port,
                })
            })
        });

        json!({
            "source_ipv4": self.announcement.identity.source_ipv4,
            "session_id": description.session_id,
            "message_hash": packet.message_hash,
            "content_sha256": self.content_sha256,
            "flow_name": description.session_name,
            "media_title": audio.and_then(|audio| audio.media_title.as_ref()),
            "origin_username": description.origin_username,
            "ptp_domain_token": audio.and_then(|audio| audio.ptp_domain_token.as_ref()),
            "channel_count": audio.map(|audio| audio.channel_count),
            "clock_offset": audio.and_then(|audio| audio.clock_offset),
            "primary_destination": primary,
            "secondary_destination": secondary,
            "encoding": audio.map(|audio| &audio.encoding),
            "sample_rate": audio.map(|audio| audio.sample_rate),
            "packet_time_microseconds": audio.and_then(|audio| audio.packet_time_microseconds),
            "direction": audio.and_then(|audio| audio.direction.as_ref()),
            "dante_origin": audio.is_some_and(|audio| audio.dante_origin),
            "routable": description.routable,
            "routability_errors": description.routability_errors,
            "advertisement_supports_multiple_interfaces": secondary.is_some(),
            "announcement_interface": self.observation.interface,
            "announcement_interfaces": self.interfaces,
            "packet_source_ipv4": self.observation.source.ip(),
            "packet_source_port": self.observation.source.port(),
            "discovered_at": timestamp(self.discovered_at),
            "refreshed_at": timestamp(self.observation.received_at),
            "expires_at": timestamp(self.expires_at),
            "authentication_length_words": packet.authentication_length_words,
            "authentication_data_hexadecimal": packet.authentication_data_hexadecimal,
            "raw_sdp": packet.raw_sdp,
            "sdp": description,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Added,
    Refreshed,
    Replaced,
    Deleted,
    Expired,
}

pub struct Change {
    pub kind: ChangeKind,
    pub identity: Identity,
    pub flow: Option<Value>,
    pub expires_monotonic: Option<Instant>,
}

struct Entry {
    flow: Flow,
    expiry_key: Key,
}

pub struct Inventory {
    flows: BTreeMap<Identity, Entry>,
    expiry: Duration,
    deadlines: DelayQueue<Identity>,
}

impl Inventory {
    pub fn new(expiry: Duration) -> Result<Self> {
        ensure!(!expiry.is_zero(), "SAP expiry must be positive");
        chrono::Duration::from_std(expiry).context("SAP expiry is out of range")?;

        Ok(Self {
            flows: BTreeMap::new(),
            deadlines: DelayQueue::new(),
            expiry,
        })
    }

    pub fn len(&self) -> usize {
        self.flows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.flows.is_empty()
    }

    pub fn apply(
        &mut self,
        announcement: Announcement,
        observation: Observation,
    ) -> Result<Vec<Change>> {
        ensure!(
            !observation.interface.is_empty(),
            "SAP announcement interface is required"
        );
        let identity = announcement.identity.clone();
        let mut changes = Vec::new();

        if self
            .flows
            .get(&identity)
            .is_some_and(|entry| entry.flow.expires_monotonic <= observation.received_monotonic)
        {
            let previous = self
                .flows
                .remove(&identity)
                .context("expired SAP flow disappeared")?;
            self.deadlines.remove(&previous.expiry_key);
            changes.push(Change {
                kind: ChangeKind::Expired,
                identity: identity.clone(),
                flow: None,
                expires_monotonic: None,
            });
        }

        let previous = self.flows.get(&identity);
        let transition = sap::transition(SapTransitionRequest {
            delete: announcement.packet.delete,
            current: session(&announcement.packet)?,
            previous: previous
                .map(|entry| session(&entry.flow.announcement.packet))
                .transpose()?,
        });
        let Some(transition) = transition else {
            return Ok(changes);
        };

        let kind = match transition {
            SapTransition::Added => ChangeKind::Added,
            SapTransition::Refreshed => ChangeKind::Refreshed,
            SapTransition::Replaced => ChangeKind::Replaced,
            SapTransition::Deleted => ChangeKind::Deleted,
        };
        let previous = self.flows.remove(&identity);

        if let Some(previous) = &previous {
            self.deadlines.remove(&previous.expiry_key);
        }

        if kind == ChangeKind::Deleted {
            changes.push(Change {
                kind,
                identity,
                flow: None,
                expires_monotonic: None,
            });
            return Ok(changes);
        }

        let flow = if kind == ChangeKind::Refreshed {
            let mut flow = previous.context("refreshed SAP flow is absent")?.flow;
            flow.refresh(observation, self.expiry)?;
            flow
        } else {
            let mut flow = Flow::new(announcement, observation, self.expiry)?;

            if let Some(previous) = previous {
                flow.discovered_at = previous.flow.discovered_at;
            }

            flow
        };
        let value = flow.value();
        let expires_monotonic = Some(flow.expires_monotonic);
        let expiry_key = self
            .deadlines
            .insert_at(identity.clone(), flow.expires_monotonic);
        self.flows
            .insert(identity.clone(), Entry { flow, expiry_key });
        changes.push(Change {
            kind,
            identity,
            flow: Some(value),
            expires_monotonic,
        });
        Ok(changes)
    }

    pub async fn expired(&mut self) -> Result<Change> {
        let expiry = self
            .deadlines
            .next()
            .await
            .context("SAP expiry queue is empty")?;
        let identity = expiry.into_inner();
        self.flows
            .remove(&identity)
            .context("expiring SAP flow is absent")?;
        Ok(Change {
            kind: ChangeKind::Expired,
            identity,
            flow: None,
            expires_monotonic: None,
        })
    }
}

fn session(packet: &SapAnnouncement) -> Result<SapSessionAnnouncement> {
    Ok(SapSessionAnnouncement {
        message_hash: NonZeroU16::new(packet.message_hash).context("SAP message hash is zero")?,
        raw_sdp: packet.raw_sdp.clone(),
    })
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}
