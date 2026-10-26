use super::super::{
    controller::Exchange,
    device_session::{controller_identity, target_identity},
};
use crate::monitoring::status::{self, StatusKind};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use netaudio_core::managed_api::inventory::Observation;
use serde_json::{Map, Value, json};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::Instant;

pub(in crate::managed) struct Received {
    packet: Vec<u8>,
    response_kind: Option<&'static str>,
    kind: StatusKind,
    peer: SocketAddr,
    observed_at: DateTime<Utc>,
    observed_monotonic: Instant,
    deadline: Instant,
    valid: bool,
    fields: Map<String, Value>,
}

impl Received {
    pub fn decode(exchange: Exchange, expected: StatusKind, lifetime: Duration) -> Result<Self> {
        let packet = exchange
            .packet
            .context("Controller status response has no device packet")?;
        let (response_kind, fields) =
            if matches!(expected, StatusKind::Latency | StatusKind::MulticastPrefix) {
                let settings = netaudio_core::responses::parse_device_settings(&packet)
                    .context("Controller settings response is malformed or rejected")?;
                let mut fields = Map::new();
                if expected == StatusKind::Latency {
                    crate::devices::latency::apply_settings(&mut fields, &settings)?;
                    fields.insert("latency_settings".into(), serde_json::to_value(settings)?);
                } else {
                    fields.insert(
                        "aes67_multicast_prefix".into(),
                        json!(settings.aes67_multicast_prefix),
                    );
                }
                (None, fields)
            } else {
                let response_kind = netaudio_core::responses::parse_notification_envelope(&packet)
                    .and_then(|envelope| envelope.response_kind)
                    .context("Controller status response has no supported envelope")?;
                let observed =
                    status::decode(response_kind, &packet, &Map::new(), exchange.received_at)?
                        .context("Controller status response has no supported decoder")?;
                ensure!(
                    observed.kind == expected,
                    "Controller returned a different status kind"
                );
                (Some(response_kind), observed.fields)
            };
        Ok(Self {
            packet,
            response_kind,
            kind: expected,
            peer: exchange.peer,
            observed_at: exchange.received_at,
            observed_monotonic: exchange.received_monotonic,
            deadline: exchange.received_monotonic + lifetime,
            valid: true,
            fields,
        })
    }

    pub fn inventory(
        first: &Exchange,
        last: &Exchange,
        kind: StatusKind,
        fields: Map<String, Value>,
        lifetime: Duration,
    ) -> Result<Self> {
        ensure!(
            first.peer == last.peer,
            "Controller topology source changed"
        );
        ensure!(
            first.received_monotonic + lifetime > Instant::now(),
            "Controller topology acquisition exceeded its freshness window"
        );
        Ok(Self {
            packet: Vec::new(),
            response_kind: None,
            kind,
            peer: last.peer,
            observed_at: last.received_at,
            observed_monotonic: last.received_monotonic,
            deadline: first.received_monotonic + lifetime,
            valid: true,
            fields,
        })
    }

    fn fresh(&self, now: Instant) -> bool {
        self.valid && self.deadline > now
    }
}

struct DeviceStatus {
    controller: String,
    statuses: BTreeMap<&'static str, Received>,
    failures: BTreeMap<&'static str, (DateTime<Utc>, String)>,
}

#[derive(Default)]
pub(in crate::managed) struct Cache {
    devices: BTreeMap<(String, String), DeviceStatus>,
}

#[derive(Clone, PartialEq)]
pub(crate) struct Projection {
    server: String,
    domain: String,
    device: String,
    fields: Map<String, Value>,
}

impl Projection {
    pub fn apply(&self, record: &mut Arc<Value>) -> Result<()> {
        if record["ddm_server_profile"] != self.server
            || record["ddm_domain_id"] != self.domain
            || record["ddm_device_id"] != self.device
            || !super::super::control::required(record)
        {
            return Ok(());
        }
        let record = Arc::make_mut(record);
        let preferred = record["preferred_leader"].clone();
        let reported = self
            .fields
            .get("clock_status")
            .and_then(|status| status.get("preferred_leader"))
            .cloned()
            .unwrap_or(Value::Null);
        let fields = record
            .as_object_mut()
            .context("Managed device record is not an object")?;
        fields.extend(
            self.fields
                .iter()
                .filter(|(field, _)| field.as_str() != "preferred_leader")
                .map(|(field, value)| (field.clone(), value.clone())),
        );
        if let Some(status) = fields
            .get_mut("clock_status")
            .and_then(Value::as_object_mut)
        {
            status.insert("preferred_leader".into(), preferred);
            status.insert("preferred_leader_source".into(), json!("managed_api"));
        }
        if let Some(status) = fields
            .get_mut("managed_status")
            .and_then(|value| value.get_mut("clock_status"))
            .and_then(Value::as_object_mut)
        {
            status.insert("reported_preferred_leader".into(), reported);
        }
        fields.remove("gain_configurable");
        fields.remove("gain_level_choices");
        crate::devices::analog::present(fields);
        record["operation_availability"] = crate::devices::capabilities::operations(record)?;
        record["network_redundancy"] = crate::devices::capabilities::redundancy(record)?;
        Ok(())
    }
}

impl Cache {
    pub fn accept(&mut self, target: &Observation, received: Received) -> Result<()> {
        let key = key(target).context("Managed status target has no domain")?;
        let controller = controller_identity(&target.device)
            .context("Managed status target has no Controller identity")?;
        let device = self.devices.entry(key).or_insert_with(|| DeviceStatus {
            controller: controller.clone(),
            statuses: BTreeMap::new(),
            failures: BTreeMap::new(),
        });
        ensure!(
            device.controller == controller,
            "Managed status target identity changed"
        );
        device.failures.remove(received.kind.as_str());
        device.statuses.insert(received.kind.as_str(), received);
        Ok(())
    }

    pub fn invalidate(&mut self, target: &Observation, kind: StatusKind, error: &str) {
        let Some(key) = key(target) else {
            return;
        };
        let Some(controller) = controller_identity(&target.device) else {
            return;
        };
        let device = self.devices.entry(key).or_insert_with(|| DeviceStatus {
            controller,
            statuses: BTreeMap::new(),
            failures: BTreeMap::new(),
        });
        if let Some(status) = device.statuses.get_mut(kind.as_str()) {
            status.valid = false;
        }
        device
            .failures
            .insert(kind.as_str(), (Utc::now(), error.to_owned()));
    }

    pub fn invalidate_device(&mut self, target: &Observation, error: &str) {
        let Some(device) = key(target).and_then(|key| self.devices.get(&key)) else {
            return;
        };
        let kinds: Vec<_> = device.statuses.values().map(|status| status.kind).collect();
        for kind in kinds {
            self.invalidate(target, kind, error);
        }
    }

    pub fn project(
        &mut self,
        observations: &[Observation],
        now: Instant,
    ) -> Result<Vec<Projection>> {
        self.devices.retain(|key, device| {
            observations.iter().any(|target| {
                key_matches(key, target)
                    && controller_identity(&target.device).as_deref() == Some(&device.controller)
            })
        });
        let mut projected = Vec::new();
        for (key, device) in &mut self.devices {
            let target = observations
                .iter()
                .find(|target| key_matches(key, target))
                .context("Managed status owner disappeared")?;
            let ready = target_identity(target, observations).is_ok()
                && target.fresh
                && target
                    .device
                    .connection
                    .as_ref()
                    .and_then(|connection| connection.state.as_deref())
                    .is_some_and(|state| state.eq_ignore_ascii_case("READY"));
            if !ready {
                for status in device.statuses.values_mut() {
                    status.valid = false;
                }
            }
            let mut ordered: Vec<_> = device.statuses.values().collect();
            ordered.sort_by_key(|status| status.observed_monotonic);
            let mut fields = Map::new();
            for status in &ordered {
                fields.extend(
                    status
                        .fields
                        .keys()
                        .map(|field| (field.clone(), Value::Null)),
                );
            }
            let mut metadata = Map::new();
            for status in ordered {
                let fresh = status.fresh(now);
                let mut source = json!({
                    "transport": "controller", "peer": status.peer.to_string(),
                    "observed_at": status.observed_at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                    "fresh": fresh,
                });
                if fresh {
                    if let Some(response_kind) = status.response_kind {
                        let observed = status::decode(
                            response_kind,
                            &status.packet,
                            &fields,
                            status.observed_at,
                        )?
                        .context("Managed status decoder became unavailable")?;
                        fields.extend(observed.fields);
                    } else {
                        fields.extend(status.fields.clone());
                    }
                } else {
                    source["last_observed"] = json!(status.fields);
                }
                metadata.insert(status.kind.as_str().into(), source);
            }
            for (kind, (attempted, error)) in &device.failures {
                let source = metadata
                    .entry(*kind)
                    .or_insert_with(|| json!({"fresh": false, "transport": "controller"}));
                source["last_attempt"] =
                    json!(attempted.to_rfc3339_opts(chrono::SecondsFormat::Micros, true));
                source["error"] = json!(error);
            }
            if let Some(source) = metadata.get("switch_configuration") {
                fields.insert("redundancy_probe_outcomes".into(), json!({
                    "switch_configuration": {
                        "outcome": if source["fresh"] == true { "response" } else if source.get("error").is_some() { "unavailable" } else { "stale" },
                        "observed_at": source["observed_at"], "error": source["error"],
                    }
                }));
            }
            fields.insert("managed_status".into(), Value::Object(metadata));
            projected.push(Projection {
                server: target.server_profile.clone(),
                domain: key.0.clone(),
                device: key.1.clone(),
                fields,
            });
        }
        Ok(projected)
    }

    pub fn deadline_after(&self, now: Instant) -> Option<Instant> {
        self.devices
            .values()
            .flat_map(|device| device.statuses.values())
            .filter(|status| status.fresh(now))
            .map(|status| status.deadline)
            .min()
    }
}

fn key(target: &Observation) -> Option<(String, String)> {
    Some((target.domain_id.clone()?, target.device.id.clone()))
}

fn key_matches(key: &(String, String), target: &Observation) -> bool {
    target.domain_id.as_ref() == Some(&key.0) && target.device.id == key.1
}
