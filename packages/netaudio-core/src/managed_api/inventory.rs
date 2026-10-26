use super::models::{Device, ReceiverChannel};
use crate::device_identity::canonical_device_mac;
use crate::receiver_capabilities::{self, Authority, Evidence, ReceiverCapabilityRequest};
use crate::subscription_status::{self, ManagedStatusRequest};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::{Arc, Weak};

pub const PERMISSIONS: [&str; 9] = [
    "aes67",
    "codec_control",
    "encoding",
    "identify",
    "locking",
    "redundancy",
    "sample_rate",
    "sample_rate_pullup",
    "static_ipv4",
];

#[derive(Clone, Serialize, PartialEq)]
pub struct Observation {
    pub device: Device,
    pub domain_id: Option<String>,
    pub domain_name: Option<String>,
    pub observed_at: Option<f64>,
    pub fresh: bool,
    pub server_profile: String,
    pub context_name: Option<String>,
    pub operation_permissions: Option<BTreeSet<String>>,
}

impl Observation {
    pub fn identity(&self) -> String {
        format!(
            "ddm:{}:{}:{}",
            self.server_profile,
            self.domain_id.as_deref().unwrap_or("unenrolled"),
            self.device.id
        )
    }

    pub fn record(&self) -> Value {
        let device = &self.device;
        let identity = device.identity.as_ref();
        let connection = device.connection.as_ref();
        let connection_state = connection.and_then(|connection| connection.state.as_deref());
        let normalized = connection_state.map(str::to_ascii_uppercase);
        let ready = self.fresh && normalized.as_deref() == Some("READY");
        let availability = if !self.fresh {
            "unknown"
        } else {
            match normalized.as_deref() {
                Some("READY") => "online",
                Some("DISCONNECTED") => "offline",
                _ => "unknown",
            }
        };
        let interfaces: Option<Vec<_>> = device
            .interfaces
            .as_ref()
            .map(|interfaces| interfaces.iter().flatten().collect());
        let mac_address = interfaces.as_ref().and_then(|interfaces| {
            interfaces
                .iter()
                .find_map(|interface| nonempty(interface.mac_address.as_deref()))
        });
        let addresses = managed_addresses(device);
        let address = addresses.first().map(String::as_str).unwrap_or("None");
        let channels = channels(device, self.fresh);
        let receiver_support = support(&channels["receivers"]);
        let permissions = self.operation_permissions.as_ref().map(|permissions| {
            PERMISSIONS
                .iter()
                .map(|permission| {
                    (
                        (*permission).to_owned(),
                        json!(permissions.contains(*permission)),
                    )
                })
                .collect::<Map<_, _>>()
        });
        let mut record = json!({
            "channels": channels, "ipv4": address, "name": device_name(device), "online": ready,
            "availability_state": availability, "server_name": self.identity(), "services": {},
            "self_connection_support": receiver_support, "subscriptions": subscriptions(device),
            "mac_address": mac_address,
            "cmc_source_identifier": null, "cmc_process": null, "cmc_process_state": "none",
            "cmc_txt_id_raw": null, "cmc_txt_process_raw": null,
            "association_state": "none", "association_generation": null,
            "manufacturer": device.manufacturer.as_ref().map(|item| item.name.as_str()).unwrap_or(""),
            "model": identity.map(|item| item.product_model_name.as_deref()).unwrap_or_else(|| Some(device.product.as_ref().map(|item| item.name.as_str()).unwrap_or(""))),
            "model_id": identity.and_then(|item| item.product_model_id.as_deref()),
            "dante_model": device.platform.as_ref().map(|item| item.name.as_str()).unwrap_or(""),
            "dante_model_id": device.platform.as_ref().map(|item| item.platform_id.as_deref()).unwrap_or(Some("")),
        });
        let versions = json!({
            "ddm_product_version": identity.and_then(|item| item.product_version.as_deref()),
            "ddm_product_software_version": identity.and_then(|item| item.product_software_version.as_deref()),
            "ddm_dante_version": identity.and_then(|item| item.dante_version.as_deref()),
            "ddm_dante_hardware_version": identity.and_then(|item| item.dante_hardware_version.as_deref()),
        });
        let topology = json!({
            "tx_count": device.tx_channels.as_ref().map(Vec::len), "rx_count": device.rx_channels.as_ref().map(Vec::len),
            "preferred_leader": device.clock_preferences.as_ref().and_then(|item| item.leader),
            "interfaces": interfaces, "direct_control_available": false,
            "field_sources": {
                "identity": "ddm", "channels": "ddm", "subscriptions": "ddm", "availability": "ddm", "clock": "ddm",
                "ddm_product_version": "ddm", "ddm_product_software_version": "ddm", "ddm_dante_version": "ddm", "ddm_dante_hardware_version": "ddm",
            },
        });
        let managed = json!({
            "inventory_id": self.identity(), "inventory_sources": ["ddm"],
            "management_state": if self.domain_id.is_some() { "managed" } else { "unenrolled" },
            "requires_managed_control": self.domain_id.is_some()
                || device.enrolment_state.as_deref().is_some_and(|state| {
                    state.eq_ignore_ascii_case("ENROLLED") || state.eq_ignore_ascii_case("MANAGED")
                }),
            "control_transports": ["ddm"], "ddm_device_id": device.id, "ddm_server_profile": self.server_profile,
            "ddm_context": self.context_name, "managed_operation_permissions": permissions,
            "ddm_domain_id": self.domain_id, "ddm_domain_name": self.domain_name,
            "ddm_enrolment_state": device.enrolment_state,
            "ddm_connection_state": connection_state,
            "ddm_connection_last_changed": connection.and_then(|item| item.last_changed.as_deref()),
            "ddm_last_sync": self.observed_at, "ddm_fresh": self.fresh,
            "ddm_identity": device.identity, "ddm_status": device.status, "ddm_capabilities": device.capabilities,
            "ddm_clock_preferences": device.clock_preferences, "ddm_clocking_state": device.clocking_state,
            "ddm_parameters": device.parameters, "ddm_inputs": device.inputs, "ddm_outputs": device.outputs,
        });
        for section in [versions, topology, managed] {
            record
                .as_object_mut()
                .expect("constructed device record")
                .extend(section.as_object().expect("constructed section").clone());
        }
        record
            .as_object_mut()
            .expect("constructed device record")
            .retain(|_, value| !value.is_null());
        record
    }
}

#[derive(Default)]
pub struct Projection {
    direct: BTreeMap<String, DirectProjection>,
}

struct DirectProjection {
    source: Weak<Value>,
    record: Arc<Value>,
}

impl Projection {
    pub fn invalidate(&mut self, name: &str) {
        self.direct.remove(name);
    }

    pub fn merge(
        &mut self,
        direct: &BTreeMap<String, Arc<Value>>,
        observations: &[Observation],
    ) -> Result<BTreeMap<String, Arc<Value>>, &'static str> {
        if direct.values().any(|record| !record.is_object()) {
            return Err("Direct device records must be objects");
        }
        let candidates: Vec<BTreeSet<&String>> = observations
            .iter()
            .map(|observation| {
                let addresses = managed_macs(&observation.device);
                let mut candidates: BTreeSet<_> = direct
                    .iter()
                    .filter(|(_, record)| {
                        record["ddm_device_id"].as_str() == Some(&observation.device.id)
                            && record["ddm_server_profile"].as_str()
                                == Some(&observation.server_profile)
                            && (addresses.is_empty()
                                || direct_macs(record).is_empty()
                                || !addresses.is_disjoint(&direct_macs(record)))
                    })
                    .map(|(key, _)| key)
                    .collect();
                if candidates.is_empty() {
                    candidates = direct
                        .iter()
                        .filter(|(_, record)| !addresses.is_disjoint(&direct_macs(record)))
                        .map(|(key, _)| key)
                        .collect();
                }
                if candidates.is_empty() {
                    let addresses = managed_addresses(&observation.device);
                    candidates = direct
                        .iter()
                        .filter(|(_, record)| {
                            direct_macs(record).is_empty()
                                && record["ipv4"]
                                    .as_str()
                                    .is_some_and(|address| addresses.contains(address))
                        })
                        .map(|(key, _)| key)
                        .collect();
                }
                candidates
            })
            .collect();
        let matches: BTreeMap<usize, &String> = candidates
            .iter()
            .enumerate()
            .filter_map(|(index, values)| {
                if values.len() != 1 {
                    return None;
                }
                let candidate = *values.first()?;
                (candidates
                    .iter()
                    .filter(|other| other.contains(candidate))
                    .count()
                    == 1)
                    .then_some((index, candidate))
            })
            .collect();
        self.direct.retain(|key, cached| {
            direct
                .get(key)
                .is_some_and(|record| cached.source.ptr_eq(&Arc::downgrade(record)))
        });
        let mut merged = BTreeMap::new();
        for (key, record) in direct {
            if matches.values().any(|candidate| *candidate == key) {
                continue;
            }
            let projected = match self.direct.get(key) {
                Some(cached) => Arc::clone(&cached.record),
                None => {
                    let projected = Arc::new(direct_record(key, record));
                    self.direct.insert(
                        key.clone(),
                        DirectProjection {
                            source: Arc::downgrade(record),
                            record: Arc::clone(&projected),
                        },
                    );
                    projected
                }
            };
            merged.insert(key.clone(), projected);
        }
        for (index, observation) in observations.iter().enumerate() {
            let record = observation.record();
            if let Some(key) = matches.get(&index) {
                let mut record = overlay(&direct[*key], &record);
                record["server_name"] = json!(key);
                merged.insert((*key).clone(), Arc::new(record));
            } else {
                let mut key = observation.identity();
                if merged.contains_key(&key) {
                    key = format!("{key}:{index}");
                }
                let mut record = record;
                record["server_name"] = json!(key);
                merged.insert(key, Arc::new(record));
            }
        }
        Ok(merged)
    }
}

fn direct_record(key: &str, record: &Value) -> Value {
    let mut record = record.clone();
    if record["management_state"] == "managed" {
        record["online"] = json!(false);
        record["availability_state"] = json!("unknown");
        record["control_transports"] = json!(["ddm"]);
    } else {
        record["inventory_id"] = json!(direct_macs(&record)
            .first()
            .cloned()
            .unwrap_or_else(|| format!("direct:{key}")));
        record["inventory_sources"] = json!(["direct"]);
        record["management_state"] = Value::Null;
        record["control_transports"] = json!(["direct"]);
        record["direct_control_available"] = json!(true);
        record["availability_state"] = json!(if record["online"] == true {
            "online"
        } else {
            "offline"
        });
        record["field_sources"] = json!({"identity": "direct", "audio_configuration": "direct"});
    }
    record
}

fn direct_macs(record: &Value) -> BTreeSet<String> {
    if let Some(address) = record["mac_address"]
        .as_str()
        .and_then(canonical_device_mac)
    {
        return BTreeSet::from([address]);
    }
    record["interfaces"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|interface| {
            [
                interface["mac_address"].as_str(),
                interface["macAddress"].as_str(),
            ]
        })
        .flatten()
        .filter_map(canonical_device_mac)
        .collect()
}

pub fn primary_mac(device: &Device) -> Option<String> {
    device
        .interfaces
        .as_ref()?
        .first()?
        .as_ref()?
        .mac_address
        .as_deref()
        .and_then(canonical_device_mac)
}

fn managed_macs(device: &Device) -> BTreeSet<String> {
    device
        .interfaces
        .iter()
        .flatten()
        .flatten()
        .filter_map(|interface| {
            interface
                .mac_address
                .as_deref()
                .and_then(canonical_device_mac)
        })
        .collect()
}

fn managed_addresses(device: &Device) -> BTreeSet<String> {
    device
        .interfaces
        .iter()
        .flatten()
        .flatten()
        .filter_map(|interface| interface.address.as_deref()?.parse::<IpAddr>().ok())
        .filter(|address| !address.is_unspecified())
        .map(|address| address.to_string())
        .collect()
}

pub fn device_name(device: &Device) -> &str {
    nonempty(Some(&device.name))
        .or_else(|| {
            device.identity.as_ref().and_then(|identity| {
                nonempty(identity.actual_name.as_deref())
                    .or_else(|| nonempty(identity.default_name.as_deref()))
            })
        })
        .unwrap_or(&device.id)
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

fn channels(device: &Device, fresh: bool) -> Value {
    let receivers: Map<_, _> = device.rx_channels.iter().flatten().map(|channel| {
        let mut record = json!({
            "name": channel.name, "ddm_channel_id": channel.id, "ddm_media_type": channel.media_type,
            "ddm_signal_presence": channel.signal_presence, "ddm_enabled": channel.enabled,
            "ddm_status": channel.status, "ddm_status_message": channel.status_message,
            "ddm_summary": channel.summary, "ddm_encryption_scheme": channel.encryption_scheme,
            "can_subscribe_self": channel.can_subscribe_self.filter(|_| fresh),
            "receiver_flags": null, "receiver_capability_flags": null,
            "managed_can_subscribe_self": channel.can_subscribe_self, "managed_can_subscribe_self_fresh": fresh,
        });
        record.as_object_mut().expect("constructed channel").retain(|key, value| !value.is_null() || matches!(key.as_str(), "can_subscribe_self" | "receiver_flags" | "receiver_capability_flags"));
        (channel.index.to_string(), record)
    }).collect();
    let transmitters: Map<_, _> = device.tx_channels.iter().flatten().map(|channel| {
        let mut record = json!({"name": channel.name, "ddm_channel_id": channel.id, "ddm_media_type": channel.media_type, "ddm_signal_presence": channel.signal_presence, "ddm_encryption_policy": channel.encryption_policy});
        record.as_object_mut().expect("constructed channel").retain(|_, value| !value.is_null());
        (channel.index.to_string(), record)
    }).collect();
    json!({"receivers": receivers, "transmitters": transmitters})
}

fn subscriptions(device: &Device) -> Vec<Value> {
    device
        .rx_channels
        .iter()
        .flatten()
        .filter(|channel| {
            nonempty(channel.subscribed_device.as_deref()).is_some()
                || nonempty(channel.subscribed_channel.as_deref()).is_some()
                || channel
                    .status
                    .as_ref()
                    .is_some_and(|status| !status.eq_ignore_ascii_case("none"))
                || channel
                    .summary
                    .as_deref()
                    .is_some_and(|summary| summary != "NONE")
        })
        .map(|channel| subscription(device, channel))
        .collect()
}

fn subscription(device: &Device, channel: &ReceiverChannel) -> Value {
    let request: ManagedStatusRequest = serde_json::from_value(json!({
        "status": channel.status, "status_message": channel.status_message, "summary": channel.summary,
    })).expect("typed managed subscription fields");
    let mut status = json!(subscription_status::managed_status(request));
    status["code"] = Value::Null;
    status["icon"] = json!("");
    let self_connection = channel.subscribed_device.as_deref() == Some(".");
    let mut record = json!({
        "rx_channel": channel.name, "rx_channel_number": channel.index, "rx_device": device_name(device),
        "tx_channel": channel.subscribed_channel,
        "tx_device": if self_connection { Some(device_name(device)) } else { channel.subscribed_device.as_deref() },
        "status": status, "ddm_status": channel.status, "ddm_status_message": channel.status_message, "ddm_summary": channel.summary,
    });
    if self_connection {
        record["self_connection"] = json!(true);
    }
    record
}

fn support(channels: &Value) -> &'static str {
    receiver_capabilities::evaluate(ReceiverCapabilityRequest {
        authority: Authority::Observed,
        channels: channels
            .as_object()
            .into_iter()
            .flat_map(Map::values)
            .map(|channel| Evidence {
                direct: channel["can_subscribe_self"].as_bool(),
                managed: None,
                managed_fresh: false,
            })
            .collect(),
    })
    .support
}

fn overlay(direct: &Value, managed: &Value) -> Value {
    let mut merged = direct.clone();
    let managed_authority = managed["management_state"] == "managed";
    let subscriptions_observed = !managed_authority
        && (direct["subscriptions"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
            || direct["channels"]["receivers"]
                .as_object()
                .is_some_and(|items| !items.is_empty()));
    merged
        .as_object_mut()
        .expect("device object")
        .retain(|key, _| !key.starts_with("ddm_"));
    for (key, value) in managed.as_object().expect("managed device object") {
        if key.starts_with("ddm_")
            || matches!(
                key.as_str(),
                "availability_state"
                    | "inventory_id"
                    | "managed_operation_permissions"
                    | "management_state"
            )
        {
            merged[key] = value.clone();
        }
    }
    merged["inventory_sources"] = json!(["direct", "ddm"]);
    merged["requires_managed_control"] = json!(
        direct["requires_managed_control"] == true || managed["requires_managed_control"] == true
    );
    merged["control_transports"] = json!(["direct", "ddm"]);
    merged["direct_control_available"] = json!(true);
    for key in [
        "name",
        "manufacturer",
        "model",
        "model_id",
        "dante_model",
        "dante_model_id",
        "ipv4",
    ] {
        let absent = |value: &Value| value.is_null() || value == "" || value == "None";
        if absent(&merged[key]) && !absent(&managed[key]) {
            merged[key] = managed[key].clone();
        }
    }
    if managed_authority {
        merged["name"] = managed["name"].clone();
    }
    if let Some(value) = managed.get("preferred_leader") {
        merged["preferred_leader"] = value.clone();
    }
    let online = direct["online"] == true || managed["online"] == true;
    merged["online"] = json!(online);
    merged["availability_state"] = if online {
        json!("online")
    } else {
        managed["availability_state"].clone()
    };
    merged["field_sources"] = managed["field_sources"].clone();
    merged["field_sources"]["identity"] = json!("direct");
    merged["field_sources"]["name"] = json!(if managed_authority { "ddm" } else { "direct" });
    merged["field_sources"]["audio_configuration"] = json!("direct");
    merged["field_sources"]["channels"] = json!(if direct["channels"]
        .as_object()
        .is_some_and(|value| !value.is_empty())
    {
        "direct"
    } else {
        "ddm"
    });
    merged["field_sources"]["subscriptions"] = json!(if subscriptions_observed {
        "direct"
    } else {
        "ddm"
    });
    if !merged["channels"].is_object() {
        merged["channels"] = json!({});
    }
    for direction in ["receivers", "transmitters"] {
        if merged["channels"][direction]
            .as_object()
            .is_none_or(Map::is_empty)
        {
            merged["channels"][direction] = managed["channels"][direction].clone();
            continue;
        }
        for (number, managed_channel) in managed["channels"][direction]
            .as_object()
            .into_iter()
            .flat_map(Map::iter)
        {
            let Some(channel) = merged["channels"][direction]
                .get_mut(number)
                .and_then(Value::as_object_mut)
            else {
                continue;
            };
            for (key, value) in managed_channel.as_object().expect("managed channel") {
                if key.starts_with("ddm_") || key.starts_with("managed_") {
                    channel.insert(key.clone(), value.clone());
                }
            }
            if direction == "receivers" {
                let result = receiver_capabilities::evaluate(ReceiverCapabilityRequest {
                    authority: Authority::Observed,
                    channels: vec![Evidence {
                        direct: channel
                            .get("direct_can_subscribe_self")
                            .and_then(Value::as_bool),
                        managed: managed_channel["managed_can_subscribe_self"].as_bool(),
                        managed_fresh: managed_channel["managed_can_subscribe_self_fresh"] == true,
                    }],
                });
                channel.insert(
                    "can_subscribe_self".into(),
                    json!(result.channels[0].supported),
                );
                if result.channels[0].conflict {
                    channel.insert("can_subscribe_self_conflict".into(), json!(true));
                } else {
                    channel.remove("can_subscribe_self_conflict");
                }
            }
        }
    }
    merged["self_connection_support"] = json!(support(&merged["channels"]["receivers"]));
    if !subscriptions_observed {
        merged["subscriptions"] = managed["subscriptions"].clone();
    }
    merged
}
