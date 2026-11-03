use crate::events::{
    DEVICE_DISCOVERED, DEVICE_PATCH, DEVICE_REMOVED, DEVICE_UPDATED, Event, MANAGED_STATUS,
    SNAPSHOT,
};
use crate::managed::sessions::Caller;
use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeSet;

const MEMBERSHIP_FIELDS: [&str; 2] = ["ddm_domain_id", "ddm_server_profile"];

pub enum Filtered {
    Keep,
    Replace(Bytes),
    Resynchronize,
    Skip,
}

pub struct EventFilter {
    caller: Caller,
    hidden: BTreeSet<String>,
}

impl EventFilter {
    pub fn new(caller: Caller) -> Self {
        Self {
            caller,
            hidden: BTreeSet::new(),
        }
    }

    pub fn apply(&mut self, event: &Event) -> Filtered {
        let value = event.value.as_ref();
        let name = value["server_name"].as_str();
        match value["event"].as_str().unwrap_or_default() {
            SNAPSHOT => {
                let mut value = value.clone();
                self.hidden.clear();
                if let Some(devices) = value.get_mut("devices").and_then(Value::as_object_mut) {
                    devices.retain(|name, record| {
                        let visible = self.caller.sees_device(record);
                        if !visible {
                            self.hidden.insert(name.clone());
                        }
                        visible
                    });
                }
                self.scope_managed(&mut value);
                encode(&value)
            }
            MANAGED_STATUS => {
                let mut value = value.clone();
                self.scope_managed(&mut value);
                encode(&value)
            }
            DEVICE_DISCOVERED | DEVICE_UPDATED => {
                let (Some(name), Some(record)) = (name, value.get("device")) else {
                    return Filtered::Keep;
                };
                if self.caller.sees_device(record) {
                    if self.hidden.remove(name) && value["event"] == DEVICE_UPDATED {
                        return Filtered::Resynchronize;
                    }
                    Filtered::Keep
                } else if self.hidden.insert(name.to_owned()) {
                    encode(&json!({"event": DEVICE_REMOVED, "revision": value["revision"], "server_name": name}))
                } else {
                    Filtered::Skip
                }
            }
            DEVICE_PATCH => {
                let moved = MEMBERSHIP_FIELDS.iter().any(|field| {
                    value["changed"].get(field).is_some()
                        || value["removed"]
                            .as_array()
                            .is_some_and(|removed| removed.iter().any(|entry| entry == field))
                });
                if moved {
                    Filtered::Resynchronize
                } else if name.is_some_and(|name| self.hidden.contains(name)) {
                    Filtered::Skip
                } else {
                    Filtered::Keep
                }
            }
            DEVICE_REMOVED => {
                if name.is_some_and(|name| self.hidden.remove(name)) {
                    Filtered::Skip
                } else {
                    Filtered::Keep
                }
            }
            _ => {
                if name.is_some_and(|name| self.hidden.contains(name)) {
                    Filtered::Skip
                } else {
                    Filtered::Keep
                }
            }
        }
    }

    fn scope_managed(&self, value: &mut Value) {
        if let Some(domains) = value
            .get_mut("managed_domains")
            .and_then(Value::as_array_mut)
        {
            domains.retain(|domain| self.caller.sees_domain_record(domain));
        }
        if let Some(managed) = value.get_mut("managed").filter(|managed| managed.is_object()) {
            managed["default_context"] = json!(match &self.caller {
                Caller::Session(session) => session.context(),
                _ => None,
            });
        }
    }
}

fn encode(value: &Value) -> Filtered {
    let mut encoded = b"data: ".to_vec();
    if serde_json::to_writer(&mut encoded, value).is_err() {
        return Filtered::Resynchronize;
    }
    encoded.extend_from_slice(b"\n\n");
    Filtered::Replace(Bytes::from(encoded))
}
