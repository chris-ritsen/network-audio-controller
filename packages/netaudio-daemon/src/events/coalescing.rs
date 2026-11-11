use super::{
    DEVICE_DISCOVERED, DEVICE_PATCH, DEVICE_REMOVED, DEVICE_UPDATED, METER_VALUES,
    SHURE_METER_VALUES, TELEMETRY_UPDATED,
};
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Meter,
    Notice,
    State,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slot {
    pub class: Class,
    pub subject: Arc<str>,
}

pub enum Combined {
    Incoming,
    Rebuilt(Value),
}

pub fn slot(value: &Value) -> Option<Slot> {
    let kind = value.get("event")?.as_str()?;
    let (class, subject) = match kind {
        DEVICE_DISCOVERED | DEVICE_PATCH | DEVICE_REMOVED | DEVICE_UPDATED => {
            (Class::State, value.get("server_name")?.as_str()?.to_owned())
        }
        METER_VALUES => (Class::Meter, value.get("server_name")?.as_str()?.to_owned()),
        SHURE_METER_VALUES => (
            Class::Meter,
            format!(
                "shure:{}:{}:{}",
                value.get("mac")?.as_str()?,
                value.get("channel")?,
                value["key"].as_str().unwrap_or("sample")
            ),
        ),
        TELEMETRY_UPDATED => (
            if value.get("telemetry").is_some_and(Value::is_object) {
                Class::State
            } else {
                Class::Notice
            },
            value.get("server_name")?.as_str()?.to_owned(),
        ),
        _ => return None,
    };

    Some(Slot {
        class,
        subject: subject.into(),
    })
}

pub fn combine(class: Class, pending: &Value, incoming: &Value) -> Option<Combined> {
    match class {
        Class::Meter => Some(Combined::Incoming),
        Class::Notice => {
            let fields: BTreeSet<&str> = [pending, incoming]
                .into_iter()
                .filter_map(|event| event.get("fields")?.as_array())
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            let mut combined = incoming.clone();
            combined["fields"] = json!(fields);
            Some(Combined::Rebuilt(combined))
        }
        Class::State => combine_state(pending, incoming),
    }
}

fn combine_state(pending: &Value, incoming: &Value) -> Option<Combined> {
    let earlier = pending.get("event")?.as_str()?;

    match incoming.get("event")?.as_str()? {
        DEVICE_UPDATED if matches!(earlier, DEVICE_DISCOVERED | DEVICE_REMOVED) => {
            let mut combined = incoming.clone();
            combined["event"] = json!(DEVICE_DISCOVERED);
            Some(Combined::Rebuilt(combined))
        }
        DEVICE_DISCOVERED | DEVICE_REMOVED | DEVICE_UPDATED => Some(Combined::Incoming),
        DEVICE_PATCH => {
            let changed = incoming.get("changed")?.as_object()?;
            let removed = incoming.get("removed")?.as_array()?;

            match earlier {
                DEVICE_DISCOVERED | DEVICE_UPDATED => {
                    let mut combined = pending.clone();
                    let record = combined.get_mut("device")?.as_object_mut()?;
                    record.extend(changed.clone());

                    for field in removed.iter().filter_map(Value::as_str) {
                        record.remove(field);
                    }

                    Some(Combined::Rebuilt(combined))
                }
                DEVICE_PATCH => {
                    let mut fields = pending.get("changed")?.as_object()?.clone();
                    let mut absent: BTreeSet<&str> = pending
                        .get("removed")?
                        .as_array()?
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|field| !changed.contains_key(*field))
                        .collect();
                    fields.extend(changed.clone());

                    for field in removed.iter().filter_map(Value::as_str) {
                        fields.remove(field);
                        absent.insert(field);
                    }

                    let mut combined = incoming.clone();
                    combined["changed"] = Value::Object(fields);
                    combined["removed"] = json!(absent);
                    Some(Combined::Rebuilt(combined))
                }
                _ => None,
            }
        }
        TELEMETRY_UPDATED => {
            let telemetry = incoming.get("telemetry")?.as_object()?;

            match earlier {
                DEVICE_DISCOVERED | DEVICE_UPDATED => {
                    let mut combined = pending.clone();
                    apply(combined.get_mut("device")?.as_object_mut()?, telemetry);
                    Some(Combined::Rebuilt(combined))
                }
                TELEMETRY_UPDATED => {
                    let mut fields = pending.get("telemetry")?.as_object()?.clone();
                    fields.extend(telemetry.clone());
                    let mut combined = incoming.clone();
                    combined["fields"] = json!(fields.keys().collect::<Vec<_>>());
                    combined["telemetry"] = Value::Object(fields);
                    Some(Combined::Rebuilt(combined))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn apply(record: &mut Map<String, Value>, telemetry: &Map<String, Value>) {
    for (field, value) in telemetry {
        if value.is_null() {
            record.remove(field);
        } else {
            record.insert(field.clone(), value.clone());
        }
    }
}
