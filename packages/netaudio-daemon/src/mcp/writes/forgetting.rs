use super::{CallError, NEXT_STEP, Prepared, PreparedKind, Records, required_text, views};
use crate::devices::registry::{DeviceRegistry, cache_precondition};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) struct PreparedForget {
    records: Vec<Arc<Value>>,
    routes: BTreeMap<String, Vec<String>>,
}

impl PreparedForget {
    pub(super) fn conditions(&self) -> BTreeMap<String, Value> {
        self.records
            .iter()
            .map(|record| {
                (
                    views::text(&record["server_name"]),
                    cache_precondition(record),
                )
            })
            .collect()
    }

    pub(super) async fn apply(
        self,
        registry: &Arc<DeviceRegistry>,
    ) -> Result<(Value, bool), CallError> {
        let forgotten = registry
            .forget(self.records)
            .await
            .map_err(|error| CallError::invalid(error.to_string()))?;
        let names: Vec<_> = forgotten
            .iter()
            .map(|record| {
                record["name"]
                    .as_str()
                    .or(record["server_name"].as_str())
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect();
        Ok((
            views::compact(json!({
                "forgotten": (!names.is_empty()).then_some(names),
                "result": forgotten.is_empty().then_some("nothing was off the network"),
                "routes_still_pointing_at_them": (!self.routes.is_empty()).then_some(self.routes),
                "note": "a forgotten device comes back by itself if it returns to the network; set_subscriptions clears routes that are no longer wanted",
            })),
            false,
        ))
    }
}

pub(super) fn prepare(request: &Value, records: &Records) -> Result<Prepared, CallError> {
    let targets = if request["every_device_off"] == true {
        records
            .values()
            .filter(|record| record["online"] == false)
            .cloned()
            .collect::<Vec<_>>()
    } else {
        let reference = required_text(request, "device")?;
        let record = views::resolve(records, reference)?
            .ok_or_else(|| CallError::invalid("device not found"))?;
        if record["online"] != false {
            return Err(CallError::invalid(format!(
                "{} is on the network; only a device that is off can be forgotten",
                views::text(&record["name"])
            )));
        }
        vec![Arc::new(record.clone())]
    };
    let mut routes = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut names = Vec::new();
    for record in &targets {
        let name = views::text(&record["name"]);
        names.push(match views::last_seen(record) {
            Some(seen) => format!("{name} ({seen})"),
            None => name.clone(),
        });
        let downstream = records
            .values()
            .flat_map(|receiver| {
                views::subscriptions(receiver)
                    .filter(|route| route["tx_device"] == name)
                    .map(|route| {
                        format!(
                            "{}:{} <- {}:{}",
                            views::text(&receiver["name"]),
                            views::text(&route["rx_channel"]),
                            name,
                            views::text(&route["tx_channel"])
                        )
                    })
            })
            .collect::<Vec<_>>();
        if !downstream.is_empty() {
            warnings.push(format!(
                "routes that point at {name} stay as they are: {}",
                downstream.join(", ")
            ));
            routes.insert(name, downstream);
        }
    }
    names.sort();
    Ok(Prepared {
        preview: views::compact(json!({
            "changed": false,
            "confirmation_required": true,
            "operation": "forget_device",
            "next": NEXT_STEP,
            "change": {
                "forget": if names.is_empty() { json!("nothing is off the network") } else { json!(names) },
                "comes_back": "a forgotten device returns by itself if it comes back on the network",
            },
            "warnings": (!warnings.is_empty()).then_some(warnings),
        })),
        devices: Vec::new(),
        kind: PreparedKind::Forget(PreparedForget {
            records: targets,
            routes,
        }),
    })
}
