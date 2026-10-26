use super::{Records, clock_view, compact, first, subscriptions, text, values};
use serde_json::{Value, json};
use std::collections::BTreeSet;

const NANOSECONDS_PER_MILLISECOND: f64 = 1_000_000.0;
const LATENCY_WARNING_FRACTION: f64 = 0.8;

pub fn diagnostics_view(payload: &Value, record: &Value, records: &Records) -> Value {
    let status = &payload["clock_state"];
    let observations = &payload["clock"];
    let leader_identity = first(&[&record["ptpv1_master_uuid"], &status["ptpv1_master_uuid"]]);
    let leader = records
        .values()
        .find(|device| !leader_identity.is_null() && device["ptpv1_device_uuid"] == leader_identity)
        .map(|device| &device["name"])
        .unwrap_or(&leader_identity);
    let role = values(&status["base_ports"])
        .next()
        .map(|port| &port["role"])
        .unwrap_or(&Value::Null);
    let offset = [&observations["heartbeat"], &observations["conmon"]]
        .into_iter()
        .filter(|series| series["current"].is_object())
        .max_by(|left, right| {
            let freshness = (left["fresh"] == true).cmp(&(right["fresh"] == true));
            freshness.then_with(|| {
                left["current"]["observed_monotonic"]
                    .as_f64()
                    .unwrap_or_default()
                    .total_cmp(
                        &right["current"]["observed_monotonic"]
                            .as_f64()
                            .unwrap_or_default(),
                    )
            })
        })
        .map_or(Value::Null, |series| series_summary(series, 1.0));
    let mut clock = compact(json!({
        "state": status["clock_state"],
        "servo": status["servo_state"],
        "source": status["clock_source"],
        "role": role,
        "preferred_leader": status["preferred_leader"],
        "leader": leader,
        "offset_ppm": offset,
        "offset_variation_warning": values(&observations["variation"]).any(|entry| entry["active"] == true).then_some(true),
    }));

    if clock.get("role").is_none() {
        let fallback = clock_view(record);

        for field in ["role", "synchronization", "preferred_leader", "role_source"] {
            if clock.get(field).is_none()
                && let Some(value) = fallback.get(field)
            {
                clock[field] = value.clone();
            }
        }
    }

    let receiver = &payload["receiver"];
    let interfaces: BTreeSet<_> = values(&receiver["paths"])
        .map(|path| text(&path["network_interface_index"]))
        .collect();
    let mut flows = Vec::new();
    let mut unattributed = 0;

    for path in values(&receiver["paths"]) {
        let flow = &path["evidence"]["flow"];

        if path["attribution_status"] != "resolved" || !flow.is_object() {
            unattributed += 1;
            continue;
        }

        let channels: Vec<_> = values(&flow["receiver_channel_numbers_by_flow_channel"])
            .flat_map(values)
            .cloned()
            .collect();
        let sources: BTreeSet<_> = subscriptions(record)
            .filter(|route| channels.contains(&route["rx_channel_number"]))
            .map(|route| text(&route["tx_device"]))
            .collect();
        let labels = super::flows::labels(&channels, &record["channels"]["receivers"]);
        let budget = path["evidence"]["configured_latency_nanoseconds"]
            .as_f64()
            .map(|value| value / NANOSECONDS_PER_MILLISECOND);
        let latency = series_summary(&path["latency"], 1.0 / NANOSECONDS_PER_MILLISECOND);
        let late = &path["late_packets"];
        let recent = late["increase_since_baseline"].as_i64();
        let interface = path["network_interface_index"].as_u64();
        let mut view = compact(json!({
            "flow": path["audio_receiver_flow_id"],
            "interface": (interfaces.len() > 1).then(|| if interface == Some(0) { "primary" } else { "secondary" }),
            "type": flow["flow_type"],
            "from": (!sources.is_empty()).then_some(sources),
            "channels": labels,
            "latency_budget_ms": budget,
            "latency_ms": latency,
            "late_packets_device_total": late["current"]["value"].as_f64().map(|value| value as i64),
            "late_packets_since_watching": recent,
            "fresh": path["latency"]["fresh"] == true && late["fresh"] == true,
        }));

        if let (Some(worst), Some(budget)) = (latency["max"].as_f64(), budget)
            && budget > 0.0
            && worst > LATENCY_WARNING_FRACTION * budget
        {
            view["problem"] = json!(
                "packets arrive close to the latency budget; raise the latency or check the network"
            );
        }

        if let Some(recent) = recent.filter(|count| *count > 0) {
            view["problem"] = json!(format!(
                "{recent} late packets since the server started watching"
            ));
        }

        flows.push(view);
    }

    let receive = if !receiver.is_object() {
        json!("no receive telemetry seen from this device")
    } else if flows.is_empty() && unattributed > 0 {
        json!("receive telemetry observed; flow attribution unavailable")
    } else {
        json!(flows)
    };
    compact(json!({
        "device": first(&[&record["name"], &payload["device"]]),
        "clock": clock,
        "receive_flows": receive,
        "unattributed_receive_flow_slots": (unattributed > 0).then_some(unattributed),
        "detail": "detail=debug returns the raw evidence without history samples",
    }))
}

pub fn diagnostics_summary(view: &Value, record: &Value) -> Value {
    let flows = view["receive_flows"].as_array();
    let worst = flows
        .into_iter()
        .flatten()
        .filter_map(|flow| {
            let latency = flow["latency_ms"]["max"].as_f64()?;
            let budget = flow["latency_budget_ms"]
                .as_f64()
                .filter(|budget| *budget > 0.0)?;
            Some((latency, budget))
        })
        .max_by(|left, right| (left.0 / left.1).total_cmp(&(right.0 / right.1)));
    let clock = &view["clock"];
    let clock_text = if clock["role"] == "Leader" {
        "Leader".to_owned()
    } else {
        let fields = [
            clock["role"].as_str(),
            clock["servo"].as_str().or_else(|| clock["state"].as_str()),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        if fields.is_empty() {
            "not reported".to_owned()
        } else {
            fields.join(", ")
        }
    };
    let measured = flows.map_or_else(|| view["receive_flows"].clone(), |flows| json!(flows.len()));
    let late = flows.filter(|flows| !flows.is_empty()).and_then(|flows| {
        flows
            .iter()
            .map(|flow| flow["late_packets_since_watching"].as_i64())
            .sum::<Option<i64>>()
    });
    compact(json!({
        "device": record["name"],
        "receive_flows": measured,
        "worst_latency_ms": worst.map(|(latency, _)| latency),
        "latency_budget_ms": worst.map(|(_, budget)| budget),
        "late_packets_since_watching": late,
        "clock": clock_text,
    }))
}

pub fn diagnostic_evidence(value: Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .filter(|(key, _)| key != "history")
                .map(|(key, value)| (key, diagnostic_evidence(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(diagnostic_evidence).collect()),
        value => value,
    }
}

fn series_summary(series: &Value, scale: f64) -> Value {
    if series["current"].is_null() {
        return Value::Null;
    }

    let scaled = |value: &Value| {
        value
            .as_f64()
            .map(|value| (value * scale * 1000.0).round_ties_even() / 1000.0)
    };
    compact(json!({
        "now": scaled(&series["current"]["value"]),
        "mean": scaled(&series["statistics"]["mean"]),
        "min": scaled(&series["statistics"]["minimum"]),
        "max": scaled(&series["statistics"]["maximum"]),
        "samples": series["statistics"]["count"],
        "since": series["statistics"]["window_start"],
        "fresh": series["fresh"],
    }))
}
