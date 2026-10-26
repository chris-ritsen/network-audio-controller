use super::{Records, channel_names, compact, first, subscriptions, text, values};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn labels(numbers: &[Value], channels: &Value) -> Vec<String> {
    let names: BTreeMap<_, _> = channel_names(channels).into_iter().collect();
    numbers
        .iter()
        .map(|number| {
            let number = text(number);
            names
                .get(number.as_str())
                .map_or_else(|| number.clone(), |name| format!("{number} {}", text(name)))
        })
        .collect()
}

pub fn flows_view(device: &Value, records: &Records) -> Value {
    let addresses: BTreeMap<_, _> = records
        .iter()
        .filter_map(|(key, device)| {
            device["ipv4"]
                .as_str()
                .map(|address| (address, device["name"].as_str().unwrap_or(key)))
        })
        .collect();
    let transmit = if device["transmitter_flows"].is_null() {
        json!("not read since the server started; get_transmit_flows reads them")
    } else {
        values(&device["transmitter_flows"])
            .map(|flow| transmit_flow(flow, &device["channels"]["transmitters"], &addresses))
            .collect()
    };
    let receive = if device["receiver_flows"].is_null() {
        json!("not read since the server started")
    } else {
        values(&device["receiver_flows"]).map(|flow| {
            let numbers: Vec<_> = values(&flow["receiver_channel_numbers_by_flow_channel"]).flat_map(values).cloned().collect();
            let sources: BTreeSet<_> = subscriptions(device).filter(|route| numbers.contains(&route["rx_channel_number"])).map(|route| text(&route["tx_device"])).collect();
            compact(json!({
                "id": flow["flow_number"],
                "type": flow["flow_type"],
                "from": (!sources.is_empty()).then_some(sources),
                "channels": labels(&numbers, &device["channels"]["receivers"]),
                "latency_ms": flow["latency_nanoseconds"].as_f64().map(|latency| latency / 1_000_000.0),
            }))
        }).collect()
    };
    json!({"transmit": transmit, "receive": receive})
}

fn transmit_flow(flow: &Value, channels: &Value, addresses: &BTreeMap<&str, &str>) -> Value {
    let numbers: Vec<Value> = if flow["channel_slots"].is_array() {
        values(&flow["channel_slots"])
            .map(|slot| slot["transmitter_channel"].clone())
            .filter(super::truthy)
            .collect()
    } else {
        values(&flow["channels"])
            .filter(|number| super::truthy(number))
            .cloned()
            .collect()
    };
    let destination = &flow["primary_destination"];
    let endpoint = destination["address"].as_str().map(|address| {
        if let Some(name) = addresses.get(address) {
            name.to_string()
        } else if super::truthy(&destination["port"]) {
            format!("{address}:{}", text(&destination["port"]))
        } else {
            address.to_owned()
        }
    });
    compact(json!({
        "id": first(&[&flow["flow_number"], &flow["identity"]["global_flow_id"]]),
        "type": flow["flow_type"],
        "to": endpoint,
        "channels": labels(&numbers, channels),
        "sample_rate_hz": first(&[&flow["sample_rate_hz"], &flow["sample_rate"]]),
        "encoding_bits": first(&[&flow["encoding_bits"], &flow["encoding"]]),
    }))
}

pub fn transmit_flows_view(inventory: &Value, device: &Value, records: &Records) -> Value {
    let addresses: BTreeMap<_, _> = records
        .iter()
        .filter_map(|(key, record)| {
            record["ipv4"]
                .as_str()
                .map(|address| (address, record["name"].as_str().unwrap_or(key)))
        })
        .collect();
    let flows: Vec<_> = values(&inventory["flows"])
        .map(|flow| transmit_flow(flow, &device["channels"]["transmitters"], &addresses))
        .collect();
    let used: BTreeSet<_> = flows
        .iter()
        .filter_map(|flow| flow["id"].as_u64())
        .collect();
    let authoring = &device["transmit_flow_authoring"];
    let unparsed: Vec<_> = values(&inventory["unparsed_records"])
        .map(|record| {
            json!({
                "record_index": record["record_index"],
                "error": record["error"],
            })
        })
        .collect();
    let next_free = (authoring["identity_field"] == "global_flow_id" && unparsed.is_empty())
        .then(|| {
            authoring["identifier_max"]
                .as_u64()
                .and_then(|maximum| (1..=maximum).find(|identifier| !used.contains(identifier)))
        })
        .flatten();
    compact(json!({
        "device": first(&[&device["name"], &inventory["device"]]),
        "flow_count": inventory["reported_flow_count"],
        "maximum_flows": device["maximum_transmit_flows"],
        "flows": flows,
        "next_free_flow_id": next_free,
        "unparsed_records": (!unparsed.is_empty()).then_some(unparsed),
    }))
}

pub fn flow_plan_view(plan: &Value, device: &Value) -> Value {
    compact(json!({
        "device": device["name"],
        "supported": plan["supported"],
        "flow": transmit_flow(&plan["specification"], &device["channels"]["transmitters"], &BTreeMap::new()),
        "problems": plan["reasons"],
    }))
}

pub fn flow_result_view(result: &Value, device: &Value, records: &Records) -> Value {
    let addresses = records
        .iter()
        .filter_map(|(key, record)| {
            record["ipv4"]
                .as_str()
                .map(|address| (address, record["name"].as_str().unwrap_or(key)))
        })
        .collect();
    let effective = result["effective"].as_object().map(|_| {
        transmit_flow(
            &result["effective"],
            &device["channels"]["transmitters"],
            &addresses,
        )
    });
    let mut view = compact(json!({
        "state": result["state"],
        "message": result["message"],
        "flow": effective,
        "not_read_back": result["comparison"]["unavailable_fields"],
        "differences": result["comparison"]["differences"],
        "media_confirmed": result["media_packet_reception_confirmed"],
        "operation_id": result["operation_id"],
        "mutation_sent": result["mutation_sent"],
    }));
    view["device_accepted"] = result["request_acknowledgement"]["accepted"].clone();
    view
}
