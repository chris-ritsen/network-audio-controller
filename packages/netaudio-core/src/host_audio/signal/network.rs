use super::*;

#[derive(Deserialize)]
struct Record {
    name: Option<String>,
    server_name: Option<String>,
    online: Option<bool>,
    #[serde(default)]
    channels: Channels,
    #[serde(default)]
    subscriptions: Vec<Subscription>,
    remembered_last_seen: Option<String>,
}

#[derive(Default, Deserialize)]
struct Channels {
    #[serde(default)]
    receivers: BTreeMap<u32, Value>,
    #[serde(default)]
    transmitters: BTreeMap<u32, Value>,
}

#[derive(Deserialize)]
struct Subscription {
    tx_device: Option<String>,
    tx_channel: Option<String>,
    rx_channel_number: u32,
    status: Option<Value>,
}

pub(super) struct Network {
    records: BTreeMap<String, Record>,
    names: BTreeMap<String, Vec<String>>,
}

pub(super) fn identity(record: &str, direction: NetworkDirection, channel: u32) -> String {
    format!("dante:{record}:{}:{channel}", direction.name())
}

fn label(channel: &Value) -> Option<&str> {
    channel["friendly_name"]
        .as_str()
        .filter(|name| !name.is_empty())
        .or_else(|| channel["name"].as_str().filter(|name| !name.is_empty()))
}

impl Network {
    pub(super) fn channel(
        &self,
        name: &str,
        direction: NetworkDirection,
        number: u32,
    ) -> Option<String> {
        let keys = self.names.get(name)?;
        if keys.len() != 1 {
            return None;
        }
        self.record_channel(&keys[0], direction, number)
    }

    pub(super) fn record_channel(
        &self,
        key: &str,
        direction: NetworkDirection,
        number: u32,
    ) -> Option<String> {
        let record = self.records.get(key)?;
        let channels = match direction {
            NetworkDirection::Receiver => &record.channels.receivers,
            NetworkDirection::Transmitter => &record.channels.transmitters,
        };
        channels
            .contains_key(&number)
            .then(|| identity(key, direction, number))
    }
}

pub(super) fn add(graph: &mut Graph, records: &BTreeMap<String, Value>) -> Result<Network, String> {
    let mut network = Network {
        records: BTreeMap::new(),
        names: BTreeMap::new(),
    };
    for (key, value) in records {
        let record: Record = serde_json::from_value(value.clone())
            .map_err(|error| format!("Invalid signal graph device {key:?}: {error}"))?;
        let Some(name) = record.name.as_ref().filter(|name| !name.is_empty()) else {
            continue;
        };
        network
            .names
            .entry(name.clone())
            .or_default()
            .push(key.clone());
        for (direction, channels) in [
            (NetworkDirection::Receiver, &record.channels.receivers),
            (NetworkDirection::Transmitter, &record.channels.transmitters),
        ] {
            for (number, channel) in channels {
                let number_text = number.to_string();
                let label = label(channel).unwrap_or(&number_text);
                let direction_name = direction.name();
                let suffix = if record.online == Some(false) {
                    "Dante, off the network"
                } else {
                    "Dante"
                };
                let label_suffix = if label == number_text {
                    String::new()
                } else {
                    format!(" {label}")
                };
                graph.add(
                    Node {
                        identity: identity(key, direction, *number),
                        label: format!("{name} {direction_name} {number}{label_suffix} ({suffix})"),
                        details: Details::Dante(DanteNode {
                            device: name.clone(),
                            record: Some(key.clone()),
                            server_name: record.server_name.clone(),
                            direction,
                            channel: Some(*number),
                            online: record.online,
                            outlet: None,
                            unresolved: None,
                        }),
                    },
                    BTreeMap::from([
                        (format!("{name} {direction_name} {number}"), 0),
                        (format!("{name} {direction_name} {label}"), 0),
                        (format!("{name}:{label}"), 1),
                        (format!("{name} {label}"), 1),
                        (label.to_owned(), 1),
                    ]),
                )?;
            }
        }
        network.records.insert(key.clone(), record);
    }
    for (key, record) in &network.records {
        for subscription in &record.subscriptions {
            let Some(device) = subscription
                .tx_device
                .as_deref()
                .filter(|name| !name.is_empty())
            else {
                continue;
            };
            let status = subscription.status.as_ref().unwrap_or(&Value::Null);
            if status["state"] == "none" {
                continue;
            }
            let Some(destination) = network.record_channel(
                key,
                NetworkDirection::Receiver,
                subscription.rx_channel_number,
            ) else {
                graph.problems.push(format!(
                    "{} reports a route to absent receiver {}",
                    record.name.as_deref().unwrap_or(key),
                    subscription.rx_channel_number
                ));
                continue;
            };
            let channel = subscription.tx_channel.as_deref().unwrap_or("");
            let ambiguous_device = network.names.get(device).is_some_and(|keys| keys.len() > 1);
            let candidates = network
                .names
                .get(device)
                .filter(|_| !ambiguous_device)
                .into_iter()
                .flatten()
                .flat_map(|key| {
                    network.records[key]
                        .channels
                        .transmitters
                        .iter()
                        .filter(move |(number, value)| {
                            label(value) == Some(channel)
                                || value["name"].as_str() == Some(channel)
                                || number.to_string() == channel
                        })
                        .map(move |(number, _)| (key, *number))
                })
                .collect::<Vec<_>>();
            let (source, resolved) = if let [(key, number)] = candidates.as_slice() {
                (identity(key, NetworkDirection::Transmitter, *number), true)
            } else {
                let reason = if ambiguous_device || candidates.len() > 1 {
                    "transmitter name is ambiguous"
                } else if network.names.contains_key(device) {
                    "transmitter channel is not in its inventory"
                } else {
                    "transmitter is not in the known inventory"
                };
                let identity = format!(
                    "unresolved:{}",
                    serde_json::to_string(&(device, channel)).map_err(|error| error.to_string())?
                );
                if !graph.nodes.contains_key(&identity) {
                    graph.add(
                        Node {
                            identity: identity.clone(),
                            label: format!("{device} tx {channel} (Dante, {reason})"),
                            details: Details::Dante(DanteNode {
                                device: device.to_owned(),
                                record: None,
                                server_name: None,
                                direction: NetworkDirection::Transmitter,
                                channel: None,
                                online: None,
                                outlet: None,
                                unresolved: Some(reason.to_owned()),
                            }),
                        },
                        BTreeMap::new(),
                    )?;
                }
                (identity, false)
            };
            let mut note = status["label"].as_str().map(str::to_owned);
            if let Some(detail) = status["detail"]
                .as_str()
                .filter(|_| !matches!(status["severity"].as_str(), None | Some("ok" | "none")))
            {
                note = Some(
                    note.map_or_else(|| detail.to_owned(), |note| format!("{note}: {detail}")),
                );
            }
            if !resolved {
                note = Some(
                    "reported subscription; its transmitter endpoint is unresolved".to_owned(),
                );
            }
            if let Some(seen) = &record.remembered_last_seen {
                note = Some(format!("its last known route, from {seen}"));
            }
            graph.link(&source, &destination, note, Evidence::Reported)?;
        }
    }
    Ok(network)
}

fn type_names(protocol: Option<&str>) -> &'static [&'static str] {
    match protocol {
        Some("ad4d") => &["wireless", "mic", "mics", "microphone", "receiver"],
        Some("p10t") => &[
            "in-ear", "in ear", "in-ears", "iem", "iems", "monitor", "monitors", "psm",
        ],
        _ => &[],
    }
}

pub(super) fn wireless(
    graph: &mut Graph,
    devices: &BTreeMap<String, Value>,
    links: &BTreeMap<String, WirelessLink>,
    network: &Network,
) -> Result<(), String> {
    for (address, device) in devices {
        let name = device["name"]
            .as_str()
            .filter(|name| !name.is_empty())
            .unwrap_or(address);
        let protocol = device["device_type"].as_str();
        let model = device["model"]
            .as_str()
            .or(protocol)
            .unwrap_or("unknown model");
        for (number, channel) in device["channels"].as_object().into_iter().flatten() {
            let number: u32 = number
                .parse()
                .map_err(|_| format!("Invalid wireless channel {number:?}"))?;
            let channel_name = channel["name"].as_str().unwrap_or("");
            let identity = format!("wireless:{address}:{number}");
            let suffix = if channel_name.is_empty() {
                String::new()
            } else {
                format!(" {channel_name}")
            };
            let mut names = BTreeMap::from([
                (format!("{name} ch{number}"), 0),
                (format!("{name} ch {number}"), 0),
                (format!("{name} {number}"), 0),
                (format!("{name}:{number}"), 0),
                (channel_name.to_owned(), 1),
                (name.to_owned(), 2),
                (address.clone(), 2),
            ]);
            if !channel_name.is_empty() {
                names.insert(format!("{name} {channel_name}"), 0);
            }
            names.extend(
                type_names(protocol)
                    .iter()
                    .map(|name| ((*name).to_owned(), 3)),
            );
            graph.add(
                Node {
                    identity: identity.clone(),
                    label: format!("{name} ch{number}{suffix} (Shure {model})"),
                    details: Details::Wireless(WirelessNode {
                        address: address.clone(),
                        number,
                        device: device.clone(),
                        channel: channel.clone(),
                    }),
                },
                names,
            )?;
            if let Some(link) = links.get(address) {
                if let Some(destination) =
                    network.record_channel(&link.record, NetworkDirection::Transmitter, number)
                {
                    graph.link(
                        &identity,
                        &destination,
                        Some("Dante output".to_owned()),
                        link.evidence,
                    )?;
                } else {
                    graph.problems.push(format!(
                        "Wireless channel {name} ch{number} maps to an absent Dante transmitter"
                    ));
                }
            }
        }
    }
    Ok(())
}
