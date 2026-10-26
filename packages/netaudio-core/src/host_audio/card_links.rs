use super::cards::{self, Bridge, Card};
use super::snapshot::JackSnapshot;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub card: String,
    pub device: Option<String>,
}

pub struct Context<'a> {
    pub host: &'a str,
    pub cards: &'a [Card],
    pub bridges: &'a BTreeMap<String, Bridge>,
    pub jack: Option<&'a JackSnapshot>,
    pub mappings: &'a BTreeMap<String, String>,
    pub configuration_revision: &'a str,
    pub devices: &'a [&'a Value],
}

#[derive(Clone, Debug, Serialize)]
pub struct Plan {
    pub command: Command,
    pub description: Value,
    pub precondition: Value,
    pub unchanged: bool,
}

pub fn prepare(command: &Command, context: &Context<'_>) -> Result<Plan, String> {
    let card = context
        .cards
        .iter()
        .find(|card| card.id == command.card)
        .ok_or_else(|| {
            let available = context
                .cards
                .iter()
                .map(|card| format!("{} ({})", card.id, card.name))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "no ALSA card {:?} on {}; cards: {}",
                command.card,
                context.host,
                if available.is_empty() {
                    "none"
                } else {
                    &available
                }
            )
        })?;
    let device = command.device.as_deref().filter(|name| !name.is_empty());
    let selected = if let Some(name) = device {
        let matches = context
            .devices
            .iter()
            .filter(|record| record["name"].as_str() == Some(name))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => return Err(format!("no Dante device named {name:?}")),
            [record] => Some(
                json!({"name":name,"device_identity":record["device_identity"],"server_name":record["server_name"]}),
            ),
            _ => {
                return Err(format!(
                    "several Dante devices are named {name:?}; give them distinct names before linking a sound card"
                ));
            }
        }
    } else {
        None
    };
    let current = context
        .mappings
        .iter()
        .filter(|(_, linked)| *linked == &card.id)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let ports = context.jack.filter(|jack| jack.available).map(|jack| {
        jack.ports
            .iter()
            .filter(|(name, port)| {
                cards::hardware_channel(port, context.cards)
                    .or_else(|| cards::bridge_channel(name, context.bridges))
                    .is_some_and(|channel| channel.card == card.id)
            })
            .count()
    });
    let unchanged = match device {
        Some(name) => current.len() == 1 && current[0] == name,
        None => current.is_empty(),
    };
    Ok(Plan {
        command: Command {
            card: card.id.clone(),
            device: device.map(str::to_owned),
        },
        description: json!({"card":card.id,"card_name":card.long_name.as_deref().unwrap_or(&card.name),"current":current.first(),"current_devices":current,"device":device,"jack_ports":ports,"previous_card_for_device":device.and_then(|name|context.mappings.get(name))}),
        precondition: json!({"card":card,"device":selected,"configuration_revision":context.configuration_revision}),
        unchanged,
    })
}
