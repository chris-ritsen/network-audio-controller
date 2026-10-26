use super::{CallError, NEXT_STEP, Prepared, PreparedKind};
use crate::host_audio::manager::Manager;
use netaudio_core::host_audio::{card_links, control};
use serde_json::{Value, json};
use std::sync::Arc;

pub(super) enum PreparedHostAudio {
    Control {
        command: control::Command,
        precondition: Value,
    },
    Card {
        command: card_links::Command,
        precondition: Value,
    },
}

pub(super) fn supports(operation: &str) -> bool {
    matches!(
        operation,
        "connect_audio_ports"
            | "disconnect_audio_ports"
            | "set_audio_volume"
            | "set_audio_mute"
            | "set_default_audio_device"
            | "move_audio_stream"
            | "link_audio_card"
    )
}

fn required(manager: &Option<Arc<Manager>>) -> Result<&Manager, CallError> {
    manager
        .as_deref()
        .ok_or_else(|| CallError::invalid("Host audio support is not running"))
}

pub(super) async fn prepare(
    operation: &str,
    arguments: &Value,
    manager: &Option<Arc<Manager>>,
) -> Result<Prepared, CallError> {
    let manager = required(manager)?;
    let mut preview = json!({"changed":false,"confirmation_required":true,"operation":operation,"next":NEXT_STEP});
    let kind = if operation == "link_audio_card" {
        let command: card_links::Command = serde_json::from_value(arguments.clone())
            .map_err(|error| CallError::invalid(error.to_string()))?;
        let plan = manager
            .prepare_card(&command)
            .await
            .map_err(|error| CallError::invalid(error.to_string()))?;
        let change = &plan.description;
        preview["change"] = json!({"card":format!("{} ({})",super::views::text(&change["card"]),super::views::text(&change["card_name"])),"from":change["current_devices"],"to":change["device"]});
        preview["jack_ports_on_card"] = change["jack_ports"].clone();
        if let Some(previous) = change["previous_card_for_device"]
            .as_str()
            .filter(|card| *card != plan.command.card)
        {
            preview["warning"] = json!(format!(
                "This also removes the device's association with sound card {previous}"
            ));
        }
        if plan.unchanged {
            preview["note"] = json!("already so; nothing would change");
        }
        PreparedHostAudio::Card {
            command: plan.command,
            precondition: plan.precondition,
        }
    } else {
        let mut request = arguments.clone();
        let fields = request
            .as_object_mut()
            .ok_or_else(|| CallError::invalid("Host audio arguments must be an object"))?;
        let action = match operation {
            "connect_audio_ports" | "disconnect_audio_ports" => {
                fields.insert("connect".into(), json!(operation == "connect_audio_ports"));
                "jack_connection"
            }
            "set_audio_volume" => "pulse_volume",
            "set_audio_mute" => "pulse_mute",
            "set_default_audio_device" => "pulse_default",
            "move_audio_stream" => "pulse_move",
            _ => return Err(CallError::invalid("Unknown host audio operation")),
        };
        fields.insert("action".into(), json!(action));
        let command: control::Command = serde_json::from_value(request)
            .map_err(|error| CallError::invalid(error.to_string()))?;
        let plan = manager
            .prepare(&command)
            .await
            .map_err(|error| CallError::invalid(error.to_string()))?;
        let change = &plan.description;
        if let control::Command::JackConnection { connect, .. } = &command {
            let source = super::views::text(&change["source"]);
            let destination = super::views::text(&change["destination"]);
            preview["change"] = json!({"connection":format!("{source} → {destination}"),"from":if change["connected_now"]==true{"connected"}else{"disconnected"},"to":change["requested"]});
            preview["source_feeds_now"] = change["source_feeds"].clone();
            preview["destination_hears_now"] = change["destination_hears"].clone();
            let others = change["destination_hears"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(|port| *port != source)
                .collect::<Vec<_>>();
            let extra = if others.is_empty() {
                String::new()
            } else if *connect {
                format!(" as well as {}", others.join(", "))
            } else {
                format!("; it would still hear {}", others.join(", "))
            };
            preview["effect"] = json!(format!(
                "{destination} would {} hearing {source}{extra}",
                if *connect { "start" } else { "stop" }
            ));
            preview["warning"] = json!(
                "A JACK patchbay or session manager may undo this; the result reports the observed state"
            );
        } else {
            preview["change"] = json!({"target":change["target"],"from":change["current"],"to":change["requested"]});
            if let Some(consumers) = plan.precondition.get("consumers") {
                preview["affected_streams"] = json!(consumers.as_array().into_iter().flatten().map(|stream|json!({"index":stream["index"],"application":stream["application"],"binary":stream["binary"],"media":stream["media"]})).collect::<Vec<_>>());
                preview["default_device"] =
                    json!(plan.precondition["default"] == plan.precondition["entry"]["name"]);
            }
            preview["effect"] = json!(match &command {
                control::Command::PulseMute {mute,..} => format!("{} would be {}",super::views::text(&change["target"]),if *mute{"muted"}else{"unmuted"}),
                control::Command::PulseVolume {..} => format!("{} would change volume from {} to {}",super::views::text(&change["target"]),super::views::text(&change["current"]),super::views::text(&change["requested"])),
                control::Command::PulseDefault {..} => "New application streams would use this default unless they select another device".to_owned(),
                _ => format!("{} would move from {} to {}",super::views::text(&change["target"]),super::views::text(&change["current"]),super::views::text(&change["requested"])),
            });
        }
        if plan.unchanged {
            preview
                .as_object_mut()
                .expect("preview object")
                .remove("effect");
            preview["note"] = json!("already so; nothing would change");
        }
        PreparedHostAudio::Control {
            command,
            precondition: plan.precondition,
        }
    };
    Ok(Prepared {
        preview,
        devices: Vec::new(),
        kind: PreparedKind::HostAudio(kind),
    })
}

impl PreparedHostAudio {
    pub fn conditions(&self) -> Value {
        match self {
            Self::Control { precondition, .. } | Self::Card { precondition, .. } => {
                precondition.clone()
            }
        }
    }

    pub async fn apply(self, manager: &Option<Arc<Manager>>) -> Result<(Value, bool), CallError> {
        let manager = required(manager)?;
        let outcome = match self {
            Self::Control {
                command,
                precondition,
            } => manager.execute(command, Some(precondition)).await?,
            Self::Card {
                command,
                precondition,
            } => manager.execute_card(command, Some(precondition)).await?,
        };
        Ok((outcome.body, outcome.status >= 400))
    }
}
