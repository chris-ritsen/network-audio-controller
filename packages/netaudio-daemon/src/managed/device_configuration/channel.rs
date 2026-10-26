use super::super::device_session::DeviceSession;
use crate::devices::operations::{ChannelDirection, ControlError, ControlOperation};
use netaudio_core::protocol::PROTOCOL_ARC_2809;
use serde_json::{Value, json};

pub(in crate::managed) struct Change<'a> {
    direction: ChannelDirection,
    number: u16,
    name: &'a str,
}

impl<'a> Change<'a> {
    pub fn from_operation(operation: &'a ControlOperation) -> Option<Self> {
        match operation {
            ControlOperation::RenameChannel {
                direction,
                number,
                name,
            } => Some(Self {
                direction: *direction,
                number: *number,
                name,
            }),
            _ => None,
        }
    }

    pub fn requested(&self) -> Value {
        json!({"channel_type": self.direction.command_value(), "channel_number": self.number, "name": self.name})
    }

    fn channel<'b>(&self, record: &'b Value) -> Result<&'b Value, ControlError> {
        let direction = match self.direction {
            ChannelDirection::Receiver => "receivers",
            ChannelDirection::Transmitter => "transmitters",
        };
        record["channels"][direction]
            .get(self.number.to_string())
            .filter(|channel| channel["ddm_channel_id"].is_string())
            .ok_or_else(|| {
                ControlError::NotFound(format!(
                    "Managed {} channel {} is unavailable",
                    self.direction.command_value(),
                    self.number,
                ))
            })
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        if self.name.trim().is_empty() {
            return Err(ControlError::Conflict(
                "Channel-name reset has no established managed transport".into(),
            ));
        }
        self.channel(record)?;
        if record["ddm_fresh"] != true || record["ddm_connection_state"] != "READY" {
            return Err(ControlError::ObservationUnavailable(
                "The selected managed device is not currently ready".into(),
            ));
        }
        match record["is_locked"].as_bool() {
            Some(false) => {}
            Some(true) => return Err(ControlError::Conflict("device is locked".into())),
            None => {
                return Err(ControlError::ObservationUnavailable(
                    "Device lock state is unknown; no channel-name command was sent".into(),
                ));
            }
        }
        let mut command = self.requested();
        command["command"] = json!("set_channel_name");
        command["protocol_id"] = json!(PROTOCOL_ARC_2809);
        netaudio_core::spec::build_command_from_json(&command.to_string())
            .map_err(|error| ControlError::Invalid(error.to_string()))?;
        Ok(Some(command))
    }

    pub async fn probe(&self, session: &mut DeviceSession) -> Result<(), ControlError> {
        let inventory = netaudio_core::channel_inventory::ChannelInventory::new(
            self.direction.command_value(),
            PROTOCOL_ARC_2809,
            1,
        )
        .map_err(|error| ControlError::Invalid(error.into()))?;
        let netaudio_core::inventory::InventoryState::Pending { next_command, .. } =
            inventory.state()
        else {
            return Err(ControlError::Internal(anyhow::anyhow!(
                "Channel frontend probe has no command"
            )));
        };
        let exchange = session
            .command(Value::Object(next_command))
            .await
            .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
        let protocol = exchange
            .packet
            .as_deref()
            .and_then(|packet| {
                netaudio_core::responses::parse_channel_name_protocol(
                    packet,
                    self.direction == ChannelDirection::Receiver,
                )
            })
            .ok_or_else(|| {
                ControlError::ObservationUnavailable(
                    "Channel naming frontend response is unavailable or malformed".into(),
                )
            })?;
        if protocol != PROTOCOL_ARC_2809 {
            return Err(ControlError::Conflict(
                "The channel naming frontend has no established managed transport".into(),
            ));
        }
        Ok(())
    }

    pub fn validate_readback(&self, before: &Value, after: &Value) -> Result<(), ControlError> {
        let before = self.channel(before)?;
        let after = self.channel(after)?;
        if ["ddm_channel_id", "ddm_media_type"]
            .iter()
            .any(|field| before[field] != after[field])
        {
            return Err(ControlError::ObservationUnavailable(
                "Managed channel identity changed during readback".into(),
            ));
        }
        Ok(())
    }

    pub fn completion(&self, after: Option<&Value>, acknowledged: Option<bool>) -> Value {
        let name = after
            .and_then(|record| self.channel(record).ok())
            .and_then(|channel| channel["name"].as_str());
        let configured = name.map(|name| name == self.name);
        json!({
            "channel_type": self.direction.command_value(), "channel_number": self.number,
            "name": name, "configuration_confirmed": configured,
            "effective_state_confirmed": acknowledged.filter(|accepted| *accepted).and(configured),
            "persistence_confirmed": null,
        })
    }
}
