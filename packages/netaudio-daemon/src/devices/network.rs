use super::{network_configuration, operations::ControlError, redundancy};
use crate::monitoring::requests::StatusQuery;
use serde_json::{Map, Value, json};

#[derive(Clone, Debug)]
pub enum Change {
    Interface(network_configuration::Configuration),
    Redundancy(redundancy::Configuration),
}

impl Change {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Interface(_) => "set_interface",
            Self::Redundancy(_) => "set_redundancy",
        }
    }

    pub fn requested(&self) -> Value {
        match self {
            Self::Interface(configuration) => configuration.requested(),
            Self::Redundancy(configuration) => json!({"mode": configuration.mode}),
        }
    }

    pub fn precondition(&self, record: &Value) -> Value {
        match self {
            Self::Interface(configuration) => configuration.precondition(record),
            Self::Redundancy(configuration) => configuration.precondition(record),
        }
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        match self {
            Self::Interface(configuration) => configuration.command(record),
            Self::Redundancy(configuration) => configuration.command(record),
        }
    }

    pub fn query(&self, before: &Value) -> StatusQuery {
        match self {
            Self::Redundancy(_) if Self::uses_switch_status(before) => {
                StatusQuery::SwitchConfiguration
            }
            _ => StatusQuery::Interface,
        }
    }

    pub fn uses_switch_status(record: &Value) -> bool {
        record["dante_redundancy"]["state_source"]["kind"] == "switch_configuration_status"
    }

    pub fn matches(&self, fields: &Map<String, Value>) -> bool {
        match self {
            Self::Interface(configuration) => fields
                .get("interfaces")
                .and_then(|interfaces| configuration.selected(interfaces))
                .is_some_and(|selected| configuration.matches(&selected["configured"])),
            Self::Redundancy(configuration) => fields
                .get("dante_redundancy")
                .is_some_and(|state| configuration.matches(state)),
        }
    }

    pub fn completion(&self, before: &Value, after: Option<&Value>, sent: bool) -> Value {
        match self {
            Self::Interface(configuration) => configuration.completion(before, after, sent),
            Self::Redundancy(configuration) => configuration.completion(before, after, sent),
        }
    }
}
