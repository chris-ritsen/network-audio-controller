use super::{aes67, analog, operations::ControlError};
use crate::monitoring::requests::StatusQuery;
use serde_json::{Map, Value, json};
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum Configuration {
    Aes67(aes67::Configuration),
    Analog(analog::Configuration),
}

impl Configuration {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Aes67(_) => "set_aes67",
            Self::Analog(_) => "analog_level",
        }
    }

    pub fn query(&self) -> StatusQuery {
        match self {
            Self::Aes67(_) => StatusQuery::Aes67,
            Self::Analog(_) => StatusQuery::Codec,
        }
    }

    pub fn deadline(&self) -> Duration {
        match self {
            Self::Aes67(_) => Duration::from_secs(2),
            Self::Analog(_) => Duration::from_secs(4),
        }
    }

    pub fn requested(&self) -> Value {
        match self {
            Self::Aes67(configuration) => json!({"enabled": configuration.enabled}),
            Self::Analog(configuration) => configuration.requested(),
        }
    }

    pub fn precondition(&self, record: &Value) -> Value {
        match self {
            Self::Aes67(configuration) => configuration.precondition(record),
            Self::Analog(configuration) => configuration.precondition(record),
        }
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        match self {
            Self::Aes67(configuration) => configuration.command(record),
            Self::Analog(configuration) => configuration.command(record),
        }
    }

    pub fn matches(&self, fields: &Map<String, Value>) -> bool {
        match self {
            Self::Aes67(configuration) => {
                fields.get("aes67_configured").and_then(Value::as_bool)
                    == Some(configuration.enabled)
            }
            Self::Analog(configuration) => configuration.matches(fields),
        }
    }

    pub fn completion(&self, observed: Option<&Map<String, Value>>, sent: bool) -> Value {
        match self {
            Self::Aes67(configuration) => configuration.completion(observed, sent),
            Self::Analog(configuration) => configuration.completion(observed, sent),
        }
    }
}
