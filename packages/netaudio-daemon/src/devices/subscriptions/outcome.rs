use super::ControlError;
use crate::devices::transport::TransportError;
use serde_json::{Value, json};
use std::collections::BTreeSet;

#[derive(Debug)]
pub struct SubscriptionFailure {
    pub cause: Box<ControlError>,
    pub acknowledged_receivers: BTreeSet<u16>,
    pub attempted_receivers: BTreeSet<u16>,
    pub readback_completed: bool,
    pub readback_errors: Vec<String>,
}

impl SubscriptionFailure {
    pub fn new(commands: &[Vec<u16>], cause: ControlError) -> Self {
        let acknowledged_commands = match &cause {
            ControlError::Rejected {
                acknowledged_commands,
                ..
            }
            | ControlError::Transport {
                acknowledged_commands,
                ..
            } => *acknowledged_commands,
            _ => 0,
        };
        Self {
            cause: Box::new(cause),
            acknowledged_receivers: commands
                .iter()
                .take(acknowledged_commands)
                .flatten()
                .copied()
                .collect(),
            attempted_receivers: commands
                .get(acknowledged_commands)
                .into_iter()
                .flatten()
                .copied()
                .collect(),
            readback_completed: false,
            readback_errors: Vec::new(),
        }
    }

    pub fn route(&self, channel: u16) -> Value {
        let accepted = self.acknowledged_receivers.contains(&channel);
        let attempted = self.attempted_receivers.contains(&channel);
        let mut result = json!({
            "ok": accepted,
            "readback_completed": self.readback_completed,
        });

        if accepted {
            result["request_acknowledged"] = json!(true);
        } else if attempted {
            result["error"] = json!(self.cause.to_string());

            match self.cause.as_ref() {
                ControlError::Rejected { .. } => result["request_acknowledged"] = json!(false),
                ControlError::Transport {
                    error: TransportError::WriteOutcomeUnknown(_),
                    ..
                } => {
                    result["outcome_unknown"] = json!(true);
                }
                _ => {}
            }
        } else {
            result["error"] = json!("not sent because an earlier command failed");
            result["request_sent"] = json!(false);
        }

        if !self.readback_errors.is_empty() {
            result["readback_errors"] = json!(self.readback_errors);
        }

        result
    }
}
