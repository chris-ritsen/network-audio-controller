use super::Response;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Clone, Deserialize, Serialize)]
pub struct Subscription {
    pub receiver_channel: u16,
    pub transmitter_device: String,
    pub transmitter_channel: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum Control {
    Name { name: String },
    Subscriptions { subscriptions: Vec<Subscription> },
    PreferredLeader { enabled: bool },
}

impl Control {
    pub fn operation_name(&self) -> &'static str {
        match self {
            Self::Name { .. } => "DeviceNameSet",
            Self::Subscriptions { .. } => "DeviceRxChannelsSubscriptionSet",
            Self::PreferredLeader { .. } => "DeviceClockingPreferredLeaderSet",
        }
    }

    pub fn query(&self) -> String {
        let name = self.operation_name();
        format!("mutation {name}($input: {name}Input!) {{ {name}(input: $input) {{ ok }} }}")
    }

    pub fn variables(&self, device: &str) -> Map<String, Value> {
        let mut input = match self {
            Self::Name { name } => json!({"name": name}),
            Self::PreferredLeader { enabled } => json!({"enabled": enabled}),
            Self::Subscriptions { subscriptions } => {
                json!({"subscriptions": subscriptions.iter().map(|subscription| json!({
                "rxChannelIndex": subscription.receiver_channel,
                "subscribedDevice": subscription.transmitter_device,
                "subscribedChannel": subscription.transmitter_channel,
            })).collect::<Vec<_>>() })
            }
        };
        input["deviceId"] = json!(device);
        Map::from_iter([("input".into(), input)])
    }

    pub fn acknowledgement(&self, response: &Response) -> Result<bool, &'static str> {
        super::mutation::acknowledgement(self.operation_name(), response)
            .map(|(accepted, _)| accepted)
    }
}
