use serde::Serialize;
use serde_json::Value;

#[derive(Serialize, schemars::JsonSchema)]
#[schemars(rename = "PresetOperationResult")]
pub struct OperationResult {
    pub device_name: String,
    pub kind: String,
    pub state: String,
    pub requested: Value,
    pub effective: Value,
    pub request_acknowledgement: Value,
    pub device_confirmation: Option<bool>,
    pub configuration_confirmation: Option<bool>,
    pub effective_state_confirmation: Option<bool>,
    pub persistence_request_acknowledgement: Value,
    pub persistence_confirmation: Option<bool>,
    pub verification_observations: Vec<Value>,
    pub message: String,
}

impl OperationResult {
    pub fn new(
        device_name: &str,
        kind: String,
        state: &str,
        requested: Value,
        message: impl Into<String>,
    ) -> Self {
        Self {
            device_name: device_name.into(),
            kind,
            state: state.into(),
            requested,
            effective: Value::Null,
            request_acknowledgement: Value::Null,
            device_confirmation: None,
            configuration_confirmation: None,
            effective_state_confirmation: None,
            persistence_request_acknowledgement: Value::Null,
            persistence_confirmation: None,
            verification_observations: Vec::new(),
            message: message.into(),
        }
    }

    pub fn failed(&self) -> bool {
        matches!(
            self.state.as_str(),
            "failed" | "rejected" | "contradicted" | "inconsistent"
        )
    }

    pub fn unverified(&self) -> bool {
        !self.failed()
            && match self.state.as_str() {
                "unchanged" => false,
                "confirmed" if self.kind == "store_current_configuration" => {
                    self.persistence_confirmation != Some(true)
                }
                "confirmed" => self.effective_state_confirmation != Some(true),
                _ => true,
            }
    }
}

#[derive(Default, Serialize, schemars::JsonSchema)]
#[schemars(rename = "PresetLoadReport")]
pub struct Report {
    pub needs_reboot: Vec<String>,
    pub results: Vec<(String, String)>,
    pub operations: Vec<OperationResult>,
    pub failures: usize,
    pub unverified: usize,
}

impl Report {
    pub fn recount(&mut self) {
        self.failures = self
            .operations
            .iter()
            .filter(|operation| operation.failed())
            .count();
        self.unverified = self
            .operations
            .iter()
            .filter(|operation| operation.unverified())
            .count();
        self.results = self
            .operations
            .iter()
            .map(|operation| (operation.device_name.clone(), operation.message.clone()))
            .collect();
    }

    pub fn record(&mut self, operation: OperationResult) {
        self.failures += usize::from(operation.failed());
        self.unverified += usize::from(operation.unverified());
        self.results
            .push((operation.device_name.clone(), operation.message.clone()));
        self.operations.push(operation);
    }
}
