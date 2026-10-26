use anyhow::{Context, Result, ensure};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

pub const SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_VIEW_LIMIT: usize = 1000;
const LATENCY_WARNING_RATIO: f64 = 0.8;
const LATENCY_RECOVERY_RATIO: f64 = 0.6;
const UTILIZATION_WARNING_PERCENT: f64 = 80.0;
const UTILIZATION_RECOVERY_PERCENT: f64 = 60.0;

#[derive(Clone, Copy)]
pub struct Thresholds {
    pub latency_warning: f64,
    pub latency_recovery: f64,
    pub utilization_warning: f64,
    pub utilization_recovery: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            latency_warning: LATENCY_WARNING_RATIO,
            latency_recovery: LATENCY_RECOVERY_RATIO,
            utilization_warning: UTILIZATION_WARNING_PERCENT,
            utilization_recovery: UTILIZATION_RECOVERY_PERCENT,
        }
    }
}

impl Thresholds {
    pub fn from_configuration(configuration: &toml::Table) -> Result<Self> {
        let defaults = Self::default();
        let read = |name, default| -> Result<f64> {
            configuration.get(name).map_or(Ok(default), |value| {
                value
                    .as_float()
                    .or_else(|| value.as_integer().map(|value| value as f64))
                    .filter(|value| value.is_finite())
                    .with_context(|| format!("daemon.{name} must be a finite number"))
            })
        };
        let thresholds = Self {
            latency_warning: read("event_flow_latency_warning_ratio", defaults.latency_warning)?,
            latency_recovery: read(
                "event_flow_latency_recovery_ratio",
                defaults.latency_recovery,
            )?,
            utilization_warning: read(
                "event_interface_utilization_warning_percent",
                defaults.utilization_warning,
            )?,
            utilization_recovery: read(
                "event_interface_utilization_recovery_percent",
                defaults.utilization_recovery,
            )?,
        };
        ensure!(
            0.0 < thresholds.latency_recovery
                && thresholds.latency_recovery < thresholds.latency_warning
                && thresholds.latency_warning <= 1.0,
            "flow latency ratios must satisfy 0 < recovery < warning <= 1"
        );
        ensure!(
            0.0 <= thresholds.utilization_recovery
                && thresholds.utilization_recovery < thresholds.utilization_warning
                && thresholds.utilization_warning <= 100.0,
            "interface utilization percentages must satisfy 0 <= recovery < warning <= 100"
        );
        Ok(thresholds)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Derivation {
    Observed,
    Derived,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LifecyclePhase {
    Requested,
    RequestAcknowledged,
    RequestRejected,
    EffectiveStateConfirmed,
    PartialUnobservable,
    Inconsistent,
    TransportOrValidationFailure,
    PersistenceRequestAcknowledged,
    PersistenceConfirmed,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    ClockRoleChanged,
    ClockStatusChanged,
    DeviceControlChanged,
    DeviceDisappeared,
    DeviceReappeared,
    InterfaceErrorCounterIncreased,
    InterfaceErrorCounterReset,
    InterfaceUtilizationHigh,
    InterfaceUtilizationRecovered,
    IssueOpened,
    IssueResolved,
    IssueUpdated,
    LeaderIdentityChanged,
    LatePacketCountIncreased,
    LatePacketCounterReset,
    MuteStateChanged,
    PtpPortStateChanged,
    ReceiverFlowLatencyHigh,
    ReceiverFlowLatencyRecovered,
    SubscriptionFailed,
    SubscriptionRecovered,
    ConfigurationOperation,
    SettingChanged,
    ChannelRenamed,
    RouteChanged,
    PresetRun,
    HostAudioChanged,
}

#[derive(Clone, Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct JournalEvent {
    pub sequence: u64,
    pub timestamp: String,
    pub kind: EventKind,
    pub severity: Severity,
    pub device_identity: String,
    pub device_name: String,
    pub server_name: String,
    pub previous_value: Value,
    pub current_value: Value,
    pub raw: Map<String, Value>,
    pub observation_source: String,
    pub derivation_status: Derivation,
    pub interface_identity: Option<String>,
    pub channel_identity: Option<String>,
    pub flow_identity: Option<String>,
    pub operation_id: Option<String>,
    pub correlation_id: Option<String>,
    pub parent_preset_run_id: Option<String>,
    pub operation_name: Option<String>,
    pub lifecycle_phase: Option<LifecyclePhase>,
    pub requested_values: Value,
    pub acknowledgement_result_code: Option<i64>,
    pub transport: Option<String>,
    pub effective_values: Value,
    pub final_operation_state: Option<String>,
    pub persistence_request_acknowledgement: Value,
    pub persistence_confirmation: Option<bool>,
}

impl JournalEvent {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.sequence != 0,
            "event sequence must be a positive integer"
        );
        DateTime::parse_from_rfc3339(&self.timestamp)
            .context("event timestamp must be an ISO-8601 timestamp")?;
        ensure!(
            !self.device_identity.is_empty(),
            "event device_identity must be a non-empty string"
        );
        ensure!(
            !self.observation_source.is_empty(),
            "event observation_source must be a non-empty string"
        );
        Ok(())
    }

    pub fn observed(
        snapshot: &Value,
        timestamp: &str,
        kind: EventKind,
        severity: Severity,
        previous: Value,
        current: Value,
        source: &str,
    ) -> Result<Self> {
        Ok(Self {
            sequence: 0,
            timestamp: timestamp.to_owned(),
            kind,
            severity,
            device_identity: device_identity(snapshot)?,
            device_name: snapshot["name"].as_str().unwrap_or_default().to_owned(),
            server_name: snapshot["server_name"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            previous_value: previous,
            current_value: current,
            raw: Map::new(),
            observation_source: source.to_owned(),
            derivation_status: Derivation::Observed,
            interface_identity: None,
            channel_identity: None,
            flow_identity: None,
            operation_id: None,
            correlation_id: None,
            parent_preset_run_id: None,
            operation_name: None,
            lifecycle_phase: None,
            requested_values: Value::Null,
            acknowledgement_result_code: None,
            transport: None,
            effective_values: Value::Null,
            final_operation_state: None,
            persistence_request_acknowledgement: Value::Null,
            persistence_confirmation: None,
        })
    }
}

#[derive(Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OperationRequest {
    pub device_snapshot: Value,
    #[serde(default)]
    pub observe_device: bool,
    pub kind: EventKind,
    pub severity: Severity,
    pub derivation_status: Derivation,
    pub operation_id: String,
    pub correlation_id: String,
    pub parent_preset_run_id: Option<String>,
    pub operation_name: String,
    pub lifecycle_phase: LifecyclePhase,
    pub requested_values: Option<Value>,
    pub acknowledgement_result_code: Option<i64>,
    pub transport: Option<String>,
    pub effective_values: Option<Value>,
    pub final_operation_state: Option<String>,
    pub persistence_request_acknowledgement: Option<Value>,
    pub persistence_confirmation: Option<bool>,
    pub evidence: Option<Map<String, Value>>,
    pub observation_source: String,
    pub interface_identity: Option<String>,
    pub channel_identity: Option<String>,
    pub flow_identity: Option<String>,
}

impl OperationRequest {
    pub fn event(&self, timestamp: &str) -> Result<JournalEvent> {
        ensure!(
            self.device_snapshot.is_object(),
            "device_snapshot must be an object"
        );
        ensure!(
            matches!(
                self.kind,
                EventKind::ConfigurationOperation | EventKind::PresetRun
            ),
            "operation event kind must be configuration_operation or preset_run"
        );

        for (name, value) in [
            ("operation_id", &self.operation_id),
            ("correlation_id", &self.correlation_id),
            ("operation_name", &self.operation_name),
            ("observation_source", &self.observation_source),
        ] {
            ensure!(!value.is_empty(), "{name} must be a non-empty string");
        }

        ensure!(
            self.parent_preset_run_id
                .as_ref()
                .is_none_or(|value| !value.is_empty()),
            "parent_preset_run_id must be a non-empty string or null"
        );
        let mut event = JournalEvent::observed(
            &self.device_snapshot,
            timestamp,
            self.kind,
            self.severity,
            Value::Null,
            json!({
                "operation": self.operation_name,
                "phase": self.lifecycle_phase,
                "state": self.final_operation_state,
            }),
            &self.observation_source,
        )?;
        event.raw = self.evidence.clone().unwrap_or_default();
        event.derivation_status = self.derivation_status;
        event.operation_id = Some(self.operation_id.clone());
        event.correlation_id = Some(self.correlation_id.clone());
        event.operation_name = Some(self.operation_name.clone());
        event.lifecycle_phase = Some(self.lifecycle_phase);
        event.parent_preset_run_id = self.parent_preset_run_id.clone();
        event.requested_values = self.requested_values.clone().unwrap_or_default();
        event.acknowledgement_result_code = self.acknowledgement_result_code;
        event.transport = self.transport.clone();
        event.effective_values = self.effective_values.clone().unwrap_or_default();
        event.final_operation_state = self.final_operation_state.clone();
        event.persistence_request_acknowledgement = self
            .persistence_request_acknowledgement
            .clone()
            .unwrap_or_default();
        event.persistence_confirmation = self.persistence_confirmation;
        event.interface_identity = self.interface_identity.clone();
        event.channel_identity = self.channel_identity.clone();
        event.flow_identity = self.flow_identity.clone();
        Ok(event)
    }
}

pub fn device_identity(snapshot: &Value) -> Result<String> {
    for field in ["device_identity", "server_name", "inventory_id"] {
        if let Some(identity) = snapshot[field].as_str().filter(|value| !value.is_empty()) {
            return Ok(identity.to_owned());
        }
    }

    snapshot["mac_address"]
        .as_str()
        .filter(|value| !value.is_empty())
        .map(|value| value.to_lowercase().replace([':', '-'], ""))
        .context("monitoring snapshots require server_name, inventory_id, or mac_address")
}

#[derive(Clone, Default)]
pub struct EventFilter {
    pub device: Option<String>,
    pub kind: Option<EventKind>,
    pub severity: Option<Severity>,
    pub since: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    pub before_sequence: Option<u64>,
}

impl EventFilter {
    pub fn parse(arguments: &Value) -> Result<Self> {
        let text = |name: &str| -> Result<Option<String>> {
            arguments
                .get(name)
                .filter(|value| !value.is_null())
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .with_context(|| format!("{name} must be a string"))
                })
                .transpose()
        };
        let kind = text("kind")?
            .map(|value| serde_json::from_value(json!(value)))
            .transpose()?;
        let severity = text("severity")?
            .map(|value| serde_json::from_value(json!(value)))
            .transpose()?;
        let since = text("since")?
            .map(|value| {
                DateTime::parse_from_rfc3339(&value)
                    .map(|value| value.with_timezone(&Utc))
                    .or_else(|_| {
                        value
                            .parse::<NaiveDateTime>()
                            .or_else(|_| {
                                NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f")
                            })
                            .map(|value| value.and_utc())
                    })
                    .or_else(|_| {
                        value
                            .parse::<NaiveDate>()
                            .map(|value| value.and_time(chrono::NaiveTime::MIN).and_utc())
                    })
                    .context("since must be an ISO-8601 timestamp")
            })
            .transpose()?;
        let count = |name: &str| -> Result<Option<u64>> {
            arguments
                .get(name)
                .filter(|value| !value.is_null())
                .map(|value| {
                    value
                        .as_u64()
                        .with_context(|| format!("{name} must be a non-negative integer"))
                })
                .transpose()
        };
        let before_sequence = count("before_sequence")?;
        ensure!(
            before_sequence.is_none_or(|value| value != 0 && value <= i64::MAX as u64),
            "before_sequence must be a positive stored event sequence"
        );
        Ok(Self {
            device: text("device")?,
            kind,
            severity,
            since,
            limit: count("limit")?.map(usize::try_from).transpose()?,
            before_sequence,
        })
    }
}
