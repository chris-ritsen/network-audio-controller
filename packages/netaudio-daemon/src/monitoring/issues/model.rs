use crate::monitoring::journal::model::{
    Derivation, EventKind, JournalEvent, Severity, device_identity,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub(super) const SCHEMA_VERSION: u32 = 1;
const IDENTITY_DIGEST_BYTES: usize = 10;

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    AddressConflict,
    SubnetConflict,
    ClockSynchronization,
    ClockMuted,
    ClockFrequencyVariation,
    BluetoothLinkLost,
    VideoSignalFailure,
    PullupMismatch,
    SubscriptionFailure,
    ReceiverHealthDegraded,
    ReceiverLatencyPressure,
    ReceiverLatePackets,
    LicensingFailure,
    SafeState,
    UpgradeRequired,
    RebootRequired,
    ConfigurationPartitionFault,
    PersistenceFault,
    TelemetryStale,
    TelemetryMissing,
    ConfigurationDivergence,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    Open,
    Resolved,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObservationState {
    Observed,
    Unobservable,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceClass {
    DirectObservation,
    DerivedState,
    Inference,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, schemars::JsonSchema)]
pub struct Scope {
    pub device_identity: String,
    pub device_name: String,
    pub server_name: String,
    pub interface_identity: Option<String>,
    pub channel_identity: Option<String>,
    pub flow_identity: Option<String>,
}

impl Scope {
    pub fn device(record: &Value) -> Result<Self> {
        Ok(Self {
            device_identity: device_identity(record)?,
            device_name: record["name"].as_str().unwrap_or_default().to_owned(),
            server_name: record["server_name"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            interface_identity: None,
            channel_identity: None,
            flow_identity: None,
        })
    }
}

pub struct Finding {
    pub kind: Kind,
    pub severity: Severity,
    pub title: String,
    pub summary: String,
    pub raw: Value,
    pub source: &'static str,
    pub evidence_class: EvidenceClass,
    pub remediation: &'static str,
}

#[derive(Clone, Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct Issue {
    pub schema_version: u32,
    pub issue_id: String,
    pub kind: Kind,
    pub severity: Severity,
    pub state: Lifecycle,
    pub first_seen: String,
    pub last_seen: String,
    pub resolved_at: Option<String>,
    pub occurrence_count: u64,
    pub observation_state: ObservationState,
    pub scope: Scope,
    pub title: String,
    pub summary: String,
    pub raw_source_fields: Value,
    pub evidence_source: String,
    pub evidence_class: EvidenceClass,
    pub suggested_remediation: String,
}

impl Issue {
    pub fn candidate(scope: Scope, timestamp: &str, finding: Finding) -> Result<Self> {
        let kind = serde_json::to_value(finding.kind)?;
        let name = kind
            .as_str()
            .context("issue kind must serialize as a string")?;
        let material = [
            name,
            &scope.device_identity,
            scope.interface_identity.as_deref().unwrap_or_default(),
            scope.channel_identity.as_deref().unwrap_or_default(),
            scope.flow_identity.as_deref().unwrap_or_default(),
        ]
        .join("|");
        let digest = Sha256::digest(material.as_bytes());
        let issue_id = format!("{name}:{}", hex::encode(&digest[..IDENTITY_DIGEST_BYTES]));
        Ok(Self {
            schema_version: SCHEMA_VERSION,
            issue_id,
            kind: finding.kind,
            severity: finding.severity,
            state: Lifecycle::Open,
            first_seen: timestamp.to_owned(),
            last_seen: timestamp.to_owned(),
            resolved_at: None,
            occurrence_count: 1,
            observation_state: ObservationState::Observed,
            scope,
            title: finding.title,
            summary: finding.summary,
            raw_source_fields: finding.raw,
            evidence_source: finding.source.to_owned(),
            evidence_class: finding.evidence_class,
            suggested_remediation: finding.remediation.to_owned(),
        })
    }

    pub fn materially_matches(&self, previous: &Self) -> bool {
        if self.kind != previous.kind
            || self.severity != previous.severity
            || self.observation_state != previous.observation_state
            || self.scope != previous.scope
        {
            return false;
        }

        if matches!(
            self.kind,
            Kind::ReceiverLatencyPressure
                | Kind::ReceiverLatePackets
                | Kind::ClockFrequencyVariation
        ) {
            return true;
        }

        self.title == previous.title
            && self.summary == previous.summary
            && self.material_evidence_matches(previous)
            && self.evidence_source == previous.evidence_source
            && self.evidence_class == previous.evidence_class
            && self.suggested_remediation == previous.suggested_remediation
    }

    fn material_evidence_matches(&self, previous: &Self) -> bool {
        if self.kind == Kind::TelemetryStale {
            return true;
        }
        let timestamp = match self.kind {
            Kind::ClockSynchronization | Kind::ClockMuted
                if self.evidence_source == "device_clock_status" =>
            {
                "clock_observed_at"
            }
            Kind::BluetoothLinkLost | Kind::VideoSignalFailure => "observed_at_unix",
            _ => return self.raw_source_fields == previous.raw_source_fields,
        };
        match (
            self.raw_source_fields.as_object(),
            previous.raw_source_fields.as_object(),
        ) {
            (Some(current), Some(previous)) => current
                .iter()
                .filter(|(key, _)| key.as_str() != timestamp)
                .eq(previous.iter().filter(|(key, _)| key.as_str() != timestamp)),
            _ => self.raw_source_fields == previous.raw_source_fields,
        }
    }

    pub fn event(&self, previous: Option<&Self>, timestamp: &str) -> Result<JournalEvent> {
        let resolved = self.state == Lifecycle::Resolved;
        let mut event = JournalEvent::observed(
            &json!({
                "device_identity": self.scope.device_identity,
                "name": self.scope.device_name,
                "server_name": self.scope.server_name,
            }),
            timestamp,
            if resolved {
                EventKind::IssueResolved
            } else if previous.is_some() {
                EventKind::IssueUpdated
            } else {
                EventKind::IssueOpened
            },
            if resolved {
                Severity::Info
            } else {
                self.severity
            },
            serde_json::to_value(previous)?,
            serde_json::to_value(self)?,
            &self.evidence_source,
        )?;
        event.raw.extend([
            ("issue_id".to_owned(), json!(self.issue_id)),
            ("issue_kind".to_owned(), json!(self.kind)),
            ("evidence_class".to_owned(), json!(self.evidence_class)),
            (
                "raw_source_fields".to_owned(),
                self.raw_source_fields.clone(),
            ),
            (
                "suggested_remediation".to_owned(),
                json!(self.suggested_remediation),
            ),
        ]);
        event.derivation_status = if self.evidence_class == EvidenceClass::DirectObservation {
            Derivation::Observed
        } else {
            Derivation::Derived
        };
        event
            .interface_identity
            .clone_from(&self.scope.interface_identity);
        event
            .channel_identity
            .clone_from(&self.scope.channel_identity);
        event.flow_identity.clone_from(&self.scope.flow_identity);
        Ok(event)
    }
}
