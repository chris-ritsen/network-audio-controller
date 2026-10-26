use super::Change;
use super::model::{Issue, Kind, Lifecycle, ObservationState, SCHEMA_VERSION};
use crate::monitoring::journal::model::{EventKind, JournalEvent, Severity};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use caseless::Caseless;
use chrono::DateTime;
use rusqlite::types::Value as Parameter;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const LAYOUT_KEY: &str = "issue_index_layout";
const LAYOUT: &str = "1";
const SCHEMA: &str = "
CREATE TABLE native_monitoring_issues (
    identifier INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_identity TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen INTEGER NOT NULL,
    resolved_at INTEGER,
    state TEXT NOT NULL,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    record TEXT NOT NULL
);
CREATE UNIQUE INDEX native_monitoring_active_issue ON native_monitoring_issues (issue_identity) WHERE state = 'open';
CREATE INDEX native_monitoring_issue_episode ON native_monitoring_issues (issue_identity, first_seen, identifier);
CREATE INDEX native_monitoring_issue_order ON native_monitoring_issues (last_seen, identifier);
CREATE INDEX native_monitoring_issue_history ON native_monitoring_issues (state, resolved_at, identifier);
CREATE INDEX native_monitoring_issue_state ON native_monitoring_issues (state, last_seen, identifier);
CREATE INDEX native_monitoring_issue_kind ON native_monitoring_issues (kind, last_seen, identifier);
CREATE INDEX native_monitoring_issue_severity ON native_monitoring_issues (severity, last_seen, identifier);
CREATE TABLE native_monitoring_issue_devices (
    identity TEXT NOT NULL,
    identifier INTEGER NOT NULL REFERENCES native_monitoring_issues(identifier) ON DELETE CASCADE,
    PRIMARY KEY (identity, identifier)
) WITHOUT ROWID;
CREATE INDEX native_monitoring_issue_device_identifier ON native_monitoring_issue_devices (identifier);
";

#[derive(Default, Deserialize, schemars::JsonSchema)]
pub struct Filter {
    pub device: Option<String>,
    pub kind: Option<Kind>,
    pub severity: Option<Severity>,
    pub state: Option<Lifecycle>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

impl Filter {
    pub fn parse(arguments: &Value) -> Result<Self> {
        let filter: Self = serde_json::from_value(arguments.clone())?;
        ensure!(
            filter
                .limit
                .is_none_or(|limit| { i64::try_from(limit).is_ok_and(|value| value < i64::MAX) }),
            "limit exceeds the supported query range"
        );
        filter.position()?;
        Ok(filter)
    }

    fn position(&self) -> Result<Option<(i64, i64)>> {
        self.cursor
            .as_ref()
            .map(|cursor| {
                let bytes = URL_SAFE_NO_PAD
                    .decode(cursor)
                    .context("invalid issue cursor")?;
                let position: (i64, i64) =
                    serde_json::from_slice(&bytes).context("invalid issue cursor")?;
                ensure!(position.1 > 0, "invalid issue cursor");
                Ok(position)
            })
            .transpose()
    }
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct Page {
    schema_version: u32,
    history_limit: usize,
    count: usize,
    active_count: usize,
    issues: Vec<Issue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

pub fn initialize(transaction: &Transaction<'_>) -> Result<()> {
    let layout: Option<String> = transaction
        .query_row(
            "SELECT value FROM native_journal_state WHERE key = ?1",
            [LAYOUT_KEY],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(layout) = layout {
        ensure!(layout == LAYOUT, "unsupported saved issue layout {layout}");
        return Ok(());
    }

    transaction.execute_batch(SCHEMA)?;
    let mut statement = transaction.prepare(
        "SELECT events.event FROM native_journal_event_index AS entry
         JOIN events ON events.sequence = entry.sequence
         WHERE entry.kind IN ('issue_opened', 'issue_updated', 'issue_resolved')
         ORDER BY entry.sequence",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let raw: String = row.get(0)?;
        let event: JournalEvent = serde_json::from_str(&raw)?;
        let opened = event.kind == EventKind::IssueOpened;
        let issue: Issue = serde_json::from_value(event.current_value)
            .context("invalid saved issue transition")?;
        ensure!(
            issue.scope.device_identity == event.device_identity,
            "saved issue device identity differs from its journal event"
        );
        save(transaction, &issue, opened)?;
    }
    transaction.execute(
        "INSERT INTO native_journal_state VALUES (?1, ?2)",
        params![LAYOUT_KEY, LAYOUT],
    )?;
    Ok(())
}

pub fn restore(transaction: &Transaction<'_>) -> Result<Vec<Issue>> {
    let mut statement =
        transaction.prepare("SELECT record FROM native_monitoring_issues WHERE state = 'open'")?;
    let records = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut issues = Vec::new();
    for record in records {
        let mut issue: Issue = serde_json::from_str(&record)?;
        issue.observation_state = ObservationState::Unobservable;
        save(transaction, &issue, false)?;
        issues.push(issue);
    }
    Ok(issues)
}

pub fn persist(transaction: &Transaction<'_>, changes: &[Change]) -> Result<()> {
    for change in changes {
        save(
            transaction,
            &change.issue,
            change
                .event
                .as_ref()
                .is_some_and(|event| event.kind == EventKind::IssueOpened),
        )?;
    }
    Ok(())
}

fn save(transaction: &Transaction<'_>, issue: &Issue, opened: bool) -> Result<()> {
    ensure!(
        issue.schema_version == SCHEMA_VERSION,
        "unsupported saved issue schema {}",
        issue.schema_version
    );
    ensure!(
        !issue.issue_id.is_empty() && !issue.scope.device_identity.is_empty(),
        "saved issue requires identities"
    );
    ensure!(
        issue.occurrence_count > 0,
        "saved issue occurrence_count must be positive"
    );
    DateTime::parse_from_rfc3339(&issue.first_seen).context("invalid issue first_seen")?;
    let observed = DateTime::parse_from_rfc3339(&issue.last_seen)
        .context("invalid issue last_seen")?
        .timestamp_micros();
    let resolved = issue
        .resolved_at
        .as_ref()
        .map(|resolved| {
            DateTime::parse_from_rfc3339(resolved).map(|value| value.timestamp_micros())
        })
        .transpose()
        .context("invalid issue resolved_at")?;
    let state = serde_json::to_value(issue.state)?;
    let kind = serde_json::to_value(issue.kind)?;
    let severity = serde_json::to_value(issue.severity)?;
    let record = serde_json::to_string(issue)?;
    let identifier: Option<i64> = if opened {
        None
    } else {
        transaction
            .query_row(
                "SELECT identifier FROM native_monitoring_issues
         WHERE issue_identity = ?1 AND first_seen = ?2
         ORDER BY identifier DESC LIMIT 1",
                params![issue.issue_id, issue.first_seen],
                |row| row.get(0),
            )
            .optional()?
    };
    let identifier = if let Some(identifier) = identifier {
        transaction.execute(
            "UPDATE native_monitoring_issues SET last_seen = ?1, state = ?2, kind = ?3, severity = ?4, record = ?5, resolved_at = ?6 WHERE identifier = ?7",
            params![observed, state.as_str(), kind.as_str(), severity.as_str(), record, resolved, identifier],
        )?;
        identifier
    } else {
        transaction.execute(
            "INSERT INTO native_monitoring_issues (issue_identity, first_seen, last_seen, state, kind, severity, record, resolved_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![issue.issue_id, issue.first_seen, observed, state.as_str(), kind.as_str(), severity.as_str(), record, resolved],
        )?;
        transaction.last_insert_rowid()
    };
    let identities = [
        &issue.scope.device_identity,
        &issue.scope.device_name,
        &issue.scope.server_name,
    ]
    .map(|identity| fold(identity));
    transaction.execute("DELETE FROM native_monitoring_issue_devices WHERE identifier = ?1 AND identity NOT IN (?2, ?3, ?4)", params![identifier, identities[0], identities[1], identities[2]])?;
    let mut aliases = transaction
        .prepare_cached("INSERT OR IGNORE INTO native_monitoring_issue_devices VALUES (?1, ?2)")?;
    for identity in identities {
        aliases.execute(params![identity, identifier])?;
    }
    Ok(())
}

pub fn page(connection: &Connection, filter: &Filter, view_limit: usize) -> Result<Page> {
    let limit = if filter.limit.is_none() && filter.cursor.is_none() {
        None
    } else {
        Some(filter.limit.unwrap_or(view_limit))
    };
    let mut query =
        "SELECT identifier, last_seen, record FROM native_monitoring_issues WHERE 1".to_owned();
    let mut parameters = Vec::new();
    if limit.is_none() {
        query.push_str(" AND (state = 'open' OR identifier IN (SELECT identifier FROM native_monitoring_issues WHERE state = 'resolved' ORDER BY resolved_at DESC, identifier DESC LIMIT ?))");
        parameters.push(Parameter::Integer(i64::try_from(view_limit)?));
    }
    for (column, value) in [
        ("state", filter.state.map(serde_json::to_value).transpose()?),
        ("kind", filter.kind.map(serde_json::to_value).transpose()?),
        (
            "severity",
            filter.severity.map(serde_json::to_value).transpose()?,
        ),
    ] {
        if let Some(value) = value {
            query.push_str(&format!(" AND {column} = ?"));
            parameters.push(Parameter::Text(
                value.as_str().context("invalid issue filter")?.to_owned(),
            ));
        }
    }
    if let Some(device) = &filter.device {
        query.push_str(" AND identifier IN (SELECT identifier FROM native_monitoring_issue_devices WHERE identity = ?)");
        parameters.push(Parameter::Text(fold(device)));
    }
    if let Some((timestamp, identifier)) = filter.position()? {
        query.push_str(" AND (last_seen, identifier) < (?, ?)");
        parameters.extend([
            Parameter::Integer(timestamp),
            Parameter::Integer(identifier),
        ]);
    }
    query.push_str(" ORDER BY last_seen DESC, identifier DESC");
    if let Some(limit) = limit {
        query.push_str(" LIMIT ?");
        parameters.push(Parameter::Integer(if limit == 0 {
            0
        } else {
            i64::try_from(limit.saturating_add(1))?
        }));
    }
    let mut statement = connection.prepare(&query)?;
    let mut rows = statement.query(params_from_iter(parameters))?;
    let mut issues = Vec::new();
    let mut last = None;
    let mut more = false;
    while let Some(row) = rows.next()? {
        if limit == Some(issues.len()) {
            more = true;
            break;
        }
        let record: String = row.get(2)?;
        issues.push(serde_json::from_str::<Issue>(&record)?);
        last = Some((row.get::<_, i64>(1)?, row.get::<_, i64>(0)?));
    }
    Ok(Page {
        schema_version: SCHEMA_VERSION,
        history_limit: view_limit,
        count: issues.len(),
        active_count: issues
            .iter()
            .filter(|issue| issue.state == Lifecycle::Open)
            .count(),
        issues,
        next_cursor: if more {
            Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&json!(last))?))
        } else {
            None
        },
    })
}

fn fold(value: &str) -> String {
    value.chars().default_case_fold().collect()
}
