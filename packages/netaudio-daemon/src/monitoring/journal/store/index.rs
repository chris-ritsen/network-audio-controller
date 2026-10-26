use super::super::model::{EventFilter, JournalEvent};
use anyhow::{Context, Result, ensure};
use caseless::Caseless;
use chrono::DateTime;
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};

const INDEX_LAYOUT: &str = "1";
const INDEX_LAYOUT_KEY: &str = "event_index_layout";
const SCHEMA: &str = "
CREATE TABLE native_journal_event_index (
    sequence INTEGER PRIMARY KEY REFERENCES events(sequence) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    observed_at INTEGER NOT NULL
);
CREATE INDEX native_journal_event_kind ON native_journal_event_index (kind, sequence);
CREATE INDEX native_journal_event_severity ON native_journal_event_index (severity, sequence);
CREATE INDEX native_journal_event_time ON native_journal_event_index (observed_at, sequence);
CREATE TABLE native_journal_event_devices (
    identity TEXT NOT NULL,
    sequence INTEGER NOT NULL REFERENCES events(sequence) ON DELETE CASCADE,
    PRIMARY KEY (identity, sequence)
) WITHOUT ROWID;
CREATE INDEX native_journal_event_device_sequence ON native_journal_event_devices (sequence);
";

pub fn initialize(transaction: &Transaction<'_>) -> Result<()> {
    let layout: Option<String> = transaction
        .query_row(
            "SELECT value FROM native_journal_state WHERE key = ?1",
            [INDEX_LAYOUT_KEY],
            |row| row.get(0),
        )
        .optional()?;

    if let Some(layout) = layout {
        ensure!(
            layout == INDEX_LAYOUT,
            "unsupported journal index layout {layout}"
        );
        return Ok(());
    }

    transaction.execute_batch(SCHEMA)?;
    let mut statement =
        transaction.prepare("SELECT sequence, event FROM events ORDER BY sequence")?;
    let mut rows = statement.query([])?;

    while let Some(row) = rows.next()? {
        let sequence: i64 = row.get(0)?;
        let raw: String = row.get(1)?;
        let event: JournalEvent =
            serde_json::from_str(&raw).context("invalid stored monitoring event")?;
        event.validate()?;
        ensure!(
            sequence > 0 && sequence as u64 == event.sequence,
            "journal event sequence mismatch"
        );
        insert(transaction, &event)?;
    }

    transaction.execute(
        "INSERT INTO native_journal_state VALUES (?1, ?2)",
        params![INDEX_LAYOUT_KEY, INDEX_LAYOUT],
    )?;
    Ok(())
}

pub fn insert(transaction: &Transaction<'_>, event: &JournalEvent) -> Result<()> {
    let kind = serde_json::to_value(event.kind)?;
    let severity = serde_json::to_value(event.severity)?;
    let timestamp = DateTime::parse_from_rfc3339(&event.timestamp)?.timestamp_micros();
    transaction
        .prepare_cached("INSERT INTO native_journal_event_index VALUES (?1, ?2, ?3, ?4)")?
        .execute(params![
            event.sequence as i64,
            kind.as_str(),
            severity.as_str(),
            timestamp
        ])?;
    let mut aliases = transaction
        .prepare_cached("INSERT OR IGNORE INTO native_journal_event_devices VALUES (?1, ?2)")?;

    for identity in [
        &event.device_identity,
        &event.device_name,
        &event.server_name,
    ] {
        aliases.execute(params![fold(identity), event.sequence as i64])?;
    }

    Ok(())
}

pub fn page(
    connection: &Connection,
    filter: &EventFilter,
    limit: usize,
) -> Result<Vec<JournalEvent>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut query = String::from(
        "SELECT events.sequence, events.event FROM native_journal_event_index AS entry
         JOIN events ON events.sequence = entry.sequence WHERE entry.sequence < ?",
    );
    let mut parameters = vec![Value::Integer(
        filter
            .before_sequence
            .map(i64::try_from)
            .transpose()?
            .unwrap_or(i64::MAX),
    )];

    for (column, value) in [
        ("kind", filter.kind.map(serde_json::to_value).transpose()?),
        (
            "severity",
            filter.severity.map(serde_json::to_value).transpose()?,
        ),
    ] {
        if let Some(value) = value {
            query.push_str(&format!(" AND entry.{column} = ?"));
            parameters.push(Value::Text(
                value.as_str().context("invalid event filter")?.to_owned(),
            ));
        }
    }

    if let Some(device) = &filter.device {
        query.push_str(
            " AND entry.sequence IN
             (SELECT sequence FROM native_journal_event_devices WHERE identity = ?)",
        );
        parameters.push(Value::Text(fold(device)));
    }

    if let Some(since) = filter.since {
        query.push_str(" AND entry.observed_at >= ?");
        parameters.push(Value::Integer(since.timestamp_micros()));
    }

    query.push_str(" ORDER BY entry.sequence DESC LIMIT ?");
    parameters.push(Value::Integer(i64::try_from(limit)?));
    let mut statement = connection.prepare(&query)?;
    let mut rows = statement.query(params_from_iter(parameters))?;
    let mut events = Vec::new();

    while let Some(row) = rows.next()? {
        let sequence: i64 = row.get(0)?;
        let raw: String = row.get(1)?;
        let event: JournalEvent =
            serde_json::from_str(&raw).context("invalid stored monitoring event")?;
        event.validate()?;
        ensure!(
            sequence > 0 && sequence as u64 == event.sequence,
            "journal event sequence mismatch"
        );
        events.push(event);
    }

    Ok(events)
}

fn fold(value: &str) -> String {
    value.chars().default_case_fold().collect()
}
