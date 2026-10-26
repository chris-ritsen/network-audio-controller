use super::model::{EventFilter, JournalEvent, SCHEMA_VERSION};
use crate::monitoring::issues;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

mod index;

const DATABASE_LAYOUT: &str = "2";
const DATABASE_BUSY_DEADLINE: Duration = Duration::from_secs(2);
const NEXT_SEQUENCE_KEY: &str = "next_sequence";
const STOPPED_AT_KEY: &str = "stopped_at";
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS layout (layout TEXT NOT NULL, saved_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS events (sequence INTEGER PRIMARY KEY, event TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS gaps (stopped_at TEXT NOT NULL, started_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS devices (identity TEXT PRIMARY KEY, seen_at TEXT, record TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_wireless_devices (mac TEXT PRIMARY KEY, record TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_journal_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_journal_gap_reasons (
    stopped_at TEXT NOT NULL,
    started_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (stopped_at, started_at)
);
";

pub struct JournalDatabase {
    _ownership: File,
    connection: Connection,
    next_sequence: u64,
    view_limit: usize,
    overview: Option<Arc<Overview>>,
}

pub struct Overview {
    pub issues: Value,
    pub events: Value,
    pub disappeared: Value,
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct EventPage {
    schema_version: u32,
    retention_limit: usize,
    count: usize,
    events: Vec<JournalEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_before_sequence: Option<u64>,
    recorded_since: Option<String>,
    observation_gaps: Vec<Value>,
}

impl JournalDatabase {
    pub fn wireless_devices(&self) -> Result<Value> {
        let mut statement = self
            .connection
            .prepare("SELECT mac, record FROM native_wireless_devices ORDER BY mac")?;
        let mut rows = statement.query([])?;
        let mut records = serde_json::Map::new();
        while let Some(row) = rows.next()? {
            let mac: String = row.get(0)?;
            let record: String = row.get(1)?;
            records.insert(mac, serde_json::from_str(&record)?);
        }
        Ok(Value::Object(records))
    }

    pub fn remember_wireless(&mut self, records: &Value) -> Result<()> {
        let records = records
            .as_object()
            .context("Remembered wireless inventory must be an object")?;
        let transaction = self.connection.transaction()?;
        for (mac, record) in records {
            ensure!(
                !mac.is_empty() && record.is_object(),
                "Invalid remembered wireless device"
            );
            transaction.execute("INSERT INTO native_wireless_devices(mac, record) VALUES (?1, ?2) ON CONFLICT(mac) DO UPDATE SET record = excluded.record", params![mac, serde_json::to_string(record)?])?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn open(path: &Path, view_limit: usize) -> Result<Self> {
        ensure!(view_limit != 0, "event_history_limit must be positive");

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let ownership = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("sqlite.lock"))?;
        fs4::FileExt::try_lock(&ownership)
            .with_context(|| format!("acquire event journal ownership {}", path.display()))?;
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(DATABASE_BUSY_DEADLINE)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let transaction = connection.transaction()?;
        transaction.execute_batch(SCHEMA)?;
        let layout: Option<(String, f64)> = transaction
            .query_row("SELECT layout, saved_at FROM layout", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .optional()?;
        let now = Utc::now();

        if let Some((layout, saved_at)) = layout {
            ensure!(
                layout == DATABASE_LAYOUT,
                "unsupported saved event journal layout {layout}"
            );
            ensure!(
                saved_at.is_finite() && saved_at >= 0.0,
                "invalid journal save time"
            );
            let stopped: Option<String> = transaction
                .query_row(
                    "SELECT value FROM native_journal_state WHERE key = ?1",
                    [STOPPED_AT_KEY],
                    |row| row.get(0),
                )
                .optional()?;
            let reason = if stopped.is_some() {
                "daemon_stopped"
            } else {
                "unclean_stop_bound"
            };
            let stopped = match stopped {
                Some(value) => {
                    DateTime::parse_from_rfc3339(&value).context("invalid journal stop time")?;
                    value
                }
                None => DateTime::<Utc>::from_timestamp_micros((saved_at * 1_000_000.0) as i64)
                    .context("invalid journal save time")?
                    .to_rfc3339_opts(SecondsFormat::Micros, true),
            };
            let started = now.to_rfc3339_opts(SecondsFormat::Micros, true);
            transaction.execute(
                "INSERT INTO gaps VALUES (?1, ?2)",
                params![stopped, started],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO native_journal_gap_reasons VALUES (?1, ?2, ?3)",
                params![stopped, started, reason],
            )?;
        } else {
            transaction.execute(
                "INSERT INTO layout VALUES (?1, ?2)",
                params![DATABASE_LAYOUT, now.timestamp_micros() as f64 / 1_000_000.0],
            )?;
        }

        let maximum: i64 =
            transaction.query_row("SELECT COALESCE(MAX(sequence), 0) FROM events", [], |row| {
                row.get(0)
            })?;
        ensure!(maximum >= 0, "invalid journal event sequence");
        let next: Option<String> = transaction
            .query_row(
                "SELECT value FROM native_journal_state WHERE key = ?1",
                [NEXT_SEQUENCE_KEY],
                |row| row.get(0),
            )
            .optional()?;
        let next_sequence = next
            .map(|value| value.parse::<u64>())
            .transpose()?
            .unwrap_or(maximum as u64 + 1);
        ensure!(
            next_sequence > maximum as u64 && next_sequence <= i64::MAX as u64,
            "invalid next event sequence"
        );
        transaction.execute(
            "DELETE FROM native_journal_state WHERE key = ?1",
            [STOPPED_AT_KEY],
        )?;
        transaction.execute(
            "INSERT INTO native_journal_state VALUES (?1, ?2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            params![NEXT_SEQUENCE_KEY, next_sequence.to_string()],
        )?;
        index::initialize(&transaction)?;
        issues::store::initialize(&transaction)?;
        transaction.commit()?;
        Ok(Self {
            _ownership: ownership,
            connection,
            next_sequence,
            view_limit,
            overview: None,
        })
    }

    pub fn append(&mut self, events: Vec<JournalEvent>) -> Result<Vec<JournalEvent>> {
        self.append_observation(events, &[])
    }

    pub fn restore_issues(&mut self) -> Result<Vec<issues::model::Issue>> {
        let transaction = self.connection.transaction()?;
        let restored = issues::store::restore(&transaction)?;
        transaction.commit()?;
        self.overview = None;
        Ok(restored)
    }

    pub fn issues(&self, filter: &issues::store::Filter) -> Result<issues::store::Page> {
        issues::store::page(&self.connection, filter, self.view_limit)
    }

    pub fn overview(&mut self) -> Result<Arc<Overview>> {
        if let Some(overview) = &self.overview {
            return Ok(Arc::clone(overview));
        }

        let overview = Arc::new(Overview {
            issues: serde_json::to_value(self.issues(&Default::default())?)?,
            events: self.overview_page(&Default::default())?,
            disappeared: self.overview_page(&EventFilter {
                kind: Some(super::model::EventKind::DeviceDisappeared),
                ..Default::default()
            })?,
        });
        self.overview = Some(Arc::clone(&overview));
        Ok(overview)
    }

    fn overview_page(&self, filter: &EventFilter) -> Result<Value> {
        let mut page = self.page(filter)?;
        for event in &mut page.events {
            if !matches!(
                event.kind,
                super::model::EventKind::SettingChanged
                    | super::model::EventKind::ChannelRenamed
                    | super::model::EventKind::RouteChanged
            ) {
                for value in [&mut event.previous_value, &mut event.current_value] {
                    if let Some(fields) = value.as_object_mut() {
                        fields.retain(|field, _| {
                            matches!(
                                field.as_str(),
                                "issue_id" | "kind" | "title" | "summary" | "raw_source_fields"
                            )
                        });
                        if let Some(source) = fields
                            .get_mut("raw_source_fields")
                            .and_then(Value::as_object_mut)
                        {
                            source.retain(|field, _| field == "status");
                            if let Some(status) =
                                source.get_mut("status").and_then(Value::as_object_mut)
                            {
                                status.retain(|field, _| field == "status");
                            }
                        }
                    }
                }
            }
            event.raw.retain(|field, _| {
                matches!(
                    field.as_str(),
                    "message"
                        | "category"
                        | "counter"
                        | "previous_observation"
                        | "current_observation"
                )
            });
            for field in ["previous_observation", "current_observation"] {
                if let Some(observation) = event.raw.get_mut(field).and_then(Value::as_object_mut) {
                    observation.retain(|field, _| field == "mac_address");
                }
            }
        }
        Ok(serde_json::to_value(page)?)
    }

    pub fn append_observation(
        &mut self,
        mut events: Vec<JournalEvent>,
        changes: &[issues::Change],
    ) -> Result<Vec<JournalEvent>> {
        if events.is_empty() && changes.is_empty() {
            return Ok(events);
        }

        let next = self
            .next_sequence
            .checked_add(events.len() as u64)
            .filter(|sequence| *sequence <= i64::MAX as u64)
            .context("journal event sequence exhausted")?;

        for (offset, event) in events.iter_mut().enumerate() {
            event.sequence = self.next_sequence + offset as u64;
            event.validate()?;
        }

        let transaction = self.connection.transaction()?;
        {
            let mut insert = transaction.prepare("INSERT INTO events VALUES (?1, ?2)")?;

            for event in &events {
                insert.execute(params![
                    event.sequence as i64,
                    serde_json::to_string(event)?
                ])?;
                index::insert(&transaction, event)?;
            }
        }

        transaction.execute(
            "UPDATE native_journal_state SET value = ?1 WHERE key = ?2",
            params![next.to_string(), NEXT_SEQUENCE_KEY],
        )?;
        transaction.execute("UPDATE layout SET saved_at = ?1", [wall_time()])?;
        issues::store::persist(&transaction, changes)?;
        transaction.commit()?;
        self.next_sequence = next;
        self.overview = None;
        Ok(events)
    }

    pub fn page(&self, filter: &EventFilter) -> Result<EventPage> {
        let limit = filter.limit.unwrap_or(self.view_limit).min(self.view_limit);
        let requested = if limit == 0 {
            0
        } else {
            limit.saturating_add(1)
        };
        let mut events = index::page(&self.connection, filter, requested)?;
        let more = events.len() > limit;
        events.truncate(limit);

        let recorded_since: Option<String> = self
            .connection
            .query_row(
                "SELECT json_extract(event, '$.timestamp') FROM events ORDER BY sequence LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let mut statement = self.connection.prepare(
            "SELECT gaps.stopped_at, gaps.started_at, COALESCE(reason, 'daemon_stopped')
             FROM gaps LEFT JOIN native_journal_gap_reasons USING (stopped_at, started_at)
             ORDER BY gaps.started_at",
        )?;
        let mut rows = statement.query([])?;
        let mut gaps = Vec::new();

        while let Some(row) = rows.next()? {
            gaps.push(json!({
                "stopped_at": row.get::<_, String>(0)?,
                "started_at": row.get::<_, String>(1)?,
                "reason": row.get::<_, String>(2)?,
            }));
        }

        Ok(EventPage {
            schema_version: SCHEMA_VERSION,
            retention_limit: self.view_limit,
            count: events.len(),
            next_before_sequence: more
                .then(|| events.last().map(|event| event.sequence))
                .flatten(),
            events,
            recorded_since,
            observation_gaps: gaps,
        })
    }

    pub fn clear(&mut self) -> Result<Value> {
        let transaction = self.connection.transaction()?;
        let cleared = transaction.execute("DELETE FROM events", [])?;
        transaction.execute("UPDATE layout SET saved_at = ?1", [wall_time()])?;
        transaction.commit()?;
        self.overview = None;
        Ok(json!({
            "cleared": cleared,
            "scope": "local_event_journal",
            "device_commands_sent": 0,
        }))
    }

    pub fn remember(
        &mut self,
        identity: &str,
        record: &Value,
        seen_at: Option<&str>,
    ) -> Result<()> {
        self.connection.execute(
            "INSERT INTO devices VALUES (?1, ?2, ?3) ON CONFLICT (identity) DO UPDATE SET
             seen_at = COALESCE(excluded.seen_at, devices.seen_at), record = excluded.record",
            params![identity, seen_at, serde_json::to_string(record)?],
        )?;
        Ok(())
    }

    pub fn forget(&mut self, identity: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM devices WHERE identity = ?1", [identity])?;
        Ok(())
    }

    pub fn remembered(&self) -> Result<Value> {
        let mut result = serde_json::Map::new();
        let mut statement = self
            .connection
            .prepare("SELECT identity, seen_at, record FROM devices")?;
        let mut rows = statement.query([])?;

        while let Some(row) = rows.next()? {
            let identity: String = row.get(0)?;
            let seen_at: Option<String> = row.get(1)?;
            let record: String = row.get(2)?;
            let record: Value = serde_json::from_str(&record)?;
            ensure!(
                record.is_object(),
                "remembered device record must be an object"
            );
            result.insert(identity, json!({"seen_at": seen_at, "record": record}));
        }

        Ok(Value::Object(result))
    }

    pub fn stop(&mut self) -> Result<()> {
        let now = Utc::now();
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO native_journal_state VALUES (?1, ?2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            params![STOPPED_AT_KEY, now.to_rfc3339_opts(SecondsFormat::Micros, true)],
        )?;
        transaction.execute(
            "UPDATE layout SET saved_at = ?1",
            [now.timestamp_micros() as f64 / 1_000_000.0],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_gap(&mut self, stopped_at: &str, started_at: &str) -> Result<()> {
        DateTime::parse_from_rfc3339(stopped_at)?;
        DateTime::parse_from_rfc3339(started_at)?;
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO gaps VALUES (?1, ?2)",
            params![stopped_at, started_at],
        )?;
        transaction.execute(
            "INSERT OR REPLACE INTO native_journal_gap_reasons VALUES (?1, ?2, 'observation_stream_gap')",
            params![stopped_at, started_at],
        )?;
        transaction.commit()?;
        self.overview = None;
        Ok(())
    }
}

fn wall_time() -> f64 {
    Utc::now().timestamp_micros() as f64 / 1_000_000.0
}
