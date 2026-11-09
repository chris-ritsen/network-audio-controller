use super::{HistoryStore, LevelHistory, SECOND_SLOTS};
use anyhow::{ensure, Context, Result};
use netaudio_core::host_audio::recording::Record;
use rusqlite::{params, Connection, Transaction};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) const EXPIRY_BATCH: usize = 2000;
const PENDING_LIMIT: usize = 4096;
const RETENTION_SECONDS: i64 = SECOND_SLOTS as i64;

#[derive(Default)]
pub(super) struct Pending {
    records: Vec<Stored>,
    sessions: BTreeMap<String, i64>,
}

pub(super) struct Stored {
    finished: bool,
    requested: bool,
    second: i64,
    sequence: i64,
    session: String,
    value: String,
}

pub(super) fn store(transaction: &Transaction, records: &[Stored]) -> Result<()> {
    let mut sessions =
        transaction.prepare("INSERT INTO native_host_recording_sessions VALUES (?1, 0, 0)")?;
    let mut inserts = transaction.prepare(
        "INSERT INTO native_host_recording_records (session, sequence, record, measured_second) VALUES (?1, ?2, ?3, ?4)",
    )?;
    let mut updates = transaction.prepare(
        "UPDATE native_host_recording_sessions SET sequence = ?2, finished = ?3 WHERE session = ?1",
    )?;
    for record in records {
        if record.requested {
            sessions.execute([&record.session])?;
        }
        inserts.execute(params![
            record.session,
            record.sequence,
            record.value,
            record.second
        ])?;
        updates.execute(params![record.session, record.sequence, record.finished])?;
    }
    Ok(())
}

pub(super) fn initialize(connection: &mut Connection) -> Result<()> {
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS native_host_recording_sessions (
            session TEXT PRIMARY KEY,
            sequence INTEGER NOT NULL,
            finished INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS native_host_recording_records (
            cursor INTEGER PRIMARY KEY AUTOINCREMENT,
            session TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            record TEXT NOT NULL,
            measured_second INTEGER,
            UNIQUE (session, sequence),
            FOREIGN KEY (session) REFERENCES native_host_recording_sessions
        );
        CREATE INDEX IF NOT EXISTS native_host_recording_time ON native_host_recording_records (measured_second);",
    )?;
    let unfinished = {
        let mut statement = transaction.prepare(
            "SELECT session, sequence FROM native_host_recording_sessions WHERE finished = 0",
        )?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let now = chrono::Utc::now();
    for (session, sequence) in unfinished {
        let record = Record::Finished {
            observed_at: now.to_rfc3339(),
            error: Some("The previous recorder owner ended without a final record; the stop time and missing coverage are unknown".to_owned()),
        };
        transaction.execute(
            "INSERT INTO native_host_recording_records (session, sequence, record, measured_second) VALUES (?1, ?2, ?3, ?4)",
            params![session, sequence + 1, serde_json::to_string(&record)?, now.timestamp()],
        )?;
        transaction.execute(
            "UPDATE native_host_recording_sessions SET sequence = ?2, finished = 1 WHERE session = ?1",
            params![session, sequence + 1],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

pub(super) fn expire(transaction: &Transaction, now: i64) -> Result<usize> {
    let undated = transaction.execute(
        "DELETE FROM native_host_recording_records WHERE cursor IN (
             SELECT cursor FROM native_host_recording_records
             WHERE measured_second IS NULL LIMIT ?1
         )",
        [EXPIRY_BATCH as i64],
    )?;
    let dated = transaction.execute(
        "DELETE FROM native_host_recording_records WHERE cursor IN (
             SELECT cursor FROM native_host_recording_records
             WHERE measured_second < ?1 ORDER BY measured_second LIMIT ?2
         )",
        params![now - RETENTION_SECONDS, (EXPIRY_BATCH - undated) as i64],
    )?;

    if undated + dated > 0 {
        transaction.execute(
            "DELETE FROM native_host_recording_sessions WHERE finished = 1 AND NOT EXISTS (
                 SELECT 1 FROM native_host_recording_records AS record
                 WHERE record.session = native_host_recording_sessions.session
             )",
            [],
        )?;
    }

    Ok(undated + dated)
}

impl HistoryStore {
    pub fn record_host_audio(
        &self,
        session: &str,
        sequence: u64,
        record: &Record,
        history: &LevelHistory,
    ) -> Result<()> {
        ensure!(
            sequence > 0 && sequence <= i64::MAX as u64,
            "Invalid recording sequence"
        );
        let sequence = sequence as i64;
        let requested = matches!(record, Record::Requested { .. });
        let finished = matches!(record, Record::Finished { .. });
        let second = match record {
            Record::Finished { observed_at, .. }
            | Record::Requested { observed_at, .. }
            | Record::Started { observed_at, .. } => {
                chrono::DateTime::parse_from_rfc3339(observed_at)?.timestamp()
            }
            Record::Frame(frame) => frame.second,
        };
        let value = serde_json::to_string(record)?;
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("Recording history queue lock poisoned"))?;
            ensure!(
                pending.records.len() < PENDING_LIMIT,
                "The recording history is not being saved"
            );
            let previous = pending.sessions.get(session).copied();
            if requested {
                ensure!(sequence == 1, "A recording must start at sequence one");
                ensure!(
                    previous.is_none(),
                    "The recording history already has this recording"
                );
            } else {
                ensure!(
                    previous == Some(sequence - 1),
                    "The recording history sequence has a gap or has already ended"
                );
            }
            if finished {
                pending.sessions.remove(session);
            } else {
                pending.sessions.insert(session.to_owned(), sequence);
            }
            pending.records.push(Stored {
                finished,
                requested,
                second,
                sequence,
                session: session.to_owned(),
                value,
            });
        }
        project(record, history)?;
        history.changed.notify_one();
        Ok(())
    }

    pub(super) fn pending_records(&self) -> Result<Vec<Stored>> {
        Ok(std::mem::take(
            &mut self
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("Recording history queue lock poisoned"))?
                .records,
        ))
    }

    pub(super) fn requeue(&self, records: Vec<Stored>) -> Result<()> {
        self.pending
            .lock()
            .map_err(|_| anyhow::anyhow!("Recording history queue lock poisoned"))?
            .records
            .splice(0..0, records);
        Ok(())
    }

    pub async fn host_audio_records(self: &Arc<Self>, after: u64, limit: u32) -> Result<Value> {
        ensure!(
            after <= i64::MAX as u64 && (1..=1000).contains(&limit),
            "Invalid recording history cursor or limit"
        );
        let permit = self
            .recording_reads
            .clone()
            .try_acquire_owned()
            .context("Recording history read capacity is busy")?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let connection = store.connection.lock().map_err(|_| anyhow::anyhow!("Signal history database lock poisoned"))?;
            let mut statement = connection.prepare("SELECT cursor, session, sequence, record FROM native_host_recording_records WHERE cursor > ?1 ORDER BY cursor LIMIT ?2")?;
            let mut rows = statement.query(params![after as i64, limit + 1])?;
            let mut records = Vec::new();
            let mut cursor = after as i64;
            let mut more = false;
            let mut bytes = 0;
            while let Some(row) = rows.next()? {
                if records.len() == limit as usize { more = true; break; }
                let stored: String = row.get(3)?;
                if !records.is_empty() && bytes + stored.len() > 8 * 1024 * 1024 { more = true; break; }
                bytes += stored.len();
                cursor = row.get(0)?;
                let record: Value = serde_json::from_str(&stored)?;
                records.push(json!({"cursor":cursor,"session":row.get::<_,String>(1)?,"sequence":row.get::<_,i64>(2)?,"record":record}));
            }
            Ok(json!({"records":records,"next_cursor":cursor,"has_more":more}))
        }).await.context("Recording history query worker failed")?
    }
}

pub(super) fn restore(connection: &Connection, history: &LevelHistory, now: i64) -> Result<()> {
    let mut statement = connection.prepare("SELECT record FROM native_host_recording_records WHERE measured_second >= ?1 ORDER BY measured_second, cursor")?;
    let mut rows = statement.query([now - RETENTION_SECONDS])?;
    while let Some(row) = rows.next()? {
        project(
            &serde_json::from_str::<Record>(&row.get::<_, String>(0)?)?,
            history,
        )?;
    }
    Ok(())
}

fn project(record: &Record, history: &LevelHistory) -> Result<()> {
    if let Record::Frame(frame) = record
        && frame.complete
    {
        for (name, reading) in &frame.levels {
            if reading.error.is_none() {
                history.record_value(
                    "jack",
                    name,
                    frame.second,
                    reading.peak_dbfs.map(|peak| peak as f32),
                    reading.peak_dbfs.is_some_and(|peak| peak >= -80.0),
                )?;
            }
        }
    }
    Ok(())
}
