use super::{HistoryStore, LevelHistory, RETENTION_SECONDS};
use anyhow::{Context, Result, ensure};
use netaudio_core::host_audio::recording::Record;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::sync::Arc;

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
    for (session, sequence) in unfinished {
        let record = Record::Finished {
            observed_at: chrono::Utc::now().to_rfc3339(),
            error: Some("The previous recorder owner ended without a final record; the stop time and missing coverage are unknown".to_owned()),
        };
        transaction.execute(
            "INSERT INTO native_host_recording_records (session, sequence, record) VALUES (?1, ?2, ?3)",
            params![session, sequence + 1, serde_json::to_string(&record)?],
        )?;
        transaction.execute(
            "UPDATE native_host_recording_sessions SET sequence = ?2, finished = 1 WHERE session = ?1",
            params![session, sequence + 1],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

impl HistoryStore {
    pub async fn record_host_audio(
        self: &Arc<Self>,
        session: &str,
        sequence: u64,
        record: &Record,
        history: Arc<LevelHistory>,
    ) -> Result<()> {
        ensure!(
            sequence > 0 && sequence <= i64::MAX as u64,
            "Invalid recording sequence"
        );
        let sequence = sequence as i64;
        let store = self.clone();
        let session = session.to_owned();
        let value = serde_json::to_string(record)?;
        let requested = matches!(record, Record::Requested { .. });
        let finished = matches!(record, Record::Finished { .. });
        let record = record.clone();
        let second = match &record {
            Record::Frame(frame) => Some(frame.second),
            _ => None,
        };
        tokio::task::spawn_blocking(move || {
            let mut connection = store.connection.lock().map_err(|_| anyhow::anyhow!("Signal history database lock poisoned"))?;
            let transaction = connection.transaction()?;
            if requested {
                ensure!(sequence == 1, "A recording must start at sequence one");
                transaction.execute("INSERT INTO native_host_recording_sessions VALUES (?1, 0, 0)", [&session])?;
            }
            let previous: Option<(i64, bool)> = transaction.query_row(
                "SELECT sequence, finished FROM native_host_recording_sessions WHERE session = ?1",
                [&session], |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional()?;
            ensure!(previous == Some((sequence - 1, false)), "The recording history sequence has a gap or has already ended");
            transaction.execute("INSERT INTO native_host_recording_records (session, sequence, record, measured_second) VALUES (?1, ?2, ?3, ?4)", params![session, sequence, value, second])?;
            transaction.execute("UPDATE native_host_recording_sessions SET sequence = ?2, finished = ?3 WHERE session = ?1", params![session, sequence, finished])?;
            transaction.commit()?;
            project(&record, &history)?;
            Ok(())
        }).await.context("Recording persistence worker failed")?
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
    let mut statement = connection.prepare("SELECT record FROM native_host_recording_records WHERE measured_second >= ?1 ORDER BY cursor")?;
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
