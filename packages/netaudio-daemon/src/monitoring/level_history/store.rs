mod recording;

use super::{LevelHistory, MINUTE_SLOTS, Minute, RETENTION_SECONDS, SECOND_SLOTS, Second, Track};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const LAYOUT_VERSION: i64 = 1;
const COMMIT_COALESCING: Duration = Duration::from_secs(1);
const DATABASE_BUSY_DEADLINE: Duration = Duration::from_secs(2);
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS native_history_layout (
    version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS native_history_tracks (
    source TEXT NOT NULL,
    point TEXT NOT NULL,
    first_second INTEGER NOT NULL,
    newest_second INTEGER NOT NULL,
    PRIMARY KEY (source, point)
);
CREATE TABLE IF NOT EXISTS native_history_seconds (
    source TEXT NOT NULL,
    point TEXT NOT NULL,
    slot INTEGER NOT NULL,
    stamp INTEGER NOT NULL,
    peak REAL,
    signal INTEGER NOT NULL,
    PRIMARY KEY (source, point, slot),
    FOREIGN KEY (source, point) REFERENCES native_history_tracks ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS native_history_minutes (
    source TEXT NOT NULL,
    point TEXT NOT NULL,
    slot INTEGER NOT NULL,
    stamp INTEGER NOT NULL,
    peak REAL,
    peak_second INTEGER NOT NULL,
    signal INTEGER NOT NULL,
    recorded INTEGER NOT NULL,
    PRIMARY KEY (source, point, slot),
    FOREIGN KEY (source, point) REFERENCES native_history_tracks ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS native_history_expiry ON native_history_tracks (newest_second);
";

pub struct HistoryStore {
    _ownership: std::fs::File,
    connection: Mutex<Connection>,
    recording_reads: Arc<tokio::sync::Semaphore>,
}

struct ChangedTrack {
    source: String,
    point: String,
    first_second: i64,
    newest_second: i64,
    seconds: Vec<(usize, Second)>,
    minutes: Vec<(usize, Minute)>,
}

impl HistoryStore {
    pub async fn open(path: &Path, history: Arc<LevelHistory>) -> Result<Arc<Self>> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let ownership = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path.with_extension("sqlite.lock"))?;
            fs4::FileExt::try_lock(&ownership)
                .with_context(|| format!("acquire signal history ownership {}", path.display()))?;
            let mut connection = Connection::open(&path)
                .with_context(|| format!("open signal history {}", path.display()))?;
            connection.busy_timeout(DATABASE_BUSY_DEADLINE)?;
            connection.pragma_update(None, "foreign_keys", "ON")?;
            connection.pragma_update(None, "journal_mode", "WAL")?;
            connection.pragma_update(None, "synchronous", "FULL")?;
            let transaction = connection.transaction()?;
            transaction.execute_batch(SCHEMA)?;
            let version: Option<i64> = transaction
                .query_row("SELECT version FROM native_history_layout", [], |row| {
                    row.get(0)
                })
                .optional()?;

            if let Some(version) = version {
                ensure!(
                    version == LAYOUT_VERSION,
                    "unsupported native signal history layout {version}"
                );
            } else {
                transaction.execute(
                    "INSERT INTO native_history_layout VALUES (?1)",
                    [LAYOUT_VERSION],
                )?;
            }

            transaction.commit()?;
            recording::initialize(&mut connection)?;
            load(&connection, &history, chrono::Utc::now().timestamp())?;
            recording::restore(&connection, &history, chrono::Utc::now().timestamp())?;
            *history
                .legacy
                .lock()
                .map_err(|_| anyhow::anyhow!("saved signal history lock poisoned"))? =
                super::legacy::load(&connection, chrono::Utc::now().timestamp())?;
            Ok(Arc::new(Self {
                _ownership: ownership,
                connection: Mutex::new(connection),
                recording_reads: Arc::new(tokio::sync::Semaphore::new(2)),
            }))
        })
        .await
        .context("signal history initialization worker failed")?
    }

    pub async fn run(
        self: Arc<Self>,
        history: Arc<LevelHistory>,
        cancellation: CancellationToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = history.changed.notified() => {}
            }

            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(COMMIT_COALESCING) => {}
            }

            self.flush(Arc::clone(&history)).await?;
        }
    }

    pub async fn flush(self: &Arc<Self>, history: Arc<LevelHistory>) -> Result<()> {
        let store = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let now = chrono::Utc::now().timestamp();
            let changes = changed_tracks(&history, now)?;
            let outcome = store.write(&changes, now);

            if outcome.is_err() {
                restore_dirty(&history, &changes)?;
            }

            outcome
        })
        .await
        .context("signal history persistence worker failed")?
    }

    fn write(&self, changes: &[ChangedTrack], now: i64) -> Result<()> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("signal history database lock poisoned"))?;
        let transaction = connection.transaction()?;
        {
            let mut tracks = transaction.prepare(
                "INSERT INTO native_history_tracks VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (source, point) DO UPDATE SET
                 first_second = excluded.first_second, newest_second = excluded.newest_second",
            )?;
            let mut seconds = transaction.prepare(
                "INSERT INTO native_history_seconds VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT (source, point, slot) DO UPDATE SET
                 stamp = excluded.stamp, peak = excluded.peak, signal = excluded.signal",
            )?;
            let mut minutes = transaction.prepare(
                "INSERT INTO native_history_minutes VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT (source, point, slot) DO UPDATE SET
                 stamp = excluded.stamp, peak = excluded.peak, peak_second = excluded.peak_second,
                 signal = excluded.signal, recorded = excluded.recorded",
            )?;

            for track in changes {
                tracks.execute(params![
                    track.source,
                    track.point,
                    track.first_second,
                    track.newest_second
                ])?;

                for (slot, second) in &track.seconds {
                    seconds.execute(params![
                        track.source,
                        track.point,
                        *slot as i64,
                        second.stamp,
                        second.peak,
                        second.signal,
                    ])?;
                }

                for (slot, minute) in &track.minutes {
                    minutes.execute(params![
                        track.source,
                        track.point,
                        *slot as i64,
                        minute.stamp,
                        minute.peak,
                        minute.peak_second,
                        minute.signal as i64,
                        minute.recorded as i64,
                    ])?;
                }
            }
        }

        transaction.execute(
            "DELETE FROM native_history_tracks WHERE newest_second < ?1",
            [now - RETENTION_SECONDS],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

fn changed_tracks(history: &LevelHistory, now: i64) -> Result<Vec<ChangedTrack>> {
    let mut sources = history.lock()?;
    let mut changes = Vec::new();

    for (source, state) in sources.iter_mut() {
        state
            .tracks
            .retain(|_, track| track.newest_second >= now - RETENTION_SECONDS);

        for (point, track) in &mut state.tracks {
            if track.dirty_seconds.is_empty() && track.dirty_minutes.is_empty() {
                continue;
            }

            changes.push(ChangedTrack {
                source: source.clone(),
                point: point.clone(),
                first_second: track.first_second,
                newest_second: track.newest_second,
                seconds: std::mem::take(&mut track.dirty_seconds)
                    .into_iter()
                    .map(|slot| (slot, track.seconds[slot]))
                    .collect(),
                minutes: std::mem::take(&mut track.dirty_minutes)
                    .into_iter()
                    .map(|slot| (slot, track.minutes[slot]))
                    .collect(),
            });
        }
    }

    sources.retain(|_, source| !source.tracks.is_empty());
    Ok(changes)
}

fn restore_dirty(history: &LevelHistory, changes: &[ChangedTrack]) -> Result<()> {
    let mut sources = history.lock()?;

    for change in changes {
        if let Some(track) = sources
            .get_mut(&change.source)
            .and_then(|source| source.tracks.get_mut(&change.point))
        {
            track
                .dirty_seconds
                .extend(change.seconds.iter().map(|(slot, _)| *slot));
            track
                .dirty_minutes
                .extend(change.minutes.iter().map(|(slot, _)| *slot));
        }
    }

    history.changed.notify_one();
    Ok(())
}

fn load(connection: &Connection, history: &LevelHistory, now: i64) -> Result<()> {
    let mut sources = history.lock()?;
    ensure!(sources.is_empty(), "signal history already initialized");
    let mut statement = connection.prepare(
        "SELECT source, point, first_second, newest_second FROM native_history_tracks
         WHERE newest_second >= ?1",
    )?;
    let mut rows = statement.query([now - RETENTION_SECONDS])?;

    while let Some(row) = rows.next()? {
        let source: String = row.get(0)?;
        let point: String = row.get(1)?;
        let first: i64 = row.get(2)?;
        let newest: i64 = row.get(3)?;
        ensure!(
            first >= 0 && newest >= first,
            "invalid saved history timestamps"
        );
        let mut track = Track::new(first);
        track.newest_second = newest;
        sources
            .entry(source)
            .or_default()
            .tracks
            .insert(point, track);
    }

    let mut statement = connection.prepare(
        "SELECT reading.source, reading.point, slot, stamp, peak, signal
         FROM native_history_seconds AS reading
         JOIN native_history_tracks AS track USING (source, point)
         WHERE stamp > ?1 AND track.newest_second >= ?2",
    )?;
    let mut rows = statement.query([now - SECOND_SLOTS as i64, now - RETENTION_SECONDS])?;

    while let Some(row) = rows.next()? {
        let source: String = row.get(0)?;
        let point: String = row.get(1)?;
        let slot = usize::try_from(row.get::<_, i64>(2)?)?;
        let stamp: i64 = row.get(3)?;
        let peak: Option<f32> = row.get(4)?;
        let signal: i64 = row.get(5)?;
        ensure!(
            stamp >= 0
                && slot == stamp as usize % SECOND_SLOTS
                && peak.is_none_or(f32::is_finite)
                && (signal == 0 || (signal == 1 && peak.is_some())),
            "invalid saved signal history second"
        );
        let track = sources
            .get_mut(&source)
            .and_then(|source| source.tracks.get_mut(&point))
            .context("saved signal second has no track")?;
        track.seconds[slot] = Second {
            stamp,
            peak,
            signal: signal != 0,
        };
    }

    let mut statement = connection.prepare(
        "SELECT reading.source, reading.point, slot, stamp, peak, peak_second, signal, recorded
         FROM native_history_minutes AS reading
         JOIN native_history_tracks AS track USING (source, point)
         WHERE stamp >= ?1 AND track.newest_second >= ?2",
    )?;
    let mut rows = statement.query([
        (now - RETENTION_SECONDS).div_euclid(super::SECONDS_PER_MINUTE),
        now - RETENTION_SECONDS,
    ])?;

    while let Some(row) = rows.next()? {
        let source: String = row.get(0)?;
        let point: String = row.get(1)?;
        let slot = usize::try_from(row.get::<_, i64>(2)?)?;
        let stamp: i64 = row.get(3)?;
        let peak: Option<f32> = row.get(4)?;
        let peak_second: i64 = row.get(5)?;
        let signal: i64 = row.get(6)?;
        let recorded: i64 = row.get(7)?;
        ensure!(
            stamp >= 0
                && slot == stamp as usize % MINUTE_SLOTS
                && peak.is_none_or(f32::is_finite)
                && signal >= 0
                && (signal == 0 || peak.is_some())
                && (0..1 << super::SECONDS_PER_MINUTE).contains(&recorded)
                && signal & !recorded == 0
                && (peak.is_none() || peak_second.div_euclid(super::SECONDS_PER_MINUTE) == stamp),
            "invalid saved signal history minute"
        );
        let track = sources
            .get_mut(&source)
            .and_then(|source| source.tracks.get_mut(&point))
            .context("saved signal minute has no track")?;
        track.minutes[slot] = Minute {
            stamp,
            peak,
            peak_second,
            signal: signal as u64,
            recorded: recorded as u64,
        };
    }

    Ok(())
}
