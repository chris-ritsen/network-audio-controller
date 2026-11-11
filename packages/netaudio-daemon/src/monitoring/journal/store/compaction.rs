use crate::monitoring::issues::model::superseded;
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::time::Duration;

const BATCH: i64 = 2_000;
const DUPLICATED_EVIDENCE: &[(&str, &str)] = &[
    ("evidence_class", "evidence_class"),
    ("issue_id", "issue_id"),
    ("issue_kind", "kind"),
    ("raw_source_fields", "raw_source_fields"),
    ("suggested_remediation", "suggested_remediation"),
];
const LAYOUT: &str = "2";
const LAYOUT_KEY: &str = "issue_event_layout";
const RECLAIM_ALLOWANCE: Duration = Duration::from_secs(1_800);
const RECLAIM_FRACTION: u64 = 4;
const RECLAIM_MINIMUM_BYTES: u64 = 64 * 1024 * 1024;
const START_ALLOWANCE: Duration = Duration::from_secs(120);

pub fn initialize(connection: &mut Connection) -> Result<()> {
    let layout: Option<String> = connection
        .query_row(
            "SELECT value FROM native_journal_state WHERE key = ?1",
            [LAYOUT_KEY],
            |row| row.get(0),
        )
        .optional()?;

    if let Some(layout) = layout {
        ensure!(
            layout == LAYOUT,
            "unsupported saved issue event layout {layout}"
        );
        return Ok(());
    }

    let mut position = 0_i64;
    let mut rewritten = 0_usize;
    let mut released = 0_usize;

    loop {
        crate::runtime::extend_start_deadline(START_ALLOWANCE);
        let transaction = connection.transaction()?;
        let batch = {
            let mut statement = transaction.prepare_cached(
                "SELECT events.sequence, events.event FROM native_journal_event_index AS entry
                 JOIN events ON events.sequence = entry.sequence
                 WHERE entry.kind IN ('issue_opened', 'issue_resolved', 'issue_updated')
                 AND entry.sequence > ?1 ORDER BY entry.sequence LIMIT ?2",
            )?;
            let rows = statement.query_map(params![position, BATCH], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        if batch.is_empty() {
            transaction.execute(
                "INSERT INTO native_journal_state VALUES (?1, ?2)",
                params![LAYOUT_KEY, LAYOUT],
            )?;
            transaction.commit()?;
            break;
        }

        {
            let mut update =
                transaction.prepare_cached("UPDATE events SET event = ?1 WHERE sequence = ?2")?;

            for (sequence, stored) in batch {
                position = sequence;
                let mut event: Value =
                    serde_json::from_str(&stored).context("invalid stored monitoring event")?;

                if !single_copy(&mut event) {
                    continue;
                }

                let record = serde_json::to_string(&event)?;
                released += stored.len().saturating_sub(record.len());
                rewritten += 1;
                update.execute(params![record, sequence])?;
            }
        }

        transaction.commit()?;
    }

    if rewritten != 0 {
        tracing::info!(
            events = rewritten,
            bytes = released,
            "stored issue events rewritten to hold each issue once"
        );
    }

    Ok(())
}

pub fn reclaim(connection: &Connection) -> Result<()> {
    let measure = |pragma: &str| {
        connection
            .query_row(pragma, [], |row| row.get::<_, i64>(0))
            .map(i64::unsigned_abs)
    };
    let free = measure("PRAGMA freelist_count")?;
    let pages = measure("PRAGMA page_count")?;
    let size = measure("PRAGMA page_size")?;

    if free.saturating_mul(size) < RECLAIM_MINIMUM_BYTES
        || free.saturating_mul(RECLAIM_FRACTION) < pages
    {
        return Ok(());
    }

    crate::runtime::extend_start_deadline(RECLAIM_ALLOWANCE);

    match connection.execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE);") {
        Ok(()) => tracing::info!(
            bytes = free.saturating_mul(size),
            "unused event journal space returned to the filesystem"
        ),
        Err(error) => {
            tracing::warn!(%error, "event journal space was not returned to the filesystem");
        }
    }

    Ok(())
}

fn single_copy(event: &mut Value) -> bool {
    let Some(fields) = event.as_object_mut() else {
        return false;
    };
    let current = fields.get("current_value").cloned().unwrap_or(Value::Null);
    let mut changed = false;

    if let Some(previous) = fields.get_mut("previous_value") {
        let reduced = superseded(previous.clone(), &current);
        changed |= reduced != *previous;
        *previous = reduced;
    }

    if let Some(evidence) = fields.get_mut("raw").and_then(Value::as_object_mut) {
        let before = evidence.len();
        evidence.retain(|field, value| {
            !DUPLICATED_EVIDENCE.iter().any(|(duplicate, source)| {
                field == duplicate && current.get(*source) == Some(&*value)
            })
        });
        changed |= evidence.len() != before;
    }

    changed
}
