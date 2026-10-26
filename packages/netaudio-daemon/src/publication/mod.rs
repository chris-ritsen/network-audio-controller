#[cfg(unix)]
pub mod bus;
mod configuration;
mod records;

use crate::state::ReadModel;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use redis::aio::MultiplexedConnection;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct Publisher {
    client: redis::Client,
    model: Arc<ReadModel>,
}

impl Publisher {
    pub fn new(
        configuration: &crate::configuration::Configuration,
        model: Arc<ReadModel>,
    ) -> Result<Option<Self>> {
        let Some(connection) = configuration::connection(configuration)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            client: redis::Client::open(connection).context("Configure Redis publication")?,
            model,
        }))
    }

    pub async fn run(self, cancellation: CancellationToken) -> Result<()> {
        let mut published = BTreeMap::new();
        let mut delay = Duration::from_millis(250);
        loop {
            let result = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                result = self.connected(&mut published, &mut delay) => result,
            };
            let error = result.expect_err("publication subscription runs until disconnected");
            tracing::warn!(%error, "Redis publication disconnected");
            self.status("disconnected", Some(error.to_string()))?;
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(delay) => {},
            }
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }

    async fn connected(
        &self,
        published: &mut BTreeMap<String, records::Record>,
        delay: &mut Duration,
    ) -> Result<()> {
        let disconnected = Arc::new(tokio::sync::Notify::new());
        let notification = disconnected.clone();
        let mut connection = self
            .client
            .get_multiplexed_async_connection_with_config(
                &redis::AsyncConnectionConfig::new()
                    .set_connection_timeout(Some(Duration::from_secs(3)))
                    .set_response_timeout(Some(Duration::from_secs(3)))
                    .set_push_sender(move |message: redis::PushInfo| {
                        if message.kind == redis::PushKind::Disconnection {
                            notification.notify_one();
                        }
                        Ok::<(), redis::aio::SendError>(())
                    }),
            )
            .await
            .map_err(connection_error)?;
        let mut subscription = self.model.subscribe(None)?;
        let mut records = records::Records::default();
        loop {
            let event = tokio::select! {
                _ = disconnected.notified() => anyhow::bail!("Redis publication connection closed"),
                event = subscription.next() => event,
            };
            let Some(event) = event else {
                break;
            };
            if !records.apply(&event.value)? {
                continue;
            }
            reconcile(
                &mut connection,
                published,
                &records.values,
                event.value["event"] == crate::events::SNAPSHOT,
            )
            .await?;
            if event.value["event"] == crate::events::SNAPSHOT {
                *delay = Duration::from_millis(250);
                self.status("ready", None)?;
            }
        }
        anyhow::bail!(if subscription.overflowed() {
            "Redis publication fell behind; acquiring a fresh snapshot"
        } else {
            "Redis publication source closed"
        })
    }

    fn status(&self, state: &str, error: Option<String>) -> Result<()> {
        let value = json!({"state":state,"error":error});
        self.model.observe(
            "redis_publication",
            value.clone(),
            json!({"event":"redis_publication_status","redis_publication":value}),
        )
    }
}

async fn reconcile(
    connection: &mut MultiplexedConnection,
    published: &mut BTreeMap<String, records::Record>,
    current: &BTreeMap<String, records::Record>,
    force: bool,
) -> Result<()> {
    let mut pipeline = redis::pipe();
    pipeline.atomic();
    let mut changed = false;
    let mut managed = false;
    for key in published.keys().filter(|key| !current.contains_key(*key)) {
        pipeline.del(key).ignore();
        changed = true;
    }
    for (key, record) in current {
        if !force && published.get(key) == Some(record) {
            continue;
        }
        match record {
            records::Record::Hash(fields) => {
                pipeline.cmd("HSET").arg(key);
                for (field, value) in fields {
                    pipeline.arg(field).arg(value);
                }
                pipeline.ignore();
            }
            records::Record::Json(value) => {
                pipeline.set(key, serde_json::to_string(value)?).ignore();
            }
        }
        managed |= key.starts_with("netaudio:daemon:ddm:");
        changed = true;
    }
    if managed {
        pipeline.publish("netaudio:daemon:ddm", "updated").ignore();
    }
    if changed {
        pipeline
            .query_async::<()>(connection)
            .await
            .map_err(connection_error)?;
        *published = current.clone();
    }
    Ok(())
}

fn connection_error(error: redis::RedisError) -> anyhow::Error {
    anyhow::anyhow!("Redis publication request failed ({:?})", error.kind())
}
