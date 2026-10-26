use super::protocol::META_CLIENT_CAPABILITIES;
use super::service::CallError;
use crate::authorization::Grant;
use crate::devices::registry::DeviceRegistry;
use crate::devices::subscriptions::SubscriptionRequest;
use crate::events::{CLIENT_QUEUE_CAPACITY, EventOptions, TELEMETRY_FIELDS};
use crate::state::{ReadModel, Subscription};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{FutureExt, StreamExt};
use rand::RngCore;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

pub const ROUTING_VIEW_URI: &str = "ui://netaudio/routing";
const APP_MIME_TYPE: &str = "text/html;profile=mcp-app";
const UI_EXTENSION: &str = "io.modelcontextprotocol/ui";
const SESSION_IDLE: Duration = Duration::from_secs(90);
const MAXIMUM_WAIT: Duration = Duration::from_secs(25);
const MAXIMUM_SESSIONS: usize = 32;
const MAXIMUM_CLIENTS: usize = 256;
const SESSION_TOKEN_BYTES: usize = 16;
const OMITTED_FIELDS: &[&str] = &[
    "clock_control_availability",
    "clock_observed_at",
    "clock_port_records",
    "clock_status",
    "ddm_identity",
    "ddm_last_sync",
    "device_controls",
    "field_sources",
    "lock_reset_status",
    "manufacturer_versions_record",
    "platform_versions_record",
    "receiver_flow_status_page",
    "receiver_telemetry_capacity",
    "settings_properties",
];
const SHOWN_EVENTS: &[&str] = &[
    "device_discovered",
    "device_patch",
    "device_removed",
    "device_updated",
    "external_flow_changed",
    "snapshot",
    "subscription_pending",
];
const ROUTING_DOCUMENT: &str =
    include_str!("../../../netaudio/src/netaudio/daemon/http/webapp/mcp-app/routing.html");

struct Session {
    identifier: String,
    client: String,
    events: AsyncMutex<Subscription>,
    cancellation: CancellationToken,
    closed: AtomicBool,
}

struct Lease {
    session: Arc<Session>,
    expires: Instant,
}

pub struct Apps {
    model: Arc<ReadModel>,
    registry: Arc<DeviceRegistry>,
    sessions: Mutex<BTreeMap<String, Lease>>,
    legacy_clients: Mutex<BTreeMap<String, Instant>>,
    changed: Notify,
    cancellation: CancellationToken,
}

impl Apps {
    pub fn new(
        model: Arc<ReadModel>,
        registry: Arc<DeviceRegistry>,
        cancellation: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            model,
            registry,
            cancellation,
            sessions: Mutex::new(BTreeMap::new()),
            legacy_clients: Mutex::new(BTreeMap::new()),
            changed: Notify::new(),
        })
    }

    pub fn note_client(&self, capabilities: &Value, grant: &Grant) -> anyhow::Result<()> {
        let mut clients = self
            .legacy_clients
            .lock()
            .map_err(|_| anyhow::anyhow!("MCP app client lock poisoned"))?;
        clients.remove(&grant.client_id);

        if shows_apps(capabilities) {
            if clients.len() >= MAXIMUM_CLIENTS
                && let Some(oldest) = clients
                    .iter()
                    .min_by_key(|(_, seen)| *seen)
                    .map(|(identifier, _)| identifier.clone())
            {
                clients.remove(&oldest);
            }

            clients.insert(grant.client_id.clone(), Instant::now());
        }

        Ok(())
    }

    pub fn client_support(
        &self,
        parameters: &Map<String, Value>,
        grant: &Grant,
    ) -> anyhow::Result<bool> {
        if let Some(capabilities) = parameters
            .get("_meta")
            .and_then(|metadata| metadata.get(META_CLIENT_CAPABILITIES))
        {
            return Ok(shows_apps(capabilities));
        }

        let clients = self
            .legacy_clients
            .lock()
            .map_err(|_| anyhow::anyhow!("MCP app client lock poisoned"))?;
        Ok(clients.contains_key(&grant.client_id))
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let deadline = {
                let mut sessions = self
                    .sessions
                    .lock()
                    .map_err(|_| anyhow::anyhow!("MCP app session lock poisoned"))?;
                let now = Instant::now();
                sessions.retain(|_, lease| {
                    if lease.expires <= now || lease.session.closed.load(Ordering::Acquire) {
                        lease.session.cancellation.cancel();
                        false
                    } else {
                        true
                    }
                });
                sessions.values().map(|lease| lease.expires).min()
            };
            let expiration = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = self.cancellation.cancelled() => break,
                _ = &mut changed => {},
                _ = expiration => {},
            }
        }

        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("MCP app session lock poisoned"))?;

        for lease in sessions.values() {
            lease.session.cancellation.cancel();
        }

        sessions.clear();
        Ok(())
    }

    pub async fn call(
        &self,
        name: &str,
        arguments: &Value,
        grant: &Grant,
        supported: bool,
    ) -> Result<Value, CallError> {
        match name {
            "app_events" => self.events(arguments, grant).await,
            "app_translations" => translations(arguments["language"].as_str().unwrap_or_default()),
            "app_request" => {
                if !supported {
                    return Ok(app_error(
                        "app_request is only for netaudio views open in an app that displays MCP Apps",
                    ));
                }

                if !grant.can_write() {
                    return Ok(app_error(format!(
                        "{} was granted read-only access, so the view cannot change routes",
                        grant.client_name
                    )));
                }

                let path = arguments["path"].as_str().unwrap_or_default();
                let body = arguments.get("body").cloned().unwrap_or_else(|| json!({}));
                let request = match path {
                    "/subscribe" => SubscriptionRequest::set(&body),
                    "/unsubscribe" => SubscriptionRequest::clear(&body),
                    _ => {
                        return Ok(app_error(format!(
                            "{path} is not available in this daemon build"
                        )));
                    }
                };
                let applied = match request {
                    Ok(request) => request.apply(&self.registry).await,
                    Err(error) => Err(error),
                };
                let (status, payload) = match applied {
                    Ok((_, count)) => {
                        let mut payload = json!({"success": true});

                        if let Some(count) = count {
                            payload["count"] = json!(count);
                        }

                        (axum::http::StatusCode::OK, payload)
                    }
                    Err(error) => crate::http::devices::control_failure(&error),
                };
                Ok(app_result(
                    json!({"status": status.as_u16(), "payload": payload}),
                    format!("{path} answered {}", status.as_u16()),
                ))
            }
            _ => Err(CallError::invalid(format!("unknown app operation {name}"))),
        }
    }

    fn session(&self, identifier: Option<&str>, grant: &Grant) -> Result<Arc<Session>, CallError> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("MCP app session lock poisoned"))?;

        if self.cancellation.is_cancelled() {
            return Err(CallError::invalid("MCP app service is stopping"));
        }

        let expires = Instant::now() + SESSION_IDLE;

        if let Some(identifier) = identifier
            && let Some(lease) = sessions.get_mut(identifier)
        {
            if lease.session.client != grant.client_id {
                return Err(CallError::invalid(
                    "app session does not belong to this client",
                ));
            }

            if !lease.session.closed.load(Ordering::Acquire) && lease.expires > Instant::now() {
                lease.expires = expires;
                self.changed.notify_one();
                return Ok(Arc::clone(&lease.session));
            }

            lease.session.cancellation.cancel();
            sessions.remove(identifier);
        }

        if sessions.len() >= MAXIMUM_SESSIONS
            && let Some(oldest) = sessions
                .iter()
                .min_by_key(|(_, lease)| lease.expires)
                .map(|(identifier, _)| identifier.clone())
            && let Some(lease) = sessions.remove(&oldest)
        {
            lease.session.cancellation.cancel();
        }

        let mut bytes = [0; SESSION_TOKEN_BYTES];
        rand::rngs::OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(anyhow::Error::from)?;
        let identifier = URL_SAFE_NO_PAD.encode(bytes);
        if sessions.contains_key(&identifier) {
            return Err(anyhow::anyhow!("MCP session identifier collision").into());
        }
        let options = EventOptions {
            patches: true,
            meters: false,
            notices: false,
            telemetry: false,
            hidden_fields: OMITTED_FIELDS
                .iter()
                .copied()
                .chain(TELEMETRY_FIELDS)
                .map(str::to_owned)
                .collect(),
        };
        let session = Arc::new(Session {
            identifier: identifier.clone(),
            client: grant.client_id.clone(),
            events: AsyncMutex::new(self.model.subscribe(Some(options))?),
            cancellation: self.cancellation.child_token(),
            closed: AtomicBool::new(false),
        });
        sessions.insert(
            identifier,
            Lease {
                session: Arc::clone(&session),
                expires,
            },
        );
        self.changed.notify_one();
        Ok(session)
    }

    async fn events(&self, arguments: &Value, grant: &Grant) -> Result<Value, CallError> {
        let session = self.session(arguments["session"].as_str(), grant)?;
        let wait = Duration::from_secs(arguments["wait_seconds"].as_u64().unwrap_or_default())
            .min(MAXIMUM_WAIT);
        let mut events = session.events.try_lock().map_err(|_| {
            CallError::invalid("an app event request is already waiting for this session")
        })?;
        let mut received = Vec::new();
        let first = if wait.is_zero() {
            events.next().now_or_never()
        } else {
            tokio::select! {
                _ = session.cancellation.cancelled() => Some(None),
                result = tokio::time::timeout(wait, events.next()) => result.ok(),
            }
        };

        if let Some(first) = first {
            match first {
                Some(event) => received.push(event),
                None => {
                    session.closed.store(true, Ordering::Release);
                    self.changed.notify_one();
                }
            }
        }

        while received.len() < CLIENT_QUEUE_CAPACITY {
            match events.next().now_or_never() {
                Some(Some(event)) => received.push(event),
                Some(None) => {
                    session.closed.store(true, Ordering::Release);
                    self.changed.notify_one();
                    break;
                }
                None => break,
            }
        }

        let shown: Vec<_> = received
            .into_iter()
            .filter_map(|event| {
                let kind = event.value["event"].as_str()?;

                if !SHOWN_EVENTS.contains(&kind) {
                    return None;
                }

                let mut value = event.value.as_ref().clone();

                if kind == "snapshot"
                    && let Some(fields) = value.as_object_mut()
                {
                    for field in ["metering", "metering_scale", "shure_devices"] {
                        fields.remove(field);
                    }
                }

                Some(value)
            })
            .collect();
        Ok(app_result(
            json!({"session": session.identifier, "events": shown}),
            format!("{} device events for an open view", shown.len()),
        ))
    }
}

fn shows_apps(capabilities: &Value) -> bool {
    capabilities["extensions"][UI_EXTENSION]["mimeTypes"]
        .as_array()
        .is_some_and(|types| types.iter().any(|kind| kind == APP_MIME_TYPE))
}

pub fn resource() -> Value {
    json!({
        "uri": ROUTING_VIEW_URI,
        "name": "Routing matrix",
        "description": "The netaudio web interface's Dante routing matrix, live, with subscriptions the user can change.",
        "mimeType": APP_MIME_TYPE,
    })
}

pub fn read_resource() -> Value {
    json!({"contents": [{
        "uri": ROUTING_VIEW_URI,
        "mimeType": APP_MIME_TYPE,
        "text": ROUTING_DOCUMENT,
        "_meta": {"ui": {"prefersBorder": true}},
    }]})
}

fn app_result(payload: Value, summary: String) -> Value {
    json!({"content": [{"type": "text", "text": summary}], "structuredContent": payload, "isError": false})
}

fn app_error(message: impl Into<String>) -> Value {
    json!({"content": [{"type": "text", "text": message.into()}], "isError": true})
}

fn translations(language: &str) -> Result<Value, CallError> {
    macro_rules! translations {
        ($($language:literal),+ $(,)?) => {
            match language {
                $($language => Some(include_str!(concat!("../../../netaudio/src/netaudio/daemon/http/webapp/i18n/", $language, ".js"))),)+
                _ => None,
            }
        };
    }

    let Some(source) = translations!(
        "de", "es", "fr", "it", "ja", "ko", "ms", "nl", "pt-BR", "ta", "zh-Hans", "zh-Hant"
    ) else {
        return Ok(app_error(format!("no translations for {language:?}")));
    };
    let body = source
        .trim()
        .strip_prefix("export default ")
        .ok_or_else(|| {
            CallError::invalid(format!(
                "the {language} translations are not a default-exported object"
            ))
        })?;
    let catalog: Value = serde_json::from_str(body.trim_end_matches(';'))
        .map_err(|error| CallError::invalid(error.to_string()))?;
    Ok(app_result(
        json!({"language": language, "catalog": catalog}),
        format!("{language} translations"),
    ))
}
