use super::protocol::META_SUBSCRIPTION_ID;
use axum::response::Sse;
use axum::response::sse::{Event, KeepAlive};
use futures_core::Stream;
use futures_util::stream;
use serde_json::{Map, Value, json};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Clone, Copy)]
pub enum Notification {
    Tools,
    Prompts,
    Resources,
}

impl Notification {
    const ALL: [Self; 3] = [Self::Tools, Self::Prompts, Self::Resources];

    fn index(self) -> usize {
        match self {
            Self::Tools => 0,
            Self::Prompts => 1,
            Self::Resources => 2,
        }
    }

    fn parameter(self) -> &'static str {
        match self {
            Self::Tools => "toolsListChanged",
            Self::Prompts => "promptsListChanged",
            Self::Resources => "resourcesListChanged",
        }
    }

    fn method(self) -> &'static str {
        match self {
            Self::Tools => "notifications/tools/list_changed",
            Self::Prompts => "notifications/prompts/list_changed",
            Self::Resources => "notifications/resources/list_changed",
        }
    }
}

pub struct Subscriptions {
    changes: watch::Sender<[u64; 3]>,
}

struct Listener {
    changes: watch::Receiver<[u64; 3]>,
    revisions: [u64; 3],
    requested: [bool; 3],
    identifier: Value,
    pending: VecDeque<Event>,
    cancellation: CancellationToken,
    expires: Option<Instant>,
}

impl Default for Subscriptions {
    fn default() -> Self {
        let (changes, _) = watch::channel([0; 3]);
        Self { changes }
    }
}

impl Subscriptions {
    pub fn notify(&self, notification: Notification) {
        self.changes.send_modify(|revisions| {
            let revision = &mut revisions[notification.index()];
            *revision = revision.wrapping_add(1);
        });
    }

    pub fn listener_count(&self) -> usize {
        self.changes.receiver_count()
    }

    pub fn listen(
        &self,
        identifier: Value,
        requested: &Value,
        cancellation: CancellationToken,
        expires_at: f64,
    ) -> Sse<impl Stream<Item = Result<Event, Infallible>> + Send + 'static> {
        let changes = self.changes.subscribe();
        let revisions = *changes.borrow();
        let requested =
            Notification::ALL.map(|notification| requested[notification.parameter()] == true);
        let accepted: Map<_, _> = Notification::ALL
            .into_iter()
            .filter(|notification| requested[notification.index()])
            .map(|notification| (notification.parameter().to_owned(), json!(true)))
            .collect();
        let mut pending = VecDeque::from([message(
            &identifier,
            "notifications/subscriptions/acknowledged",
            json!({"notifications": accepted}),
        )]);

        for notification in Notification::ALL {
            if requested[notification.index()] {
                pending.push_back(message(&identifier, notification.method(), json!({})));
            }
        }

        let expires = if expires_at.is_finite() {
            let now = chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0;
            Duration::try_from_secs_f64((expires_at - now).max(0.0))
                .ok()
                .and_then(|duration| Instant::now().checked_add(duration))
        } else {
            None
        };
        let listener = Listener {
            changes,
            revisions,
            requested,
            identifier,
            pending,
            cancellation,
            expires,
        };
        let events = stream::unfold(listener, |mut listener| async move {
            loop {
                if listener.cancellation.is_cancelled()
                    || listener
                        .expires
                        .is_some_and(|expires| expires <= Instant::now())
                {
                    return None;
                }

                if let Some(event) = listener.pending.pop_front() {
                    return Some((Ok(event), listener));
                }

                let expiration = async {
                    match listener.expires {
                        Some(expires) => tokio::time::sleep_until(expires).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    _ = listener.cancellation.cancelled() => return None,
                    _ = expiration => return None,
                    changed = listener.changes.changed() => {
                        if changed.is_err() {
                            return None;
                        }

                        let current = *listener.changes.borrow_and_update();

                        for notification in Notification::ALL {
                            let index = notification.index();

                            if current[index] != listener.revisions[index] && listener.requested[index] {
                                listener.pending.push_back(message(&listener.identifier, notification.method(), json!({})));
                            }
                        }

                        listener.revisions = current;
                    }
                }
            }
        });
        Sse::new(events).keep_alive(
            KeepAlive::new()
                .interval(KEEPALIVE_INTERVAL)
                .text("keepalive"),
        )
    }
}

fn message(identifier: &Value, method: &str, mut parameters: Value) -> Event {
    parameters["_meta"] = json!({META_SUBSCRIPTION_ID: identifier});
    Event::default().data(
        json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": parameters,
        })
        .to_string(),
    )
}
