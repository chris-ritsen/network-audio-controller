use super::{Credential, Error, Transport, client::Client};
use base64::Engine;
use rand::RngCore;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

const ABSOLUTE_LIFETIME: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const IDLE_LIFETIME: Duration = Duration::from_secs(12 * 60 * 60);
const MAXIMUM_SESSIONS: usize = 256;
pub const COOKIE: &str = "__Host-netaudio-session";

tokio::task_local! {
    static CALLER: Caller;
}

#[derive(Clone)]
pub enum Caller {
    Anonymous,
    Owner,
    Session(Arc<Session>),
}

impl Caller {
    pub fn current() -> Self {
        CALLER.try_with(Clone::clone).unwrap_or(Self::Anonymous)
    }

    pub async fn scope<F: Future>(self, future: F) -> F::Output {
        CALLER.scope(self, future).await
    }

    pub fn login(&self, server: &str) -> Result<Option<Login>, Error> {
        match self {
            Self::Owner => Ok(None),
            Self::Session(session) => session
                .login(server)
                .map(Some)
                .ok_or(Error::Forbidden("Log in to this DDM server first")),
            Self::Anonymous => Err(Error::Forbidden("Log in to DDM first")),
        }
    }

    pub fn sees_domain(&self, server: Option<&str>, domain: Option<&str>) -> bool {
        let Some(domain) = domain.filter(|domain| !domain.is_empty()) else {
            return true;
        };
        match self {
            Self::Owner => true,
            Self::Session(session) => server
                .and_then(|server| session.login(server))
                .is_some_and(|login| login.domains.contains(domain)),
            Self::Anonymous => false,
        }
    }

    pub fn sees_device(&self, record: &Value) -> bool {
        self.sees_domain(
            record["ddm_server_profile"].as_str(),
            record["ddm_domain_id"].as_str(),
        )
    }

    pub fn sees_domain_record(&self, domain: &Value) -> bool {
        match domain["id"].as_str() {
            Some(identifier) => {
                self.sees_domain(domain["ddm_server_profile"].as_str(), Some(identifier))
            }
            None => self.is_owner(),
        }
    }

    pub fn is_owner(&self) -> bool {
        matches!(self, Self::Owner)
    }
}

#[derive(Clone)]
pub struct Login {
    pub client: Client,
    pub credential: Credential,
    pub domains: BTreeSet<String>,
    pub transport: Transport,
}

pub struct Session {
    expires: Instant,
    last_used: Mutex<Instant>,
    state: Mutex<SessionState>,
    changes: watch::Sender<u64>,
}

#[derive(Default)]
struct SessionState {
    context: Option<String>,
    logins: BTreeMap<String, Login>,
}

impl Session {
    fn new(now: Instant) -> Self {
        Self {
            expires: now + ABSOLUTE_LIFETIME,
            last_used: Mutex::new(now),
            state: Mutex::new(SessionState::default()),
            changes: watch::channel(0).0,
        }
    }

    fn live(&self, now: Instant) -> bool {
        now < self.expires
            && self
                .last_used
                .lock()
                .is_ok_and(|used| now < *used + IDLE_LIFETIME)
    }

    pub fn expires(&self) -> Instant {
        self.expires
    }

    pub fn login(&self, server: &str) -> Option<Login> {
        self.state.lock().ok()?.logins.get(server).cloned()
    }

    pub fn servers(&self) -> BTreeSet<String> {
        self.state
            .lock()
            .map(|state| state.logins.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn context(&self) -> Option<String> {
        self.state.lock().ok()?.context.clone()
    }

    pub fn only_server(&self) -> Option<String> {
        let state = self.state.lock().ok()?;
        (state.logins.len() == 1)
            .then(|| state.logins.keys().next().cloned())
            .flatten()
    }

    pub fn sign_in(&self, server: String, login: Login) {
        if let Ok(mut state) = self.state.lock() {
            state.logins.insert(server, login);
        }
        self.changed();
    }

    pub fn sign_out(&self, server: &str) -> bool {
        let remaining = self.state.lock().map(|mut state| {
            state.logins.remove(server);
            if state.logins.is_empty() {
                state.context = None;
            }
            !state.logins.is_empty()
        });
        self.changed();
        remaining.unwrap_or(false)
    }

    pub fn select(&self, context: String) {
        if let Ok(mut state) = self.state.lock() {
            state.context = Some(context);
        }
        self.changed();
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    fn changed(&self) {
        self.changes.send_modify(|generation| *generation += 1);
    }
}

#[derive(Default)]
pub struct Sessions {
    entries: Mutex<BTreeMap<String, Arc<Session>>>,
}

impl Sessions {
    pub fn find(&self, token: &str) -> Option<Arc<Session>> {
        let now = Instant::now();
        let mut entries = self.entries.lock().ok()?;
        let key = digest(token);
        let session = entries.get(&key)?;
        if !session.live(now) {
            entries.remove(&key);
            return None;
        }
        if let Ok(mut used) = session.last_used.lock() {
            *used = now;
        }
        Some(Arc::clone(session))
    }

    pub fn open(&self) -> Result<(String, Arc<Session>), Error> {
        let mut bytes = [0_u8; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|_| Error::Persistence("Session identity could not be generated"))?;
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        let now = Instant::now();
        let session = Arc::new(Session::new(now));
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::Conflict("Session store is unavailable"))?;
        entries.retain(|_, session| session.live(now));
        if entries.len() >= MAXIMUM_SESSIONS {
            let oldest = entries
                .iter()
                .min_by_key(|(_, session)| {
                    session
                        .last_used
                        .lock()
                        .map(|used| *used)
                        .unwrap_or(session.expires)
                })
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                entries.remove(&oldest);
            }
        }
        entries.insert(digest(&token), Arc::clone(&session));
        Ok((token, session))
    }

    pub fn end(&self, session: &Arc<Session>) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.retain(|_, entry| !Arc::ptr_eq(entry, session));
        }
        session.changed();
    }
}

fn digest(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}
