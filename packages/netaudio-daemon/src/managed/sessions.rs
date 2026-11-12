use super::{client::Client, Credential, Error, Transport};
use base64::Engine;
use rand::RngCore;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use subtle::ConstantTimeEq;
use tokio::sync::watch;
use tokio::time::Instant;
use zeroize::Zeroizing;

const ABSOLUTE_LIFETIME: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const IDLE_LIFETIME: Duration = Duration::from_secs(12 * 60 * 60);
const MAXIMUM_SESSIONS: usize = 256;
const RENEWAL_AGE: Duration = Duration::from_secs(15 * 60);
const RENEWAL_RETRY: Duration = Duration::from_secs(30);
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
    pub renewal: Option<Arc<Renewal>>,
    pub transport: Transport,
    pub username: Option<String>,
}

pub struct Renewal {
    due: Mutex<Option<SystemTime>>,
    gate: tokio::sync::Mutex<()>,
    password: Zeroizing<String>,
}

impl Renewal {
    pub fn new(password: &str) -> Self {
        Self {
            due: Mutex::new(Some(SystemTime::now() + RENEWAL_AGE)),
            gate: tokio::sync::Mutex::new(()),
            password: Zeroizing::new(password.to_owned()),
        }
    }

    pub async fn begin(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        if !self.is_due() {
            return None;
        }
        let gate = self.gate.lock().await;
        self.is_due().then_some(gate)
    }

    pub fn due(&self) -> Option<SystemTime> {
        self.due.lock().ok().and_then(|due| *due)
    }

    pub fn failed(&self) {
        self.schedule(Some(SystemTime::now() + RENEWAL_RETRY));
    }

    pub fn matches(&self, password: &str) -> bool {
        self.password.as_bytes().ct_eq(password.as_bytes()).into()
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn refused(&self) {
        self.schedule(None);
    }

    pub fn renewed(&self) {
        self.schedule(Some(SystemTime::now() + RENEWAL_AGE));
    }

    fn is_due(&self) -> bool {
        self.due().is_some_and(|due| due <= SystemTime::now())
    }

    fn schedule(&self, due: Option<SystemTime>) {
        if let Ok(mut held) = self.due.lock() {
            *held = due;
        }
    }
}

pub struct Session {
    changes: watch::Sender<u64>,
    ended: AtomicBool,
    expires: Instant,
    last_used: Mutex<Instant>,
    state: Mutex<SessionState>,
}

#[derive(Default)]
struct SessionState {
    context: Option<String>,
    logins: BTreeMap<String, Login>,
}

impl Session {
    fn new(now: Instant) -> Self {
        Self {
            changes: watch::channel(0).0,
            ended: AtomicBool::new(false),
            expires: now + ABSOLUTE_LIFETIME,
            last_used: Mutex::new(now),
            state: Mutex::new(SessionState::default()),
        }
    }

    fn live(&self, now: Instant) -> bool {
        !self.ended.load(Ordering::Acquire) && now < self.deadline()
    }

    fn finish(&self) {
        self.ended.store(true, Ordering::Release);
        self.changed();
    }

    pub fn active(&self) -> bool {
        self.live(Instant::now())
    }

    pub fn deadline(&self) -> Instant {
        self.last_used
            .lock()
            .map(|used| (*used + IDLE_LIFETIME).min(self.expires))
            .unwrap_or(self.expires)
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

pub struct Sessions {
    entries: Mutex<BTreeMap<String, Arc<Session>>>,
    loopback_owner: bool,
}

impl Sessions {
    pub fn new(loopback_owner: bool) -> Self {
        Self {
            entries: Mutex::default(),
            loopback_owner,
        }
    }

    pub fn loopback_owner(&self) -> bool {
        self.loopback_owner
    }

    pub fn find(&self, token: &str) -> Option<Arc<Session>> {
        let now = Instant::now();
        let mut entries = self.entries.lock().ok()?;
        let key = digest(token);
        let session = entries.get(&key)?;
        if !session.live(now) {
            if let Some(session) = entries.remove(&key) {
                session.finish();
            }
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
        entries.retain(|_, session| {
            let live = session.live(now);
            if !live {
                session.finish();
            }
            live
        });
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
            if let Some(session) = oldest.and_then(|oldest| entries.remove(&oldest)) {
                session.finish();
            }
        }
        entries.insert(digest(&token), Arc::clone(&session));
        Ok((token, session))
    }

    pub fn end(&self, session: &Arc<Session>) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.retain(|_, entry| !Arc::ptr_eq(entry, session));
        }
        session.finish();
    }
}

fn digest(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}
