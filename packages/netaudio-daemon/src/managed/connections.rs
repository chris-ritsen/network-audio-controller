use super::{
    client::Client,
    configuration::validate_name,
    registry::{Output, Registry},
    sessions::{Caller, Login as SessionLogin, Renewal, Session},
    store::{Change, Snapshot},
    Credential, Error, Limits, Transport,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Deserialize)]
pub struct Login {
    pub controller_certificate_sha256: Option<String>,
    pub server: String,
    pub url: Option<String>,
    #[serde(flatten)]
    pub authentication: Authentication,
}

#[derive(Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum Authentication {
    Password { username: String, password: String },
    ApiKey { api_key: String },
    Saved,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Profile {
    Edit {
        server: String,
        name: String,
        url: String,
    },
    Remove {
        server: String,
    },
}

#[derive(Deserialize)]
pub struct Selection {
    pub context: Option<String>,
    pub server: Option<String>,
    pub domain_id: Option<String>,
}

impl Registry {
    pub(super) async fn login(&self, request: Login, snapshot: Snapshot) -> Result<Output, Error> {
        validate_name(&request.server)?;
        let existing = snapshot.settings.servers.get(&request.server);
        let url = request
            .url
            .as_deref()
            .filter(|url| !url.is_empty())
            .or_else(|| existing.and_then(|server| server.url.as_deref()))
            .ok_or(Error::Invalid("Enter the DDM server URL"))?
            .to_owned();
        super::client::endpoint(&url)?;
        if existing.is_some_and(|server| server.url.as_deref() != Some(&url)) {
            return Err(Error::Invalid(
                "That profile belongs to a different server; choose a new profile name",
            ));
        }
        let certificate = existing.and_then(|server| server.trust_certificate_file.clone());
        let saved_pin = existing.and_then(|server| server.controller_certificate_sha256.clone());
        let offered_pin = match &saved_pin {
            Some(_) => None,
            None => offered_pin(request.controller_certificate_sha256.as_deref())?,
        };
        let pin = saved_pin.or_else(|| offered_pin.clone());
        let transport = tokio::task::spawn_blocking(move || {
            Transport::new(certificate.as_deref(), pin.as_deref())
        })
        .await
        .map_err(|_| Error::Persistence("Managed trust configuration worker failed"))??;
        let credential = match request.authentication {
            Authentication::Password { username, password } => {
                Client::authenticate(&url, &username, &password, &transport, Limits::default())
                    .await?
            }
            Authentication::ApiKey { api_key } => Credential::new(api_key)?,
            Authentication::Saved => Credential::File(
                existing
                    .and_then(|server| server.credential_file.clone())
                    .ok_or(Error::Invalid(
                        "The selected profile has no saved credentials",
                    ))?,
            ),
        };
        let client = Client::new(&url, credential.clone(), transport, Limits::default())?;
        let (response, inventory) = client.inventory().await?;
        let mut verified = super::inventory::InventoryState::default();
        if !verified.apply(response, inventory)? || !verified.fresh() {
            return Err(Error::Invalid(
                "DDM did not grant access to its device inventory",
            ));
        }
        self.save_configuration(
            snapshot,
            Change::Login {
                controller_certificate_sha256: offered_pin,
                name: request.server.clone(),
                url,
                credential: credential.read().await?,
            },
        )
        .await?;
        self.refresh_server(&request.server).await?;
        self.connection_output()
    }

    pub(super) async fn logout(&self, name: String, snapshot: Snapshot) -> Result<Output, Error> {
        self.save_configuration(snapshot, Change::Logout { name })
            .await?;
        self.connection_output()
    }

    pub(super) async fn edit_profile(
        &self,
        request: Profile,
        snapshot: Snapshot,
    ) -> Result<Output, Error> {
        let change = match request {
            Profile::Edit { server, name, url } => Change::Edit {
                current_name: server,
                name,
                url,
            },
            Profile::Remove { server } => Change::Remove { name: server },
        };
        self.save_configuration(snapshot, change).await?;
        self.refresh(None).await?;
        self.connection_output()
    }

    pub(super) async fn select_context(
        &self,
        request: Selection,
        snapshot: Snapshot,
        inventory: Option<super::mutations::Inventory>,
    ) -> Result<Output, Error> {
        let change = if let Some(context) = request.context.filter(|context| !context.is_empty()) {
            Change::Select { name: context }
        } else {
            let server = request
                .server
                .ok_or(Error::Invalid("Select a saved DDM server"))?;
            let domain_id = request.domain_id.ok_or(Error::Invalid(
                "Select a domain reported by the chosen server",
            ))?;
            if inventory.is_none() && !self.refresh_server(&server).await? {
                return Err(Error::Response("Could not verify current DDM domains"));
            }
            let domains = self
                .domains(None)
                .map_err(|_| Error::Conflict("Managed inventory is unavailable"))?;
            let domains: Vec<_> = domains
                .iter()
                .filter(|domain| {
                    domain["ddm_server_profile"] == server
                        && domain["id"] == domain_id
                        && domain["ddm_fresh"] == true
                })
                .collect();
            if domains.len() != 1 {
                return Err(Error::Invalid(
                    "Select a domain reported by the chosen server",
                ));
            }
            let domain = domains[0];
            let name = context_name(&snapshot, &server, &domain_id, domain["name"].as_str());
            Change::Context {
                server,
                name,
                domain_id,
                domain_name: domain["name"].as_str().map(str::to_owned),
                select: true,
            }
        };
        self.save_configuration(snapshot, change).await?;
        self.connection_output()
    }

    pub(super) async fn load_configuration(&self) -> Result<Snapshot, Error> {
        let store = self.store.clone();
        let (snapshot, resources) = tokio::task::spawn_blocking(move || {
            let snapshot = store.snapshot()?;
            let resources = super::registry::resources(&snapshot.settings);
            Ok::<_, Error>((snapshot, resources))
        })
        .await
        .map_err(|_| Error::Persistence("Managed configuration worker failed"))??;
        if self.resources_changed(&snapshot, &resources)? {
            self.install_resources(snapshot.clone(), resources).await?;
        }
        Ok(snapshot)
    }

    async fn save_configuration(&self, snapshot: Snapshot, change: Change) -> Result<(), Error> {
        let store = self.store.clone();
        let result = tokio::task::spawn_blocking(move || store.update(snapshot.revision, change))
            .await
            .map_err(|_| Error::Persistence("Managed configuration worker failed"))?;
        match result {
            Ok(snapshot) => self.install(snapshot).await,
            Err(error) => {
                let store = self.store.clone();
                match tokio::task::spawn_blocking(move || store.snapshot()).await {
                    Ok(Ok(snapshot)) => self.install(snapshot).await?,
                    _ => tracing::error!(
                        "Managed configuration could not be read after a failed update"
                    ),
                }
                Err(error)
            }
        }
    }

    fn connection_output(&self) -> Result<Output, Error> {
        Ok(Output {
            status: 200,
            value: self
                .connections()
                .map_err(|_| Error::Conflict("Managed configuration is unavailable"))?,
        })
    }
}

impl Registry {
    pub async fn authenticate(
        &self,
        request: Login,
    ) -> Result<(String, SessionLogin, bool), Error> {
        validate_name(&request.server)?;
        let requested = request.url.as_deref().filter(|url| !url.is_empty());
        let offered = offered_pin(request.controller_certificate_sha256.as_deref())?;
        let (url, certificate, pin, saved) = match self.source(&request.server) {
            Ok(source) => {
                let configuration = &source.configuration;
                let url = configuration
                    .url
                    .clone()
                    .ok_or(Error::Invalid("The DDM server has no URL"))?;
                if requested.is_some_and(|value| value != url) {
                    return Err(Error::Invalid(
                        "That profile belongs to a different server; choose a new profile name",
                    ));
                }
                (
                    url,
                    configuration.trust_certificate_file.clone(),
                    offered.or_else(|| configuration.controller_certificate_sha256.clone()),
                    true,
                )
            }
            Err(_) => (
                requested
                    .ok_or(Error::Invalid("Enter the DDM server URL"))?
                    .to_owned(),
                None,
                offered,
                false,
            ),
        };
        super::client::endpoint(&url)?;
        let transport = tokio::task::spawn_blocking(move || {
            Transport::new(certificate.as_deref(), pin.as_deref())
        })
        .await
        .map_err(|_| Error::Persistence("Managed trust configuration worker failed"))??;
        let (credential, renewal, username) = match request.authentication {
            Authentication::Password { username, password } => {
                if let Some(login) = self
                    .held_login(&request.server, &url, &username, &password)
                    .await
                {
                    return Ok((request.server, login, true));
                }
                let issued =
                    Client::authenticate(&url, &username, &password, &transport, Limits::default())
                        .await?;
                (
                    Credential::renewable(issued.read().await?)?,
                    Some(Arc::new(Renewal::new(&password))),
                    Some(username),
                )
            }
            Authentication::ApiKey { api_key } => (Credential::new(api_key)?, None, None),
            Authentication::Saved => {
                return Err(Error::Invalid("Enter your DDM email address and password"));
            }
        };
        let client = Client::new(
            &url,
            credential.clone(),
            transport.clone(),
            Limits::default(),
        )?;
        let domains = visible_domains(&request.server, &client).await?;
        if !saved {
            self.add_server(&request.server, &url).await?;
        }
        Ok((
            request.server,
            SessionLogin {
                client,
                credential,
                domains,
                renewal,
                transport,
                username,
            },
            false,
        ))
    }

    pub(super) async fn renew(&self, server: &str, login: &SessionLogin) {
        let Some(renewal) = &login.renewal else {
            return;
        };
        let Some(_renewing) = renewal.begin().await else {
            return;
        };
        let outcome = async {
            let url = self
                .source(server)?
                .configuration
                .url
                .clone()
                .ok_or(Error::Invalid("The DDM server has no URL"))?;
            let username = login.username.as_deref().ok_or(Error::Authentication)?;
            let issued = Client::authenticate(
                &url,
                username,
                renewal.password(),
                &login.transport,
                Limits::default(),
            )
            .await?;
            login.credential.renew(issued.read().await?)
        }
        .await;
        match outcome {
            Ok(()) => {
                renewal.renewed();
                tracing::info!(server, "DDM login renewed");
            }
            Err(Error::Authentication) => {
                renewal.refused();
                tracing::warn!(server, "DDM refused to renew a login");
            }
            Err(error) => {
                renewal.failed();
                tracing::warn!(%error, server, "DDM login renewal failed");
            }
        }
    }

    pub(super) async fn renew_caller(&self, caller: &Caller) {
        let Caller::Session(session) = caller else {
            return;
        };
        for server in session.servers() {
            if let Some(login) = session.login(&server) {
                self.renew(&server, &login).await;
            }
        }
    }

    async fn held_login(
        &self,
        server: &str,
        url: &str,
        username: &str,
        password: &str,
    ) -> Option<SessionLogin> {
        let held = self.held_logins(server).ok()?.into_iter().find(|login| {
            login.username.as_deref() == Some(username)
                && login.client.serves(Some(url))
                && login
                    .renewal
                    .as_ref()
                    .is_some_and(|renewal| renewal.matches(password))
        })?;
        let domains = visible_domains(server, &held.client).await.ok()?;
        Some(SessionLogin { domains, ..held })
    }

    async fn add_server(&self, name: &str, url: &str) -> Result<(), Error> {
        let _configuration = self.configuration_change.lock().await;
        let snapshot = self.load_configuration().await?;
        match snapshot.settings.servers.get(name) {
            Some(server) if server.url.as_deref() == Some(url) => Ok(()),
            Some(_) => Err(Error::Invalid(
                "That profile belongs to a different server; choose a new profile name",
            )),
            None => {
                self.save_configuration(
                    snapshot,
                    Change::Add {
                        name: name.to_owned(),
                        url: url.to_owned(),
                    },
                )
                .await
            }
        }
    }

    pub(super) async fn refresh_session(&self, session: &Session) -> Result<(), Error> {
        for server in session.servers() {
            if let Some(mut login) = session.login(&server) {
                login.domains = visible_domains(&server, &login.client).await?;
                session.sign_in(server.clone(), login);
                self.choose_lender(&server).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn select_session_context(
        &self,
        request: Selection,
        snapshot: Snapshot,
        session: &Arc<Session>,
    ) -> Result<Output, Error> {
        let (server, domain_id) =
            if let Some(context) = request.context.filter(|context| !context.is_empty()) {
                let saved = snapshot
                    .settings
                    .contexts
                    .get(&context)
                    .ok_or(Error::Invalid("Select a saved DDM context"))?;
                (saved.server.clone(), saved.domain_id.clone())
            } else {
                (
                    request
                        .server
                        .ok_or(Error::Invalid("Select a saved DDM server"))?,
                    request.domain_id.ok_or(Error::Invalid(
                        "Select a domain reported by the chosen server",
                    ))?,
                )
            };
        let login = session
            .login(&server)
            .ok_or(Error::Forbidden("Log in to this DDM server first"))?;
        if !login.domains.contains(&domain_id) {
            return Err(Error::Invalid(
                "Select a domain reported by the chosen server",
            ));
        }
        let domain_name = self
            .domains(None)
            .map_err(|_| Error::Conflict("Managed inventory is unavailable"))?
            .iter()
            .find(|domain| domain["ddm_server_profile"] == server && domain["id"] == domain_id)
            .and_then(|domain| domain["name"].as_str().map(str::to_owned));
        let name = context_name(&snapshot, &server, &domain_id, domain_name.as_deref());
        if !snapshot.settings.contexts.contains_key(&name) {
            self.save_configuration(
                snapshot,
                Change::Context {
                    server,
                    name: name.clone(),
                    domain_id,
                    domain_name,
                    select: false,
                },
            )
            .await?;
        }
        session.select(name);
        Ok(Output {
            status: 200,
            value: self.connections_for(&Caller::Session(Arc::clone(session)))?,
        })
    }

    pub fn connections_for(&self, caller: &Caller) -> Result<Value, Error> {
        if caller.is_owner() {
            return self
                .connections()
                .map_err(|_| Error::Conflict("Managed configuration is unavailable"));
        }
        let session = match caller {
            Caller::Session(session) => Some(session),
            _ => None,
        };
        let state = self.state_snapshot()?;
        let settings = &state.settings;
        Ok(json!({
            "contexts": settings.contexts.iter()
                .filter(|(_, context)| caller.sees_domain(Some(&context.server), Some(&context.domain_id)))
                .map(|(name, context)| json!({"domain_id": context.domain_id, "domain_name": context.domain_name, "name": name, "server": context.server}))
                .collect::<Vec<_>>(),
            "default_context": session.and_then(|session| session.context()),
            "servers": settings.servers.iter()
                .map(|(name, server)| {
                    let login = session.and_then(|session| session.login(name));
                    json!({"configured": login.is_some(), "name": name, "url": server.url, "username": login.and_then(|login| login.username)})
                })
                .collect::<Vec<_>>(),
        }))
    }
}

async fn visible_domains(server: &str, client: &Client) -> Result<BTreeSet<String>, Error> {
    let (response, inventory) = client.inventory().await?;
    let mut verified = super::inventory::InventoryState::default();
    if !verified.apply(response, inventory)? || !verified.fresh() {
        return Err(Error::Invalid(
            "DDM did not grant access to its device inventory",
        ));
    }
    Ok(verified
        .domains(server, &BTreeMap::new())
        .iter()
        .filter_map(|domain| domain["id"].as_str().map(str::to_owned))
        .collect())
}

fn context_name(
    snapshot: &Snapshot,
    server: &str,
    domain_id: &str,
    domain_name: Option<&str>,
) -> String {
    snapshot
        .settings
        .contexts
        .iter()
        .find(|(_, context)| context.server == server && context.domain_id == domain_id)
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| {
            let base: String = format!(
                "{server}-{}",
                domain_name
                    .filter(|name| !name.is_empty())
                    .unwrap_or("domain")
            )
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || "_.-".contains(character) {
                    character
                } else {
                    '-'
                }
            })
            .collect();
            let mut name = base.clone();
            let mut suffix = 2;
            while snapshot.settings.contexts.contains_key(&name) {
                name = format!("{base}-{suffix}");
                suffix += 1;
            }
            name
        })
}

fn offered_pin(value: Option<&str>) -> Result<Option<String>, Error> {
    Ok(super::transport::controller_pin(value.filter(|value| !value.is_empty()))?.map(hex::encode))
}

pub fn payload<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, Error> {
    serde_json::from_value(value)
        .map_err(|_| Error::Invalid("Managed request contains missing or invalid fields"))
}

pub fn server_name(value: &Value) -> Result<String, Error> {
    value["server"]
        .as_str()
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .ok_or(Error::Invalid("Select a saved DDM server"))
}

pub fn context(value: &Value) -> Result<Option<String>, Error> {
    match value.get("context") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(context)) => Ok(Some(context.clone())),
        _ => Err(Error::Invalid("context must be a string")),
    }
}
