use super::{
    Error,
    configuration::validate_name,
    connections::{Authentication, Profile},
    mutations::Inventory,
    operations::Operation,
    registry::{Registry, Request},
    store::Snapshot,
};
use netaudio_core::managed_api::{inventory::Observation, mutation::Mutation};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Clone, Serialize, PartialEq)]
pub struct Conditions {
    configuration: String,
    facts: Value,
}

pub struct Plan {
    pub operation: Operation,
    pub conditions: Conditions,
    pub change: Value,
}

pub struct SelectionInventory {
    pub domains: Vec<Value>,
    pub devices: Vec<Value>,
}

pub(super) struct Inspection {
    pub conditions: Conditions,
    pub inventory: Option<Inventory>,
    pub change: Value,
}

impl Registry {
    pub async fn prepare(&self, operation: Operation) -> Result<Plan, Error> {
        let (response, received) = tokio::sync::oneshot::channel();
        self.requests
            .try_send(Request::Prepare {
                operation,
                response,
            })
            .map_err(queue_error)?;
        received.await.map_err(|_| {
            Error::Conflict("Managed service stopped before the preview was available")
        })?
    }

    pub async fn selection_inventory(
        &self,
        server: Option<String>,
    ) -> Result<SelectionInventory, Error> {
        let (response, received) = tokio::sync::oneshot::channel();
        self.requests
            .try_send(Request::Selection { server, response })
            .map_err(queue_error)?;
        received.await.map_err(|_| {
            Error::Conflict("Managed service stopped before inventory was available")
        })?
    }

    pub(super) async fn selection_owned(
        &self,
        server: Option<String>,
    ) -> Result<SelectionInventory, Error> {
        let _configuration = self.configuration_change.lock().await;
        let snapshot = self.load_configuration().await?;
        let servers: Vec<_> = if let Some(server) = server {
            require_server(&snapshot, &server)?;
            vec![server]
        } else {
            snapshot
                .settings
                .servers
                .iter()
                .filter(|(_, server)| server.enabled())
                .map(|(name, _)| name.clone())
                .collect()
        };
        let mut selected = SelectionInventory {
            domains: Vec::new(),
            devices: Vec::new(),
        };
        for server in servers {
            let inventory = self.fresh_inventory(&server).await?;
            selected.domains.extend(inventory.domains);
            selected
                .devices
                .extend(inventory.devices.iter().map(Observation::record));
        }
        Ok(selected)
    }

    pub(super) async fn prepare_owned(&self, operation: Operation) -> Result<Plan, Error> {
        let _configuration = self.configuration_change.lock().await;
        let snapshot = self.load_configuration().await?;
        let inspection = self.inspect(&operation, &snapshot).await?;
        Ok(Plan {
            operation,
            conditions: inspection.conditions,
            change: inspection.change,
        })
    }

    pub(super) async fn inspect(
        &self,
        operation: &Operation,
        snapshot: &Snapshot,
    ) -> Result<Inspection, Error> {
        let mut inventory = None;
        let change = match operation {
            Operation::Mutation(request) => {
                let current = self.fresh_inventory(&request.server).await?;
                current.validate(&request.mutation)?;
                let change = current.mutation_change(&request.mutation)?;
                inventory = Some(current);
                change
            }
            Operation::Enrollment(request) => {
                let current = self.fresh_inventory(&request.server).await?;
                let change = request.change(&current)?;
                inventory = Some(current);
                change
            }
            Operation::Login(request) => {
                validate_name(&request.server)?;
                let existing = snapshot.settings.servers.get(&request.server);
                let url = request
                    .url
                    .as_deref()
                    .filter(|url| !url.is_empty())
                    .or_else(|| existing.and_then(|server| server.url.as_deref()))
                    .ok_or(Error::Invalid("Enter the DDM server URL"))?;
                super::client::endpoint(url)?;
                if existing.is_some_and(|server| server.url.as_deref() != Some(url)) {
                    return Err(Error::Invalid(
                        "That profile belongs to a different server; choose a new profile name",
                    ));
                }
                let method = match &request.authentication {
                    Authentication::Password { username, password } => {
                        if username.trim().is_empty() || password.is_empty() {
                            return Err(Error::Invalid("Provide a username and password"));
                        }
                        "password"
                    }
                    Authentication::ApiKey { api_key } => {
                        super::Credential::new(api_key.clone())?;
                        "api_key"
                    }
                    Authentication::Saved => {
                        if existing
                            .and_then(|server| server.credential_file.as_ref())
                            .is_none()
                        {
                            return Err(Error::Invalid(
                                "The selected profile has no saved credentials",
                            ));
                        }
                        "saved"
                    }
                };
                json!({"server": request.server, "url": url, "method": method, "saved_login": "replace after successful authentication"})
            }
            Operation::Logout(name) => {
                require_server(snapshot, name)?;
                json!({"server": name, "saved_login": "remove", "inventory": "disconnect"})
            }
            Operation::Profile(Profile::Edit { server, name, url }) => {
                require_server(snapshot, server)?;
                validate_name(name)?;
                super::client::endpoint(url)?;
                if server != name && snapshot.settings.servers.contains_key(name) {
                    return Err(Error::Conflict(
                        "A server profile with that name already exists",
                    ));
                }
                let previous = &snapshot.settings.servers[server];
                json!({"server": {"from": server, "to": name}, "url": {"from": previous.url, "to": url}, "saved_login_removed": previous.url.as_deref() != Some(url)})
            }
            Operation::Profile(Profile::Remove { server }) => {
                require_server(snapshot, server)?;
                let contexts: Vec<_> = snapshot
                    .settings
                    .contexts
                    .iter()
                    .filter(|(_, context)| context.server == *server)
                    .map(|(name, _)| name)
                    .collect();
                json!({"remove_server": server, "remove_contexts": contexts})
            }
            Operation::Context(request) => {
                if let Some(name) = request.context.as_ref().filter(|name| !name.is_empty()) {
                    let context = snapshot
                        .settings
                        .contexts
                        .get(name)
                        .ok_or(Error::Invalid("Unknown DDM context"))?;
                    json!({"context": {"from": snapshot.settings.default_context, "to": name}, "server": context.server, "domain_id": context.domain_id, "domain_name": context.domain_name})
                } else {
                    let server = request
                        .server
                        .as_deref()
                        .ok_or(Error::Invalid("Select a saved DDM server"))?;
                    let domain = request.domain_id.as_deref().ok_or(Error::Invalid(
                        "Select a domain reported by the chosen server",
                    ))?;
                    let current = self.fresh_inventory(server).await?;
                    let domain = current.domain_change(domain, false)?;
                    inventory = Some(current);
                    json!({"server": server, "domain": domain, "previous_context": snapshot.settings.default_context})
                }
            }
            Operation::Refresh { .. } | Operation::Graphql { .. } => {
                return Err(Error::Invalid(
                    "This operation does not use managed write previews",
                ));
            }
        };
        Ok(Inspection {
            conditions: Conditions {
                configuration: self.state_revision()?,
                facts: change.clone(),
            },
            inventory,
            change,
        })
    }

    pub(super) async fn fresh_inventory(&self, name: &str) -> Result<Inventory, Error> {
        let source = self.source(name)?;
        if !self.refresh_server(name).await? {
            return Err(Error::Response("Could not verify current DDM inventory"));
        }
        let inventory = self.command_inventory(name, &source)?;
        if !inventory.fresh {
            return Err(Error::Conflict(
                "Current managed inventory is not complete and fresh",
            ));
        }
        Ok(inventory)
    }
}

impl Inventory {
    fn mutation_change(&self, mutation: &Mutation) -> Result<Value, Error> {
        Ok(match mutation {
            Mutation::AddDomain { name } => json!({"create_domain": name}),
            Mutation::RenameDomain { id, name } => {
                json!({"domain": self.domain_change(id, false)?, "name": name})
            }
            Mutation::RemoveDomain { id } => {
                json!({"remove_domain": self.domain_change(id, true)?})
            }
            Mutation::EnrollDevice { id, domain_id } => {
                json!({"device": device_change(self.device(id)?), "to": self.domain_change(domain_id, false)?})
            }
            Mutation::UnenrollDevice { id } => {
                json!({"device": device_change(self.device(id)?), "to": Value::Null})
            }
        })
    }

    pub(super) fn domain_change(&self, id: &str, members: bool) -> Result<Value, Error> {
        self.domain(id)?;
        let domain = self
            .domains
            .iter()
            .find(|domain| domain["id"] == id)
            .ok_or(Error::Invalid("Managed domain is unavailable"))?;
        let mut value = json!({"id": id, "name": domain["name"]});
        if members {
            let members: Vec<_> = self
                .devices
                .iter()
                .filter(|device| device.domain_id.as_deref() == Some(id))
                .map(device_change)
                .collect();
            value["devices"] = json!(members);
        }
        Ok(value)
    }
}

pub(super) fn device_change(observation: &Observation) -> Value {
    let record = observation.record();
    let subscriptions: Vec<_> = record["subscriptions"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|route| {
            json!({
                "rx_channel_number": route["rx_channel_number"], "rx_channel": route["rx_channel"],
                "tx_device": route["tx_device"], "tx_channel": route["tx_channel"],
            })
        })
        .collect();
    json!({
        "id": observation.device.id,
        "name": observation.device.name,
        "identity": observation.device.identity,
        "primary_mac": netaudio_core::managed_api::inventory::primary_mac(&observation.device),
        "domain_id": observation.domain_id,
        "domain_name": observation.domain_name,
        "enrollment": observation.device.enrolment_state,
        "connection": observation.device.connection.as_ref().and_then(|connection| connection.state.as_deref()),
        "subscriptions": subscriptions,
    })
}

fn require_server(snapshot: &Snapshot, name: &str) -> Result<(), Error> {
    if snapshot.settings.servers.contains_key(name) {
        Ok(())
    } else {
        Err(Error::Invalid("Select a saved DDM server"))
    }
}

fn queue_error(error: tokio::sync::mpsc::error::TrySendError<Request>) -> Error {
    match error {
        tokio::sync::mpsc::error::TrySendError::Full(_) => Error::Busy,
        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
            Error::Conflict("Managed service is stopping")
        }
    }
}
