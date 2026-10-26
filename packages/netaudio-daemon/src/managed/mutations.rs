use super::{
    Error,
    audit::Audit,
    registry::{Output, Registry},
};
use netaudio_core::managed_api::{inventory::Observation, mutation::Mutation};
use serde_json::{Value, json};

pub struct Request {
    pub server: String,
    pub mutation: Mutation,
    pub correlation_id: Option<String>,
}

pub(super) struct Inventory {
    pub fresh: bool,
    pub domains: Vec<Value>,
    pub devices: Vec<Observation>,
}

impl Request {
    pub fn parse(action: &str, value: Value) -> Result<Self, Error> {
        let text = |name: &str| -> Result<String, Error> {
            value
                .get(name)
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .ok_or(Error::Invalid(
                    "Select a server and provide the required managed operation fields",
                ))
        };
        let server = text("server")?;
        super::configuration::validate_name(&server)?;
        let mutation = match action {
            "domains" => Mutation::AddDomain {
                name: text("name")?.trim().into(),
            },
            "domains/update" => match text("action")?.as_str() {
                "rename" => Mutation::RenameDomain {
                    id: text("domain_id")?,
                    name: text("name")?.trim().into(),
                },
                "remove" => Mutation::RemoveDomain {
                    id: text("domain_id")?,
                },
                _ => return Err(Error::Invalid("Choose Rename or Remove")),
            },
            "enrollment" => match text("action")?.as_str() {
                "enroll" => Mutation::EnrollDevice {
                    id: text("device_id")?,
                    domain_id: text("domain_id")?,
                },
                "unenroll" => Mutation::UnenrollDevice {
                    id: text("device_id")?,
                },
                _ => return Err(Error::Invalid("Choose Enroll or Unenroll")),
            },
            _ => return Err(Error::Invalid("Unknown managed mutation")),
        };
        Ok(Self {
            server,
            mutation,
            correlation_id: None,
        })
    }
}

impl Inventory {
    pub(super) fn validate(&self, mutation: &Mutation) -> Result<(), Error> {
        if !self.fresh {
            return Err(Error::Conflict(
                "Current managed inventory is not complete and fresh",
            ));
        }
        match mutation {
            Mutation::AddDomain { name } => {
                if self.domains.iter().any(|domain| domain["name"] == *name) {
                    return Err(Error::Conflict("A domain with that name already exists"));
                }
            }
            Mutation::RenameDomain { id, .. } | Mutation::RemoveDomain { id } => self.domain(id)?,
            Mutation::EnrollDevice { id, domain_id } => {
                self.domain(domain_id)?;
                if self.device(id)?.domain_id.is_some() {
                    return Err(Error::Conflict(
                        "Device is already enrolled; unenroll it before selecting another domain",
                    ));
                }
            }
            Mutation::UnenrollDevice { id } => {
                if self.device(id)?.domain_id.is_none() {
                    return Err(Error::Conflict("Device is not enrolled in a domain"));
                }
            }
        }
        Ok(())
    }

    pub(super) fn domain(&self, id: &str) -> Result<(), Error> {
        if self
            .domains
            .iter()
            .filter(|domain| domain["id"] == id && domain["ddm_fresh"] == true)
            .count()
            == 1
        {
            Ok(())
        } else {
            Err(Error::Invalid(
                "Select a domain reported by the chosen server",
            ))
        }
    }

    pub(super) fn device(&self, id: &str) -> Result<&Observation, Error> {
        let mut matches = self.devices.iter().filter(|item| item.device.id == id);
        let device = matches
            .next()
            .filter(|item| {
                item.fresh
                    && item
                        .device
                        .connection
                        .as_ref()
                        .and_then(|connection| connection.state.as_deref())
                        == Some("READY")
            })
            .ok_or(Error::Conflict(
                "Device is not currently available on the selected managed server",
            ))?;
        if matches.next().is_some() {
            return Err(Error::Conflict("Managed device identity is ambiguous"));
        }
        Ok(device)
    }

    fn observed(&self, mutation: &Mutation, acknowledged_domain: Option<&Value>) -> bool {
        if !self.fresh {
            return false;
        }
        match mutation {
            Mutation::AddDomain { name } => acknowledged_domain
                .and_then(|domain| domain["id"].as_str())
                .is_some_and(|id| {
                    self.domains
                        .iter()
                        .any(|domain| domain["id"] == id && domain["name"] == *name)
                }),
            Mutation::RenameDomain { id, name } => self
                .domains
                .iter()
                .any(|domain| domain["id"] == *id && domain["name"] == *name),
            Mutation::RemoveDomain { id } => !self.domains.iter().any(|domain| domain["id"] == *id),
            Mutation::EnrollDevice { id, domain_id } => self
                .device(id)
                .is_ok_and(|device| device.domain_id.as_deref() == Some(domain_id)),
            Mutation::UnenrollDevice { id } => self
                .device(id)
                .is_ok_and(|device| device.domain_id.is_none()),
        }
    }
}

impl Registry {
    pub(super) async fn mutate(
        &self,
        request: Request,
        inventory: Option<Inventory>,
    ) -> Result<Output, Error> {
        let source = self.source(&request.server)?;
        let client = source.client.as_ref().ok_or_else(|| {
            source
                .setup_error
                .clone()
                .unwrap_or(Error::Conflict("Log in to the selected DDM server first"))
        })?;
        let mut audit = Audit::begin(&self.journal, &request).await?;
        let preflight = async {
            if inventory.is_none() && !self.refresh_server(&request.server).await? {
                return Err(Error::Response("Could not verify current DDM inventory"));
            }
            self.command_inventory(&request.server, &source)?
                .validate(&request.mutation)
        }
        .await;
        if let Err(error) = preflight {
            let value = json!({"operation_id": audit.identifier(), "accepted": false, "request_attempted": false, "request_sent": false, "state_verified": false, "state": "refused", "error": error.to_string()});
            audit.finish(&self.journal, &value).await?;
            return Ok(Output {
                status: match error {
                    Error::Invalid(_) => 400,
                    Error::Busy => 503,
                    Error::Response(_) => 502,
                    _ => 409,
                },
                value,
            });
        }
        let response = client
            .execute(
                &request.mutation.query(),
                request.mutation.variables(),
                Some(request.mutation.operation_name()),
            )
            .await;
        let mut value = json!({"operation_id": audit.identifier(), "request_attempted": true, "request_sent": null, "accepted": null, "state_verified": false, "state": "unconfirmed"});
        match response {
            Ok(response) => {
                value["request_sent"] = json!(true);
                match request.mutation.acknowledgement(&response) {
                    Ok((accepted, result)) => {
                        value["accepted"] = json!(accepted);
                        if let Some(domain) = result.get("domain") {
                            value["domain"] = domain.clone();
                        }
                        if !accepted {
                            value["error"] = json!(
                                "DDM rejected the request; check current state and permissions"
                            );
                        }
                    }
                    Err(error) => value["error"] = json!(error),
                }
                if !response.errors.is_empty() {
                    value["graphql_errors"] = json!(response.errors);
                }
            }
            Err(error) => {
                value["request_sent"] = json!(match &error {
                    Error::Invalid(_) | Error::Credential(_) | Error::Busy => Some(false),
                    Error::Authentication
                    | Error::Status(_)
                    | Error::Response(_)
                    | Error::TooLarge => Some(true),
                    _ => None,
                });
                value["error"] = json!(error.to_string());
            }
        }
        let acknowledgement_error = audit.acknowledgement(&self.journal, &value).await.err();
        let readback = self.refresh_server(&request.server).await;
        match readback {
            Ok(true) => {
                let inventory = self.command_inventory(&request.server, &source)?;
                value["readback_fresh"] = json!(inventory.fresh);
                value["state_verified"] =
                    json!(inventory.observed(&request.mutation, value.get("domain")));
            }
            Ok(false) => {
                value["readback_fresh"] = json!(false);
                value["readback_error"] = json!("Managed inventory readback was unavailable");
            }
            Err(error) => {
                value["readback_fresh"] = json!(false);
                value["readback_error"] = json!(error.to_string());
            }
        }
        let verified = value["state_verified"] == true;
        let accepted = value["accepted"].as_bool();
        value["state"] = json!(match (accepted, verified) {
            (Some(false), true) => "inconsistent",
            (_, true) => "confirmed",
            (Some(true), false) => "accepted",
            (Some(false), false) => "rejected",
            (None, false) => "unconfirmed",
        });
        let mut status = match accepted {
            Some(false) => 409,
            Some(true) if matches!(request.mutation, Mutation::AddDomain { .. }) => 201,
            Some(true) => 200,
            None => 502,
        };
        if let Some(error) = acknowledgement_error {
            value["audit_error"] = json!(error.to_string());
            status = 500;
        }
        if let Err(error) = audit.finish(&self.journal, &value).await {
            value["audit_error"] = json!(error.to_string());
            status = 500;
        }
        Ok(Output { status, value })
    }
}
