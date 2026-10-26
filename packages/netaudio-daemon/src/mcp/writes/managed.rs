use super::{CallError, NEXT_STEP, Prepared, PreparedKind, required_text, views};
use crate::managed::{
    operations::{self, Operation},
    planning::Plan,
    registry::Registry,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) struct PreparedManaged {
    plan: Plan,
    operation: String,
}

impl PreparedManaged {
    pub fn conditions(&self) -> Value {
        json!(self.plan.conditions)
    }

    pub async fn apply(self, registry: &Registry) -> Result<(Value, bool), CallError> {
        let output = registry
            .execute_conditionally(self.plan.operation, Some(self.plan.conditions))
            .await
            .map_err(|error| CallError::invalid(error.to_string()))?;
        let mut result = output.value;
        result["operation"] = json!(self.operation);
        Ok((result, output.status >= 400))
    }
}

pub fn supports(operation: &str) -> bool {
    matches!(
        operation,
        "login_ddm"
            | "logout_ddm"
            | "edit_ddm_profile"
            | "select_ddm_context"
            | "create_ddm_domain"
            | "update_ddm_domain"
            | "set_ddm_enrollment"
    )
}

pub(super) async fn prepare(
    operation: &str,
    request: &Value,
    registry: &Registry,
) -> Result<Prepared, CallError> {
    let mut resolved = request.clone();
    if matches!(operation, "update_ddm_domain" | "set_ddm_enrollment")
        || operation == "select_ddm_context" && request.get("context").is_none()
    {
        let inventory = registry
            .selection_inventory(optional_text(&resolved, "server")?.map(str::to_owned))
            .await
            .map_err(|error| CallError::invalid(error.to_string()))?;
        if operation == "set_ddm_enrollment" {
            resolve_device(&mut resolved, &inventory.devices)?;
        }
        if operation != "set_ddm_enrollment" || resolved["action"] == "enroll" {
            resolve_domain(&mut resolved, &inventory.domains)?;
        }
    }
    let action = match operation {
        "login_ddm" => "login",
        "logout_ddm" => "logout",
        "edit_ddm_profile" => "profile",
        "select_ddm_context" => "context",
        "create_ddm_domain" => "domains",
        "update_ddm_domain" => "domains/update",
        "set_ddm_enrollment" => "enrollment",
        _ => return Err(CallError::invalid("Unknown managed write operation")),
    };
    let command = if action == "enrollment" {
        let domain_id = match required_text(&resolved, "action")? {
            "enroll" => Some(required_text(&resolved, "domain_id")?.into()),
            "unenroll" => None,
            _ => return Err(CallError::invalid("Choose Enroll or Unenroll")),
        };
        Operation::Enrollment(crate::managed::enrollment::Request {
            server: required_text(&resolved, "server")?.into(),
            device_id: required_text(&resolved, "device_id")?.into(),
            domain_id,
        })
    } else {
        operations::parse(action, resolved)
            .map_err(|error| CallError::invalid(error.to_string()))?
    };
    let plan = registry
        .prepare(command)
        .await
        .map_err(|error| CallError::invalid(error.to_string()))?;
    let mut preview = json!({"changed": false, "confirmation_required": true, "operation": operation, "next": NEXT_STEP, "change": plan.change});
    match operation {
        "set_ddm_enrollment" => {
            let before = &plan.change["device"];
            preview["device"] = before["name"].clone();
            preview["change"] = json!({"domain": {"from": label(&before["domain_name"], &before["domain_id"]), "to": label(&plan.change["to"]["name"], &plan.change["to"]["id"])}});
            if plan.change["already_selected"] == true {
                preview["effect"] =
                    json!("The device already has this enrollment; no request will be sent");
            } else if !before["domain_id"].is_null() && !plan.change["to"].is_null() {
                preview["effect"] = json!(
                    "Unenrolls the device, verifies its identity and unmanaged state, then enrolls it in the destination domain. If the first step cannot be verified, the move stops. Routes and clocking may be interrupted"
                );
            } else {
                preview["effect"] = json!(
                    "Changes the device's domain membership. Routes and clocking may be interrupted where other devices are no longer in the same domain"
                );
            }
        }
        "update_ddm_domain" if plan.change.get("remove_domain").is_some() => {
            let domain = &plan.change["remove_domain"];
            let members: Vec<_> = domain["devices"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|device| device["name"].clone())
                .collect();
            preview["change"] = json!({"remove_domain": label(&domain["name"], &domain["id"]), "devices_leaving": members});
            preview["effect"] = json!(
                "Removes this domain; its enrolled devices become unmanaged and their routes and clocking may be interrupted"
            );
        }
        "login_ddm" => {
            preview["effect"] = json!(
                "Authenticates and verifies inventory before saving the login. A failed login preserves the previous saved credentials"
            )
        }
        "logout_ddm" => {
            preview["effect"] = json!(
                "Removes this saved login and disconnects managed inventory. Devices keep running"
            )
        }
        _ => {}
    }
    Ok(Prepared {
        preview,
        devices: Vec::new(),
        kind: PreparedKind::Managed(PreparedManaged {
            plan,
            operation: operation.into(),
        }),
    })
}

fn resolve_device(request: &mut Value, devices: &[Value]) -> Result<(), CallError> {
    let server = optional_text(request, "server")?;
    let identifier = optional_text(request, "device_id")?;
    let selected: Vec<_> = devices
        .iter()
        .filter(|device| {
            server.is_none_or(|server| device["ddm_server_profile"] == server)
                && identifier.is_none_or(|identifier| device["ddm_device_id"] == identifier)
        })
        .collect();
    let selected = if let Some(selector) = optional_text(request, "device")? {
        let records = selected
            .iter()
            .map(|record| {
                (
                    views::text(&record["server_name"]),
                    Arc::new((*record).clone()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        views::resolve(&records, selector)?
            .cloned()
            .ok_or_else(|| {
                CallError::invalid(
                    "The selected device is not uniquely available in managed inventory",
                )
            })?
    } else {
        if identifier.is_none() || selected.len() != 1 {
            return Err(CallError::invalid(
                "Name the device, or give its DDM server and unique device_id",
            ));
        }
        selected[0].clone()
    };
    request["server"] = selected["ddm_server_profile"].clone();
    request["device_id"] = selected["ddm_device_id"].clone();
    Ok(())
}

fn resolve_domain(request: &mut Value, domains: &[Value]) -> Result<(), CallError> {
    let server = optional_text(request, "server")?;
    let identifier = optional_text(request, "domain_id")?;
    let selector = optional_text(request, "domain")?;
    if identifier.is_none() && selector.is_none() {
        return Err(CallError::invalid("Name the domain or provide its ID"));
    }
    let candidates: Vec<_> = domains
        .iter()
        .filter(|domain| {
            server.is_none_or(|server| domain["ddm_server_profile"] == server)
                && identifier.is_none_or(|identifier| domain["id"] == identifier)
        })
        .collect();
    let selected = if let Some(selector) = selector {
        let exact: Vec<_> = candidates
            .iter()
            .copied()
            .filter(|domain| domain["id"] == selector || domain["name"] == selector)
            .collect();
        if exact.is_empty() {
            candidates
                .into_iter()
                .filter(|domain| {
                    domain["name"]
                        .as_str()
                        .is_some_and(|name| caseless::default_caseless_match_str(name, selector))
                })
                .collect()
        } else {
            exact
        }
    } else {
        candidates
    };
    let [domain] = selected.as_slice() else {
        return Err(CallError::invalid(
            "Domain is missing or ambiguous; provide the server and domain ID",
        ));
    };
    request["server"] = domain["ddm_server_profile"].clone();
    request["domain_id"] = domain["id"].clone();
    Ok(())
}

fn optional_text<'a>(value: &'a Value, field: &str) -> Result<Option<&'a str>, CallError> {
    match value.get(field) {
        None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value)),
        _ => Err(CallError::invalid(format!(
            "{field} must be a non-empty string"
        ))),
    }
}

fn label(name: &Value, identifier: &Value) -> Value {
    name.as_str()
        .filter(|name| !name.is_empty())
        .or(identifier.as_str())
        .map_or(json!("no domain (unmanaged)"), |label| json!(label))
}
