use super::{
    Error,
    audit::Audit,
    mutations::{Inventory, Request as MutationRequest},
    planning::device_change,
    registry::{Output, Registry},
};
use netaudio_core::managed_api::{inventory::primary_mac, mutation::Mutation};
use serde_json::{Value, json};

pub struct Request {
    pub server: String,
    pub device_id: String,
    pub domain_id: Option<String>,
}

impl Request {
    pub(super) fn change(&self, inventory: &Inventory) -> Result<Value, Error> {
        if !inventory.fresh {
            return Err(Error::Conflict(
                "Current managed inventory is not complete and fresh",
            ));
        }
        let device = inventory.device(&self.device_id)?;
        let destination = self
            .domain_id
            .as_deref()
            .map(|id| inventory.domain_change(id, false))
            .transpose()?;
        if device.domain_id.is_some()
            && self.domain_id.is_some()
            && device.domain_id != self.domain_id
        {
            let identity = primary_mac(&device.device).ok_or(Error::Conflict("A domain move requires a reported primary MAC address to identify the device after unenrollment"))?;
            if inventory
                .devices
                .iter()
                .filter(|device| primary_mac(&device.device).as_deref() == Some(&identity))
                .count()
                != 1
            {
                return Err(Error::Conflict(
                    "Device identity is ambiguous; a domain move cannot be prepared",
                ));
            }
        }
        Ok(
            json!({"server": self.server, "device": device_change(device), "to": destination, "already_selected": device.domain_id == self.domain_id}),
        )
    }
}

impl Registry {
    pub(super) async fn enroll(
        &self,
        request: Request,
        inventory: Option<Inventory>,
    ) -> Result<Output, Error> {
        let inventory = match inventory {
            Some(inventory) => inventory,
            None => self.fresh_inventory(&request.server).await?,
        };
        let change = request.change(&inventory)?;
        let device = inventory.device(&request.device_id)?;
        let before = device.domain_id.clone();
        let mut audit = Audit::begin_named(
            &self.journal,
            &request.server,
            "SetEnrollment",
            change,
            None,
        )
        .await?;
        let mut result = json!({"operation_id": audit.identifier(), "request_attempted": false, "request_sent": false, "accepted": null, "state_verified": false, "state": "unconfirmed", "device": device.device.name, "from_domain_id": before, "requested_domain_id": request.domain_id, "steps": []});
        let outcome = self
            .enrollment_steps(&request, inventory, audit.identifier(), &mut result)
            .await;
        let mut status = match outcome {
            Ok(status) => status,
            Err(error) => {
                result["error"] = json!(error.to_string());
                result["state"] = json!(if result["request_attempted"] == true {
                    "partial"
                } else {
                    "refused"
                });
                409
            }
        };
        if result["state_verified"] != true && result["state"] == "confirmed" {
            result["state"] = json!("partial");
        }
        if before.is_some()
            && request.domain_id.is_some()
            && before != request.domain_id
            && result["steps"]
                .as_array()
                .is_some_and(|steps| steps.len() < 2)
            && result["accepted"] != false
        {
            result["accepted"] = Value::Null;
        }
        if let Err(error) = audit.acknowledgement(&self.journal, &result).await {
            result["audit_error"] = json!(error.to_string());
            status = 500;
        }
        if let Err(error) = audit.finish(&self.journal, &result).await {
            result["audit_error"] = json!(error.to_string());
            status = 500;
        }
        Ok(Output {
            status,
            value: result,
        })
    }

    async fn enrollment_steps(
        &self,
        request: &Request,
        inventory: Inventory,
        identifier: &str,
        result: &mut Value,
    ) -> Result<u16, Error> {
        let original = inventory.device(&request.device_id)?;
        if original.domain_id == request.domain_id {
            result["state"] = json!("confirmed");
            result["state_verified"] = json!(true);
            result["changed"] = json!(false);
            result["readback_fresh"] = json!(true);
            result["now_in"] = json!(
                original
                    .domain_name
                    .as_deref()
                    .or(original.domain_id.as_deref())
                    .unwrap_or("no domain (unmanaged)")
            );
            return Ok(200);
        }
        let moving = original.domain_id.is_some() && request.domain_id.is_some();
        let identity = primary_mac(&original.device);
        let mut device_id = request.device_id.clone();
        if moving {
            let destination = request
                .domain_id
                .as_deref()
                .ok_or(Error::Invalid("Select a destination domain"))?;
            let expected_domain = inventory.domain_change(destination, false)?;
            let output = self
                .mutate(
                    MutationRequest {
                        server: request.server.clone(),
                        mutation: Mutation::UnenrollDevice {
                            id: device_id.clone(),
                        },
                        correlation_id: Some(identifier.into()),
                    },
                    Some(inventory),
                )
                .await?;
            let accepted = output.value["accepted"] == true;
            record_step(result, output.value);
            if output.status >= 400 || !accepted {
                result["error"] = json!(
                    "Leaving the previous domain was not acknowledged; the destination enrollment was not sent"
                );
                return Ok(output.status.max(409));
            }
            let source = self.source(&request.server)?;
            let current = self.command_inventory(&request.server, &source)?;
            if !current.fresh {
                return Err(Error::Conflict(
                    "Leaving the previous domain was not verified; the destination enrollment was not sent",
                ));
            }
            let device = current.enrollment_device(&device_id, identity.as_deref())?;
            if device.domain_id.is_some() {
                return Err(Error::Conflict(
                    "Leaving the previous domain was not verified; the destination enrollment was not sent",
                ));
            }
            device_id = device.device.id.clone();
            result["left_previous_domain"] = json!(true);
            result["now_in"] = json!("no domain (unmanaged)");
            if current.domain_change(destination, false)? != expected_domain {
                return Err(Error::Conflict(
                    "The destination domain changed during the move; destination enrollment was not sent",
                ));
            }
            result
                .as_object_mut()
                .expect("enrollment result is an object")
                .remove("now_in");
            let output = self
                .mutate(
                    MutationRequest {
                        server: request.server.clone(),
                        mutation: Mutation::EnrollDevice {
                            id: device_id.clone(),
                            domain_id: request
                                .domain_id
                                .clone()
                                .ok_or(Error::Invalid("Select a destination domain"))?,
                        },
                        correlation_id: Some(identifier.into()),
                    },
                    Some(current),
                )
                .await?;
            let status = output.status;
            record_step(result, output.value);
            return self.finish_enrollment(
                request,
                &device_id,
                identity.as_deref(),
                status,
                result,
            );
        }
        let mutation = match &request.domain_id {
            Some(domain_id) => Mutation::EnrollDevice {
                id: device_id.clone(),
                domain_id: domain_id.clone(),
            },
            None => Mutation::UnenrollDevice {
                id: device_id.clone(),
            },
        };
        let output = self
            .mutate(
                MutationRequest {
                    server: request.server.clone(),
                    mutation,
                    correlation_id: Some(identifier.into()),
                },
                Some(inventory),
            )
            .await?;
        let status = output.status;
        record_step(result, output.value);
        self.finish_enrollment(request, &device_id, identity.as_deref(), status, result)
    }

    fn finish_enrollment(
        &self,
        request: &Request,
        device_id: &str,
        identity: Option<&str>,
        status: u16,
        result: &mut Value,
    ) -> Result<u16, Error> {
        let source = self.source(&request.server)?;
        let inventory = self.command_inventory(&request.server, &source)?;
        result["readback_fresh"] = json!(inventory.fresh);
        if inventory.fresh {
            let device = inventory.enrollment_device(device_id, identity)?;
            let verified = device.domain_id == request.domain_id;
            result["state_verified"] = json!(verified);
            result["changed"] = json!(verified);
            result["now_in"] = json!(
                device
                    .domain_name
                    .as_deref()
                    .or(device.domain_id.as_deref())
                    .unwrap_or("no domain (unmanaged)")
            );
            result["device_id"] = json!(device.device.id);
            if verified {
                result["state"] = json!(if result["accepted"] == false {
                    "inconsistent"
                } else {
                    "confirmed"
                });
            }
        }
        if result["state_verified"] != true {
            result["state"] = json!(match result["accepted"].as_bool() {
                Some(true) => "accepted",
                Some(false) => "rejected",
                None => "unconfirmed",
            });
            result["readback"] = json!(
                "The requested enrollment is not verified; inspect the current managed inventory before another change"
            );
        }
        Ok(status)
    }
}

fn record_step(result: &mut Value, step: Value) {
    result["request_attempted"] =
        json!(result["request_attempted"] == true || step["request_attempted"] == true);
    result["request_sent"] = if result["request_sent"] == true || step["request_sent"] == true {
        json!(true)
    } else if result["request_sent"].is_null() || step["request_sent"].is_null() {
        Value::Null
    } else {
        json!(false)
    };
    result["accepted"] = step["accepted"].clone();
    result["readback_fresh"] = step["readback_fresh"].clone();
    result["state"] = step["state"].clone();
    result["state_verified"] = json!(false);
    if let Some(error) = step.get("error") {
        result["error"] = error.clone();
    }
    result["steps"]
        .as_array_mut()
        .expect("enrollment steps are an array")
        .push(step);
}

impl Inventory {
    fn enrollment_device(
        &self,
        identifier: &str,
        identity: Option<&str>,
    ) -> Result<&netaudio_core::managed_api::inventory::Observation, Error> {
        if let Some(device) = self
            .devices
            .iter()
            .find(|device| device.device.id == identifier)
        {
            if identity
                .is_some_and(|identity| primary_mac(&device.device).as_deref() != Some(identity))
            {
                return Err(Error::Conflict(
                    "The managed device identity changed during enrollment",
                ));
            }
            return self.device(identifier);
        }
        let identity = identity.ok_or(Error::Conflict("The device is missing after enrollment and has no primary MAC address for identification"))?;
        let matches: Vec<_> = self
            .devices
            .iter()
            .filter(|device| primary_mac(&device.device).as_deref() == Some(identity))
            .collect();
        let [device] = matches.as_slice() else {
            return Err(Error::Conflict(
                "Device identity after enrollment is missing or ambiguous",
            ));
        };
        self.device(&device.device.id)
    }
}
