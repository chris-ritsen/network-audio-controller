use super::Response;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum Mutation {
    AddDomain { name: String },
    RenameDomain { id: String, name: String },
    RemoveDomain { id: String },
    EnrollDevice { id: String, domain_id: String },
    UnenrollDevice { id: String },
}

impl Mutation {
    pub fn operation_name(&self) -> &'static str {
        match self {
            Self::AddDomain { .. } => "DomainAdd",
            Self::RenameDomain { .. } => "DomainUpdate",
            Self::RemoveDomain { .. } => "DomainRemove",
            Self::EnrollDevice { .. } => "DevicesEnroll",
            Self::UnenrollDevice { .. } => "DevicesUnenroll",
        }
    }

    pub fn query(&self) -> String {
        let name = self.operation_name();
        let fields = if matches!(self, Self::AddDomain { .. }) {
            "ok domain { id name }"
        } else {
            "ok"
        };
        format!("mutation {name}($input: {name}Input!) {{ {name}(input: $input) {{ {fields} }} }}")
    }

    pub fn variables(&self) -> Map<String, Value> {
        let input = match self {
            Self::AddDomain { name } => json!({"name": name, "icon": "OTHER"}),
            Self::RenameDomain { id, name } => json!({"id": id, "name": name}),
            Self::RemoveDomain { id } => json!({"id": id}),
            Self::EnrollDevice { id, domain_id } => {
                json!({"deviceIds": [id], "domainId": domain_id, "clearConfig": false})
            }
            Self::UnenrollDevice { id } => json!({"deviceIds": [id], "clearConfig": false}),
        };
        Map::from_iter([("input".into(), input)])
    }

    pub fn acknowledgement<'a>(
        &self,
        response: &'a Response,
    ) -> Result<(bool, &'a Map<String, Value>), &'static str> {
        acknowledgement(self.operation_name(), response)
    }
}

pub(super) fn acknowledgement<'a>(
    operation: &str,
    response: &'a Response,
) -> Result<(bool, &'a Map<String, Value>), &'static str> {
    if !response.errors.is_empty() {
        return Err(
            "Managed mutation returned GraphQL errors; check current state before retrying",
        );
    }
    let result = response
        .data
        .as_ref()
        .and_then(|data| data.get(operation))
        .and_then(Value::as_object)
        .ok_or("Managed mutation result must be an object")?;
    let accepted = result
        .get("ok")
        .and_then(Value::as_bool)
        .ok_or("Managed mutation result ok must be a boolean")?;
    Ok((accepted, result))
}
