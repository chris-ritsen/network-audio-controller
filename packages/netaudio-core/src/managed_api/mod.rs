pub mod control;
pub mod inventory;
pub mod models;
pub mod mutation;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const INVENTORY_OPERATION: &str = "NetAudioManagedInventory";
pub const INVENTORY_QUERY: &str = include_str!("inventory.graphql");
pub const PASSWORD_OPERATION: &str = "UserLoginWithPassword";
pub const PASSWORD_MUTATION: &str = "mutation UserLoginWithPassword($input: UserLoginWithPasswordInput!) { UserLoginWithPassword(input: $input) { ok token } }";

#[derive(Debug)]
pub enum LoginError {
    Rejected,
    Invalid(&'static str),
}

impl std::fmt::Display for LoginError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected => formatter.write_str("DDM rejected username/password login"),
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for LoginError {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Issue {
    pub message: String,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Response {
    pub data: Option<Map<String, Value>>,
    pub errors: Vec<Issue>,
}

impl Response {
    pub fn parse(bytes: &[u8]) -> Result<Self, &'static str> {
        let mut value: Map<String, Value> = serde_json::from_slice(bytes)
            .map_err(|_| "Managed API response must be a JSON object")?;
        let errors: Vec<Issue> = serde_json::from_value(
            value
                .remove("errors")
                .unwrap_or_else(|| Value::Array(Vec::new())),
        )
        .map_err(|_| "Managed API errors must be objects containing a message string")?;
        let data = match value.remove("data") {
            None if errors.is_empty() => return Err("Managed API response omitted data"),
            None | Some(Value::Null) => None,
            Some(Value::Object(data)) => Some(data),
            Some(_) => return Err("Managed API data must be an object or null"),
        };

        Ok(Self { data, errors })
    }

    pub fn successful(&self) -> bool {
        self.data.is_some() && self.errors.is_empty()
    }

    pub fn inventory(&self) -> Result<Option<models::Inventory>, &'static str> {
        let Some(data) = &self.data else {
            return Ok(None);
        };

        if self.errors.is_empty()
            && (!data.contains_key("domains") || !data.contains_key("unenrolledDevices"))
        {
            return Err("Managed API inventory omitted a root field");
        }

        if self.errors.is_empty()
            && (data["domains"].is_null() || data["unenrolledDevices"].is_null())
        {
            return Err("Managed API inventory roots must be non-null lists");
        }

        serde_json::from_value(Value::Object(data.clone()))
            .map(Some)
            .map_err(|_| "Managed API inventory contains an invalid field")
    }

    pub fn password_credential(&self) -> Result<String, LoginError> {
        if !self.successful() {
            return Err(LoginError::Rejected);
        }

        let result = self
            .data
            .as_ref()
            .and_then(|data| data.get(PASSWORD_OPERATION))
            .and_then(Value::as_object)
            .ok_or(LoginError::Invalid(
                "Managed API login result must be an object",
            ))?;

        match result.get("ok").and_then(Value::as_bool) {
            Some(true) => {}
            Some(false) => return Err(LoginError::Rejected),
            None => {
                return Err(LoginError::Invalid(
                    "Managed API login result ok must be a boolean",
                ));
            }
        }

        let token = result
            .get("token")
            .and_then(Value::as_str)
            .ok_or(LoginError::Invalid(
                "Managed API login result token is invalid",
            ))?;
        validate_credential(token).map_err(LoginError::Invalid)?;
        Ok(token.to_owned())
    }
}

pub fn validate_credential(value: &str) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > 64 * 1024 {
        return Err("Managed API credential must contain between 1 byte and 64 KiB");
    }

    if !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
        return Err("Managed API credential has an invalid format");
    }

    Ok(())
}
