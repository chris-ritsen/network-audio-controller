use super::{Error, connections};
use serde::Deserialize;
use serde_json::{Map, Value};

pub enum Operation {
    Refresh {
        context: Option<String>,
    },
    Graphql {
        query: String,
        variables: serde_json::Map<String, Value>,
        operation_name: Option<String>,
        context: Option<String>,
    },
    Login(super::connections::Login),
    Logout(String),
    Profile(super::connections::Profile),
    Context(super::connections::Selection),
    Mutation(super::mutations::Request),
    Enrollment(super::enrollment::Request),
}

pub fn parse(action: &str, value: Value) -> Result<Operation, Error> {
    if !value.is_object() {
        return Err(Error::Invalid("Managed request must be a JSON object"));
    }
    match action {
        "refresh" => Ok(Operation::Refresh {
            context: connections::context(&value)?,
        }),
        "graphql" => {
            #[derive(Deserialize)]
            struct Graphql {
                query: String,
                variables: Option<Map<String, Value>>,
                operation_name: Option<String>,
                context: Option<String>,
            }
            let request: Graphql = connections::payload(value)?;
            if request.query.trim().is_empty() {
                return Err(Error::Invalid("query must be a non-empty string"));
            }
            Ok(Operation::Graphql {
                query: request.query,
                variables: request.variables.unwrap_or_default(),
                operation_name: request.operation_name,
                context: request.context,
            })
        }
        "login" => Ok(Operation::Login(connections::payload(value)?)),
        "logout" => Ok(Operation::Logout(connections::server_name(&value)?)),
        "profile" => Ok(Operation::Profile(connections::payload(value)?)),
        "context" => Ok(Operation::Context(connections::payload(value)?)),
        "domains" | "domains/update" | "enrollment" => Ok(Operation::Mutation(
            super::mutations::Request::parse(action, value)?,
        )),
        _ => Err(Error::Invalid("Unknown managed operation")),
    }
}
