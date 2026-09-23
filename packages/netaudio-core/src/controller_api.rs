//! Controller HTTP bootstrap facts; networking and credentials storage belong to the host.
use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

use crate::dapi::ManagedCredential;

pub const AUTH_PORT: u16 = 8443;
pub const VERSIONS_PATH: &str = "/dapi";

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ControllerApiRoutes {
    pub endpoints: &'static str,
    pub login: &'static str,
}

pub fn routes(data: &[u8]) -> Result<ControllerApiRoutes, &'static str> {
    let versions: Vec<String> = serde_json::from_slice(data)
        .map_err(|_| "DDM returned an invalid Controller API version list")?;

    if !versions.iter().any(|version| version == "v2") {
        return Err("DDM does not advertise the observed v2 Controller API");
    }

    Ok(ControllerApiRoutes {
        endpoints: "/dapi/v2/endpoints",
        login: "/dapi/v2/login",
    })
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ControllerEndpoints {
    pub service_port: NonZeroU16,
    pub device_port: NonZeroU16,
    pub graphql_url: String,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ControllerLogin {
    pub auth_token: String,
    pub endpoints: ControllerEndpoints,
}

#[derive(Deserialize)]
struct Response {
    #[serde(rename = "servicePort")]
    service_port: NonZeroU16,
    #[serde(rename = "devicePort")]
    device_port: NonZeroU16,
    #[serde(rename = "graphQl")]
    graphql_url: String,
    #[serde(rename = "authToken", default)]
    auth_token: serde_json::Value,
}

impl Response {
    fn parse(data: &[u8]) -> Result<Self, &'static str> {
        // Never include response contents in errors: a login response carries a credential.
        let response: Self = serde_json::from_slice(data)
            .map_err(|_| "DDM returned invalid Controller endpoints")?;

        if response.graphql_url.is_empty() {
            return Err("DDM returned an invalid GraphQL endpoint");
        }

        Ok(response)
    }

    fn endpoints(self) -> ControllerEndpoints {
        ControllerEndpoints {
            service_port: self.service_port,
            device_port: self.device_port,
            graphql_url: self.graphql_url,
        }
    }
}

pub fn endpoints(data: &[u8]) -> Result<ControllerEndpoints, &'static str> {
    Ok(Response::parse(data)?.endpoints())
}

pub fn login(data: &[u8]) -> Result<Option<ControllerLogin>, &'static str> {
    let response = Response::parse(data)?;
    let Some(token) = response.auth_token.as_str() else {
        return Ok(None);
    };

    if ManagedCredential::ControllerToken(token.to_owned())
        .validate()
        .is_err()
    {
        return Ok(None);
    }

    Ok(Some(ControllerLogin {
        auth_token: token.to_owned(),
        endpoints: response.endpoints(),
    }))
}
