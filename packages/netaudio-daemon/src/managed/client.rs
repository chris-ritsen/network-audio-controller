use super::{Credential, Error, Limits, Transport};
use netaudio_core::managed_api::{self, Response};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Method, Url};
use serde_json::{Map, Value, json};

#[derive(Clone)]
pub struct Client {
    endpoint: Url,
    credential: Credential,
    transport: Transport,
    limits: Limits,
}

pub fn endpoint(value: &str) -> Result<Url, Error> {
    let endpoint = Url::parse(value).map_err(|_| Error::Invalid("Managed API URL is invalid"))?;

    if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
        return Err(Error::Invalid(
            "Managed API URL must use http or https and include a host",
        ));
    }

    if endpoint.path() != "/graphql" || endpoint.query().is_some() || endpoint.fragment().is_some()
    {
        return Err(Error::Invalid(
            "Managed API URL path must be exactly /graphql",
        ));
    }

    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err(Error::Invalid(
            "Managed API URL must not contain credentials",
        ));
    }

    Ok(endpoint)
}

impl Client {
    pub fn new(
        url: &str,
        credential: Credential,
        transport: Transport,
        limits: Limits,
    ) -> Result<Self, Error> {
        Ok(Self {
            endpoint: endpoint(url)?,
            credential,
            transport,
            limits: limits.validate()?,
        })
    }

    pub async fn execute(
        &self,
        query: &str,
        variables: Map<String, Value>,
        operation_name: Option<&str>,
    ) -> Result<Response, Error> {
        tokio::time::timeout(
            self.limits.timeout,
            self.execute_within_deadline(query, variables, operation_name),
        )
        .await
        .map_err(|_| Error::Deadline)?
    }

    async fn execute_within_deadline(
        &self,
        query: &str,
        variables: Map<String, Value>,
        operation_name: Option<&str>,
    ) -> Result<Response, Error> {
        if query.trim().is_empty() {
            return Err(Error::Invalid("GraphQL query must be a non-empty string"));
        }

        let credential = self.credential.read().await?;
        let mut headers = headers();
        let mut authorization = HeaderValue::from_str(&credential)
            .map_err(|_| Error::Credential("Managed API credential has an invalid format"))?;
        authorization.set_sensitive(true);
        headers.insert(AUTHORIZATION, authorization);
        let mut payload = json!({"query": query, "variables": variables});

        if let Some(name) = operation_name {
            payload["operationName"] = json!(name);
        }

        request(
            &self.transport,
            self.endpoint.clone(),
            headers,
            payload,
            self.limits,
        )
        .await
    }

    pub async fn inventory(
        &self,
    ) -> Result<(Response, Option<managed_api::models::Inventory>), Error> {
        let response = self
            .execute(
                managed_api::INVENTORY_QUERY,
                Map::new(),
                Some(managed_api::INVENTORY_OPERATION),
            )
            .await?;
        let inventory = response.inventory().map_err(Error::Response)?;
        Ok((response, inventory))
    }

    pub async fn authenticate(
        url: &str,
        username: &str,
        password: &str,
        transport: &Transport,
        limits: Limits,
    ) -> Result<Credential, Error> {
        if username.is_empty() || password.is_empty() {
            return Err(Error::Invalid(
                "Username and password must be non-empty strings",
            ));
        }

        let response = request(
            transport,
            endpoint(url)?,
            headers(),
            json!({
                "operationName": managed_api::PASSWORD_OPERATION,
                "query": managed_api::PASSWORD_MUTATION,
                "variables": {"input": {"email": username, "password": password}},
            }),
            limits,
        )
        .await?;
        Credential::new(
            response
                .password_credential()
                .map_err(|error| match error {
                    managed_api::LoginError::Rejected => Error::Authentication,
                    managed_api::LoginError::Invalid(message) => Error::Response(message),
                })?,
        )
    }
}

fn headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers
}

async fn request(
    transport: &Transport,
    url: Url,
    headers: HeaderMap,
    payload: Value,
    limits: Limits,
) -> Result<Response, Error> {
    let body = serde_json::to_vec(&payload)
        .map_err(|_| Error::Invalid("Managed API request could not be encoded"))?;

    if body.len() > crate::http::MAXIMUM_REQUEST_BYTES {
        return Err(Error::Invalid("Managed API request exceeds 8 MiB"));
    }

    let response = transport
        .request(Method::POST, url, headers, body, limits)
        .await?;
    Response::parse(&response).map_err(Error::Response)
}
