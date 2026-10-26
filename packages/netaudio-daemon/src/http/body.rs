use super::ApiError;
use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use std::time::Duration;

const REQUEST_BODY_DEADLINE: Duration = Duration::from_secs(5);

pub struct RequestBody(pub Bytes);

impl<State: Send + Sync> FromRequest<State> for RequestBody {
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &State) -> Result<Self, Self::Rejection> {
        match tokio::time::timeout(REQUEST_BODY_DEADLINE, Bytes::from_request(request, state)).await
        {
            Ok(Ok(body)) => Ok(Self(body)),
            Ok(Err(error)) => Err(ApiError {
                status: error.status(),
                message: error.body_text(),
            }),
            Err(_) => Err(ApiError {
                status: StatusCode::REQUEST_TIMEOUT,
                message: "request body timed out".to_owned(),
            }),
        }
    }
}
