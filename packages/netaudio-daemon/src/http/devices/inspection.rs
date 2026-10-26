use super::{ApiError, DeviceService, control_error, json_value};
use crate::devices::operations::ControlOperation;
use crate::devices::registry::Inspection;
use axum::Router;
use axum::extract::{Extension, Path, State};
use axum::response::Response;
use axum::routing::get;
use std::sync::Arc;

pub(super) fn routes(router: Router<DeviceService>) -> Router<DeviceService> {
    router
        .route(
            "/lock-status/{device}",
            get(inspect).layer(Extension(Inspection::Lock)),
        )
        .route(
            "/redundancy/{device}",
            get(inspect).layer(Extension(Inspection::Redundancy)),
        )
        .route(
            "/interfaces/{device}",
            get(inspect).layer(Extension(Inspection::Network)),
        )
}

async fn inspect(
    State(service): State<DeviceService>,
    Extension(inspection): Extension<Inspection>,
    Path(device): Path<String>,
) -> Result<Response, ApiError> {
    match service
        .registry
        .apply(&device, ControlOperation::InspectStatus(inspection))
        .await
    {
        Ok(result) => Ok(json_value(Arc::new(result.observation.ok_or_else(
            || ApiError::unavailable("device status was not reported"),
        )?))),
        Err(error) => Ok(control_error(error)),
    }
}
