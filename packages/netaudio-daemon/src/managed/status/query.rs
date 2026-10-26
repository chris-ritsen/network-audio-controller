use crate::{
    devices::operations::ControlError,
    monitoring::{requests::StatusQuery, status::StatusKind},
};
use serde_json::{Value, json};

#[derive(Clone, Copy)]
pub(in crate::managed) enum Query {
    Status(StatusQuery),
    Latency,
    MulticastPrefix,
}

impl From<StatusQuery> for Query {
    fn from(query: StatusQuery) -> Self {
        Self::Status(query)
    }
}

impl Query {
    pub fn kind(self) -> StatusKind {
        match self {
            Self::Status(query) => query.kind(),
            Self::Latency => StatusKind::Latency,
            Self::MulticastPrefix => StatusKind::MulticastPrefix,
        }
    }

    pub fn command(self, device: &str) -> Result<Value, ControlError> {
        match self {
            Self::Status(StatusQuery::InterfaceStatistics) => Err(ControlError::Unavailable(
                "Managed interface statistics are not integrated yet".into(),
            )),
            Self::Status(query) => Ok(query.command(Some(device))?),
            Self::Latency | Self::MulticastPrefix => Ok(json!({"command": "query_latency_config"})),
        }
    }
}
