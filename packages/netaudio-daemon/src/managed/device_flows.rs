use super::{
    control::{DeviceRequest, FreshTarget},
    device_session::DeviceSession,
    device_topology,
    inventory::freshness,
    registry::{Registry, Source},
    status::observations::Received,
};
use crate::{
    devices::{
        flows::canonical_inventory,
        operations::{ControlCompletion, ControlError},
    },
    monitoring::status::StatusKind,
};
use serde_json::{Map, json};
use std::sync::Arc;

impl Registry {
    pub(super) async fn controller_transmit_flows(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
    ) -> Result<ControlCompletion, ControlError> {
        let fresh = self.fresh_target(request, source, revision).await?;
        let result = self.observe_transmit_flows(source, &fresh).await;
        if let Err(error) = &result {
            self.invalidate_controller_status(
                source,
                &fresh.target,
                StatusKind::TransmitFlows,
                &error.to_string(),
            )?;
        }
        result
    }

    async fn observe_transmit_flows(
        &self,
        source: &Arc<Source>,
        fresh: &FreshTarget,
    ) -> Result<ControlCompletion, ControlError> {
        let mut session = DeviceSession::open(source, fresh).await?;
        let protocol = netaudio_core::protocol::arc_protocol(None, true)
            .map_err(|error| unavailable(error.to_string()))?
            .ok_or_else(|| unavailable("Managed ARC protocol is unavailable".into()))?
            .protocol_id;
        let first =
            device_topology::query(&mut session, json!({"command":"channel_count"})).await?;
        let counts = device_topology::counts(&first)?;
        let inventory = device_topology::inventory(&mut session, "tx_flows", protocol).await?;
        let last = device_topology::query(&mut session, json!({"command":"channel_count"})).await?;
        let verified = device_topology::counts(&last)?;
        if !device_topology::same_topology(&counts, &verified) {
            return Err(unavailable(
                "Channel counts changed during managed transmit-flow observation".into(),
            ));
        }
        let mut canonical = canonical_inventory(&inventory, protocol)?;
        let record = self.status_record(fresh)?;
        canonical["device"] = record["server_name"].clone();
        let fields = Map::from_iter([
            ("flow_protocol_id".into(), json!(protocol)),
            ("transmitter_flows".into(), inventory["flows"].clone()),
            ("transmitter_flow_inventory".into(), inventory),
        ]);
        let received = Received::inventory(
            &first,
            &last,
            StatusKind::TransmitFlows,
            fields,
            freshness(&source.configuration),
        )
        .map_err(|error| unavailable(error.to_string()))?;
        self.accept_controller_status(source, &fresh.target, received)?;
        Ok(ControlCompletion {
            acknowledged_commands: 0,
            readback_completed: true,
            readback_errors: Vec::new(),
            observation: Some(canonical),
        })
    }
}

fn unavailable(message: String) -> ControlError {
    ControlError::ObservationUnavailable(message)
}
