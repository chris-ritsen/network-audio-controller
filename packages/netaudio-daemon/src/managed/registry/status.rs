use super::{Registry, Source, current_entry};
use crate::{
    devices::operations::ControlError,
    managed::{
        device_session::{controller_identity, target_identity},
        status::observations::Received,
    },
    monitoring::status::StatusKind,
};
use netaudio_core::managed_api::inventory::Observation;
use std::sync::Arc;

impl Registry {
    pub(in crate::managed) fn accept_controller_status(
        &self,
        source: &Arc<Source>,
        target: &Observation,
        received: Received,
    ) -> Result<(), ControlError> {
        let mut state = self.lock().map_err(unavailable)?;
        let contexts = state.snapshot.settings.contexts.clone();
        let entry =
            current_entry(&mut state, &target.server_profile, source).map_err(unavailable)?;
        let observations = entry
            .inventory
            .observations(&target.server_profile, &contexts);
        let current = observations
            .iter()
            .find(|current| {
                current.device.id == target.device.id
                    && current.domain_id == target.domain_id
                    && controller_identity(&current.device) == controller_identity(&target.device)
                    && current.fresh
                    && current
                        .device
                        .connection
                        .as_ref()
                        .and_then(|connection| connection.state.as_deref())
                        .is_some_and(|state| state.eq_ignore_ascii_case("READY"))
            })
            .ok_or_else(|| {
                ControlError::Conflict("Managed status owner changed during observation".into())
            })?;
        target_identity(current, &observations)?;
        entry.controller_status.accept(current, received)?;
        self.publish_locked(&mut state)?;
        self.changed.notify_one();
        Ok(())
    }

    pub(in crate::managed) fn invalidate_controller_device(
        &self,
        source: &Arc<Source>,
        target: &Observation,
        error: &str,
    ) -> Result<(), ControlError> {
        let mut state = self.lock().map_err(unavailable)?;
        let entry =
            current_entry(&mut state, &target.server_profile, source).map_err(unavailable)?;
        entry.controller_status.invalidate_device(target, error);
        self.publish_locked(&mut state)?;
        self.changed.notify_one();
        Ok(())
    }

    pub(in crate::managed) fn invalidate_controller_status(
        &self,
        source: &Arc<Source>,
        target: &Observation,
        kind: StatusKind,
        error: &str,
    ) -> Result<(), ControlError> {
        let mut state = self.lock().map_err(unavailable)?;
        let entry =
            current_entry(&mut state, &target.server_profile, source).map_err(unavailable)?;
        entry.controller_status.invalidate(target, kind, error);
        self.publish_locked(&mut state)?;
        self.changed.notify_one();
        Ok(())
    }
}

fn unavailable(error: super::super::Error) -> ControlError {
    ControlError::Unavailable(error.to_string())
}
