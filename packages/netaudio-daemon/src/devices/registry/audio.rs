use super::{Device, DeviceRegistry, DeviceTransport};
use crate::devices::audio_configuration::{AudioConfiguration, AudioKind};
use anyhow::{Result, bail};
use serde_json::{Map, Value};
use std::time::Duration;

const INITIAL_OBSERVATION_DEADLINE: Duration = Duration::from_secs(3);

impl DeviceRegistry {
    pub(super) async fn read_audio(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        kind: AudioKind,
        expected: Option<&AudioConfiguration>,
    ) -> Result<Map<String, Value>> {
        let key = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || !state.record.online {
                bail!("device identity changed before audio configuration observation");
            }

            state.record.key.clone()
        };
        let duration = expected.map_or(INITIAL_OBSERVATION_DEADLINE, |_| kind.deadline());
        let observed = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
            observed = self.status_requests.query_with_deadline(
                transport,
                &key,
                kind.query(),
                duration,
                |fields| expected.is_none_or(|expected| expected.matches(fields)),
            ) => observed?,
        };
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation {
            bail!("device identity changed during audio configuration observation");
        }

        Ok(observed)
    }
}
