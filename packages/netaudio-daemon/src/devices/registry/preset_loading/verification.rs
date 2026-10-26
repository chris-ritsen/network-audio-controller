use super::{DeviceRegistry, PreparedDevice, Selected};
use crate::presets::report::Report;
use anyhow::Result;
use netaudio_core::presets::planning::{ActionKind, ActionState, RequestedAction};
use serde_json::{Map, json};
use tokio::time::Instant;

impl DeviceRegistry {
    pub(super) async fn verify_preset_result(
        &self,
        selected: &[Selected],
        prepared: &[PreparedDevice],
        indices: &[(usize, usize, usize)],
        report: &mut Report,
        deadline: Instant,
    ) {
        for (device, target) in selected.iter().enumerate() {
            let accepted = indices
                .iter()
                .filter(|(owner, _, result)| {
                    *owner == device
                        && !report.operations[*result].failed()
                        && !report.operations[*result].unverified()
                })
                .copied()
                .collect::<Vec<_>>();
            if accepted.is_empty() {
                continue;
            }
            let evidence = tokio::time::timeout_at(deadline, self.preset_inventory(target)).await;
            let evidence = match evidence {
                Ok(Ok(evidence)) => Ok(evidence),
                Ok(Err(error)) => Err(format!("Final inventory read failed: {error:#}")),
                Err(_) => Err("Preset deadline expired before final readback completed.".into()),
            };
            for (_, action, result) in accepted {
                let expected = &prepared[device].actions[action];
                let verification = async {
                    let evidence = evidence
                        .as_ref()
                        .map_err(|error| anyhow::anyhow!(error.clone()))?;
                    let request = RequestedAction {
                        kind: expected.presentation.kind,
                        requested: expected.presentation.requested.clone(),
                    };
                    let mut operations = expected.operations.clone();
                    if request.kind == ActionKind::ExternalReceiverSubscriptions {
                        operations.clear();
                    }
                    let action = self
                        .plan_preset_action(
                            target,
                            evidence,
                            &request,
                            &mut operations,
                            &Map::new(),
                        )
                        .await?;
                    if target.managed().is_none() {
                        evidence.record(target)?;
                    } else {
                        target.presentation()?;
                    }
                    Ok::<_, anyhow::Error>(action)
                };
                let verification: Result<_> =
                    match tokio::time::timeout_at(deadline, verification).await {
                        Ok(result) => result,
                        Err(_) => Err(anyhow::anyhow!(
                            "Preset deadline expired during final action readback."
                        )),
                    };
                let result = &mut report.operations[result];
                result.verification_observations.push(json!({
                    "phase": "before_final_readback", "state": result.state,
                    "effective": result.effective, "effective_state_confirmation": result.effective_state_confirmation,
                }));
                match verification {
                    Ok(action) => {
                        let network = matches!(
                            action.kind,
                            ActionKind::Interface | ActionKind::RedundancyMode
                        );
                        result
                            .verification_observations
                            .push(json!({"phase": "final_readback", "action": action}));
                        result.effective = action.current;
                        if action.state == ActionState::Unchanged {
                            result.effective_state_confirmation = if network {
                                action.details["effective_state_confirmed"].as_bool()
                            } else {
                                Some(true)
                            };
                            if network {
                                result.configuration_confirmation =
                                    action.details["configuration_confirmed"].as_bool();
                            }
                            if result.effective_state_confirmation == Some(true) {
                                result.message = "Fresh final readback confirms the requested state after the preset actions.".into();
                            } else {
                                result.state = "configuration_confirmed".into();
                                result.message = "Configured settings match, but final effective device state is not confirmed.".into();
                            }
                        } else {
                            result.state = if action.state == ActionState::Change {
                                "inconsistent"
                            } else {
                                "unverified"
                            }
                            .into();
                            result.effective_state_confirmation =
                                if action.state == ActionState::Change {
                                    Some(false)
                                } else {
                                    None
                                };
                            result.message = format!(
                                "Final readback did not confirm the requested state: {}",
                                action.reason
                            );
                        }
                    }
                    Err(error) => {
                        result.state = "unverified".into();
                        result.effective_state_confirmation = None;
                        result.message = format!("Final readback unavailable: {error:#}");
                        result.verification_observations.push(
                            json!({"phase": "final_readback", "error": format!("{error:#}")}),
                        );
                    }
                }
            }
        }
        report.recount();
    }
}
