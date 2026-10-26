use super::observation::InventoryEvidence;
use super::{DeviceRegistry, PreparedAction, PreparedDevice, Selected};
use crate::devices::external_subscription::ExternalSubscription;
use crate::devices::operations::ControlOperation;
use crate::devices::panels;
use crate::monitoring::requests::StatusQuery;
use crate::presets::{diff, operations};
use anyhow::{Context, Result, ensure};
use netaudio_core::presets::planning::{Action, ActionKind, ActionState, RequestedAction};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

fn reject_conflicting_performance(
    actions: &mut [PreparedAction],
    record: &crate::devices::record::DeviceRecord,
) {
    let mut properties = BTreeMap::<u16, Vec<(usize, u32)>>::new();
    for (index, action) in actions.iter().enumerate() {
        if !matches!(
            action.presentation.state,
            ActionState::Change | ActionState::Unchanged
        ) {
            continue;
        }
        for operation in &action.operations {
            if let ControlOperation::Performance(operation) = operation
                && let Ok(plan) = operation.plan(record)
            {
                for (property, value) in plan.requested {
                    if property != netaudio_core::commands::PROPERTY_PRE_3_COMPATIBILITY {
                        properties.entry(property).or_default().push((index, value));
                    }
                }
            }
        }
    }
    let mut conflicts = BTreeMap::<usize, Vec<Value>>::new();
    for (property, requests) in properties {
        if requests.windows(2).any(|pair| pair[0].1 != pair[1].1) {
            for (index, _) in &requests {
                conflicts.entry(*index).or_default().push(json!({
                    "property": property,
                    "requests": requests.iter().map(|(owner, value)| json!({"kind": actions[*owner].presentation.kind, "value": value})).collect::<Vec<_>>(),
                }));
            }
        }
    }
    for (index, conflicts) in conflicts {
        let action = &mut actions[index].presentation;
        action.state = ActionState::Ambiguous;
        action.reason = "Requested performance settings require contradictory values for shared device properties; these actions will not be sent.".into();
        action.details = json!({"conflicting_properties": conflicts});
    }
}

impl DeviceRegistry {
    pub(super) async fn plan_preset_device(
        &self,
        target: &Selected,
        confirm_destructive: bool,
    ) -> Result<PreparedDevice> {
        let evidence = self.preset_inventory(target).await?;
        let mut actions = Vec::new();
        let mut projected_audio = Map::new();
        for request in &target.target.actions {
            let mut operations = operations::operations(request, confirm_destructive)?;
            let presentation = match Box::pin(self.plan_preset_action(
                target,
                &evidence,
                request,
                &mut operations,
                &projected_audio,
            ))
            .await
            {
                Ok(action) => action,
                Err(error) => {
                    request.result(ActionState::Unavailable, Value::Null, format!("{error:#}"))
                }
            };
            if presentation.state == ActionState::Change {
                match request.kind {
                    ActionKind::SampleRate => {
                        projected_audio.insert("sample_rate_hz".into(), request.requested.clone());
                    }
                    ActionKind::Encoding => {
                        projected_audio.insert("encoding".into(), request.requested.clone());
                    }
                    _ => {}
                }
            }
            actions.push(PreparedAction {
                presentation,
                operations,
            });
        }
        if target.managed().is_none() {
            reject_conflicting_performance(&mut actions, &evidence.record(target)?);
        }
        ensure!(
            target.managed().is_some() || target.checkpoint()? == evidence.generation,
            "device inventory changed during fresh preset planning"
        );
        Ok(PreparedDevice {
            generation: target.checkpoint()?,
            actions,
        })
    }

    pub(super) async fn replan_preset_action(
        &self,
        target: &Selected,
        action: &PreparedAction,
    ) -> Result<PreparedAction> {
        let evidence = self.preset_inventory(target).await?;
        let request = RequestedAction {
            kind: action.presentation.kind,
            requested: action.presentation.requested.clone(),
        };
        let mut operations = action.operations.clone();
        if request.kind == ActionKind::ExternalReceiverSubscriptions {
            operations.clear();
        }
        let presentation = self
            .plan_preset_action(target, &evidence, &request, &mut operations, &Map::new())
            .await?;
        ensure!(
            target.managed().is_some() || target.checkpoint()? == evidence.generation,
            "inventory changed while replanning a dependent preset action"
        );
        Ok(PreparedAction {
            presentation,
            operations,
        })
    }

    pub(super) async fn plan_preset_action(
        &self,
        target: &Selected,
        evidence: &InventoryEvidence,
        request: &RequestedAction,
        operations: &mut Vec<ControlOperation>,
        projected_audio: &Map<String, Value>,
    ) -> Result<Action> {
        if let Some(record) = &evidence.managed {
            return self
                .plan_managed_preset_action(target, record, request, operations)
                .await;
        }
        if request.kind == ActionKind::Preserved {
            return Ok(request.result(
                ActionState::Unsupported,
                Value::Null,
                "This preset field is preserved but has no established writer.",
            ));
        }
        if request.kind == ActionKind::Interface && operations.is_empty() {
            return Ok(request.result(ActionState::Unsupported, Value::Null, "Only explicitly identified primary and secondary Dante interfaces have established writers."));
        }
        if let Some(ControlOperation::SampleRate(operation)) = operations.first() {
            let mut preview = operation.clone();
            preview.preview = true;
            let completion = self
                .execute_recorded_operation(
                    target.direct()?,
                    &ControlOperation::SampleRate(preview),
                    None,
                    None,
                )
                .await?;
            let observed = completion
                .observation
                .context("sample-rate preview was unavailable")?;
            let preflight = &observed["preflight"];
            let current = preflight["current_sample_rate_hertz"].clone();
            let mut action = request.compare(current.clone(), current == operation.target);
            if action.state == ActionState::Change
                && let Some((state, reason)) = operation.refusal(preflight)
            {
                action.state = if state == "unsupported" {
                    ActionState::Unsupported
                } else {
                    ActionState::Unavailable
                };
                action.reason = reason.into();
            }
            action.details = observed;
            return Ok(action);
        }
        if let Some(ControlOperation::Panel(panels::Operation::Apply(change))) = operations.first()
        {
            let completion = self
                .execute_recorded_operation(
                    target.direct()?,
                    &ControlOperation::Panel(panels::Operation::Plan(change.clone())),
                    None,
                    None,
                )
                .await?;
            let observed = completion
                .observation
                .context("panel plan was unavailable")?;
            let plan = &observed["plan"];
            let state = serde_json::from_value(plan["action"].clone())?;
            let mut action = request.result(
                state,
                plan["before"].clone(),
                plan["reason"]
                    .as_str()
                    .unwrap_or("Fresh device-control plan."),
            );
            action.details = observed;
            return Ok(action);
        }
        match request.kind {
            ActionKind::TransmitterChannelNames | ActionKind::TransmitFlow => {
                evidence.channels(false)?
            }
            ActionKind::ReceiverChannelNames
            | ActionKind::ReceiverSubscriptions
            | ActionKind::ExternalReceiverSubscriptions => evidence.channels(true)?,
            ActionKind::Latency => evidence.require("settings")?,
            ActionKind::ReceiveFlowPerformance
            | ActionKind::TransmitFlowPerformance
            | ActionKind::UnicastPerformance
            | ActionKind::ReceiveFlowDefaultSlots => {
                evidence.require("settings")?;
                evidence.require("properties")?;
            }
            _ => {}
        }
        if matches!(
            request.kind,
            ActionKind::ReceiverSubscriptions | ActionKind::ExternalReceiverSubscriptions
        ) {
            evidence.require("receiver_flows")?;
        }
        let queries: &[StatusQuery] = match request.kind {
            ActionKind::Encoding => &[StatusQuery::Encoding],
            ActionKind::SampleRatePullup => &[StatusQuery::Pullup],
            ActionKind::ClockConfiguration => &[StatusQuery::Clock],
            ActionKind::CodecGain => &[StatusQuery::Codec],
            ActionKind::RedundancyMode | ActionKind::Interface => &[StatusQuery::Interface],
            ActionKind::ReceiveFlowPerformance
            | ActionKind::TransmitFlowPerformance
            | ActionKind::UnicastPerformance
            | ActionKind::ReceiveFlowDefaultSlots => &[StatusQuery::Platform],
            ActionKind::ExternalReceiverSubscriptions => &[
                StatusQuery::SampleRate,
                StatusQuery::Encoding,
                StatusQuery::Aes67,
            ],
            _ => &[],
        };
        let mut fresh = Map::new();
        for query in queries {
            fresh.extend(self.preset_query(target, *query).await?);
        }
        if request.kind == ActionKind::RedundancyMode {
            let mut record = evidence.record(target)?.presentation()?;
            record
                .as_object_mut()
                .context("device presentation is not an object")?
                .extend(fresh.clone());
            if crate::devices::capabilities::operation(&record, "redundancy")?.supported
                != Some(false)
            {
                fresh.extend(
                    self.preset_query(target, StatusQuery::SwitchConfiguration)
                        .await?,
                );
            }
        }
        let mut record = evidence.record(target)?;
        record.properties.extend(fresh);
        if request.kind == ActionKind::TransmitFlow {
            return self
                .plan_preset_flow(target, request, operations, projected_audio)
                .await;
        }
        if request.kind == ActionKind::ExternalReceiverSubscriptions {
            let current = record.presentation()?;
            record.properties.extend(projected_audio.clone());
            let mut action = self.plan_preset_external(request, &record, operations)?;
            if !projected_audio.is_empty() {
                action.details["observed_audio"] = json!({"sample_rate_hz": current["sample_rate_hz"], "encoding": current["encoding"]});
                action.details["prerequisite_audio_changes"] = json!(projected_audio);
            }
            return Ok(action);
        }
        diff::compare(request, operations, &record)
    }

    async fn plan_preset_flow(
        &self,
        target: &Selected,
        request: &RequestedAction,
        operations: &[ControlOperation],
        projected_audio: &Map<String, Value>,
    ) -> Result<Action> {
        let completion = self
            .execute_recorded_operation(
                target.direct()?,
                &ControlOperation::InspectTransmitFlows,
                None,
                None,
            )
            .await?;
        let inventory = completion
            .observation
            .context("fresh transmit flow inventory was unavailable")?;
        ensure!(
            inventory["unparsed_records"]
                .as_array()
                .is_some_and(Vec::is_empty),
            "fresh transmit flow inventory contains unparsed records"
        );
        let flows = inventory["flows"]
            .as_array()
            .context("fresh transmit flow inventory has no flows")?;
        let identity = &request.requested["identity"]["global_flow_id"];
        let comparison = |current: &Value| -> Result<Value> {
            Ok(serde_json::to_value(
                netaudio_core::flow_readback::compare_transmit_flows(&serde_json::from_value(
                    json!({"requested": request.requested, "effective": current}),
                )?),
            )?)
        };
        if !identity.is_null()
            && let Some(current) = flows
                .iter()
                .find(|flow| flow["identity"]["global_flow_id"] == *identity)
        {
            let comparison = comparison(current)?;
            let mut action = if comparison["matches"] == true {
                request.compare(current.clone(), true)
            } else {
                request.result(ActionState::Ambiguous, current.clone(), "The identified flow differs or lacks required durable fields; destructive replacement is refused.")
            };
            action.details = comparison;
            return Ok(action);
        }
        if identity.is_null() {
            let matches = flows
                .iter()
                .map(|flow| Ok((flow, comparison(flow)?)))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .filter(|(_, comparison)| comparison["matches"] == true)
                .map(|(flow, _)| flow.clone())
                .collect::<Vec<_>>();
            if matches.len() == 1 {
                return Ok(request.compare(matches[0].clone(), true));
            }
            if matches.len() > 1 {
                return Ok(request.result(
                    ActionState::Ambiguous,
                    json!(matches),
                    "Multiple existing flows match the identity-free request.",
                ));
            }
        }
        let Some(ControlOperation::FlowAuthoring(operation)) = operations.first() else {
            anyhow::bail!("transmit flow operation was not prepared");
        };
        for query in [StatusQuery::SampleRate, StatusQuery::Encoding] {
            self.preset_query(target, query).await?;
        }
        if request.requested["media_mode"] == "rtp_aes67" {
            self.preset_query(target, StatusQuery::Aes67).await?;
        }
        let (mut record, _) = target.record()?;
        let current = record.presentation()?;
        record.properties.extend(projected_audio.clone());
        let plan = operation.plan(&record)?;
        let mut observed = json!({"plan": plan.presentation, "mutation_sent": false});
        if !projected_audio.is_empty() {
            observed["observed_audio"] = json!({"sample_rate_hz": current["sample_rate_hz"], "encoding": current["encoding"]});
            observed["prerequisite_audio_changes"] = json!(projected_audio);
        }
        let reasons = observed["plan"]["reasons"]
            .as_array()
            .context("flow plan has no reasons")?;
        let mut action = if reasons.is_empty() {
            request.compare(Value::Null, false)
        } else {
            request.result(
                ActionState::Unsupported,
                Value::Null,
                reasons
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        };
        action.details = observed;
        Ok(action)
    }

    fn plan_preset_external(
        &self,
        request: &RequestedAction,
        record: &crate::devices::record::DeviceRecord,
        operations: &mut Vec<ControlOperation>,
    ) -> Result<Action> {
        let subscriptions = request
            .requested
            .as_object()
            .context("external subscriptions must be an object")?;
        let identities: Vec<_> = subscriptions.iter().map(|(number, subscription)| -> Result<_> {
            Ok(json!({"receiver_channel": number.parse::<u16>()?, "flow_slot": subscription["flow_slot"], "source_ipv4": subscription["flow_identity"]["source_ipv4"], "session_id": subscription["flow_identity"]["session_id"], "interface_endpoints": subscription["interface_endpoints"]}))
        }).collect::<Result<_>>()?;
        let readback = serde_json::to_value(netaudio_core::spec::external_subscription_readback(&json!({"kind": "identities", "identities": identities, "inventory": record.properties.get("receiver_flow_status_page")}).to_string()).map_err(|error| anyhow::anyhow!("{error}"))?)?;
        let matches = readback["arc_effective_state_confirmed"]
            .as_bool()
            .context("complete fresh receiver-flow inventory is unavailable")?;
        let mut action =
            request.compare(readback["observed_effective_identities"].clone(), matches);
        action.details = readback;
        if matches {
            return Ok(action);
        }
        let mut groups: BTreeMap<(String, String), Vec<(u16, u16)>> = BTreeMap::new();
        for (number, subscription) in subscriptions {
            let identity = &subscription["flow_identity"];
            let source = identity["source_ipv4"]
                .as_str()
                .context("external source address is unavailable")?;
            let session = identity["session_id"]
                .as_u64()
                .context("external session identity is unavailable")?;
            let slot = subscription["flow_slot"]
                .as_u64()
                .and_then(|slot| u16::try_from(slot).ok())
                .context("external flow slot is invalid")?;
            groups
                .entry((source.into(), session.to_string()))
                .or_default()
                .push((number.parse()?, slot));
        }
        for ((source, session), mut mappings) in groups {
            mappings.sort_unstable();
            let operation = ExternalSubscription::prepare(
                &json!({"source_ipv4": source, "session_id": session, "receiver_channel_ids": mappings.iter().map(|(number, _)| number).collect::<Vec<_>>(), "flow_slot_assignments": mappings.iter().map(|(_, slot)| slot).collect::<Vec<_>>()}),
                &self.model,
            )?;
            let command = operation.command(record)?;
            let intent = serde_json::to_value(
                netaudio_core::spec::external_subscription_readback(
                    &json!({"kind": "command", "specification": command, "inventory": null})
                        .to_string(),
                )
                .map_err(|error| anyhow::anyhow!("{error}"))?,
            )?;
            let mut expected: Vec<_> = action.details["requested_effective_identities"]
                .as_array()
                .context("normalized external identities are unavailable")?
                .iter()
                .filter(|identity| {
                    mappings
                        .iter()
                        .any(|(number, _)| identity["receiver_channel"] == *number)
                })
                .cloned()
                .collect();
            let mut intended = intent["requested_effective_identities"]
                .as_array()
                .context("external command identities are unavailable")?
                .clone();
            expected.sort_by_key(|identity| identity["receiver_channel"].as_u64());
            intended.sort_by_key(|identity| identity["receiver_channel"].as_u64());
            ensure!(
                intended == expected,
                "current SAP announcement does not match the preset's exact endpoints and slot assignments"
            );
            operations.push(ControlOperation::ExternalSubscription(operation));
        }
        Ok(action)
    }
}
