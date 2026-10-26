use super::Provider;
use anyhow::Result;
use netaudio_core::host_audio::{
    control::{Command, Operation, observed, prepare},
    protocol::{Event, RequestAction},
    snapshot::Snapshot,
};
use netaudio_host_worker::{
    input::{Event as InputEvent, Input},
    output::Output,
};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

pub(super) fn execute(
    provider: &mut Provider,
    input: &mut Input,
    output: &mut Output,
    sequence: u64,
    operation_id: String,
    command: Command,
    precondition: Option<Value>,
) -> Result<bool> {
    let snapshot = provider.refresh(output)?;
    let host = std::env::var("NETAUDIO_HOST_NAME").unwrap_or_default();
    let plan = match prepare(&snapshot, &command, &host) {
        Ok(plan) => plan,
        Err(error) => {
            output.send(Event::Completed {
                request_sequence: sequence,
                status: 400,
                body: json!({"error":error,"change_sent":false}),
            })?;
            return Ok(true);
        }
    };
    if precondition
        .as_ref()
        .is_some_and(|expected| expected != &plan.precondition)
    {
        output.send(Event::Completed {
            request_sequence: sequence,
            status: 409,
            body: json!({"error":"audio state changed since preview","change_sent":false}),
        })?;
        return Ok(true);
    }
    let Operation::JackConnection {
        source,
        destination,
        connect,
    } = &plan.operation
    else {
        unreachable!();
    };
    let mut body = plan.description.clone();
    body["operation_id"] = json!(operation_id);
    if plan.unchanged {
        body["now"] = body["requested"].clone();
        body["result"] = json!("already so; nothing changed");
        body["change_sent"] = json!(false);
        body["state_verified"] = json!(true);
        output.send(Event::Completed {
            request_sequence: sequence,
            status: 200,
            body,
        })?;
        return Ok(true);
    }
    output.send(Event::Dispatching {
        request_sequence: sequence,
        operation_id,
        plan: plan.description.clone(),
    })?;
    let client = provider.client.as_client();
    let result = if *connect {
        client.connect_ports_by_name(source, destination)
    } else {
        client.disconnect_ports_by_name(source, destination)
    };
    if let Err(error) = result {
        body["error"] = json!(error.to_string());
        body["change_sent"] = Value::Null;
        body["state_verified"] = Value::Null;
        output.send(Event::Completed {
            request_sequence: sequence,
            status: 502,
            body,
        })?;
        return Ok(true);
    }
    body["change_sent"] = json!(true);
    body["accepted"] = json!(true);
    let deadline = Instant::now() + Duration::from_millis(1500);
    let mut latest = provider.refresh(output)?;
    loop {
        match input.receive_source(Some(deadline))? {
            InputEvent::Changed => latest = provider.refresh(output)?,
            InputEvent::Deadline => break,
            InputEvent::Closed => return Ok(false),
            InputEvent::Request(request) => {
                if matches!(request.action, RequestAction::Shutdown) {
                    return Ok(false);
                }
                input.defer(request)?;
            }
        }
    }
    let identity_matches = match (&snapshot, &latest) {
        (Snapshot::Jack(before), Snapshot::Jack(after)) => {
            [source, destination].into_iter().all(|name| {
                before.ports.get(name).map(|port| port.identity)
                    == after.ports.get(name).map(|port| port.identity)
            })
        }
        _ => false,
    };
    let outcome = identity_matches
        .then(|| observed(&latest, &plan.operation))
        .flatten();
    let verified = outcome.as_ref().is_some_and(|(verified, _)| *verified);
    body["now"] = outcome
        .map(|(_, value)| json!(value))
        .unwrap_or(Value::Null);
    body["state_verified"] = json!(verified);
    body["result"] = json!(if verified {
        "applied"
    } else {
        "JACK accepted the change; the requested state is not confirmed"
    });
    if let Snapshot::Jack(snapshot) = latest {
        body["source_feeds"] = json!(snapshot.ports.get(source).map(|port| &port.connections));
        body["destination_hears"] = json!(
            snapshot
                .ports
                .get(destination)
                .map(|port| &port.connections)
        );
    }
    output.send(Event::Completed {
        request_sequence: sequence,
        status: if verified { 200 } else { 409 },
        body,
    })?;
    Ok(true)
}
