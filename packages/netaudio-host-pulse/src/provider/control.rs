use super::Provider;
use anyhow::{Result, ensure};
use libpulse_binding::{
    operation::Operation as PulseOperation,
    volume::{ChannelVolumes, Volume},
};
use netaudio_core::host_audio::{
    control::{Command, Operation, observed, prepare},
    protocol::Event,
    snapshot::PulseKind,
};
use netaudio_host_worker::{input::Input, output::Output};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

pub(super) fn execute(
    provider: &mut Provider,
    input: &mut Input,
    output: &mut Output,
    sequence: u64,
    operation_id: String,
    command: Command,
    precondition: Option<Value>,
) -> Result<()> {
    let snapshot = provider.refresh(input, output)?;
    let host = std::env::var("NETAUDIO_HOST_NAME").unwrap_or_default();
    let plan = match prepare(&snapshot, &command, &host) {
        Ok(plan) => plan,
        Err(error) => {
            return output.send(Event::Completed {
                request_sequence: sequence,
                status: 400,
                body: json!({"error":error,"change_sent":false}),
            });
        }
    };
    if precondition
        .as_ref()
        .is_some_and(|expected| expected != &plan.precondition)
    {
        return output.send(Event::Completed {
            request_sequence: sequence,
            status: 409,
            body: json!({"error":"audio state changed since preview","change_sent":false}),
        });
    }
    let mut body = plan.description.clone();
    body["operation_id"] = json!(operation_id);
    if plan.unchanged {
        body["now"] = body["requested"].clone();
        body["result"] = json!("already so; nothing changed");
        body["change_sent"] = json!(false);
        body["state_verified"] = json!(true);
        return output.send(Event::Completed {
            request_sequence: sequence,
            status: 200,
            body,
        });
    }
    let volumes = match &plan.operation {
        Operation::PulseVolume { volumes, .. } => {
            ensure!(
                volumes.len() <= usize::from(ChannelVolumes::CHANNELS_MAX),
                "invalid PulseAudio channel count"
            );
            let mut channels = ChannelVolumes::default();
            channels.set_len(volumes.len() as u8);
            for (channel, volume) in channels.get_mut().iter_mut().zip(volumes) {
                *channel = Volume(*volume);
            }
            Some(channels)
        }
        _ => None,
    };
    output.send(Event::Dispatching {
        request_sequence: sequence,
        operation_id,
        plan: plan.description.clone(),
    })?;
    let result = Arc::new(Mutex::new(None));
    let callback_result = result.clone();
    let callback_wake = provider.wake.clone();
    let callback = Box::new(move |success| {
        *callback_result
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(success);
        callback_wake.notify();
    });
    provider.mainloop.lock();
    let mut introspector = provider.context.introspect();
    let operation: PulseOperation<dyn FnMut(bool)> = match &plan.operation {
        Operation::PulseVolume { kind, index, .. } => {
            let volumes = volumes.as_ref().expect("validated volume command");
            match kind {
                PulseKind::Sink => {
                    introspector.set_sink_volume_by_index(*index, volumes, Some(callback))
                }
                PulseKind::Source => {
                    introspector.set_source_volume_by_index(*index, volumes, Some(callback))
                }
                PulseKind::SinkInput => {
                    introspector.set_sink_input_volume(*index, volumes, Some(callback))
                }
                PulseKind::SourceOutput => {
                    introspector.set_source_output_volume(*index, volumes, Some(callback))
                }
            }
        }
        Operation::PulseMute { kind, index, mute } => match kind {
            PulseKind::Sink => introspector.set_sink_mute_by_index(*index, *mute, Some(callback)),
            PulseKind::Source => {
                introspector.set_source_mute_by_index(*index, *mute, Some(callback))
            }
            PulseKind::SinkInput => introspector.set_sink_input_mute(*index, *mute, Some(callback)),
            PulseKind::SourceOutput => {
                introspector.set_source_output_mute(*index, *mute, Some(callback))
            }
        },
        Operation::PulseDefault {
            kind: PulseKind::Sink,
            name,
            ..
        } => provider.context.set_default_sink(name, callback),
        Operation::PulseDefault {
            kind: PulseKind::Source,
            name,
            ..
        } => provider.context.set_default_source(name, callback),
        Operation::PulseMove {
            kind: PulseKind::SinkInput,
            index,
            destination,
            ..
        } => introspector.move_sink_input_by_index(*index, *destination, Some(callback)),
        Operation::PulseMove {
            kind: PulseKind::SourceOutput,
            index,
            destination,
            ..
        } => introspector.move_source_output_by_index(*index, *destination, Some(callback)),
        _ => unreachable!(),
    };
    drop(introspector);
    provider.mainloop.unlock();
    body["change_sent"] = json!(true);
    match provider.complete(operation, result, input) {
        Ok(true) => {}
        Ok(false) => {
            body["error"] = json!("PulseAudio rejected the operation");
            body["accepted"] = json!(false);
            body["state_verified"] = json!(false);
            return output.send(Event::Completed {
                request_sequence: sequence,
                status: 502,
                body,
            });
        }
        Err(error) => {
            body["error"] = json!(format!("{error:#}"));
            body["state_verified"] = Value::Null;
            output.send(Event::Completed {
                request_sequence: sequence,
                status: 502,
                body,
            })?;
            return Err(error);
        }
    }
    body["accepted"] = json!(true);
    let latest = match provider.refresh(input, output) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            body["error"] = json!(format!("readback failed: {error:#}"));
            body["state_verified"] = Value::Null;
            output.send(Event::Completed {
                request_sequence: sequence,
                status: 502,
                body,
            })?;
            return Err(error);
        }
    };
    let outcome = observed(&latest, &plan.operation);
    let verified = outcome.as_ref().is_some_and(|(verified, _)| *verified);
    body["now"] = outcome
        .map(|(_, value)| json!(value))
        .unwrap_or(Value::Null);
    body["state_verified"] = json!(verified);
    body["result"] = json!(if verified { "applied" } else { "not confirmed" });
    output.send(Event::Completed {
        request_sequence: sequence,
        status: if verified { 200 } else { 409 },
        body,
    })
}
