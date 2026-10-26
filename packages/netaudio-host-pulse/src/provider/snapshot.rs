use super::{Provider, projection, wait};
use anyhow::{Context, Result, bail, ensure};
use libpulse_binding::callbacks::ListResult;
use netaudio_core::host_audio::{
    protocol::MAXIMUM_FRAME_BYTES,
    snapshot::{PulseEntry, PulseServer, PulseSnapshot},
};
use netaudio_host_worker::input::{Input, Wake};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};

#[derive(Default)]
struct Collection {
    server: Option<PulseServer>,
    entries: [Vec<PulseEntry>; 4],
    complete: [bool; 4],
    error: Option<String>,
    bytes: usize,
}

impl Collection {
    fn add(&mut self, bucket: usize, result: ListResult<PulseEntry>) {
        match result {
            ListResult::Item(entry) if self.error.is_none() => match serde_json::to_vec(&entry) {
                Ok(bytes) => {
                    self.bytes = self.bytes.saturating_add(bytes.len());
                    if self.bytes > MAXIMUM_FRAME_BYTES / 2 {
                        self.error =
                            Some("PulseAudio inventory exceeds the worker frame budget".into());
                    } else {
                        self.entries[bucket].push(entry);
                    }
                }
                Err(error) => self.error = Some(error.to_string()),
            },
            ListResult::End => self.complete[bucket] = true,
            ListResult::Error => self.error = Some("PulseAudio inventory read failed".into()),
            _ => {}
        }
    }
}

fn collect<T>(
    collection: &Arc<Mutex<Collection>>,
    wake: &Wake,
    bucket: usize,
    result: ListResult<&T>,
    project: impl FnOnce(&T) -> PulseEntry,
) {
    let result = match result {
        ListResult::Item(info) => ListResult::Item(project(info)),
        ListResult::End => ListResult::End,
        ListResult::Error => ListResult::Error,
    };
    collection
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .add(bucket, result);
    wake.notify();
}

pub(super) fn read(provider: &mut Provider, input: &mut Input) -> Result<(u64, PulseSnapshot)> {
    for _ in 0..3 {
        let revision = provider.source_revision.load(Ordering::Acquire);
        let collection = gather(provider, input)?;
        if revision != provider.source_revision.load(Ordering::Acquire) {
            continue;
        }
        let [mut sinks, mut sources, mut sink_inputs, mut source_outputs] = collection.entries;
        for entries in [
            &mut sinks,
            &mut sources,
            &mut sink_inputs,
            &mut source_outputs,
        ] {
            entries.sort_by_key(|entry| entry.index);
            ensure!(
                !entries
                    .windows(2)
                    .any(|entries| entries[0].index == entries[1].index),
                "PulseAudio returned duplicate object identities"
            );
        }
        let sink_names: BTreeMap<_, _> = sinks
            .iter()
            .filter_map(|entry| entry.name.as_ref().map(|name| (entry.index, name.clone())))
            .collect();
        let source_names: BTreeMap<_, _> = sources
            .iter()
            .filter_map(|entry| entry.name.as_ref().map(|name| (entry.index, name.clone())))
            .collect();
        for stream in &mut sink_inputs {
            stream.sink = stream
                .destination_index
                .and_then(|index| sink_names.get(&index).cloned());
        }
        for stream in &mut source_outputs {
            stream.source = stream
                .destination_index
                .and_then(|index| source_names.get(&index).cloned());
        }
        return Ok((
            revision,
            PulseSnapshot {
                available: true,
                reason: None,
                seen_running: true,
                connected_at: Some(provider.connected_at.clone()),
                server: collection
                    .server
                    .context("PulseAudio server information was not returned")?,
                sinks,
                sources,
                sink_inputs,
                source_outputs,
                changes: Vec::new(),
            },
        ));
    }
    bail!("PulseAudio state changed during each bounded snapshot attempt")
}

fn gather(provider: &mut Provider, input: &mut Input) -> Result<Collection> {
    let collection = Arc::new(Mutex::new(Collection::default()));
    provider.mainloop.lock();
    let introspector = provider.context.introspect();
    let result = collection.clone();
    let wake = provider.wake.clone();
    let mut server = introspector.get_server_info(move |info| {
        result
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .server = Some(projection::server(info));
        wake.notify();
    });
    let result = collection.clone();
    let wake = provider.wake.clone();
    let mut sinks = introspector
        .get_sink_info_list(move |info| collect(&result, &wake, 0, info, projection::sink));
    let result = collection.clone();
    let wake = provider.wake.clone();
    let mut sources = introspector
        .get_source_info_list(move |info| collect(&result, &wake, 1, info, projection::source));
    let result = collection.clone();
    let wake = provider.wake.clone();
    let mut sink_inputs = introspector.get_sink_input_info_list(move |info| {
        collect(&result, &wake, 2, info, projection::sink_input)
    });
    let result = collection.clone();
    let wake = provider.wake.clone();
    let mut source_outputs = introspector.get_source_output_info_list(move |info| {
        collect(&result, &wake, 3, info, projection::source_output)
    });
    drop(introspector);
    provider.mainloop.unlock();
    let deadline = Instant::now() + Duration::from_secs(3);
    let completion = (|| {
        loop {
            {
                let collected = collection.lock().unwrap_or_else(|error| error.into_inner());
                if let Some(error) = &collected.error {
                    bail!("{error}");
                }
                if collected.server.is_some() && collected.complete.iter().all(|done| *done) {
                    return Ok(());
                }
            }
            provider.check()?;
            wait(input, deadline)?;
        }
    })();
    provider.mainloop.lock();
    if completion.is_err() {
        server.cancel();
        sinks.cancel();
        sources.cancel();
        sink_inputs.cancel();
        source_outputs.cancel();
    }
    drop((server, sinks, sources, sink_inputs, source_outputs));
    provider.mainloop.unlock();
    completion?;
    Ok(std::mem::take(
        &mut *collection.lock().unwrap_or_else(|error| error.into_inner()),
    ))
}
