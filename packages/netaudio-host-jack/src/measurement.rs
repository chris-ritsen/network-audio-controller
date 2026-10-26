mod capture;

use crate::capture::{Channel, Connection, Connections, Notifications, State, identity, verify};
use anyhow::{Context, Result, bail, ensure};
use capture::Processing;
use netaudio_core::host_audio::{
    measurement::{Reading, Readings, Request, decibels},
    protocol::{Event, RequestAction},
};
use netaudio_host_worker::{
    input::{Event as InputEvent, Input},
    output::Output,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub fn run(output: &mut Output) -> Result<()> {
    let mut input = Input::new()?;
    loop {
        match input.receive(None)? {
            InputEvent::Closed => return Ok(()),
            InputEvent::Changed => {}
            InputEvent::Deadline => unreachable!(),
            InputEvent::Request(request) => match request.action {
                RequestAction::Shutdown => return Ok(()),
                RequestAction::Measure {
                    request: mut measurement,
                } => {
                    measurement.validate().map_err(anyhow::Error::msg)?;
                    let Some(readings) = measure(measurement, &mut input)? else {
                        return Ok(());
                    };
                    output.send(Event::Measurement {
                        request_sequence: request.sequence,
                        readings,
                    })?;
                }
                _ => bail!("The JACK measurement worker received an unsupported request"),
            },
        }
    }
}

fn measure(request: Request, input: &mut Input) -> Result<Option<Readings>> {
    let mut readings = Readings::new();
    if request.targets.is_empty() {
        return Ok(Some(readings));
    }
    let (client, _) = jack::Client::new("netaudio-meter", jack::ClientOptions::NO_START_SERVER)
        .context("Connect the measurement worker to the existing JACK server")?;
    let sample_rate = client.sample_rate();
    ensure!(sample_rate > 0, "JACK returned an invalid sample rate");
    let mut channels = Vec::new();
    let mut inputs = Vec::new();
    for (target, sources) in request.targets {
        if sources.is_empty() {
            readings.insert(target, Reading::Disconnected { connected: false });
            continue;
        }
        let port = client.register_port(
            &format!("meter_{}", channels.len() + 1),
            jack::AudioIn::default(),
        )?;
        let prepare = (|| -> Result<Vec<(String, u64)>> {
            let mut identities = Vec::new();
            for name in sources {
                let source = client
                    .port_by_name(&name)
                    .with_context(|| format!("JACK source {name} does not exist"))?;
                ensure!(
                    source.flags().contains(jack::PortFlags::IS_OUTPUT)
                        && source.port_type()? == "32 bit float mono audio",
                    "JACK source {name} is not an audio output"
                );
                let (identifier, _) = identity(&client, &source)?;
                identities.push((source.name()?, identifier));
            }
            identities.sort();
            identities.dedup();
            Ok(identities)
        })();
        match prepare {
            Ok(sources) => {
                inputs.push(identity(&client, &port)?.1);
                channels.push(Channel {
                    target,
                    sources,
                    input: port,
                    peak: 0.0,
                    energy: 0.0,
                    invalid: false,
                });
            }
            Err(error) => {
                client.unregister_port(port)?;
                readings.insert(
                    target,
                    Reading::Unavailable {
                        error: format!("{error:#}"),
                    },
                );
            }
        }
    }
    if channels.is_empty() {
        return Ok(Some(readings));
    }
    let connections: Connections = channels
        .iter()
        .map(|channel| Ok((channel.input.name()?, channel.sources.clone())))
        .collect::<Result<_>>()?;
    let notifications = channels
        .iter()
        .flat_map(|channel| {
            let destination = unsafe {
                jack_sys::jack_uuid_to_index(jack_sys::jack_port_uuid(channel.input.raw()))
            };
            channel.sources.iter().map(move |(_, identity)| Connection {
                source: unsafe { jack_sys::jack_uuid_to_index(*identity) },
                destination,
                connected: AtomicBool::new(false),
            })
        })
        .collect();
    let clock_before = client.time();
    let wall_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
    let clock_after = client.time();
    let reference = clock_before + (clock_after - clock_before) / 2;
    let state = Arc::new(State {
        wake: input.wake(),
        enabled: AtomicBool::new(false),
        started: AtomicBool::new(false),
        finished: AtomicBool::new(false),
        stopped: AtomicBool::new(false),
        failure: AtomicU8::new(0),
        inputs,
        sources: channels
            .iter()
            .flat_map(|channel| {
                channel
                    .sources
                    .iter()
                    .map(|(_, identity)| unsafe { jack_sys::jack_uuid_to_index(*identity) })
            })
            .collect(),
        connections: notifications,
        sample_rate,
    });
    let wanted = (request.seconds * f64::from(sample_rate)).ceil() as u64;
    let processing = Processing {
        state: state.clone(),
        channels,
        wanted,
        frames: 0,
        first_microseconds: None,
        next_frame: None,
    };
    let active = client
        .activate_async(Notifications(state.clone()), processing)
        .context("Activate the JACK measurement inputs")?;
    let deadline = Instant::now() + Duration::from_secs_f64(request.seconds + 3.0);
    let result = (|| -> Result<bool> {
        for (destination, sources) in &connections {
            for (source, _) in sources {
                active
                    .as_client()
                    .connect_ports_by_name(source, destination)?;
            }
        }
        while !state.connected() {
            ensure!(
                !state.stopped.load(Ordering::Acquire),
                "The JACK server stopped during measurement setup"
            );
            if let Some(error) = state.error() {
                bail!("{error}");
            }
            if !wait(input, deadline)? {
                return Ok(false);
            }
        }
        verify(active.as_client(), &connections)?;
        if let Some(error) = state.error() {
            bail!("{error}");
        }
        state.enabled.store(true, Ordering::Release);
        while !state.finished.load(Ordering::Acquire) {
            ensure!(
                !state.stopped.load(Ordering::Acquire),
                "The JACK server stopped during measurement"
            );
            if let Some(error) = state.error() {
                bail!("{error}");
            }
            if !wait(input, deadline)? {
                return Ok(false);
            }
        }
        ensure!(
            !state.stopped.load(Ordering::Acquire),
            "The JACK server stopped during measurement"
        );
        if let Some(error) = state.error() {
            bail!("{error}");
        }
        verify(active.as_client(), &connections)?;
        ensure!(
            active.as_client().sample_rate() == sample_rate,
            "The JACK sample rate changed during measurement"
        );
        Ok(true)
    })();
    let (_client, _, processing) = active
        .deactivate()
        .context("Deactivate the JACK measurement inputs")?;
    if !result? {
        return Ok(None);
    }
    let first = processing
        .first_microseconds
        .context("JACK returned no measurement frames")?;
    let measured = i128::try_from(wall_time)? + i128::from(first) - i128::from(reference);
    let measured_at = netaudio_host_worker::timestamp_microseconds(i64::try_from(measured)?)
        .context("JACK measurement time is outside the supported range")?;
    for channel in processing.channels {
        let reading = if channel.invalid {
            Reading::Unavailable {
                error: "The JACK source returned nonfinite audio samples".into(),
            }
        } else {
            Reading::Level {
                peak_dbfs: decibels(channel.peak),
                rms_dbfs: decibels((channel.energy / processing.frames as f64).sqrt()),
                frames: processing.frames,
                sample_rate,
                measured_at: measured_at.clone(),
                duration_seconds: processing.frames as f64 / f64::from(sample_rate),
            }
        };
        readings.insert(channel.target, reading);
    }
    Ok(Some(readings))
}

fn wait(input: &mut Input, deadline: Instant) -> Result<bool> {
    match input.receive(Some(deadline))? {
        InputEvent::Changed => Ok(true),
        InputEvent::Deadline => bail!("The JACK measurement deadline expired"),
        InputEvent::Closed => Ok(false),
        InputEvent::Request(request) => match request.action {
            RequestAction::Shutdown => Ok(false),
            _ => bail!("A JACK measurement was requested while another was active"),
        },
    }
}
