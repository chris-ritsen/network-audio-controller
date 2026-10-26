mod capture;

use crate::capture::{Channel, Connection, Connections, Notifications, State, identity, verify};
use anyhow::{Context, Result, bail, ensure};
use capture::{Processing, Row};
use netaudio_core::host_audio::{
    measurement::decibels,
    protocol::{Event, RequestAction},
    recording::{Configuration, Frame, Level, Record},
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
                RequestAction::Record { configuration } => {
                    configuration.validate().map_err(anyhow::Error::msg)?;
                    return record(output, &mut input, request.sequence, configuration);
                }
                _ => bail!("The JACK recording worker received an unsupported request"),
            },
        }
    }
}

fn receive(input: &mut Input, deadline: Option<Instant>) -> Result<bool> {
    match input.receive(deadline)? {
        InputEvent::Closed => Ok(false),
        InputEvent::Changed => Ok(true),
        InputEvent::Deadline => bail!("JACK recording setup deadline expired"),
        InputEvent::Request(request) => match request.action {
            RequestAction::Shutdown => Ok(false),
            _ => bail!("The JACK recording worker received another request while recording"),
        },
    }
}

fn record(
    output: &mut Output,
    input: &mut Input,
    sequence: u64,
    configuration: Configuration,
) -> Result<()> {
    let (client, _) = jack::Client::new(
        "netaudio-meter-history",
        jack::ClientOptions::NO_START_SERVER,
    )
    .context("Connect the recorder to the existing JACK server")?;
    ensure!(
        client.sample_rate() == configuration.sample_rate,
        "JACK recording sample rate changed before setup"
    );
    let mut channels = Vec::new();
    let mut inputs = Vec::new();
    let mut sources = Vec::new();
    let mut connections = Vec::new();
    for (name, expected) in &configuration.sources {
        let source = client
            .port_by_name(name)
            .with_context(|| format!("JACK recording source {name} disappeared"))?;
        let (identifier, source_index) = identity(&client, &source)?;
        ensure!(
            identifier == *expected
                && source.flags().contains(jack::PortFlags::IS_OUTPUT)
                && source.port_type()? == "32 bit float mono audio",
            "JACK recording source {name} changed identity or type"
        );
        let port = client.register_port(
            &format!("record_{}", channels.len() + 1),
            jack::AudioIn::default(),
        )?;
        let destination = identity(&client, &port)?.1;
        inputs.push(destination);
        sources.push(source_index);
        connections.push(Connection {
            source: source_index,
            destination,
            connected: AtomicBool::new(false),
        });
        channels.push(Channel {
            target: name.clone(),
            sources: vec![(name.clone(), *expected)],
            input: port,
            peak: 0.0,
            energy: 0.0,
            invalid: false,
        });
    }
    let expected: Connections = channels
        .iter()
        .map(|channel| Ok((channel.input.name()?, channel.sources.clone())))
        .collect::<Result<_>>()?;
    let clock_before = client.time();
    let wall_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i128;
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
        sources,
        connections,
        sample_rate: configuration.sample_rate,
    });
    let (producer, mut consumer) = rtrb::RingBuffer::new(channels.len() * 8);
    let mut rows = vec![Row::default(); channels.len()];
    let processing = Processing {
        state: state.clone(),
        rows: rows.clone(),
        channels,
        producer,
        clock_offset: wall_time - i128::from(reference) * 1000,
        window: Row::default(),
        complete_start: false,
        pending_complete: false,
        next_frame: None,
        next_microseconds: None,
    };
    let active = client
        .activate_async(Notifications(state.clone()), processing)
        .context("Activate JACK recording inputs")?;
    let result = (|| -> Result<()> {
        for (destination, sources) in &expected {
            for (source, _) in sources {
                active
                    .as_client()
                    .connect_ports_by_name(source, destination)?;
            }
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while !state.connected() {
            check(&state)?;
            if !receive(input, Some(deadline))? {
                return Ok(());
            }
        }
        verify(active.as_client(), &expected)?;
        check(&state)?;
        send(
            output,
            sequence,
            Record::Started {
                configuration: configuration.clone(),
                observed_at: netaudio_host_worker::timestamp(),
            },
        )?;
        state.enabled.store(true, Ordering::Release);
        loop {
            drain(output, sequence, &configuration, &mut consumer, &mut rows)?;
            check(&state)?;
            if !receive(input, None)? {
                break;
            }
        }
        Ok(())
    })();
    state.enabled.store(false, Ordering::Release);
    state.finished.store(true, Ordering::Release);
    let (_, _, mut processing) = active
        .deactivate()
        .context("Deactivate JACK recording inputs")?;
    drain(output, sequence, &configuration, &mut consumer, &mut rows)?;
    if processing.window.frames > 0 {
        let complete = processing.pending_complete;
        emit(output, sequence, &configuration, processing.rows(complete))?;
    }
    send(
        output,
        sequence,
        Record::Finished {
            observed_at: netaudio_host_worker::timestamp(),
            error: result.as_ref().err().map(|error| format!("{error:#}")),
        },
    )?;
    result
}

fn check(state: &State) -> Result<()> {
    ensure!(
        !state.stopped.load(Ordering::Acquire),
        "The JACK server stopped during recording"
    );
    if let Some(error) = state.error() {
        bail!("{error}");
    }
    Ok(())
}

fn send(output: &mut Output, sequence: u64, record: Record) -> Result<()> {
    output.send(Event::Recording {
        request_sequence: sequence,
        record,
    })
}

fn drain(
    output: &mut Output,
    sequence: u64,
    configuration: &Configuration,
    consumer: &mut rtrb::Consumer<Row>,
    rows: &mut [Row],
) -> Result<()> {
    while consumer.pop_entire_slice(rows).is_ok() {
        emit(output, sequence, configuration, rows)?;
    }
    Ok(())
}

fn emit(
    output: &mut Output,
    sequence: u64,
    configuration: &Configuration,
    rows: &[Row],
) -> Result<()> {
    let first = rows.first().context("JACK recording has no rows")?;
    let levels = configuration
        .sources
        .keys()
        .zip(rows)
        .map(|(name, row)| {
            (
                name.clone(),
                Level {
                    peak_amplitude: (!row.invalid).then_some(row.peak),
                    squared_amplitude_sum: (!row.invalid).then_some(row.energy),
                    peak_dbfs: (!row.invalid).then(|| decibels(row.peak)).flatten(),
                    rms_dbfs: (!row.invalid)
                        .then(|| decibels((row.energy / row.frames as f64).sqrt()))
                        .flatten(),
                    error: row
                        .invalid
                        .then(|| "JACK source contains nonfinite audio samples".to_owned()),
                },
            )
        })
        .collect();
    send(
        output,
        sequence,
        Record::Frame(Frame {
            second: first.second,
            started_at: netaudio_host_worker::timestamp_microseconds(
                (first.start_nanoseconds / 1000) as i64,
            )
            .context("JACK recording start time is invalid")?,
            ended_at: netaudio_host_worker::timestamp_microseconds(
                (first.end_nanoseconds / 1000) as i64,
            )
            .context("JACK recording end time is invalid")?,
            frames: first.frames,
            sample_rate: configuration.sample_rate,
            complete: first.complete,
            levels,
        }),
    )
}
