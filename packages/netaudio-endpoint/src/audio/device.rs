use anyhow::{Context, Result, bail};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, Host, SampleFormat, SizedSample, Stream, StreamConfig};
use serde::Serialize;

use super::bridge::{InputCallback, OutputCallback};
use super::{CHANNELS, Configuration};

#[derive(Serialize)]
pub struct DeviceInformation {
    pub identifier: String,
    pub description: String,
    pub input_formats: Vec<String>,
    pub output_formats: Vec<String>,
    pub input_error: Option<String>,
    pub output_error: Option<String>,
}

#[derive(Serialize)]
pub struct Inventory {
    pub compiled_backends: Vec<String>,
    pub selected_backend: Option<String>,
    pub devices: Vec<DeviceInformation>,
}

fn host(name: &str) -> Result<Host> {
    let identifier = cpal::ALL_HOSTS
        .iter()
        .find(|identifier| identifier.name().eq_ignore_ascii_case(name))
        .context("requested native audio backend is not compiled for this platform")?;
    Ok(cpal::host_from_id(*identifier)?)
}

fn describe_formats(
    configurations: impl Iterator<Item = cpal::SupportedStreamConfigRange>,
) -> Vec<String> {
    configurations
        .map(|configuration| {
            format!(
                "{} channels; {}–{} Hz; {:?}; {:?}",
                configuration.channels(),
                configuration.min_sample_rate(),
                configuration.max_sample_rate(),
                configuration.sample_format(),
                configuration.buffer_size(),
            )
        })
        .collect()
}

pub fn inventory(backend: Option<&str>) -> Result<Inventory> {
    let compiled_backends = cpal::ALL_HOSTS
        .iter()
        .map(|identifier| identifier.name().to_lowercase())
        .chain(cfg!(feature = "jack").then(|| "jack".to_owned()))
        .collect();

    let mut inventory = Inventory {
        compiled_backends,
        selected_backend: backend.map(str::to_owned),
        devices: Vec::new(),
    };
    let Some(backend) = backend else {
        return Ok(inventory);
    };

    if backend == "jack" {
        if !inventory.compiled_backends.iter().any(|name| name == backend) {
            bail!("this build does not include JACK");
        }

        return Ok(inventory);
    }

    for device in host(backend)?.devices()? {
        let input = device.supported_input_configs();
        let output = device.supported_output_configs();
        inventory.devices.push(DeviceInformation {
            identifier: device.id()?.to_string(),
            description: format!("{:?}", device.description()?),
            input_error: input.as_ref().err().map(ToString::to_string),
            output_error: output.as_ref().err().map(ToString::to_string),
            input_formats: input.map(describe_formats).unwrap_or_default(),
            output_formats: output.map(describe_formats).unwrap_or_default(),
        });
    }

    Ok(inventory)
}

fn select(host: &Host, identifier: Option<&str>, input: bool) -> Result<Device> {
    if let Some(identifier) = identifier {
        return host
            .device_by_id(&identifier.parse()?)
            .context("selected native audio device is unavailable");
    }

    if input {
        host.default_input_device().context("selected backend has no default input device")
    } else {
        host.default_output_device().context("selected backend has no default output device")
    }
}

fn format(
    device: &Device,
    input: bool,
    configuration: &Configuration,
) -> Result<(SampleFormat, StreamConfig)> {
    let channels = if input {
        configuration.input_channels
    } else {
        configuration.output_channels
    };
    let required_channels = *channels.iter().max().expect("channel mapping is not empty");
    let configurations: Vec<_> = if input {
        device.supported_input_configs()?.collect()
    } else {
        device.supported_output_configs()?.collect()
    };
    let mut formats: Vec<_> = configurations
        .into_iter()
        .filter(|candidate| {
            candidate.channels() >= required_channels
                && candidate.contains_rate(configuration.sample_rate)
                && match candidate.buffer_size() {
                    cpal::SupportedBufferSize::Range {
                        min,
                        max,
                    } => (*min..=*max).contains(&configuration.buffer_frames),
                    cpal::SupportedBufferSize::Unknown => true,
                }
        })
        .map(|candidate| (candidate.sample_format(), candidate.channels()))
        .collect();
    formats.sort_by_key(|(format, channels)| {
        let preference = match format {
            SampleFormat::F32 => 0,
            SampleFormat::I32 => 1,
            SampleFormat::F64 => 2,
            SampleFormat::I16 => 3,
            _ => 4,
        };
        (*channels, preference)
    });
    let (format, channels) = formats.first().copied().context(
        "device does not support the selected channel mapping, sample rate and callback size",
    )?;
    Ok((
        format,
        StreamConfig {
            channels,
            sample_rate: configuration.sample_rate,
            buffer_size: cpal::BufferSize::Fixed(configuration.buffer_frames),
        },
    ))
}

fn input_stream<Sample>(
    device: &Device,
    configuration: StreamConfig,
    channels: [u16; CHANNELS],
    mut callback: InputCallback,
) -> Result<Stream>
where
    Sample: SizedSample,
    f32: cpal::FromSample<Sample>,
{
    let state = callback.state.clone();
    let frame_channels = usize::from(configuration.channels);
    let selected = channels.map(|channel| usize::from(channel - 1));
    Ok(device.build_input_stream(
        configuration,
        move |samples: &[Sample], _| {
            callback.process(
                samples
                    .chunks_exact(frame_channels)
                    .map(|frame| selected.map(|channel| frame[channel].to_sample::<f32>())),
            );
        },
        move |error| state.report(stream_fault(error.kind())),
        None,
    )?)
}

fn output_stream<Sample>(
    device: &Device,
    configuration: StreamConfig,
    channels: [u16; CHANNELS],
    mut callback: OutputCallback,
) -> Result<Stream>
where
    Sample: SizedSample + cpal::FromSample<f32>,
{
    let state = callback.state.clone();
    let frame_channels = usize::from(configuration.channels);
    let selected = channels.map(|channel| usize::from(channel - 1));
    Ok(device.build_output_stream(
        configuration,
        move |samples: &mut [Sample], _| {
            samples.fill(Sample::EQUILIBRIUM);

            for frame in samples.chunks_exact_mut(frame_channels) {
                let next = callback.next();

                for (channel, sample) in selected.into_iter().zip(next) {
                    frame[channel] = Sample::from_sample(sample);
                }
            }

            callback.completed();
        },
        move |error| state.report(stream_fault(error.kind())),
        None,
    )?)
}

pub(super) struct Streams {
    input: Stream,
    output: Stream,
}

impl Streams {
    pub fn open(
        configuration: &Configuration,
        input: InputCallback,
        output: OutputCallback,
    ) -> Result<Self> {
        let host = host(&configuration.backend)?;
        let input_device = select(&host, configuration.input_device.as_deref(), true)?;
        let output_device = select(&host, configuration.output_device.as_deref(), false)?;
        let (input_format, input_configuration) = format(&input_device, true, configuration)?;
        let (output_format, output_configuration) = format(&output_device, false, configuration)?;

        macro_rules! build {
            ($function:ident, $format:expr, $device:expr, $stream:expr, $channels:expr, $callback:expr) => {
                match $format {
                    SampleFormat::F32 => $function::<f32>($device, $stream, $channels, $callback),
                    SampleFormat::F64 => $function::<f64>($device, $stream, $channels, $callback),
                    SampleFormat::I8 => $function::<i8>($device, $stream, $channels, $callback),
                    SampleFormat::I16 => $function::<i16>($device, $stream, $channels, $callback),
                    SampleFormat::I24 => {
                        $function::<cpal::I24>($device, $stream, $channels, $callback)
                    }
                    SampleFormat::I32 => $function::<i32>($device, $stream, $channels, $callback),
                    SampleFormat::I64 => $function::<i64>($device, $stream, $channels, $callback),
                    SampleFormat::U8 => $function::<u8>($device, $stream, $channels, $callback),
                    SampleFormat::U16 => $function::<u16>($device, $stream, $channels, $callback),
                    SampleFormat::U32 => $function::<u32>($device, $stream, $channels, $callback),
                    SampleFormat::U64 => $function::<u64>($device, $stream, $channels, $callback),
                    other => bail!("native audio sample format {other:?} is unsupported"),
                }?
            };
        }

        let input = build!(
            input_stream,
            input_format,
            &input_device,
            input_configuration,
            configuration.input_channels,
            input
        );
        let output = build!(
            output_stream,
            output_format,
            &output_device,
            output_configuration,
            configuration.output_channels,
            output
        );
        output.play()?;
        input.play()?;
        Ok(Self {
            input,
            output,
        })
    }

    pub fn close(self) {
        drop(self.input);
        drop(self.output);
    }
}

fn stream_fault(kind: cpal::ErrorKind) -> super::Fault {
    use super::Fault;

    match kind {
        cpal::ErrorKind::Xrun => Fault::Xrun,
        cpal::ErrorKind::DeviceNotAvailable => Fault::DeviceUnavailable,
        cpal::ErrorKind::HostUnavailable => Fault::HostUnavailable,
        cpal::ErrorKind::DeviceChanged => Fault::DeviceChanged,
        cpal::ErrorKind::StreamInvalidated | cpal::ErrorKind::UnsupportedConfig => {
            Fault::StreamInvalidated
        }
        cpal::ErrorKind::PermissionDenied => Fault::PermissionDenied,
        cpal::ErrorKind::RealtimeDenied => Fault::RealtimeDenied,
        _ => Fault::BackendFailure,
    }
}
