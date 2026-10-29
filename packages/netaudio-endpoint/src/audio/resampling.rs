use anyhow::Result;
use netaudio_core::native_audio::SampleFormat;
use netaudio_core::native_audio::playout::quantum_frames;
use rubato::{
    Resampler, SincFixedIn, SincFixedOut, SincInterpolationParameters, SincInterpolationType,
    WindowFunction,
};

use super::{ClockRelationship, Configuration, FloatSamples};

const FILTER_LENGTH: usize = 128;
const FILTER_CUTOFF: f32 = 0.95;
const FILTER_OVERSAMPLING: usize = 128;
const MAXIMUM_RATIO_DEVIATION: f64 = 0.005;
const MAXIMUM_RELATIVE_RATIO: f64 = 1.0 / (1.0 - MAXIMUM_RATIO_DEVIATION);
const OCCUPANCY_SMOOTHING: f64 = 0.001;
const PROPORTIONAL_GAIN: f64 = 0.005;
const INTEGRAL_GAIN: f64 = 0.000_000_05;

fn parameters() -> SincInterpolationParameters {
    SincInterpolationParameters {
        sinc_len: FILTER_LENGTH,
        f_cutoff: FILTER_CUTOFF,
        interpolation: SincInterpolationType::Cubic,
        oversampling_factor: FILTER_OVERSAMPLING,
        window: WindowFunction::BlackmanHarris2,
    }
}

struct Correction {
    target: f64,
    occupancy: f64,
    integral: f64,
}

impl Correction {
    fn new(target: usize) -> Self {
        Self {
            target: target as f64,
            occupancy: target as f64,
            integral: 0.0,
        }
    }

    fn ratio(&mut self, frames: usize, nominal_ratio: f64) -> f64 {
        self.occupancy += OCCUPANCY_SMOOTHING * (frames as f64 - self.occupancy);
        let error = (self.target - self.occupancy) / self.target;
        self.integral = (self.integral + error * INTEGRAL_GAIN)
            .clamp(-MAXIMUM_RATIO_DEVIATION, MAXIMUM_RATIO_DEVIATION);
        let relative = 1.0
            + (error * PROPORTIONAL_GAIN + self.integral)
                .clamp(-MAXIMUM_RATIO_DEVIATION, MAXIMUM_RATIO_DEVIATION);
        (nominal_ratio * relative)
            .clamp(nominal_ratio / MAXIMUM_RELATIVE_RATIO, nominal_ratio * MAXIMUM_RELATIVE_RATIO)
    }
}

pub struct Capture {
    converter: Option<SincFixedOut<f32>>,
    input: Vec<Vec<f32>>,
    output: Vec<Vec<f32>>,
    correction: Correction,
    ratio: f64,
    target: usize,
    quantum_frames: usize,
}

impl Capture {
    pub fn new(configuration: &Configuration, network_format: SampleFormat) -> Result<Self> {
        let quantum_frames = quantum_frames(network_format.sample_rate());
        let channels = configuration.input_channels.len();
        let ratio = f64::from(network_format.sample_rate()) / f64::from(configuration.sample_rate);
        let converter = match configuration.clock_relationship {
            ClockRelationship::Independent => Some(SincFixedOut::new(
                ratio,
                MAXIMUM_RELATIVE_RATIO,
                parameters(),
                quantum_frames,
                channels,
            )?),
            ClockRelationship::NetworkSynchronized => None,
        };
        let input = converter.as_ref().map_or_else(
            || vec![vec![0.0; quantum_frames]; channels],
            |converter| converter.input_buffer_allocate(true),
        );
        let output = vec![vec![0.0; quantum_frames]; channels];

        Ok(Self {
            converter,
            input,
            output,
            correction: Correction::new(configuration.buffered_frames),
            ratio,
            target: configuration.buffered_frames,
            quantum_frames,
        })
    }

    pub fn reset(&mut self) -> Result<()> {
        if let Some(converter) = &mut self.converter {
            converter.reset();
            converter.set_resample_ratio(self.ratio, false)?;
        }

        self.correction = Correction::new(self.target);
        Ok(())
    }

    pub fn needed(&self) -> usize {
        self.converter.as_ref().map_or(self.quantum_frames, Resampler::input_frames_next)
    }

    pub fn maximum_input_frames(&self) -> usize {
        self.converter.as_ref().map_or(self.quantum_frames, Resampler::input_frames_max)
    }

    pub fn convert(
        &mut self,
        frames: impl Iterator<Item = FloatSamples>,
        occupancy: usize,
    ) -> Result<Vec<FloatSamples>> {
        let needed = self.needed();

        for (index, frame) in frames.take(needed).enumerate() {
            for (channel, sample) in frame.into_iter().enumerate() {
                self.input[channel][index] = sample;
            }
        }

        if let Some(converter) = &mut self.converter {
            converter.process_into_buffer(&self.input, &mut self.output, None)?;
            converter.set_resample_ratio(self.correction.ratio(occupancy, self.ratio), true)?;
        } else {
            for channel in 0..self.input.len() {
                self.output[channel].copy_from_slice(&self.input[channel]);
            }
        }

        Ok((0..self.quantum_frames)
            .map(|index| {
                (0..self.output.len()).map(|channel| self.output[channel][index]).collect()
            })
            .collect())
    }
}

pub struct Playback {
    converter: Option<SincFixedIn<f32>>,
    input: Vec<Vec<f32>>,
    output: Vec<Vec<f32>>,
    correction: Correction,
    ratio: f64,
    target: usize,
    quantum_frames: usize,
    channel_generations: Vec<u64>,
    retained_frames: Vec<usize>,
}

impl Playback {
    pub fn new(configuration: &Configuration, network_format: SampleFormat) -> Result<Self> {
        let quantum_frames = quantum_frames(network_format.sample_rate());
        let channels = configuration.output_channels.len();
        let ratio = f64::from(configuration.sample_rate) / f64::from(network_format.sample_rate());
        let converter = match configuration.clock_relationship {
            ClockRelationship::Independent => Some(SincFixedIn::new(
                ratio,
                MAXIMUM_RELATIVE_RATIO,
                parameters(),
                quantum_frames,
                channels,
            )?),
            ClockRelationship::NetworkSynchronized => None,
        };
        let output = converter.as_ref().map_or_else(
            || vec![vec![0.0; quantum_frames]; channels],
            |converter| converter.output_buffer_allocate(true),
        );

        Ok(Self {
            converter,
            input: vec![vec![0.0; quantum_frames]; channels],
            output,
            correction: Correction::new(configuration.buffered_frames),
            ratio,
            target: configuration.buffered_frames,
            quantum_frames,
            channel_generations: vec![0; channels],
            retained_frames: vec![0; channels],
        })
    }

    pub fn receipt_delay_quanta(&self) -> usize {
        if self.converter.is_some() {
            FILTER_LENGTH.div_ceil(self.quantum_frames) + 1
        } else {
            0
        }
    }

    pub fn maximum_output_frames(&self) -> usize {
        self.converter.as_ref().map_or(self.quantum_frames, Resampler::output_frames_max)
    }

    pub fn reset(&mut self) -> Result<()> {
        if let Some(converter) = &mut self.converter {
            converter.reset();
            converter.set_resample_ratio(self.ratio, false)?;
        }

        self.correction = Correction::new(self.target);
        self.channel_generations.fill(0);
        self.retained_frames.fill(0);
        Ok(())
    }

    pub fn select_channels(&mut self, generations: &[u64]) -> Vec<bool> {
        generations
            .iter()
            .enumerate()
            .map(|(index, generation)| {
                if self.channel_generations[index] != *generation {
                    self.channel_generations[index] = *generation;
                    self.retained_frames[index] = if self.converter.is_some() {
                        2 * FILTER_LENGTH
                    } else {
                        0
                    };
                }
                self.retained_frames[index] == 0 && *generation != 0
            })
            .collect()
    }

    pub fn convert(
        &mut self,
        frames: impl Iterator<Item = FloatSamples>,
        occupancy: usize,
    ) -> Result<Vec<FloatSamples>> {
        for (index, frame) in frames.take(self.quantum_frames).enumerate() {
            for (channel, sample) in frame.into_iter().enumerate() {
                self.input[channel][index] = sample;
            }
        }

        let produced = if let Some(converter) = &mut self.converter {
            converter.set_resample_ratio(self.correction.ratio(occupancy, self.ratio), true)?;
            converter.process_into_buffer(&self.input, &mut self.output, None)?.1
        } else {
            for channel in 0..self.input.len() {
                self.output[channel].copy_from_slice(&self.input[channel]);
            }

            self.quantum_frames
        };

        for retained in &mut self.retained_frames {
            *retained = retained.saturating_sub(self.quantum_frames);
        }

        Ok((0..produced)
            .map(|index| {
                (0..self.output.len()).map(|channel| self.output[channel][index]).collect()
            })
            .collect())
    }
}
