use anyhow::Result;
use rubato::{
    Resampler, SincFixedIn, SincFixedOut, SincInterpolationParameters, SincInterpolationType,
    WindowFunction,
};

use super::{
    CHANNELS, ClockRelationship, Configuration, FloatSamples, QUANTUM_FRAMES, SAMPLE_RATE,
};

const FILTER_LENGTH: usize = 128;
const FILTER_CUTOFF: f32 = 0.95;
const FILTER_OVERSAMPLING: usize = 128;
const MAXIMUM_RATIO_DEVIATION: f64 = 0.005;
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

    fn ratio(&mut self, frames: usize) -> f64 {
        self.occupancy += OCCUPANCY_SMOOTHING * (frames as f64 - self.occupancy);
        let error = (self.target - self.occupancy) / self.target;
        self.integral = (self.integral + error * INTEGRAL_GAIN)
            .clamp(-MAXIMUM_RATIO_DEVIATION, MAXIMUM_RATIO_DEVIATION);
        1.0 + (error * PROPORTIONAL_GAIN + self.integral)
            .clamp(-MAXIMUM_RATIO_DEVIATION, MAXIMUM_RATIO_DEVIATION)
    }
}

pub struct Capture {
    converter: Option<SincFixedOut<f32>>,
    input: Vec<Vec<f32>>,
    output: Vec<Vec<f32>>,
    correction: Correction,
    ratio: f64,
    target: usize,
}

impl Capture {
    pub fn new(configuration: &Configuration) -> Result<Self> {
        let ratio = f64::from(SAMPLE_RATE) / f64::from(configuration.sample_rate);
        let converter = match configuration.clock_relationship {
            ClockRelationship::Independent => Some(SincFixedOut::new(
                ratio,
                1.0 + MAXIMUM_RATIO_DEVIATION,
                parameters(),
                QUANTUM_FRAMES,
                CHANNELS,
            )?),
            ClockRelationship::NetworkSynchronized => None,
        };
        let input = converter.as_ref().map_or_else(
            || vec![vec![0.0; QUANTUM_FRAMES]; CHANNELS],
            |converter| converter.input_buffer_allocate(true),
        );
        let output = vec![vec![0.0; QUANTUM_FRAMES]; CHANNELS];

        Ok(Self {
            converter,
            input,
            output,
            correction: Correction::new(configuration.buffered_frames),
            ratio,
            target: configuration.buffered_frames,
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
        self.converter.as_ref().map_or(QUANTUM_FRAMES, Resampler::input_frames_next)
    }

    pub fn maximum_input_frames(&self) -> usize {
        self.converter.as_ref().map_or(QUANTUM_FRAMES, Resampler::input_frames_max)
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
            converter.set_resample_ratio(self.ratio * self.correction.ratio(occupancy), true)?;
        } else {
            for channel in 0..CHANNELS {
                self.output[channel].copy_from_slice(&self.input[channel]);
            }
        }

        Ok((0..QUANTUM_FRAMES)
            .map(|index| std::array::from_fn(|channel| self.output[channel][index]))
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
}

impl Playback {
    pub fn new(configuration: &Configuration) -> Result<Self> {
        let ratio = f64::from(configuration.sample_rate) / f64::from(SAMPLE_RATE);
        let converter = match configuration.clock_relationship {
            ClockRelationship::Independent => Some(SincFixedIn::new(
                ratio,
                1.0 + MAXIMUM_RATIO_DEVIATION,
                parameters(),
                QUANTUM_FRAMES,
                CHANNELS,
            )?),
            ClockRelationship::NetworkSynchronized => None,
        };
        let output = converter.as_ref().map_or_else(
            || vec![vec![0.0; QUANTUM_FRAMES]; CHANNELS],
            |converter| converter.output_buffer_allocate(true),
        );

        Ok(Self {
            converter,
            input: vec![vec![0.0; QUANTUM_FRAMES]; CHANNELS],
            output,
            correction: Correction::new(configuration.buffered_frames),
            ratio,
            target: configuration.buffered_frames,
        })
    }

    pub fn receipt_delay_quanta(&self) -> usize {
        if self.converter.is_some() {
            FILTER_LENGTH.div_ceil(QUANTUM_FRAMES) + 1
        } else {
            0
        }
    }

    pub fn maximum_output_frames(&self) -> usize {
        self.converter.as_ref().map_or(QUANTUM_FRAMES, Resampler::output_frames_max)
    }

    pub fn reset(&mut self) {
        if let Some(converter) = &mut self.converter {
            converter.reset();
        }

        self.correction = Correction::new(self.target);
    }

    pub fn convert(
        &mut self,
        frames: impl Iterator<Item = FloatSamples>,
        occupancy: usize,
    ) -> Result<Vec<FloatSamples>> {
        for (index, frame) in frames.take(QUANTUM_FRAMES).enumerate() {
            for (channel, sample) in frame.into_iter().enumerate() {
                self.input[channel][index] = sample;
            }
        }

        let produced = if let Some(converter) = &mut self.converter {
            converter.set_resample_ratio(self.ratio * self.correction.ratio(occupancy), true)?;
            converter.process_into_buffer(&self.input, &mut self.output, None)?.1
        } else {
            for channel in 0..CHANNELS {
                self.output[channel].copy_from_slice(&self.input[channel]);
            }

            QUANTUM_FRAMES
        };

        Ok((0..produced)
            .map(|index| std::array::from_fn(|channel| self.output[channel][index]))
            .collect())
    }
}
