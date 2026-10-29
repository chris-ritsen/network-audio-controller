use std::sync::Arc;

use anyhow::{Result, bail};
use jack::{
    AudioIn, AudioOut, Client, ClientOptions, Control, NotificationHandler, Port, ProcessHandler,
    ProcessScope,
};

use super::bridge::{InputCallback, OutputCallback, State};
use super::{Configuration, Fault};

struct Notifications {
    state: Arc<State>,
    sample_rate: u32,
}

impl NotificationHandler for Notifications {
    unsafe fn shutdown(&mut self, _: jack::ClientStatus, _: &str) {
        self.state.report(Fault::HostUnavailable);
    }

    fn sample_rate(&mut self, _: &Client, rate: u32) -> Control {
        if rate != self.sample_rate {
            self.state.report(Fault::SampleRateChanged);
            return Control::Quit;
        }

        Control::Continue
    }

    fn xrun(&mut self, _: &Client) -> Control {
        self.state.report(Fault::Xrun);
        Control::Continue
    }
}

struct Process {
    input_ports: Vec<Port<AudioIn>>,
    output_ports: Vec<Port<AudioOut>>,
    input_samples: Vec<f32>,
    output_samples: Vec<f32>,
    input: InputCallback,
    output: OutputCallback,
    buffer_frames: u32,
}

impl ProcessHandler for Process {
    fn process(&mut self, _: &Client, scope: &ProcessScope) -> Control {
        if scope.n_frames() != self.buffer_frames {
            self.input.state.report(Fault::BufferSizeChanged);
            return Control::Quit;
        }

        let input_channels = self.input_ports.len();
        for (channel, port) in self.input_ports.iter().enumerate() {
            for (frame, sample) in port.as_slice(scope).iter().enumerate() {
                self.input_samples[frame * input_channels + channel] = *sample;
            }
        }
        self.input.process(scope.n_frames() as usize, |frame, channel| {
            self.input_samples[frame * input_channels + channel]
        });

        let output_channels = self.output_ports.len();
        for frame in self.output_samples.chunks_exact_mut(output_channels) {
            self.output.next(frame);
        }
        for (channel, port) in self.output_ports.iter_mut().enumerate() {
            for (frame, sample) in port.as_mut_slice(scope).iter_mut().enumerate() {
                *sample = self.output_samples[frame * output_channels + channel];
            }
        }

        self.output.completed();
        Control::Continue
    }

    fn buffer_size(&mut self, _: &Client, frames: u32) -> Control {
        if frames != self.buffer_frames {
            self.input.state.report(Fault::BufferSizeChanged);
            return Control::Quit;
        }

        Control::Continue
    }
}

pub(super) struct Stream(jack::AsyncClient<Notifications, Process>);

impl Stream {
    pub fn open(
        configuration: &Configuration,
        input: InputCallback,
        output: OutputCallback,
    ) -> Result<Self> {
        if configuration.input_device.is_some() || configuration.output_device.is_some() {
            bail!("JACK exposes named ports; device selectors do not apply");
        }

        if !configuration
            .input_channels
            .iter()
            .enumerate()
            .all(|(index, channel)| usize::from(*channel) == index + 1)
            || !configuration
                .output_channels
                .iter()
                .enumerate()
                .all(|(index, channel)| usize::from(*channel) == index + 1)
        {
            bail!("JACK channel maps must enumerate the published ports in order starting at one");
        }

        let (client, _) = Client::new(
            &configuration.client_name,
            ClientOptions::NO_START_SERVER | ClientOptions::USE_EXACT_NAME,
        )?;

        if client.sample_rate() != configuration.sample_rate
            || client.buffer_size() != configuration.buffer_frames
        {
            bail!(
                "JACK is running at {} Hz with {} frames; configuration requests {} Hz with {} frames",
                client.sample_rate(),
                client.buffer_size(),
                configuration.sample_rate,
                configuration.buffer_frames
            );
        }

        let input_ports = configuration
            .input_channels
            .iter()
            .map(|channel| client.register_port(&format!("send_{channel}"), AudioIn::default()))
            .collect::<Result<Vec<_>, _>>()?;
        let output_ports = configuration
            .output_channels
            .iter()
            .map(|channel| client.register_port(&format!("receive_{channel}"), AudioOut::default()))
            .collect::<Result<Vec<_>, _>>()?;
        let notifications = Notifications {
            state: input.state.clone(),
            sample_rate: configuration.sample_rate,
        };
        let process = Process {
            input_ports,
            output_ports,
            input_samples: vec![
                0.0;
                configuration.input_channels.len()
                    * configuration.buffer_frames as usize
            ],
            output_samples: vec![
                0.0;
                configuration.output_channels.len()
                    * configuration.buffer_frames as usize
            ],
            input,
            output,
            buffer_frames: configuration.buffer_frames,
        };
        Ok(Self(client.activate_async(notifications, process)?))
    }

    pub fn close(self) -> Result<()> {
        self.0.deactivate()?;
        Ok(())
    }
}
