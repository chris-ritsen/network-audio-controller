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
    input_ports: [Port<AudioIn>; 2],
    output_ports: [Port<AudioOut>; 2],
    input: InputCallback,
    output: OutputCallback,
    buffer_frames: u32,
}

impl ProcessHandler for Process {
    fn process(&mut self, _: &Client, scope: &ProcessScope) -> Control {
        let left = self.input_ports[0].as_slice(scope);
        let right = self.input_ports[1].as_slice(scope);
        self.input.process(left.iter().zip(right).map(|(left, right)| [*left, *right]));
        let [left_port, right_port] = &mut self.output_ports;
        let left = left_port.as_mut_slice(scope);
        let right = right_port.as_mut_slice(scope);

        for (left, right) in left.iter_mut().zip(right.iter_mut()) {
            [*left, *right] = self.output.next();
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

        if configuration.input_channels != [1, 2] || configuration.output_channels != [1, 2] {
            bail!("JACK exposes two ports in each direction; channel mappings must be [1, 2]");
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

        let input_ports = [
            client.register_port("send_1", AudioIn::default())?,
            client.register_port("send_2", AudioIn::default())?,
        ];
        let output_ports = [
            client.register_port("receive_1", AudioOut::default())?,
            client.register_port("receive_2", AudioOut::default())?,
        ];
        let notifications = Notifications {
            state: input.state.clone(),
            sample_rate: configuration.sample_rate,
        };
        let process = Process {
            input_ports,
            output_ports,
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
