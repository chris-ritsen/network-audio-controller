use anyhow::{Context, Result, ensure};
use netaudio_host_worker::input::Wake;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
};

pub struct State {
    pub wake: Arc<Wake>,
    pub enabled: AtomicBool,
    pub started: AtomicBool,
    pub finished: AtomicBool,
    pub stopped: AtomicBool,
    pub failure: AtomicU8,
    pub inputs: Vec<jack::PortId>,
    pub sources: Vec<jack::PortId>,
    pub connections: Vec<Connection>,
    pub sample_rate: u32,
}

pub struct Connection {
    pub source: jack::PortId,
    pub destination: jack::PortId,
    pub connected: AtomicBool,
}

impl State {
    pub fn fail(&self, reason: u8) {
        if self.started.load(Ordering::Acquire) && !self.finished.load(Ordering::Acquire) {
            self.invalidate(reason);
        }
    }

    pub fn invalidate(&self, reason: u8) {
        if !self.finished.load(Ordering::Acquire) {
            let _ = self
                .failure
                .compare_exchange(0, reason, Ordering::AcqRel, Ordering::Acquire);
            self.wake.notify();
        }
    }

    pub fn connected(&self) -> bool {
        self.connections
            .iter()
            .all(|connection| connection.connected.load(Ordering::Acquire))
    }

    pub fn error(&self) -> Option<&'static str> {
        match self.failure.load(Ordering::Acquire) {
            0 => None,
            1 => Some("JACK capture connections changed during capture"),
            2 => Some("A JACK capture source disappeared or changed identity"),
            3 => Some("The JACK sample rate changed during capture"),
            4 => Some("JACK reported an xrun during capture"),
            5 => Some("JACK capture frames are discontinuous"),
            6 => Some("JACK could not provide capture timing"),
            7 => Some("JACK recording queue exceeded its capacity"),
            _ => Some("JACK capture failed"),
        }
    }
}

pub struct Notifications(pub Arc<State>);

impl jack::NotificationHandler for Notifications {
    unsafe fn shutdown(&mut self, _: jack::ClientStatus, _: &str) {
        self.0.stopped.store(true, Ordering::Release);
        self.0.wake.notify();
    }

    fn sample_rate(&mut self, _: &jack::Client, rate: jack::Frames) -> jack::Control {
        if rate != self.0.sample_rate {
            self.0.invalidate(3);
        }
        jack::Control::Continue
    }

    fn port_registration(&mut self, _: &jack::Client, port: jack::PortId, registered: bool) {
        if !registered && (self.0.sources.contains(&port) || self.0.inputs.contains(&port)) {
            self.0.invalidate(2);
        }
    }

    fn port_rename(
        &mut self,
        _: &jack::Client,
        port: jack::PortId,
        _: &str,
        _: &str,
    ) -> jack::Control {
        if self.0.sources.contains(&port) || self.0.inputs.contains(&port) {
            self.0.invalidate(2);
        }
        jack::Control::Continue
    }

    fn ports_connected(
        &mut self,
        _: &jack::Client,
        first: jack::PortId,
        second: jack::PortId,
        connected: bool,
    ) {
        let mut matched = false;
        for connection in &self.0.connections {
            if (connection.source == first && connection.destination == second)
                || (connection.source == second && connection.destination == first)
            {
                matched = true;
                let previous = connection.connected.swap(connected, Ordering::AcqRel);
                if previous != connected {
                    self.0.fail(1);
                }
                self.0.wake.notify();
            }
        }
        if !matched && (self.0.inputs.contains(&first) || self.0.inputs.contains(&second)) {
            self.0.invalidate(1);
        }
    }

    fn xrun(&mut self, _: &jack::Client) -> jack::Control {
        self.0.fail(4);
        jack::Control::Continue
    }
}

pub struct Channel {
    pub target: String,
    pub sources: Vec<(String, u64)>,
    pub input: jack::Port<jack::AudioIn>,
    pub peak: f64,
    pub energy: f64,
    pub invalid: bool,
}

pub fn identity<Specification>(
    client: &jack::Client,
    port: &jack::Port<Specification>,
) -> Result<(u64, jack::PortId)> {
    let identity = unsafe { jack_sys::jack_port_uuid(port.raw()) };
    let index = unsafe { jack_sys::jack_uuid_to_index(identity) };
    let observed = client
        .port_by_id(index)
        .context("JACK port identity is unavailable")?;
    ensure!(
        unsafe { jack_sys::jack_port_uuid(observed.raw()) } == identity,
        "JACK port identity does not match its index"
    );
    Ok((identity, index))
}

pub type Connections = Vec<(String, Vec<(String, u64)>)>;

pub fn verify(client: &jack::Client, connections: &Connections) -> Result<()> {
    for (name, sources) in connections {
        let input = client
            .port_by_name(name)
            .context("A JACK capture input disappeared")?;
        let mut connections = input.get_connections();
        connections.sort();
        let expected: Vec<_> = sources.iter().map(|(name, _)| name.clone()).collect();
        ensure!(connections == expected, "JACK capture connections changed");
        for (name, expected) in sources {
            let port = client
                .port_by_name(name)
                .context("A JACK capture source disappeared")?;
            ensure!(
                unsafe { jack_sys::jack_port_uuid(port.raw()) } == *expected,
                "A JACK capture source changed identity"
            );
        }
    }
    Ok(())
}
