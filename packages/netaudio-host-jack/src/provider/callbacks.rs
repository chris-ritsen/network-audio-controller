use netaudio_host_worker::input::Wake;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

pub struct State {
    pub wake: Arc<Wake>,
    pub xruns: AtomicU64,
    pub last_xrun: AtomicU64,
    stopped: AtomicBool,
}

impl State {
    pub fn new(wake: Arc<Wake>) -> Self {
        Self {
            wake,
            xruns: AtomicU64::new(0),
            last_xrun: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

pub struct Notifications(pub Arc<State>);

impl jack::NotificationHandler for Notifications {
    unsafe fn shutdown(&mut self, _: jack::ClientStatus, _: &str) {
        self.0.stopped.store(true, Ordering::Release);
        self.0.wake.notify();
    }

    fn sample_rate(&mut self, _: &jack::Client, _: jack::Frames) -> jack::Control {
        self.0.wake.notify();
        jack::Control::Continue
    }

    fn client_registration(&mut self, _: &jack::Client, _: &str, _: bool) {
        self.0.wake.notify();
    }

    fn port_registration(&mut self, _: &jack::Client, _: jack::PortId, _: bool) {
        self.0.wake.notify();
    }

    fn port_rename(
        &mut self,
        _: &jack::Client,
        _: jack::PortId,
        _: &str,
        _: &str,
    ) -> jack::Control {
        self.0.wake.notify();
        jack::Control::Continue
    }

    fn ports_connected(&mut self, _: &jack::Client, _: jack::PortId, _: jack::PortId, _: bool) {
        self.0.wake.notify();
    }

    fn graph_reorder(&mut self, _: &jack::Client) -> jack::Control {
        self.0.wake.notify();
        jack::Control::Continue
    }

    fn xrun(&mut self, _: &jack::Client) -> jack::Control {
        if let Ok(time) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            let milliseconds = time.as_millis().min(u64::MAX as u128) as u64;
            let previous = self.0.last_xrun.load(Ordering::Relaxed);
            if milliseconds.abs_diff(previous) >= 100 {
                self.0.last_xrun.store(milliseconds, Ordering::Release);
                self.0.xruns.fetch_add(1, Ordering::Release);
                self.0.wake.notify();
            }
        }
        jack::Control::Continue
    }
}

pub struct Processing(pub Arc<State>);

impl jack::ProcessHandler for Processing {
    fn process(&mut self, _: &jack::Client, _: &jack::ProcessScope) -> jack::Control {
        jack::Control::Continue
    }

    fn buffer_size(&mut self, _: &jack::Client, _: jack::Frames) -> jack::Control {
        self.0.wake.notify();
        jack::Control::Continue
    }
}
