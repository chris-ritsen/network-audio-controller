use crate::capture::{Channel, State};
use std::sync::{Arc, atomic::Ordering};

pub struct Processing {
    pub state: Arc<State>,
    pub channels: Vec<Channel>,
    pub wanted: u64,
    pub frames: u64,
    pub first_microseconds: Option<u64>,
    pub next_frame: Option<u32>,
}

impl jack::ProcessHandler for Processing {
    fn process(&mut self, _: &jack::Client, scope: &jack::ProcessScope) -> jack::Control {
        if !self.state.enabled.load(Ordering::Acquire)
            || self.state.finished.load(Ordering::Acquire)
            || self.state.failure.load(Ordering::Acquire) != 0
        {
            return jack::Control::Continue;
        }
        self.state.started.store(true, Ordering::Release);
        if !self.state.connected() {
            self.state.fail(1);
            return jack::Control::Continue;
        }
        let Ok(cycle) = scope.cycle_times() else {
            self.state.fail(6);
            return jack::Control::Continue;
        };
        if self
            .next_frame
            .is_some_and(|expected| expected != cycle.current_frames)
        {
            self.state.fail(5);
            return jack::Control::Continue;
        }
        self.next_frame = Some(cycle.current_frames.wrapping_add(scope.n_frames()));
        self.first_microseconds.get_or_insert(cycle.current_usecs);
        let count = (self.wanted - self.frames).min(u64::from(scope.n_frames())) as usize;
        for channel in &mut self.channels {
            for sample in &channel.input.as_slice(scope)[..count] {
                let sample = f64::from(*sample);
                if !sample.is_finite() {
                    channel.invalid = true;
                    continue;
                }
                channel.peak = channel.peak.max(sample.abs());
                channel.energy += sample * sample;
            }
        }
        self.frames += count as u64;
        if self.frames == self.wanted {
            self.state.finished.store(true, Ordering::Release);
            self.state.wake.notify();
        }
        jack::Control::Continue
    }
}
