use crate::capture::{Channel, State};
use std::sync::{Arc, atomic::Ordering};

#[derive(Clone, Copy, Default)]
pub struct Row {
    pub second: i64,
    pub start_nanoseconds: u64,
    pub end_nanoseconds: u64,
    pub frames: u64,
    pub complete: bool,
    pub peak: f64,
    pub energy: f64,
    pub invalid: bool,
}

pub struct Processing {
    pub state: Arc<State>,
    pub channels: Vec<Channel>,
    pub producer: rtrb::Producer<Row>,
    pub rows: Vec<Row>,
    pub clock_offset: i128,
    pub window: Row,
    pub complete_start: bool,
    pub pending_complete: bool,
    pub next_frame: Option<u32>,
    pub next_microseconds: Option<u64>,
}

impl Processing {
    pub fn rows(&mut self, complete: bool) -> &[Row] {
        for (row, channel) in self.rows.iter_mut().zip(&self.channels) {
            *row = Row {
                peak: channel.peak,
                energy: channel.energy,
                invalid: channel.invalid,
                complete,
                ..self.window
            };
        }
        &self.rows
    }

    fn publish(&mut self) -> bool {
        self.pending_complete =
            self.complete_start && self.state.failure.load(Ordering::Acquire) == 0;
        self.rows(self.pending_complete);
        if self.producer.push_entire_slice(&self.rows).is_err() {
            self.state.invalidate(7);
            return false;
        }
        for channel in &mut self.channels {
            channel.peak = 0.0;
            channel.energy = 0.0;
            channel.invalid = false;
        }
        self.window = Row::default();
        self.complete_start = true;
        self.pending_complete = false;
        self.state.wake.notify();
        true
    }
}

impl jack::ProcessHandler for Processing {
    fn process(&mut self, _: &jack::Client, scope: &jack::ProcessScope) -> jack::Control {
        if !self.state.enabled.load(Ordering::Acquire)
            || self.state.failure.load(Ordering::Acquire) != 0
        {
            return jack::Control::Continue;
        }
        self.state.started.store(true, Ordering::Release);
        if !self.state.connected() {
            self.state.invalidate(1);
            return jack::Control::Continue;
        }
        let Ok(cycle) = scope.cycle_times() else {
            self.state.invalidate(6);
            return jack::Control::Continue;
        };
        if self
            .next_frame
            .is_some_and(|frame| frame != cycle.current_frames)
            || self
                .next_microseconds
                .is_some_and(|time| time != cycle.current_usecs)
        {
            self.state.invalidate(5);
            return jack::Control::Continue;
        }
        let frames = u64::from(scope.n_frames());
        let start = self.clock_offset + i128::from(cycle.current_usecs) * 1000;
        let end = self.clock_offset + i128::from(cycle.next_usecs) * 1000;
        if start < 0 || end <= start || end > i128::from(u64::MAX) || frames == 0 {
            self.state.invalidate(6);
            return jack::Control::Continue;
        }
        let start = start as u64;
        let span = (end - i128::from(start)) as u64;
        self.next_frame = Some(cycle.current_frames.wrapping_add(scope.n_frames()));
        self.next_microseconds = Some(cycle.next_usecs);
        let mut offset = 0;
        while offset < frames {
            let time = start + (u128::from(span) * u128::from(offset) / u128::from(frames)) as u64;
            if self.window.frames == 0 {
                self.window.second = (time / 1_000_000_000) as i64;
                self.window.start_nanoseconds = time;
            }
            let boundary = (self.window.second as u64 + 1) * 1_000_000_000;
            let through = (u128::from(boundary.saturating_sub(start)) * u128::from(frames))
                .div_ceil(u128::from(span))
                .min(u128::from(frames)) as u64;
            if through <= offset {
                self.state.invalidate(6);
                return jack::Control::Continue;
            }
            for channel in &mut self.channels {
                for sample in &channel.input.as_slice(scope)[offset as usize..through as usize] {
                    let sample = f64::from(*sample);
                    if sample.is_finite() {
                        channel.peak = channel.peak.max(sample.abs());
                        channel.energy += sample * sample;
                    } else {
                        channel.invalid = true;
                    }
                }
            }
            self.window.frames += through - offset;
            self.window.end_nanoseconds =
                start + (u128::from(span) * u128::from(through) / u128::from(frames)) as u64;
            offset = through;
            if self.window.end_nanoseconds >= boundary && !self.publish() {
                return jack::Control::Continue;
            }
        }
        jack::Control::Continue
    }
}
