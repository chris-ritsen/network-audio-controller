use std::collections::BTreeMap;
use std::io;

use anyhow::Result;
use serde::Serialize;

use super::sockets::Monotonic;

pub struct Started {
    monotonic_nanoseconds: u64,
    thread_nanoseconds: u64,
}

fn thread_time() -> Result<u64> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(u64::try_from(value.tv_sec)? * 1_000_000_000 + u64::try_from(value.tv_nsec)?)
}

impl Started {
    pub fn now(time: &Monotonic) -> Result<Self> {
        Ok(Self {
            monotonic_nanoseconds: time.now(),
            thread_nanoseconds: thread_time()?,
        })
    }
}

#[derive(Default, Serialize)]
pub struct Observation {
    pub started_nanoseconds: u64,
    pub elapsed_nanoseconds: u64,
    pub thread_nanoseconds: u64,
    pub deadline_overrun_nanoseconds: u64,
}

#[derive(Default, Serialize)]
pub struct Timing {
    pub observations: u64,
    pub total_thread_nanoseconds: u64,
    pub maximum_elapsed: Observation,
    pub maximum_deadline_overrun: Observation,
}

#[derive(Default, Serialize)]
pub struct Measurements(BTreeMap<&'static str, Timing>);

impl Measurements {
    pub fn finish(
        &mut self,
        operation: &'static str,
        started: Started,
        time: &Monotonic,
        deadline: Option<u64>,
    ) -> Result<()> {
        let finished = time.now();
        let thread_nanoseconds = thread_time()?.saturating_sub(started.thread_nanoseconds);
        let elapsed_nanoseconds = finished.saturating_sub(started.monotonic_nanoseconds);
        let deadline_overrun_nanoseconds = deadline.map_or(0, |due| finished.saturating_sub(due));
        let timing = self.0.entry(operation).or_default();
        timing.observations += 1;
        timing.total_thread_nanoseconds += thread_nanoseconds;
        let observation = || Observation {
            started_nanoseconds: started.monotonic_nanoseconds,
            elapsed_nanoseconds,
            thread_nanoseconds,
            deadline_overrun_nanoseconds,
        };
        if elapsed_nanoseconds > timing.maximum_elapsed.elapsed_nanoseconds {
            timing.maximum_elapsed = observation();
        }
        if deadline_overrun_nanoseconds
            > timing.maximum_deadline_overrun.deadline_overrun_nanoseconds
        {
            timing.maximum_deadline_overrun = observation();
        }
        Ok(())
    }
}
