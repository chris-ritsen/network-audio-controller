use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::pin;
use std::rc::Rc;

use anyhow::Result;
use serde::Serialize;

use super::sockets::Monotonic;

struct ThreadObservation {
    nanoseconds: u64,
    voluntary_switches: u64,
    involuntary_switches: u64,
}

pub struct Started {
    monotonic_nanoseconds: u64,
    thread: Option<ThreadObservation>,
}

fn thread_observation() -> Result<ThreadObservation> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let usage = unsafe { usage.assume_init() };
    Ok(ThreadObservation {
        nanoseconds: u64::try_from(value.tv_sec)? * 1_000_000_000 + u64::try_from(value.tv_nsec)?,
        voluntary_switches: usage.ru_nvcsw.try_into()?,
        involuntary_switches: usage.ru_nivcsw.try_into()?,
    })
}

impl Started {
    pub fn now(time: &Monotonic) -> Result<Self> {
        Ok(Self {
            monotonic_nanoseconds: time.now(),
            thread: Some(thread_observation()?),
        })
    }

    pub fn wall(time: &Monotonic) -> Self {
        Self {
            monotonic_nanoseconds: time.now(),
            thread: None,
        }
    }
}

#[derive(Default, Serialize)]
pub struct Observation {
    pub started_nanoseconds: u64,
    pub elapsed_nanoseconds: u64,
    pub thread_nanoseconds: Option<u64>,
    pub voluntary_switches: Option<u64>,
    pub involuntary_switches: Option<u64>,
    pub deadline_overrun_nanoseconds: u64,
    pub ready: Option<bool>,
}

#[derive(Default, Serialize)]
pub struct Timing {
    pub observations: u64,
    pub total_elapsed_nanoseconds: u64,
    pub total_thread_nanoseconds: Option<u64>,
    pub pending_polls_past_deadline: u64,
    pub maximum_elapsed: Observation,
    pub maximum_deadline_overrun: Observation,
    pub maximum_pending_deadline_overrun: Observation,
}

#[derive(Clone, Default, Serialize)]
#[serde(transparent)]
pub struct Measurements {
    operations: Rc<RefCell<BTreeMap<&'static str, Timing>>>,
    #[serde(skip)]
    thread_observations: bool,
}

impl Measurements {
    pub fn new(thread_observations: bool) -> Self {
        Self {
            thread_observations,
            ..Self::default()
        }
    }

    pub fn start(&self, time: &Monotonic) -> Result<Started> {
        if self.thread_observations {
            Started::now(time)
        } else {
            Ok(Started::wall(time))
        }
    }

    pub fn finish(
        &self,
        operation: &'static str,
        started: Started,
        time: &Monotonic,
        deadline: Option<u64>,
    ) -> Result<()> {
        self.record(operation, started, time, deadline, None)
    }

    fn record(
        &self,
        operation: &'static str,
        started: Started,
        time: &Monotonic,
        deadline: Option<u64>,
        ready: Option<bool>,
    ) -> Result<()> {
        let finished = time.now();
        let thread = match started.thread {
            Some(before) => {
                let after = thread_observation()?;
                Some(ThreadObservation {
                    nanoseconds: after.nanoseconds.saturating_sub(before.nanoseconds),
                    voluntary_switches: after
                        .voluntary_switches
                        .saturating_sub(before.voluntary_switches),
                    involuntary_switches: after
                        .involuntary_switches
                        .saturating_sub(before.involuntary_switches),
                })
            }
            None => None,
        };
        let elapsed_nanoseconds = finished.saturating_sub(started.monotonic_nanoseconds);
        let deadline_overrun_nanoseconds = deadline.map_or(0, |due| finished.saturating_sub(due));
        let mut measurements = self.operations.borrow_mut();
        let timing = measurements.entry(operation).or_default();
        timing.observations += 1;
        timing.total_elapsed_nanoseconds += elapsed_nanoseconds;
        if let Some(thread) = &thread {
            *timing.total_thread_nanoseconds.get_or_insert(0) += thread.nanoseconds;
        }
        let observation = || Observation {
            started_nanoseconds: started.monotonic_nanoseconds,
            elapsed_nanoseconds,
            thread_nanoseconds: thread.as_ref().map(|thread| thread.nanoseconds),
            voluntary_switches: thread.as_ref().map(|thread| thread.voluntary_switches),
            involuntary_switches: thread.as_ref().map(|thread| thread.involuntary_switches),
            deadline_overrun_nanoseconds,
            ready,
        };
        if elapsed_nanoseconds > timing.maximum_elapsed.elapsed_nanoseconds {
            timing.maximum_elapsed = observation();
        }
        if deadline_overrun_nanoseconds
            > timing.maximum_deadline_overrun.deadline_overrun_nanoseconds
        {
            timing.maximum_deadline_overrun = observation();
        }
        if ready == Some(false) && deadline_overrun_nanoseconds > 0 {
            timing.pending_polls_past_deadline += 1;
            if deadline_overrun_nanoseconds
                > timing.maximum_pending_deadline_overrun.deadline_overrun_nanoseconds
            {
                timing.maximum_pending_deadline_overrun = observation();
            }
        }
        Ok(())
    }

    pub async fn poll<T>(
        &self,
        operation: &'static str,
        time: &Monotonic,
        deadline: Option<u64>,
        future: impl Future<Output = T>,
    ) -> Result<T> {
        self.observe(operation, time, deadline, future, false).await
    }

    pub async fn task<T>(
        &self,
        operation: &'static str,
        time: &Monotonic,
        deadline: Option<u64>,
        future: impl Future<Output = T>,
    ) -> Result<T> {
        self.observe(operation, time, deadline, future, self.thread_observations).await
    }

    async fn observe<T>(
        &self,
        operation: &'static str,
        time: &Monotonic,
        deadline: Option<u64>,
        future: impl Future<Output = T>,
        include_thread: bool,
    ) -> Result<T> {
        let mut future = pin!(future);
        poll_fn(|context| {
            let started = if include_thread {
                Started::now(time)?
            } else {
                Started::wall(time)
            };
            let result = future.as_mut().poll(context);
            self.record(operation, started, time, deadline, Some(result.is_ready()))?;
            result.map(Ok)
        })
        .await
    }
}
