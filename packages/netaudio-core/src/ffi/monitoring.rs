use super::*;

pub struct NetaudioClockTracker {
    inner: Mutex<crate::heartbeat_clock::ClockObservations>,
}

unsafe fn clock_tracker_lock<'a>(
    tracker: *mut NetaudioClockTracker,
) -> Result<MutexGuard<'a, crate::heartbeat_clock::ClockObservations>, FfiError> {
    unsafe { tracker.as_ref() }
        .ok_or(NetaudioStatus::NullPointer)?
        .inner
        .lock()
        .map_err(|_| FfiError::new(NetaudioStatus::InternalPanic, "clock tracker is poisoned"))
}

/// Create a bounded clock observation owner. Free exactly once after all calls finish.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_tracker_new(
    out_tracker: *mut *mut NetaudioClockTracker,
) -> NetaudioStatus {
    guard(|| {
        if out_tracker.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        unsafe {
            *out_tracker = Box::into_raw(Box::new(NetaudioClockTracker {
                inner: Mutex::new(Default::default()),
            }));
        }
        Ok(())
    })
}

/// Free retained observations. Passing null is allowed; concurrent use is not.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_tracker_free(tracker: *mut NetaudioClockTracker) {
    if !tracker.is_null() {
        drop(unsafe { Box::from_raw(tracker) });
    }
}

/// Accept an incremental ClockObservationRequest with previous=null.
/// Mutations have no output-buffer retry; read snapshots separately.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_tracker_accept(
    tracker: *mut NetaudioClockTracker,
    json: *const c_char,
    out_changed: *mut bool,
) -> NetaudioStatus {
    guard(|| {
        if out_changed.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }
        unsafe { *out_changed = false };
        let request = unsafe { decode_json(c_string(json)?)? };
        let mut state = unsafe { clock_tracker_lock(tracker)? };
        let changed = crate::heartbeat_clock::observe_in_place(&mut state, request)
            .map_err(|error| FfiError::new(NetaudioStatus::InvalidJson, error))?;
        unsafe { *out_changed = changed };
        Ok(())
    })
}

/// Read current state, optionally including retained history. Buffer retries are read-only.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_tracker_snapshot(
    tracker: *mut NetaudioClockTracker,
    include_history: bool,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let state = unsafe { clock_tracker_lock(tracker)? };
        if include_history {
            return unsafe { write_json(&*state, out_buffer, out_capacity, out_length) };
        }
        let summary = crate::heartbeat_clock::ClockObservations {
            warning_enabled: state.warning_enabled,
            variation: state.variation.clone(),
            heartbeat: state.heartbeat.summary(),
            conmon: state.conmon.summary(),
            diagnostics: state.diagnostics.clone(),
        };
        unsafe { write_json(&summary, out_buffer, out_capacity, out_length) }
    })
}

/// Accept source-specific clock observations and compute bounded statistics.
#[no_mangle]
pub unsafe extern "C" fn netaudio_clock_observation_update(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let input = c_string(json)?;
            let request = decode_json(input)?;
            crate::heartbeat_clock::observe(request)
                .map_err(|error| FfiError::new(NetaudioStatus::InvalidJson, error))
        })
    }
}

/// Return detailed and signal_presence scales, each indexed by the raw byte value.
/// Each entry contains dbfs (null for special values) and state. Callers must select
/// the scale matching the sample source; heartbeat dBFS values are estimates.
#[no_mangle]
pub unsafe extern "C" fn netaudio_metering_scale(
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            Ok(crate::metering::metering_scale())
        })
    }
}

/// Interpret parsed connection-health records and prior stream observations.
/// Returns null for replayed or absent updates. Invalid evidence changes no state.
#[no_mangle]
pub unsafe extern "C" fn netaudio_connection_health_update(
    json: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    unsafe {
        json_output((out_buffer, out_capacity, out_length), || {
            let input = c_string(json)?;
            let request = decode_json(input)?;
            let update = crate::heartbeat_connection_health::plan_update(request)
                .map_err(crate::spec::SpecError::InvalidJson)?;

            Ok(update)
        })
    }
}
