use super::*;
use crate::conmon_export::ExportCollector;

pub struct NetaudioExportCollector {
    inner: Mutex<ExportCollector>,
}

/// Create a bounded collector. The caller owns the handle and must free it once.
#[no_mangle]
pub unsafe extern "C" fn netaudio_export_new(
    configuration: *const c_char,
    out_collector: *mut *mut NetaudioExportCollector,
) -> NetaudioStatus {
    guard(|| {
        if out_collector.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }

        unsafe { *out_collector = ptr::null_mut() };
        let configuration = decode_json(unsafe { c_string(configuration)? })?;
        unsafe {
            *out_collector = Box::into_raw(Box::new(NetaudioExportCollector {
                inner: Mutex::new(ExportCollector::new(configuration)),
            }));
        }

        Ok(())
    })
}

/// Free a collector. No concurrent calls may use it. Passing null is allowed.
#[no_mangle]
pub unsafe extern "C" fn netaudio_export_free(collector: *mut NetaudioExportCollector) {
    if !collector.is_null() {
        drop(unsafe { Box::from_raw(collector) });
    }
}

/// Accept a parsed fragment and return match/completion state. Repeating an
/// accepted fragment is idempotent, including output-buffer size retries.
#[no_mangle]
pub unsafe extern "C" fn netaudio_export_accept(
    collector: *mut NetaudioExportCollector,
    fragment: *const c_char,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let collector = unsafe { collector.as_ref() }.ok_or(NetaudioStatus::NullPointer)?;
        let mut collector = collector.inner.lock().map_err(|_| {
            FfiError::new(NetaudioStatus::InternalPanic, "export state is poisoned")
        })?;
        let fragment = decode_json(unsafe { c_string(fragment)? })?;
        let progress = collector
            .accept(fragment)
            .map_err(|message| FfiError::new(NetaudioStatus::InvalidPage, message))?;
        unsafe { write_json(&progress, out_buffer, out_capacity, out_length) }
    })
}
