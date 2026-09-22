use super::*;
use crate::inventory::Inventory;

pub struct NetaudioInventory {
    inner: Mutex<Inventory>,
}

fn inventory_error(message: &'static str) -> FfiError {
    FfiError::new(NetaudioStatus::InvalidPage, message)
}

unsafe fn inventory_lock<'a>(
    inventory: *mut NetaudioInventory,
) -> Result<MutexGuard<'a, Inventory>, FfiError> {
    let inventory = unsafe { inventory.as_ref() }.ok_or(NetaudioStatus::NullPointer)?;
    inventory
        .inner
        .lock()
        .map_err(|_| FfiError::new(NetaudioStatus::InternalPanic, "inventory state is poisoned"))
}

#[no_mangle]
/// Create an inventory operation for an explicitly selected protocol revision.
/// Kind must be "rx_channels", "tx_channels", "rx_flows", or "tx_flows".
/// The caller owns the returned handle and must free it exactly once.
pub unsafe extern "C" fn netaudio_inventory_new(
    kind: *const c_char,
    protocol_id: u16,
    maximum_pages: usize,
    out_inventory: *mut *mut NetaudioInventory,
) -> NetaudioStatus {
    guard(|| {
        if out_inventory.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }

        unsafe { *out_inventory = ptr::null_mut() };
        let kind = unsafe { c_string(kind)? };
        let inventory =
            Inventory::new(kind, protocol_id, maximum_pages).map_err(inventory_error)?;
        unsafe {
            *out_inventory = Box::into_raw(Box::new(NetaudioInventory {
                inner: Mutex::new(inventory),
            }));
        }

        Ok(())
    })
}

#[no_mangle]
/// Free an inventory handle. No concurrent calls may use the handle.
/// Passing null is allowed.
pub unsafe extern "C" fn netaudio_inventory_free(inventory: *mut NetaudioInventory) {
    if !inventory.is_null() {
        drop(unsafe { Box::from_raw(inventory) });
    }
}

#[no_mangle]
/// Accept one complete wire response. Failure leaves the operation unchanged.
pub unsafe extern "C" fn netaudio_inventory_accept(
    inventory: *mut NetaudioInventory,
    data: *const u8,
    data_len: usize,
) -> NetaudioStatus {
    guard(|| {
        if data.is_null() {
            return Err(NetaudioStatus::NullPointer.into());
        }

        let response = unsafe { std::slice::from_raw_parts(data, data_len) };
        unsafe { inventory_lock(inventory)? }
            .accept(response)
            .map_err(inventory_error)
    })
}

#[no_mangle]
/// Return JSON with next_command and inventory. Exactly one is non-null.
/// Reading state does not advance pagination, including buffer-size retries.
pub unsafe extern "C" fn netaudio_inventory_state(
    inventory: *mut NetaudioInventory,
    out_buffer: *mut u8,
    out_capacity: usize,
    out_length: *mut usize,
) -> NetaudioStatus {
    guard(|| {
        unsafe { prepare_output(out_buffer, out_capacity, out_length)? };
        let state = unsafe { inventory_lock(inventory)? }.state();
        unsafe { write_json(&state, out_buffer, out_capacity, out_length) }
    })
}
