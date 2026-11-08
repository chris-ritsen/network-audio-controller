use anyhow::{bail, Result};
use std::ffi::c_void;
use std::sync::Arc;
use tokio::sync::Notify;
use windows_sys::Win32::Foundation::{HANDLE, NO_ERROR};
use windows_sys::Win32::NetworkManagement::IpHelper::{
    CancelMibChangeNotify2, NotifyIpInterfaceChange, NotifyUnicastIpAddressChange,
    MIB_IPINTERFACE_ROW, MIB_NOTIFICATION_TYPE, MIB_UNICASTIPADDRESS_ROW,
};
use windows_sys::Win32::Networking::WinSock::AF_UNSPEC;

pub struct Changes {
    addresses: HANDLE,
    interfaces: HANDLE,
    notify: Arc<Notify>,
}

unsafe impl Send for Changes {}

impl Changes {
    pub fn new() -> Result<Self> {
        let notify = Arc::new(Notify::new());
        let context = Arc::as_ptr(&notify).cast::<c_void>();
        let mut changes = Self {
            addresses: std::ptr::null_mut(),
            interfaces: std::ptr::null_mut(),
            notify,
        };
        let status = unsafe {
            NotifyIpInterfaceChange(
                AF_UNSPEC,
                Some(interface_changed),
                context,
                false,
                &mut changes.interfaces,
            )
        };
        if status != NO_ERROR {
            bail!("register for network interface changes failed with Windows error {status}");
        }
        let status = unsafe {
            NotifyUnicastIpAddressChange(
                AF_UNSPEC,
                Some(address_changed),
                context,
                false,
                &mut changes.addresses,
            )
        };
        if status != NO_ERROR {
            bail!("register for network address changes failed with Windows error {status}");
        }
        Ok(changes)
    }

    pub async fn next(&mut self) -> Result<()> {
        self.notify.notified().await;
        Ok(())
    }
}

impl Drop for Changes {
    fn drop(&mut self) {
        for handle in [self.addresses, self.interfaces] {
            if !handle.is_null() {
                unsafe { CancelMibChangeNotify2(handle) };
            }
        }
    }
}

unsafe extern "system" fn address_changed(
    context: *const c_void,
    _: *const MIB_UNICASTIPADDRESS_ROW,
    _: MIB_NOTIFICATION_TYPE,
) {
    unsafe { &*context.cast::<Notify>() }.notify_one();
}

unsafe extern "system" fn interface_changed(
    context: *const c_void,
    _: *const MIB_IPINTERFACE_ROW,
    _: MIB_NOTIFICATION_TYPE,
) {
    unsafe { &*context.cast::<Notify>() }.notify_one();
}
