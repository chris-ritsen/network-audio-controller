use std::ffi::{c_char, c_void, CStr};

type CfTypeRef = *const c_void;
type IoObject = u32;

const IO_MAIN_PORT_DEFAULT: u32 = 0;
const SERVICE_PLANE: &CStr = c"IOService";
const MAC_ADDRESS_PROPERTY: &CStr = c"IOMACAddress";
const UTF8_STRING_ENCODING: u32 = 0x0800_0100;

#[link(name = "IOKit", kind = "framework")]
extern "C" {
    fn IOBSDNameMatching(main_port: u32, options: u32, name: *const c_char) -> *mut c_void;
    fn IOServiceGetMatchingService(main_port: u32, matching: *mut c_void) -> IoObject;
    fn IORegistryEntryGetParentEntry(
        entry: IoObject,
        plane: *const c_char,
        parent: *mut IoObject,
    ) -> i32;
    fn IORegistryEntryCreateCFProperty(
        entry: IoObject,
        key: CfTypeRef,
        allocator: CfTypeRef,
        options: u32,
    ) -> CfTypeRef;
    fn IOObjectRelease(object: IoObject) -> i32;
}

#[link(name = "CoreFoundation", kind = "framework")]
extern "C" {
    fn CFStringCreateWithCString(
        allocator: CfTypeRef,
        text: *const c_char,
        encoding: u32,
    ) -> CfTypeRef;
    fn CFGetTypeID(value: CfTypeRef) -> usize;
    fn CFDataGetTypeID() -> usize;
    fn CFDataGetLength(data: CfTypeRef) -> isize;
    fn CFDataGetBytePtr(data: CfTypeRef) -> *const u8;
    fn CFRelease(value: CfTypeRef);
}

struct RegistryEntry(IoObject);

impl RegistryEntry {
    fn new(object: IoObject) -> Option<Self> {
        (object != 0).then_some(Self(object))
    }
}

impl Drop for RegistryEntry {
    fn drop(&mut self) {
        unsafe { IOObjectRelease(self.0) };
    }
}

struct OwnedValue(CfTypeRef);

impl OwnedValue {
    fn new(value: CfTypeRef) -> Option<Self> {
        (!value.is_null()).then_some(Self(value))
    }
}

impl Drop for OwnedValue {
    fn drop(&mut self) {
        unsafe { CFRelease(self.0) };
    }
}

// getifaddrs masks link addresses on recent macOS; the I/O Registry reports the controller's.
pub(super) fn interface_mac(name: &CStr) -> Option<[u8; 6]> {
    let matching = unsafe { IOBSDNameMatching(IO_MAIN_PORT_DEFAULT, 0, name.as_ptr()) };
    if matching.is_null() {
        return None;
    }
    // IOServiceGetMatchingService consumes the matching dictionary.
    let interface =
        RegistryEntry::new(unsafe { IOServiceGetMatchingService(IO_MAIN_PORT_DEFAULT, matching) })?;
    let mut parent = 0;
    let status =
        unsafe { IORegistryEntryGetParentEntry(interface.0, SERVICE_PLANE.as_ptr(), &mut parent) };
    if status != 0 {
        return None;
    }
    let controller = RegistryEntry::new(parent)?;
    let key = OwnedValue::new(unsafe {
        CFStringCreateWithCString(
            std::ptr::null(),
            MAC_ADDRESS_PROPERTY.as_ptr(),
            UTF8_STRING_ENCODING,
        )
    })?;
    let value = OwnedValue::new(unsafe {
        IORegistryEntryCreateCFProperty(controller.0, key.0, std::ptr::null(), 0)
    })?;
    if unsafe { CFGetTypeID(value.0) != CFDataGetTypeID() || CFDataGetLength(value.0) != 6 } {
        return None;
    }
    let mut mac_address = [0u8; 6];
    unsafe {
        std::ptr::copy_nonoverlapping(CFDataGetBytePtr(value.0), mac_address.as_mut_ptr(), 6);
    }
    Some(mac_address)
}
