use std::net::{Ipv4Addr, UdpSocket};

const DANTE_CLOCK_MULTICAST: &str = "224.0.0.231";

pub fn local_ipv4() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind(("0.0.0.0", 0)).ok()?;
    socket.connect((DANTE_CLOCK_MULTICAST, 1)).ok()?;
    match socket.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(address) => Some(address),
        _ => None,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
pub fn discover_host_mac() -> Option<[u8; 6]> {
    let local_octets = local_ipv4()?.octets();
    let interfaces = InterfaceAddresses::collect()?;
    let name = interfaces
        .entries()
        .find(|entry| ipv4_octets(entry) == Some(local_octets))
        .map(interface_name)?;
    let mac_address = interfaces
        .entries()
        .filter(|entry| interface_name(entry) == name)
        .filter_map(link_mac_address)
        .find(|mac_address| mac_address.iter().any(|byte| *byte != 0));
    mac_address
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
struct InterfaceAddresses {
    head: *mut libc::ifaddrs,
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
impl InterfaceAddresses {
    fn collect() -> Option<InterfaceAddresses> {
        let mut head = std::ptr::null_mut();
        if unsafe { libc::getifaddrs(&mut head) } != 0 {
            return None;
        }
        Some(InterfaceAddresses { head })
    }

    fn entries(&self) -> impl Iterator<Item = &libc::ifaddrs> {
        let mut next = self.head as *const libc::ifaddrs;
        std::iter::from_fn(move || {
            let current = unsafe { next.as_ref() }?;
            next = current.ifa_next;
            Some(current)
        })
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
impl Drop for InterfaceAddresses {
    fn drop(&mut self) {
        unsafe { libc::freeifaddrs(self.head) };
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
fn interface_name(entry: &libc::ifaddrs) -> &std::ffi::CStr {
    unsafe { std::ffi::CStr::from_ptr(entry.ifa_name) }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
fn ipv4_octets(entry: &libc::ifaddrs) -> Option<[u8; 4]> {
    if entry.ifa_addr.is_null() {
        return None;
    }
    let address = unsafe { &*entry.ifa_addr };
    if address.sa_family as i32 != libc::AF_INET {
        return None;
    }
    let internet_address = unsafe { &*(entry.ifa_addr as *const libc::sockaddr_in) };
    Some(internet_address.sin_addr.s_addr.to_ne_bytes())
}

#[cfg(target_os = "linux")]
fn link_mac_address(entry: &libc::ifaddrs) -> Option<[u8; 6]> {
    if entry.ifa_addr.is_null() {
        return None;
    }
    let address = unsafe { &*entry.ifa_addr };
    if address.sa_family as i32 != libc::AF_PACKET {
        return None;
    }
    let link_address = unsafe { &*(entry.ifa_addr as *const libc::sockaddr_ll) };
    if link_address.sll_halen != 6 {
        return None;
    }
    let mut mac_address = [0u8; 6];
    mac_address.copy_from_slice(&link_address.sll_addr[..6]);
    Some(mac_address)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn link_mac_address(entry: &libc::ifaddrs) -> Option<[u8; 6]> {
    if entry.ifa_addr.is_null() {
        return None;
    }
    let address = unsafe { &*entry.ifa_addr };
    if address.sa_family as i32 != libc::AF_LINK {
        return None;
    }
    let link_address = unsafe { &*(entry.ifa_addr as *const libc::sockaddr_dl) };
    if link_address.sdl_alen != 6 {
        return None;
    }
    let mut mac_address = [0u8; 6];
    unsafe {
        let payload = link_address.sdl_data.as_ptr() as *const u8;
        std::ptr::copy_nonoverlapping(
            payload.add(link_address.sdl_nlen as usize),
            mac_address.as_mut_ptr(),
            6,
        );
    }
    Some(mac_address)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
pub fn discover_host_mac() -> Option<[u8; 6]> {
    None
}
