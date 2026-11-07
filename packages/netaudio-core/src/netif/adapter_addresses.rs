use std::ffi::{c_char, c_void};
use std::net::Ipv4Addr;

const AF_INET: u16 = 2;
const IF_OPER_STATUS_UP: u32 = 1;
const MAC_ADDRESS_LENGTH: u32 = 6;

#[repr(C)]
struct SockaddrIn {
    family: u16,
    port: u16,
    address: [u8; 4],
}

#[repr(C)]
struct SocketAddress {
    sockaddr: *const SockaddrIn,
    length: i32,
}

#[repr(C)]
struct UnicastAddress {
    alignment: u64,
    next: *const UnicastAddress,
    address: SocketAddress,
}

// The documented prefix of IP_ADAPTER_ADDRESSES_LH. Nothing past oper_status is read.
#[repr(C)]
struct AdapterAddresses {
    alignment: u64,
    next: *const AdapterAddresses,
    adapter_name: *const c_char,
    first_unicast_address: *const UnicastAddress,
    first_anycast_address: *const c_void,
    first_multicast_address: *const c_void,
    first_dns_server_address: *const c_void,
    dns_suffix: *const u16,
    description: *const u16,
    friendly_name: *const u16,
    physical_address: [u8; 8],
    physical_address_length: u32,
    flags: u32,
    mtu: u32,
    if_type: u32,
    oper_status: u32,
}

unsafe fn holds_address(adapter: &AdapterAddresses, address: Ipv4Addr) -> bool {
    let mut unicast = adapter.first_unicast_address;
    while let Some(entry) = unsafe { unicast.as_ref() } {
        if let Some(socket_address) = unsafe { entry.address.sockaddr.as_ref() } {
            if socket_address.family == AF_INET && socket_address.address == address.octets() {
                return true;
            }
        }
        unicast = entry.next;
    }
    false
}

// An adapter with no data-link layer reports a zero length, and that is the answer for its
// address: another adapter's MAC does not identify the interface carrying this IPv4 source.
unsafe fn mac_for_address(
    first_adapter: *const AdapterAddresses,
    address: Ipv4Addr,
) -> Option<[u8; 6]> {
    let mut next = first_adapter;
    while let Some(adapter) = unsafe { next.as_ref() } {
        if adapter.oper_status == IF_OPER_STATUS_UP && unsafe { holds_address(adapter, address) } {
            if adapter.physical_address_length != MAC_ADDRESS_LENGTH {
                return None;
            }
            let mut mac_address = [0u8; 6];
            mac_address.copy_from_slice(&adapter.physical_address[..6]);
            return Some(mac_address);
        }
        next = adapter.next;
    }
    None
}

#[cfg(windows)]
pub(super) fn interface_mac(address: Ipv4Addr) -> Option<[u8; 6]> {
    const ERROR_SUCCESS: u32 = 0;
    const ERROR_BUFFER_OVERFLOW: u32 = 111;
    const GAA_FLAG_SKIP_ANYCAST: u32 = 0x2;
    const GAA_FLAG_SKIP_MULTICAST: u32 = 0x4;
    const GAA_FLAG_SKIP_DNS_SERVER: u32 = 0x8;
    const INITIAL_BUFFER_BYTES: u32 = 15_000;
    const ATTEMPTS: usize = 3;

    #[link(name = "iphlpapi")]
    extern "system" {
        fn GetAdaptersAddresses(
            family: u32,
            flags: u32,
            reserved: *mut c_void,
            adapter_addresses: *mut AdapterAddresses,
            size_pointer: *mut u32,
        ) -> u32;
    }

    let mut size = INITIAL_BUFFER_BYTES;
    for _ in 0..ATTEMPTS {
        let mut buffer = vec![0u64; (size as usize).div_ceil(size_of::<u64>())];
        let status = unsafe {
            GetAdaptersAddresses(
                u32::from(AF_INET),
                GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_DNS_SERVER,
                std::ptr::null_mut(),
                buffer.as_mut_ptr().cast(),
                &mut size,
            )
        };
        match status {
            ERROR_SUCCESS => return unsafe { mac_for_address(buffer.as_ptr().cast(), address) },
            ERROR_BUFFER_OVERFLOW => {}
            _ => return None,
        }
    }
    None
}
