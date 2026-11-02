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

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::offset_of;
    use std::ptr::{null, null_mut};

    const SELECTED: Ipv4Addr = Ipv4Addr::new(192, 168, 1, 62);
    const MAC_A: [u8; 8] = [0x00, 0x0D, 0x3A, 0x01, 0x02, 0x03, 0, 0];
    const MAC_B: [u8; 8] = [0x00, 0x0D, 0x3A, 0x0A, 0x0B, 0x0C, 0, 0];

    fn sockaddr(family: u16, address: Ipv4Addr) -> SockaddrIn {
        SockaddrIn {
            family,
            port: 0,
            address: address.octets(),
        }
    }

    fn unicast(sockaddr: *const SockaddrIn, next: *const UnicastAddress) -> UnicastAddress {
        UnicastAddress {
            alignment: 0,
            next,
            address: SocketAddress {
                sockaddr,
                length: size_of::<SockaddrIn>() as i32,
            },
        }
    }

    fn adapter(
        first_unicast_address: *const UnicastAddress,
        physical_address: [u8; 8],
        physical_address_length: u32,
        oper_status: u32,
        next: *const AdapterAddresses,
    ) -> AdapterAddresses {
        AdapterAddresses {
            alignment: 0,
            next,
            adapter_name: null(),
            first_unicast_address,
            first_anycast_address: null(),
            first_multicast_address: null(),
            first_dns_server_address: null(),
            dns_suffix: null(),
            description: null(),
            friendly_name: null(),
            physical_address,
            physical_address_length,
            flags: 0,
            mtu: 1500,
            if_type: 6,
            oper_status,
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn the_adapter_prefix_has_the_documented_layout() {
        assert_eq!(offset_of!(AdapterAddresses, first_unicast_address), 24);
        assert_eq!(offset_of!(AdapterAddresses, physical_address), 80);
        assert_eq!(offset_of!(AdapterAddresses, physical_address_length), 88);
        assert_eq!(offset_of!(AdapterAddresses, oper_status), 104);
        assert_eq!(offset_of!(UnicastAddress, address), 16);
    }

    #[test]
    fn the_adapter_holding_the_source_address_supplies_the_mac() {
        let other_address = sockaddr(AF_INET, Ipv4Addr::new(10, 0, 0, 5));
        let selected_address = sockaddr(AF_INET, SELECTED);
        let second_unicast = unicast(&selected_address, null());
        let first_unicast = unicast(&other_address, &second_unicast);
        let other_unicast = unicast(&other_address, null());
        let selected = adapter(&first_unicast, MAC_B, 6, IF_OPER_STATUS_UP, null());
        let other = adapter(&other_unicast, MAC_A, 6, IF_OPER_STATUS_UP, &selected);

        assert_eq!(
            unsafe { mac_for_address(&other, SELECTED) },
            Some([0x00, 0x0D, 0x3A, 0x0A, 0x0B, 0x0C])
        );
    }

    #[test]
    fn an_adapter_without_a_six_byte_address_has_no_mac() {
        let selected_address = sockaddr(AF_INET, SELECTED);
        let selected_unicast = unicast(&selected_address, null());
        let no_link_layer = adapter(&selected_unicast, [0; 8], 0, IF_OPER_STATUS_UP, null());
        let long_address = adapter(&selected_unicast, MAC_A, 8, IF_OPER_STATUS_UP, null());

        assert_eq!(unsafe { mac_for_address(&no_link_layer, SELECTED) }, None);
        assert_eq!(unsafe { mac_for_address(&long_address, SELECTED) }, None);
    }

    #[test]
    fn another_adapters_mac_does_not_stand_in_for_the_selected_one() {
        let selected_address = sockaddr(AF_INET, SELECTED);
        let other_address = sockaddr(AF_INET, Ipv4Addr::new(10, 0, 0, 5));
        let selected_unicast = unicast(&selected_address, null());
        let other_unicast = unicast(&other_address, null());
        let other = adapter(&other_unicast, MAC_A, 6, IF_OPER_STATUS_UP, null());
        let no_link_layer = adapter(&selected_unicast, [0; 8], 0, IF_OPER_STATUS_UP, &other);

        assert_eq!(unsafe { mac_for_address(&no_link_layer, SELECTED) }, None);
        assert_eq!(unsafe { mac_for_address(&other, SELECTED) }, None);
    }

    #[test]
    fn a_down_adapter_or_unexpected_family_is_skipped() {
        const IF_OPER_STATUS_DOWN: u32 = 2;
        let selected_address = sockaddr(AF_INET, SELECTED);
        let wrong_family = sockaddr(23, SELECTED);
        let selected_unicast = unicast(&selected_address, null());
        let wrong_unicast = unicast(&wrong_family, null());
        let up = adapter(&selected_unicast, MAC_B, 6, IF_OPER_STATUS_UP, null());
        let down = adapter(&selected_unicast, MAC_A, 6, IF_OPER_STATUS_DOWN, &up);
        let wrong = adapter(&wrong_unicast, MAC_A, 6, IF_OPER_STATUS_UP, null());

        assert_eq!(
            unsafe { mac_for_address(&down, SELECTED) },
            Some([0x00, 0x0D, 0x3A, 0x0A, 0x0B, 0x0C])
        );
        assert_eq!(unsafe { mac_for_address(&wrong, SELECTED) }, None);
    }

    #[test]
    fn an_empty_list_or_address_entry_has_no_mac() {
        let empty_entry = unicast(null(), null());
        let no_address = adapter(&empty_entry, MAC_A, 6, IF_OPER_STATUS_UP, null());
        let no_unicast = adapter(null(), MAC_A, 6, IF_OPER_STATUS_UP, null());

        assert_eq!(unsafe { mac_for_address(null_mut(), SELECTED) }, None);
        assert_eq!(unsafe { mac_for_address(&no_address, SELECTED) }, None);
        assert_eq!(unsafe { mac_for_address(&no_unicast, SELECTED) }, None);
    }

    // Asks Windows through a separate path so the hand-declared layout is checked against the
    // system. Continuous integration must have an address here, or this would prove nothing.
    #[cfg(windows)]
    #[test]
    fn the_selected_adapter_matches_what_windows_reports() {
        let in_ci = std::env::var_os("CI").is_some();
        let Some(address) = crate::netif::local_ipv4().filter(|value| !value.is_unspecified())
        else {
            assert!(!in_ci, "the CI host has no selected IPv4 address");
            return;
        };
        let script = format!(
            "(Get-NetIPAddress -IPAddress {address} | Get-NetAdapter -IncludeHidden).MacAddress"
        );
        let output = std::process::Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", &script])
            .output()
            .expect("PowerShell is available on Windows");
        let reported = String::from_utf8_lossy(&output.stdout)
            .trim()
            .replace('-', "")
            .to_ascii_lowercase();
        let found = interface_mac(address).map(|mac_address| {
            mac_address
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        });
        println!("selected {address}: lookup {found:?}, PowerShell {reported:?}");
        if reported.is_empty() {
            assert!(!in_ci, "PowerShell reported no adapter for {address}");
            return;
        }
        assert_eq!(found.as_deref(), Some(reported.as_str()));
    }
}
