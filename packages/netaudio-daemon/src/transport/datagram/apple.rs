use nix::libc;
use std::ffi::c_int;
use std::io;
use std::mem::{size_of, zeroed};
use std::net::Ipv4Addr;
use std::os::fd::AsRawFd;
use tokio::net::UdpSocket;

// IP_ADD_MEMBERSHIP ignores the index in an ip_mreqn here and picks the interface itself, so a
// second interface collides with the first. MCAST_JOIN_GROUP honors the requested index.
pub fn join(socket: &UdpSocket, group: Ipv4Addr, interface_index: u32) -> io::Result<()> {
    change_membership(socket, libc::MCAST_JOIN_GROUP, group, interface_index)
}

pub fn leave(socket: &UdpSocket, group: Ipv4Addr, interface_index: u32) -> io::Result<()> {
    change_membership(socket, libc::MCAST_LEAVE_GROUP, group, interface_index)
}

fn change_membership(
    socket: &UdpSocket,
    option: c_int,
    group: Ipv4Addr,
    interface_index: u32,
) -> io::Result<()> {
    let request = group_request(group, interface_index);
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_IP,
            option,
            (&raw const request).cast(),
            size_of::<libc::group_req>() as libc::socklen_t,
        )
    };

    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn group_request(group: Ipv4Addr, interface_index: u32) -> libc::group_req {
    let mut address: libc::sockaddr_in = unsafe { zeroed() };
    address.sin_len = size_of::<libc::sockaddr_in>() as u8;
    address.sin_family = libc::AF_INET as libc::sa_family_t;
    address.sin_addr.s_addr = u32::from_ne_bytes(group.octets());

    let mut request: libc::group_req = unsafe { zeroed() };
    request.gr_interface = interface_index;
    unsafe {
        (&raw mut request.gr_group)
            .cast::<libc::sockaddr_in>()
            .write_unaligned(address);
    }
    request
}
