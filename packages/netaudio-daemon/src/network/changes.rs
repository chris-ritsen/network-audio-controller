#[cfg(target_os = "ios")]
mod ios;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
mod windows;

#[cfg(target_os = "ios")]
pub use ios::{announce, Changes};
#[cfg(target_os = "linux")]
pub use linux::Changes;
#[cfg(target_os = "macos")]
pub use macos::Changes;
#[cfg(windows)]
pub use windows::Changes;
