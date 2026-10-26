mod authorization;
mod body;
mod connection;
pub mod devices;
pub mod documentation;
pub mod external_flows;
pub mod inventory;
pub mod journal;
pub mod managed;
pub mod mcp;
pub mod presets;
mod response;
pub mod versioning;
pub mod web;

pub use authorization::AuthorizationService;
pub use body::RequestBody;
pub use connection::ConnectionContext;
pub use response::ApiError;

pub const MAXIMUM_REQUEST_BYTES: usize = 8 * 1024 * 1024;
pub mod host_audio;
pub mod settings;
pub mod shure;
