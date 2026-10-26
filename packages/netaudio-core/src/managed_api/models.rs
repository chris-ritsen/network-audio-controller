use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DeviceIdentity {
    pub id: String,
    #[serde(rename(deserialize = "instanceId"))]
    pub instance_id: String,
    #[serde(rename(deserialize = "defaultName"))]
    pub default_name: Option<String>,
    #[serde(rename(deserialize = "actualName"))]
    pub actual_name: Option<String>,
    #[serde(rename(deserialize = "productModelId"))]
    pub product_model_id: Option<String>,
    #[serde(rename(deserialize = "productModelName"))]
    pub product_model_name: Option<String>,
    #[serde(rename(deserialize = "productVersion"))]
    pub product_version: Option<String>,
    #[serde(rename(deserialize = "productSoftwareVersion"))]
    pub product_software_version: Option<String>,
    #[serde(rename(deserialize = "danteVersion"))]
    pub dante_version: Option<String>,
    #[serde(rename(deserialize = "danteHardwareVersion"))]
    pub dante_hardware_version: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct NamedEntity {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Platform {
    pub id: String,
    pub name: String,
    #[serde(rename(deserialize = "platformId"))]
    pub platform_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct NetworkInterface {
    pub id: String,
    #[serde(rename(deserialize = "macAddress"))]
    pub mac_address: Option<String>,
    pub address: Option<String>,
    pub netmask: Option<i64>,
    pub subnet: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DeviceConnection {
    pub id: String,
    pub state: Option<String>,
    #[serde(rename(deserialize = "lastChanged"))]
    pub last_changed: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockPreferences {
    pub id: String,
    #[serde(rename(deserialize = "externalWordClock"))]
    pub external_word_clock: Option<bool>,
    pub leader: Option<bool>,
    #[serde(rename(deserialize = "unicastClocking"))]
    pub unicast_clocking: Option<bool>,
    #[serde(rename(deserialize = "v1UnicastDelayRequests"))]
    pub v1_unicast_delay_requests: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Capabilities {
    pub id: String,
    #[serde(rename(deserialize = "CAN_WRITE_PREFERRED_MASTER"))]
    pub can_write_preferred_master: Option<bool>,
    #[serde(rename(deserialize = "CAN_WRITE_EXT_WORD_CLOCK"))]
    pub can_write_external_word_clock: Option<bool>,
    #[serde(rename(deserialize = "CAN_WRITE_SLAVE_ONLY"))]
    pub can_write_follower_only: Option<bool>,
    #[serde(rename(deserialize = "CAN_WRITE_UNICAST_DELAY_REQUESTS"))]
    pub can_write_unicast_delay_requests: Option<bool>,
    #[serde(rename(deserialize = "CAN_UNICAST_CLOCKING"))]
    pub can_unicast_clocking: Option<bool>,
    #[serde(rename(deserialize = "DDM_V_1_1_CLOCK_MESSAGES_SUPPORTED"))]
    pub ddm_v1_1_clock_messages_supported: Option<bool>,
    #[serde(rename(deserialize = "CAN_ENCRYPT_MEDIA"))]
    pub can_encrypt_media: Option<bool>,
    #[serde(rename(deserialize = "CAN_RESET"))]
    pub can_reset: Option<bool>,
    #[serde(rename(deserialize = "RTP_AUDIO_SUPPORTED"))]
    pub rtp_audio_supported: Option<bool>,
    #[serde(rename(deserialize = "RTP_AUDIO_SUPPORT_SUPPRESSED"))]
    pub rtp_audio_support_suppressed: Option<bool>,
    #[serde(rename(deserialize = "mediaTypes"))]
    pub media_types: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockingState {
    pub id: String,
    pub locked: Option<String>,
    #[serde(rename(deserialize = "grandLeader"))]
    pub grand_leader: Option<bool>,
    #[serde(rename(deserialize = "followerWithoutLeader"))]
    pub follower_without_leader: Option<bool>,
    #[serde(rename(deserialize = "multicastLeader"))]
    pub multicast_leader: Option<bool>,
    #[serde(rename(deserialize = "unicastLeader"))]
    pub unicast_leader: Option<bool>,
    #[serde(rename(deserialize = "unicastFollower"))]
    pub unicast_follower: Option<bool>,
    #[serde(rename(deserialize = "muteStatus"))]
    pub mute_status: Option<String>,
    #[serde(rename(deserialize = "frequencyOffset"))]
    pub frequency_offset: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AlertMessage {
    pub id: String,
    pub connectivity: Option<String>,
    pub clocking: Option<String>,
    pub latency: Option<String>,
    pub subscriptions: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DeviceStatus {
    pub id: String,
    pub summary: Option<String>,
    pub clocking: Option<String>,
    pub connectivity: Option<String>,
    pub latency: Option<String>,
    pub subscriptions: Option<String>,
    #[serde(rename(deserialize = "alertMessage"))]
    pub alert_message: Option<AlertMessage>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DomainStatus {
    pub summary: Option<String>,
    pub clocking: Option<String>,
    pub connectivity: Option<String>,
    pub latency: Option<String>,
    pub subscriptions: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SignalPresence {
    pub id: String,
    #[serde(rename(deserialize = "leveldBFS"))]
    pub level_dbfs: f64,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReceiverChannel {
    pub id: String,
    pub index: i64,
    pub enabled: Option<bool>,
    pub name: Option<String>,
    #[serde(rename(deserialize = "subscribedDevice"))]
    pub subscribed_device: Option<String>,
    #[serde(rename(deserialize = "subscribedChannel"))]
    pub subscribed_channel: Option<String>,
    pub status: Option<String>,
    #[serde(rename(deserialize = "statusMessage"))]
    pub status_message: Option<String>,
    pub summary: Option<String>,
    #[serde(rename(deserialize = "mediaType"))]
    pub media_type: Option<String>,
    #[serde(rename(deserialize = "encryptionScheme"))]
    pub encryption_scheme: Option<String>,
    #[serde(rename(deserialize = "canSubscribeSelf"))]
    pub can_subscribe_self: Option<bool>,
    #[serde(rename(deserialize = "signalPresence"))]
    pub signal_presence: Option<SignalPresence>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TransmitterChannel {
    pub id: String,
    pub index: i64,
    pub name: Option<String>,
    #[serde(rename(deserialize = "mediaType"))]
    pub media_type: Option<String>,
    #[serde(rename(deserialize = "encryptionPolicy"))]
    pub encryption_policy: Option<String>,
    #[serde(rename(deserialize = "signalPresence"))]
    pub signal_presence: Option<SignalPresence>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DeviceParameter {
    #[serde(rename(deserialize = "__typename"))]
    pub typename: String,
    pub id: String,
    pub path: String,
    pub key: String,
    #[serde(deserialize_with = "required_value")]
    pub value: Value,
    pub label: String,
    pub settable: bool,
    #[serde(default, rename(deserialize = "defaultValue"))]
    pub default_value: Value,
    pub units: Option<String>,
    #[serde(rename(deserialize = "applyMode"))]
    pub apply_mode: Option<String>,
    pub group: Option<String>,
    #[serde(rename(deserialize = "renderingHint"))]
    pub rendering_hint: Option<String>,
    pub options: Option<Vec<Value>>,
    #[serde(default, rename(deserialize = "min"))]
    pub minimum: Value,
    #[serde(default, rename(deserialize = "max"))]
    pub maximum: Value,
    #[serde(default)]
    pub precision: Value,
    pub regex: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DevicePort {
    #[serde(rename(deserialize = "__typename"))]
    pub typename: String,
    pub id: String,
    pub key: String,
    pub parameters: Option<Vec<Option<DeviceParameter>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Device {
    pub id: String,
    pub name: String,
    #[serde(rename(deserialize = "domainId"))]
    pub domain_id: Option<String>,
    pub r#type: Option<String>,
    #[serde(rename(deserialize = "enrolmentState"))]
    pub enrolment_state: Option<String>,
    pub identity: Option<DeviceIdentity>,
    pub manufacturer: Option<NamedEntity>,
    pub platform: Option<Platform>,
    pub product: Option<NamedEntity>,
    pub interfaces: Option<Vec<Option<NetworkInterface>>>,
    pub connection: Option<DeviceConnection>,
    #[serde(rename(deserialize = "clockPreferences"))]
    pub clock_preferences: Option<ClockPreferences>,
    pub capabilities: Option<Capabilities>,
    #[serde(rename(deserialize = "clockingState"))]
    pub clocking_state: Option<ClockingState>,
    pub status: Option<DeviceStatus>,
    #[serde(rename(deserialize = "rxChannels"))]
    pub rx_channels: Option<Vec<ReceiverChannel>>,
    #[serde(rename(deserialize = "txChannels"))]
    pub tx_channels: Option<Vec<TransmitterChannel>>,
    pub parameters: Option<Vec<DeviceParameter>>,
    pub inputs: Option<Vec<Option<DevicePort>>>,
    pub outputs: Option<Vec<Option<DevicePort>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Domain {
    pub id: String,
    pub name: Option<String>,
    pub status: Option<DomainStatus>,
    pub devices: Option<Vec<Option<Device>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Inventory {
    pub domains: Option<Vec<Option<Domain>>>,
    #[serde(rename(deserialize = "unenrolledDevices"))]
    pub unenrolled_devices: Option<Vec<Device>>,
}

fn required_value<'de, Deserializer>(
    deserializer: Deserializer,
) -> Result<Value, Deserializer::Error>
where
    Deserializer: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    if value.is_null() {
        return Err(serde::de::Error::custom("parameter value must not be null"));
    }

    Ok(value)
}
