use netaudio_core::capabilities;
use schemars::{generate::SchemaSettings, JsonSchema};
use serde_json::{json, Value};

macro_rules! registry {
    ({ $($name:literal: $schema:expr),* $(,)? }) => {
        serde_json::Map::from_iter([$(($name.to_owned(), $schema)),*])
    };
}

fn output<T: JsonSchema>() -> Value {
    serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_serialize()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .expect("schema is serializable")
}

fn main() {
    use netaudio_core::protocol::{
        MULTICAST_GROUP_CONTROL_MONITORING, MULTICAST_GROUP_HEARTBEAT, SERVICE_ARC, SERVICE_CHAN,
        SERVICE_CMC, SERVICE_DBC, SERVICE_VIDEO,
    };
    let outputs = registry!({
        "MeteringScales": output::<netaudio_core::metering::MeteringScales>(),
        "SubscriptionStatus": output::<netaudio_core::subscription_status::SubscriptionStatus>(),
        "ClockSubdomainPresentation": output::<netaudio_core::clock_configuration::ClockSubdomainPresentation>(),
        "ClockSources": output::<netaudio_core::clock_configuration::ClockSources>(),
        "Availability": output::<capabilities::Availability>(),
    });
    let inputs = registry!({
        "ClockSourceFacts": input::<netaudio_core::clock_configuration::ClockSourceFacts>(),
        "DeviceIdentityRequest": input::<netaudio_core::device_identity::DeviceIdentityRequest>(),
        "AvailabilityRequest": input::<capabilities::AvailabilityRequest>(),
    });
    let schemas = json!({
        "inputs": inputs,
        "outputs": outputs,
        "protocols": netaudio_core::protocol::protocol_catalog(),
        "discovery": {
            "CLOCK_CONTROL_PROFILES": {"current": 0x073au16, "legacy": 0x0734u16},
            "CONTROLLER_AUTH_PORT": netaudio_core::controller_api::AUTH_PORT,
            "CONTROLLER_VERSIONS_PATH": netaudio_core::controller_api::VERSIONS_PATH,
            "SAP_MULTICAST_ADDRESS": netaudio_core::sap::MULTICAST_ADDRESS,
            "SAP_PORT": netaudio_core::sap::PORT,
            "SERVICE_ARC": SERVICE_ARC, "SERVICE_CHAN": SERVICE_CHAN, "SERVICE_CMC": SERVICE_CMC,
            "SERVICE_DBC": SERVICE_DBC, "SERVICE_VIDEO": SERVICE_VIDEO,
            "SERVICES": [SERVICE_ARC, SERVICE_CHAN, SERVICE_CMC, SERVICE_DBC, SERVICE_VIDEO],
            "MEDIA_SERVICE_TYPES": [SERVICE_CHAN, SERVICE_VIDEO],
            "MULTICAST_GROUP_HEARTBEAT": MULTICAST_GROUP_HEARTBEAT,
            "MULTICAST_GROUP_CONTROL_MONITORING": MULTICAST_GROUP_CONTROL_MONITORING,
        },
        "audio_choices": capabilities::sample_rate_pullup_choices(),
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&schemas).expect("schema is serializable")
    );
}

fn input<T: JsonSchema>() -> Value {
    serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_deserialize()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .expect("schema is serializable")
}
