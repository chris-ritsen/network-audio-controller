use anyhow::{Context, Result, ensure};
use schemars::generate::{Contract, SchemaSettings};
use schemars::{JsonSchema, SchemaGenerator};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

pub(super) fn generate() -> Result<Value> {
    let mut document: Value = serde_json::from_str(include_str!("../../../assets/openapi.json"))?;
    document["info"]["version"] = crate::http::versioning::CONTRACT_VERSION.into();
    for methods in document["paths"]
        .as_object_mut()
        .context("OpenAPI paths missing")?
        .values_mut()
    {
        for operation in methods
            .as_object_mut()
            .context("invalid path item")?
            .values_mut()
        {
            let parameters = operation
                .as_object_mut()
                .context("invalid operation")?
                .entry("parameters")
                .or_insert_with(|| serde_json::json!([]));
            parameters.as_array_mut().context("invalid operation parameters")?.push(serde_json::json!({
                "name": "NetAudio-API-Version", "in": "header", "required": false,
                "description": "Pin the public API major. Omission selects 1. Unsupported values are rejected before dispatch.",
                "schema": {"type": "string", "enum": ["1"], "default": "1"}
            }));
            let responses = operation["responses"]
                .as_object_mut()
                .context("operation responses missing")?;
            for (status, description) in [
                ("400", "Invalid request or duplicate API version headers."),
                ("406", "The requested public API version is not supported."),
            ] {
                responses.entry(status).or_insert_with(|| serde_json::json!({
                    "description": description,
                    "content": {"application/json": {"schema": {"type": "object", "required": ["error"], "properties": {"error": {"type": "string"}}}}}
                }));
            }
            for response in responses.values_mut() {
                if response.get("$ref").is_none() {
                    let headers = response
                        .as_object_mut()
                        .context("invalid response")?
                        .entry("headers")
                        .or_insert_with(|| serde_json::json!({}));
                    headers["NetAudio-API-Version"] = serde_json::json!({"description": "Selected public API major.", "schema": {"type": "string", "const": "1"}});
                }
            }
        }
    }
    let schemas = document["components"]["schemas"]
        .as_object_mut()
        .context("OpenAPI schemas must be an object")?;
    let mut settings = SchemaSettings::draft2020_12();
    settings.definitions_path = "/components/schemas".into();
    settings.contract = Contract::Serialize;
    let mut generator = settings.into_generator();
    let mut roots = Map::new();
    register::<netaudio_core::host_audio::recording::Record>(
        &mut generator,
        &mut roots,
        "HostRecordingRecord",
    )?;
    register::<crate::http::host_audio::HistoryReading>(
        &mut generator,
        &mut roots,
        "HostHistorySummary",
    )?;
    register::<netaudio_core::host_audio::measurement::Reading>(
        &mut generator,
        &mut roots,
        "HostMeterReading",
    )?;
    register::<netaudio_core::host_audio::snapshot::JackSnapshot>(
        &mut generator,
        &mut roots,
        "HostJackSnapshot",
    )?;
    register::<netaudio_core::host_audio::snapshot::PulseSnapshot>(
        &mut generator,
        &mut roots,
        "HostPulseSnapshot",
    )?;
    register::<netaudio_core::host_audio::cards::Card>(
        &mut generator,
        &mut roots,
        "HostSoundCard",
    )?;
    register::<netaudio_core::host_audio::cards::Bridge>(
        &mut generator,
        &mut roots,
        "HostSoundBridge",
    )?;
    register::<crate::devices::capabilities::OperationAvailability<'_>>(
        &mut generator,
        &mut roots,
        "Availability",
    )?;
    register::<netaudio_core::commands::ClockControlAvailability>(
        &mut generator,
        &mut roots,
        "ClockControlAvailability",
    )?;
    register::<netaudio_core::responses::PtpClockStatus>(
        &mut generator,
        &mut roots,
        "PtpClockStatus",
    )?;
    register::<netaudio_core::subscription_status::SubscriptionStatus>(
        &mut generator,
        &mut roots,
        "SubscriptionStatus",
    )?;
    register::<netaudio_core::metering::MeteringScales>(
        &mut generator,
        &mut roots,
        "MeteringScales",
    )?;
    register::<netaudio_core::flow_readback::FlowComparison>(
        &mut generator,
        &mut roots,
        "FlowComparison",
    )?;
    register::<netaudio_core::responses::CommandReceipt>(
        &mut generator,
        &mut roots,
        "CommandReceipt",
    )?;
    register::<netaudio_core::sample_rate_topology::TopologyImpact>(
        &mut generator,
        &mut roots,
        "TopologyImpact",
    )?;
    register::<crate::monitoring::journal::store::EventPage>(
        &mut generator,
        &mut roots,
        "EventPage",
    )?;
    register::<crate::monitoring::journal::model::JournalEvent>(
        &mut generator,
        &mut roots,
        "JournalEvent",
    )?;
    register::<netaudio_core::device_controls::planning::PanelPlan>(
        &mut generator,
        &mut roots,
        "PanelPlan",
    )?;
    register::<netaudio_core::device_controls::presentation::PanelPresentation>(
        &mut generator,
        &mut roots,
        "PanelPresentation",
    )?;
    register::<crate::monitoring::issues::store::Page>(&mut generator, &mut roots, "IssuePage")?;
    register::<netaudio_core::presets::planning::Action>(
        &mut generator,
        &mut roots,
        "PresetAction",
    )?;
    register::<crate::presets::report::Report>(&mut generator, &mut roots, "PresetLoadReport")?;
    append(schemas, generator.definitions().clone())?;
    append(schemas, roots)?;

    let mut settings = SchemaSettings::draft2020_12();
    settings.definitions_path = "/components/schemas".into();
    let mut generator = settings.into_generator();
    let mut roots = Map::new();
    register::<crate::http::host_audio::HistoryRequest>(
        &mut generator,
        &mut roots,
        "HostHistoryRequest",
    )?;
    register::<crate::host_audio::signal::TraceRequest>(
        &mut generator,
        &mut roots,
        "HostTraceRequest",
    )?;
    register::<crate::host_audio::signal::PointsRequest>(
        &mut generator,
        &mut roots,
        "HostPointLevelsRequest",
    )?;
    register::<crate::http::host_audio::RecordingsRequest>(
        &mut generator,
        &mut roots,
        "HostRecordingsRequest",
    )?;
    register::<netaudio_core::host_audio::measurement::Request>(
        &mut generator,
        &mut roots,
        "HostMeterRequest",
    )?;
    register::<netaudio_core::sample_rate_topology::TopologySnapshot>(
        &mut generator,
        &mut roots,
        "TopologySnapshot",
    )?;
    register::<netaudio_core::commands::ClockPortControl>(
        &mut generator,
        &mut roots,
        "ClockPortControl",
    )?;
    register::<netaudio_core::flow_specification::TransmitFlowSpecification>(
        &mut generator,
        &mut roots,
        "TransmitFlowSpecification",
    )?;
    register::<crate::monitoring::journal::model::OperationRequest>(
        &mut generator,
        &mut roots,
        "OperationRequest",
    )?;
    append(schemas, generator.definitions().clone())?;
    append(schemas, roots)?;
    validate_references(&document, &document)?;
    let mut identifiers = BTreeSet::new();

    for operations in document["paths"]
        .as_object()
        .context("OpenAPI paths missing")?
        .values()
    {
        for operation in operations
            .as_object()
            .context("invalid path item")?
            .values()
        {
            let identifier = operation["operationId"]
                .as_str()
                .context("operation ID missing")?;
            ensure!(
                identifiers.insert(identifier),
                "duplicate operation ID {identifier}"
            );
        }
    }

    Ok(document)
}

fn register<Schema: JsonSchema>(
    generator: &mut SchemaGenerator,
    roots: &mut Map<String, Value>,
    name: &str,
) -> Result<()> {
    let schema = serde_json::to_value(generator.subschema_for::<Schema>())?;

    if schema["$ref"].as_str() != Some(&format!("#/components/schemas/{name}")) {
        roots.insert(name.to_owned(), schema);
    }

    Ok(())
}

fn append(schemas: &mut Map<String, Value>, additions: Map<String, Value>) -> Result<()> {
    for (name, schema) in additions {
        if let Some(existing) = schemas.get(&name) {
            ensure!(*existing == schema, "conflicting API schema {name}");
        } else {
            schemas.insert(name, schema);
        }
    }

    Ok(())
}

fn validate_references(value: &Value, document: &Value) -> Result<()> {
    match value {
        Value::Object(fields) => {
            if let Some(reference) = fields.get("$ref").and_then(Value::as_str) {
                let pointer = reference
                    .strip_prefix('#')
                    .context("external API schema reference")?;
                ensure!(
                    document.pointer(pointer).is_some(),
                    "unresolved API reference {reference}"
                );
            }

            for value in fields.values() {
                validate_references(value, document)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_references(value, document)?;
            }
        }
        _ => {}
    }

    Ok(())
}
