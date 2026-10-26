use super::catalog::{Catalog, Tool};
use super::protocol;
use super::subscriptions::Subscriptions;
use super::views::{self, Records};
use super::writes::Writes;
use crate::authorization::{Grant, WRITE_SCOPE};
use crate::devices::operations::ControlOperation;
use crate::devices::registry::{DeviceRegistry, DiagnosticOptions};
use crate::state::ReadModel;
use futures_util::{StreamExt, stream};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

mod cache;
mod host_audio;
mod presets;

const METER_SNAPSHOT_CONCURRENCY: usize = 16;
const INSTRUCTIONS: &str = "Use get_network_overview for network health, get_routing for the patch, find_channels to locate channels by label, and get_device for details. Use discover_tools to find other operations and their exact schemas, then invoke_tool to run them. Before a write, call it without confirmed to see the proposed change. Tell the user what will change and wait for agreement, then confirm with the preview_token. A device that is off the network and routes waiting for it are not failures. Preserve source timestamps and distinguish command acceptance from observed device state.";

pub struct Service {
    host_audio: Option<Arc<crate::host_audio::manager::Manager>>,
    signal: Option<Arc<crate::host_audio::signal::Service>>,
    observations: Arc<crate::host_audio::observations::Observations>,
    pub catalog: Catalog,
    pub information: Arc<Value>,
    pub model: Arc<ReadModel>,
    pub subscriptions: Subscriptions,
    pub apps: Arc<Apps>,
    writes: Writes,
    registry: Arc<DeviceRegistry>,
    metering: Arc<crate::monitoring::metering_streams::MeteringStreams>,
    cancellation: CancellationToken,
    journal: Arc<crate::monitoring::journal::service::Journal>,
    presets: Arc<crate::presets::Service>,
    managed: Arc<crate::managed::registry::Registry>,
    shure: Arc<crate::shure::Registry>,
    overview: cache::ViewCache,
    device_list: cache::ViewCache,
}

pub struct Dependencies {
    pub host_audio: Option<Arc<crate::host_audio::manager::Manager>>,
    pub signal: Option<Arc<crate::host_audio::signal::Service>>,
    pub observations: Arc<crate::host_audio::observations::Observations>,
    pub model: Arc<ReadModel>,
    pub information: Arc<Value>,
    pub registry: Arc<DeviceRegistry>,
    pub metering: Arc<crate::monitoring::metering_streams::MeteringStreams>,
    pub cancellation: CancellationToken,
    pub journal: Arc<crate::monitoring::journal::service::Journal>,
    pub presets: Arc<crate::presets::Service>,
    pub managed: Arc<crate::managed::registry::Registry>,
    pub shure: Arc<crate::shure::Registry>,
}

#[derive(Debug)]
pub struct CallError {
    pub code: i32,
    pub message: String,
}

impl CallError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: protocol::INVALID_PARAMS,
            message: message.into(),
        }
    }

    pub(super) fn unavailable(operation: &str) -> Self {
        Self::invalid(format!("{operation} is not available in this daemon build"))
    }
}

impl From<anyhow::Error> for CallError {
    fn from(error: anyhow::Error) -> Self {
        let code = if error.is::<crate::state::AmbiguousDevice>() {
            protocol::INVALID_PARAMS
        } else {
            protocol::INTERNAL_ERROR
        };
        Self {
            code,
            message: error.to_string(),
        }
    }
}

impl Service {
    pub fn new(dependencies: Dependencies) -> anyhow::Result<Arc<Self>> {
        let Dependencies {
            host_audio,
            signal,
            observations,
            model,
            information,
            registry,
            metering,
            cancellation,
            journal,
            presets,
            managed,
            shure,
        } = dependencies;
        Ok(Arc::new(Self {
            catalog: Catalog::load()?,
            information,
            apps: Apps::new(
                Arc::clone(&model),
                Arc::clone(&registry),
                cancellation.clone(),
            ),
            subscriptions: Subscriptions::default(),
            writes: Writes::new(
                Arc::clone(&registry),
                Arc::clone(&model),
                Arc::clone(&metering),
                Arc::clone(&presets),
                Arc::clone(&managed),
                Arc::clone(&shure),
                host_audio.clone(),
            )
            .with_signal(signal.clone()),
            registry,
            model,
            metering,
            cancellation,
            journal,
            presets,
            managed,
            shure,
            host_audio,
            signal,
            observations,
            overview: cache::ViewCache::default(),
            device_list: cache::ViewCache::default(),
        }))
    }

    pub fn identity(&self) -> Value {
        json!({"name": "netaudio", "version": self.information["version"]})
    }

    pub async fn call(
        &self,
        method: &str,
        parameters: &Map<String, Value>,
        grant: &Grant,
    ) -> Result<Value, CallError> {
        match method {
            "initialize" => {
                self.apps.note_client(
                    parameters.get("capabilities").unwrap_or(&Value::Null),
                    grant,
                )?;
                Ok(json!({
                "protocolVersion": protocol::LEGACY_VERSION,
                "capabilities": {
                    "prompts": {"listChanged": false},
                    "resources": {"listChanged": false},
                    "tools": {"listChanged": false},
                },
                "serverInfo": self.identity(),
                "instructions": self.instructions(),
                }))
            }
            "server/discover" => Ok(json!({
                "supportedVersions": [protocol::MODERN_VERSION],
                "capabilities": {
                    "prompts": {"listChanged": true},
                    "resources": {"listChanged": true},
                    "tools": {"listChanged": true},
                },
                "instructions": self.instructions(),
            })),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(self.catalog.public_tools.clone()),
            "tools/call" => self.tool_call(parameters, grant).await,
            "resources/list" => {
                let mut resources: Vec<_> = self
                    .catalog
                    .resources
                    .iter()
                    .map(|resource| resource.document())
                    .collect();
                resources.push(apps::resource());
                Ok(json!({"resources": resources}))
            }
            "resources/templates/list" => Ok(json!({
                "resourceTemplates": [self.catalog.device_resource_template],
            })),
            "resources/read" => self.resource(parameters).await,
            "prompts/list" => Ok(json!({"prompts": []})),
            _ => Err(CallError {
                code: protocol::METHOD_NOT_FOUND,
                message: format!("unknown method {method}"),
            }),
        }
    }

    async fn tool_call(
        &self,
        parameters: &Map<String, Value>,
        grant: &Grant,
    ) -> Result<Value, CallError> {
        let mut name = parameters
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let empty = json!({});
        let mut arguments = parameters
            .get("arguments")
            .filter(|value| !value.is_null())
            .unwrap_or(&empty);
        let mut indirect = false;
        let supports_apps = self.apps.client_support(parameters, grant)?;

        if let Some(tool) = self.catalog.app_tools.get(name) {
            let problems = tool.argument_errors(arguments);

            if !problems.is_empty() {
                return Ok(protocol::tool_result(
                    json!({
                        "error": format!("invalid arguments for {name}"),
                        "problems": problems,
                        "input_schema": tool.definition.input_schema,
                    }),
                    true,
                ));
            }

            return self.apps.call(name, arguments, grant, supports_apps).await;
        }

        loop {
            if let Some(planned) = self.catalog.planned.get(name) {
                return Ok(protocol::tool_result(planned.unavailable(), true));
            }

            let tool = self
                .catalog
                .tools
                .get(name)
                .ok_or_else(|| self.unknown_tool(name))?;
            let Some(fields) = arguments.as_object() else {
                return Err(CallError::invalid("arguments must be an object"));
            };
            let problems = tool.argument_errors(arguments);

            if !problems.is_empty() {
                return Ok(protocol::tool_result(
                    json!({
                        "error": format!("invalid arguments for {name}"),
                        "problems": problems,
                        "input_schema": tool.definition.input_schema,
                    }),
                    true,
                ));
            }

            if name == "discover_tools" {
                return Ok(protocol::tool_result(
                    self.catalog.discover(
                        arguments["query"].as_str().unwrap_or_default(),
                        arguments["limit"].as_u64().unwrap_or(20) as usize,
                        arguments["offset"].as_u64().unwrap_or_default() as usize,
                        arguments["include_schema"].as_bool().unwrap_or_default(),
                    ),
                    false,
                ));
            }

            if name == "invoke_tool" {
                name = arguments["name"].as_str().unwrap_or_default();

                if indirect || matches!(name, "discover_tools" | "invoke_tool") {
                    return Ok(protocol::tool_result(
                        json!({"error": "invoke_tool runs operations; call discover_tools directly"}),
                        true,
                    ));
                }

                arguments = arguments.get("arguments").unwrap_or(&empty);
                indirect = true;
                continue;
            }

            if name == "show_routing_matrix" {
                let records = self.model.records()?;
                let routing = views::routing_view(&records, &json!({}));
                let heading = if indirect {
                    "The routing matrix was not shown: apps open it only when show_routing_matrix is called directly, not through invoke_tool. If it is missing from your tools, the app's list of netaudio tools is out of date and needs refreshing.".to_owned()
                } else if supports_apps {
                    let narrowed: Vec<_> = ["receivers", "transmitters"]
                        .iter()
                        .filter_map(|side| {
                            arguments[*side]
                                .as_str()
                                .filter(|value| !value.is_empty())
                                .map(|value| format!("{side} matching {value:?}"))
                        })
                        .collect();
                    if narrowed.is_empty() {
                        "The routing matrix is open for the user.".to_owned()
                    } else {
                        format!(
                            "The routing matrix is open for the user, showing {}.",
                            narrowed.join(" and ")
                        )
                    }
                } else {
                    "This app cannot display the interactive routing matrix.".to_owned()
                };
                return Ok(
                    json!({"content": [{"type": "text", "text": format!("{heading} Current routes: {routing}")}], "isError": false}),
                );
            }

            if !grant.can_write() && !tool.definition.read_only {
                return Ok(protocol::tool_result(
                    json!({
                        "error": format!("{} was granted read-only access; {name} needs the {WRITE_SCOPE} scope.", grant.client_name),
                    }),
                    true,
                ));
            }

            let mut request = tool.definition.defaults.clone();
            request.extend(fields.clone());
            let request = Value::Object(request);
            if Writes::supports(name) {
                return Ok(match self.writes.call(name, &request, grant).await {
                    Ok((payload, failed)) => protocol::tool_result(payload, failed),
                    Err(error) => protocol::tool_result(json!({"error": error.message}), true),
                });
            }

            let result = self.dispatch(tool, &request).await;
            return Ok(match result {
                Ok(payload) => protocol::tool_result(payload, false),
                Err(error) => protocol::tool_result(json!({"error": error.message}), true),
            });
        }
    }

    async fn dispatch(&self, tool: &Tool, arguments: &Value) -> Result<Value, CallError> {
        if tool.definition.name == "get_network_overview" {
            return self.network_overview().await;
        }
        if tool.definition.name == "trace_signal" {
            let service = self
                .signal
                .as_ref()
                .ok_or_else(|| CallError::invalid("Host audio support is not running"))?;
            let request = serde_json::from_value(arguments.clone())
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let outcome = service.trace(request).await?;
            if outcome.status >= 400 {
                return Err(CallError::invalid(views::text(&outcome.body["error"])));
            }
            return Ok(views::compact(outcome.body));
        }
        match tool.definition.name.as_str() {
            "get_ddm_status" => return Ok(self.managed.status()?),
            "get_ddm_connections" => return Ok(self.managed.connections()?),
            "get_ddm_domains" => {
                return Ok(views::managed::domains(
                    self.managed.domains(None)?,
                    arguments,
                ));
            }
            "refresh_ddm" => {
                let output = self
                    .managed
                    .execute(crate::managed::operations::Operation::Refresh {
                        context: arguments["context"].as_str().map(str::to_owned),
                    })
                    .await
                    .map_err(|error| CallError::invalid(error.to_string()))?;
                if output.status >= 400 {
                    return Err(CallError::invalid(
                        "Managed inventory refresh failed; inspect get_ddm_status for the source error",
                    ));
                }
                return Ok(output.value);
            }
            _ => {}
        }
        if matches!(
            tool.definition.name.as_str(),
            "inspect_device_controls" | "plan_device_control"
        ) {
            let device = arguments["device"]
                .as_str()
                .ok_or_else(|| CallError::invalid("device must be a string"))?;
            let operation = if tool.definition.name == "inspect_device_controls" {
                crate::devices::panels::Operation::Inspect
            } else {
                crate::devices::panels::Operation::Plan(
                    super::panels::change(arguments, &self.registry).await?,
                )
            };
            let completion = self
                .registry
                .apply(device, ControlOperation::Panel(operation))
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let observation = completion
                .observation
                .ok_or_else(|| CallError::unavailable("device controls"))?;
            return Ok(if tool.definition.name == "inspect_device_controls" {
                views::panels::controls(&observation, arguments)
            } else {
                views::panels::plan(&observation["plan"])
            });
        }

        if tool.definition.name == "list_presets" {
            return self.presets.list().await.map_err(CallError::from);
        }

        if tool.definition.name == "preview_preset" {
            return self.preview_preset(arguments).await;
        }

        if tool.definition.name == "get_issues" {
            let records = self.inventory().await?;
            let mut request = arguments.clone();
            if let Some(selector) = arguments["device"].as_str()
                && let Some(record) = views::resolve(&records, selector)?
            {
                request["device"] = record["server_name"].clone();
            }
            let page = serde_json::to_value(self.journal.issues(Default::default()).await?)
                .map_err(anyhow::Error::from)?;
            let mut view = views::issues_view(&page, &request, &records);
            if arguments["device"].is_null() {
                let conditions = if let Some(signal) = &self.signal {
                    signal.conditions().await?
                } else {
                    views::wireless::conditions(&self.shure.records()?)
                };
                if !conditions.is_empty() {
                    view["host_and_wireless"] = json!(conditions);
                }
            }
            return Ok(view);
        }

        if tool.definition.name == "get_event_journal" {
            let mut filter = crate::monitoring::journal::model::EventFilter::parse(arguments)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            if let Some(device) = &filter.device
                && let Some(record) = views::resolve(&self.model.records()?, device)?
            {
                filter.device = record["server_name"].as_str().map(str::to_owned);
            }
            filter.limit = None;
            let page = serde_json::to_value(self.journal.page(filter).await?)
                .map_err(anyhow::Error::from)?;
            return Ok(views::events_view(&page, arguments));
        }

        if tool.definition.name == "get_signal_levels" {
            return self.signal_levels(arguments).await;
        }

        if tool.definition.name == "plan_transmit_flow" {
            let selector = arguments["device"]
                .as_str()
                .ok_or_else(|| CallError::invalid("device must be a string"))?;
            let operation = crate::devices::flow_authoring::Operation::create(
                &arguments["specification"],
                true,
            )
            .map_err(|error| CallError::invalid(error.to_string()))?;
            let completion = self
                .registry
                .plan_transmit_flow(selector, &operation)
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let observation = completion
                .observation
                .ok_or_else(|| CallError::unavailable("flow plan"))?;
            let records = self.model.records()?;
            let device = require_device(&records, &arguments["device"])?;
            let mut view = views::flow_plan_view(&observation["plan"], device);

            if view["supported"] == true {
                view["next"] = json!("create_transmit_flow with the same specification creates it");
            }

            return Ok(view);
        }

        if tool.definition.name == "get_transmit_flows" {
            let selector = arguments["device"]
                .as_str()
                .ok_or_else(|| CallError::invalid("device must be a string"))?;
            let completion = self
                .registry
                .apply(selector, ControlOperation::InspectTransmitFlows)
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let inventory = completion
                .observation
                .ok_or_else(|| CallError::unavailable("transmitter flow readback"))?;

            if arguments["detail"] == "debug" {
                return Ok(inventory);
            }

            let records = self.model.records()?;
            let device = require_device(&records, &arguments["device"])?;
            return Ok(views::transmit_flows_view(&inventory, device, &records));
        }

        let name = tool.definition.name.as_str();
        let records = if matches!(name, "list_devices" | "get_device" | "get_device_record") {
            self.inventory().await?
        } else {
            self.model.records()?
        };
        let payload = match name {
            "get_host_audio" => {
                let manager = self
                    .host_audio
                    .as_ref()
                    .ok_or_else(|| CallError::invalid("Host audio support is not running"))?;
                let outcome = manager.read(arguments["host"].as_str()).await?;
                if outcome.status >= 400 {
                    return Err(CallError::invalid(views::text(&outcome.body["error"])));
                }
                views::host_audio::view(&outcome.body, arguments)?
            }
            "get_wireless_devices" => {
                let mut result = views::wireless::view(
                    &self.shure.records()?,
                    &records,
                    &self.shure.correlations.snapshot()?,
                );
                let remembered = views::wireless::remembered(self.shure.remembered()?);
                if !remembered.is_empty() {
                    result["remembered_devices"] = json!(remembered);
                }
                let discovery = self.shure.discovery_state()?;
                if discovery["state"] != "ready" {
                    result["discovery"] = discovery;
                }
                result
            }
            "get_external_flows" => {
                let flows = self.model.external_flow_records()?;

                if flows.as_object().is_some_and(|flows| flows.is_empty()) {
                    json!({"result": "no AES67 or RTP streams are being announced on the network"})
                } else {
                    flows
                }
            }
            "get_metering_status" => self.metering.status()?,
            "get_device_diagnostics" => {
                let options = || DiagnosticOptions {
                    receiver_history: false,
                    clock_history: false,
                    ..DiagnosticOptions::default()
                };

                if let Some(selector) = arguments["device"].as_str() {
                    let device = require_device(&records, &arguments["device"])?;
                    let snapshot = self
                        .registry
                        .diagnostics_snapshot(selector, options())
                        .map_err(|error| CallError::invalid(error.to_string()))?;

                    if arguments["detail"] == "debug" {
                        views::diagnostic_evidence(snapshot)
                    } else {
                        views::diagnostics_view(&snapshot, device, &records)
                    }
                } else {
                    self.stream_health(&records)
                }
            }
            "get_server_info" => self.information.as_ref().clone(),
            "list_devices" => {
                let managed = self.managed.status()?;
                let mut view = self.device_list.read(&records, &managed, None, || {
                    views::device_list(&records, managed["fresh"] == true)
                })?;
                self.annotate_computers(&mut view, None).await?;
                view
            }
            "get_device" => {
                let device = require_device(&records, &arguments["device"])?;
                let sections = arguments["sections"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                let mut view = views::device_view(
                    device,
                    &sections,
                    self.managed.status()?["fresh"] == true,
                    &records,
                );
                if let Some(seen) = device.get("remembered_last_seen") {
                    let state = views::local_time(seen).map_or_else(
                        || "off the network".to_owned(),
                        |time| format!("off the network, last seen {time}"),
                    );
                    view["state"] = json!(format!("{state}; this is how it was then"));
                }
                self.annotate_computers(&mut view, device["name"].as_str())
                    .await?;
                view
            }
            "get_device_record" => {
                let device = require_device(&records, &arguments["device"])?;
                let fields = arguments["fields"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                return Ok(views::record_fields(
                    device,
                    &fields,
                    &self.catalog.device_record_fields,
                ));
            }
            "get_routing" => {
                let mut request = arguments.clone();

                if !request["device"].is_null() {
                    let device = require_device(&records, &request["device"])?;
                    request["device"] = device["name"].clone();
                }

                views::routing_view(&records, &request)
            }
            "get_clock_status" => views::clock_status(&records, arguments),
            "find_channels" => {
                let mut found = views::find_channels(&records, arguments);
                views::channel_signals(&mut found, &self.registry.metering.cached()?, &records);
                if arguments["channel_type"]
                    .as_str()
                    .is_none_or(|kind| kind == "any")
                    && let Some(signal) = &self.signal
                {
                    match signal
                        .find(arguments["query"].as_str().unwrap_or_default())
                        .await
                    {
                        Ok(Some(hosts)) => found["jack_pulseaudio_and_wireless"] = hosts,
                        Ok(None) => {}
                        Err(error) => found["host_audio_error"] = json!(error.to_string()),
                    }
                }
                found
            }
            _ => return Err(CallError::unavailable(name)),
        };
        Ok(views::compact(payload))
    }

    async fn inventory(&self) -> Result<Records, CallError> {
        let remembered = self.journal.remembered().await?;
        let mut records = self.model.records()?;
        views::include_remembered(&mut records, remembered);
        Ok(records)
    }

    async fn signal_levels(&self, arguments: &Value) -> Result<Value, CallError> {
        if !arguments["points"].is_null() {
            let service = self
                .signal
                .as_ref()
                .ok_or_else(|| CallError::invalid("Host audio support is not running"))?;
            let points = match &arguments["points"] {
                Value::String(point) => vec![point.clone()],
                Value::Array(points) => points
                    .iter()
                    .map(|point| {
                        point
                            .as_str()
                            .map(str::to_owned)
                            .ok_or_else(|| CallError::invalid("Signal points must be strings"))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                _ => {
                    return Err(CallError::invalid(
                        "points must be a string or list of strings",
                    ));
                }
            };
            let period = views::signal_history::HistoryRequest::parse(
                arguments,
                chrono::Utc::now().timestamp(),
            )
            .map_err(|error| CallError::invalid(error.to_string()))?;
            let outcome = service
                .points(
                    crate::host_audio::signal::PointsRequest {
                        points,
                        seconds: 0.5,
                    },
                    period,
                )
                .await?;
            return Ok(outcome.body);
        }

        let period =
            views::signal_history::HistoryRequest::parse(arguments, chrono::Utc::now().timestamp())
                .map_err(|error| CallError::invalid(error.to_string()))?;
        let records = self.model.records()?;
        let selected = arguments["device"]
            .as_str()
            .map(|_| require_device(&records, &arguments["device"]))
            .transpose()?;
        let targets: Vec<String> = records
            .iter()
            .filter(|(_, record)| {
                record["online"] == true
                    && record["detailed_metering_supported"] != false
                    && record["per_channel_signal_presence_supported"] != true
                    && selected
                        .is_none_or(|selected| selected["server_name"] == record["server_name"])
            })
            .map(|(name, _)| name.clone())
            .collect();
        if let Some(period) = period {
            let mut references = Vec::new();
            let mut unavailable = Vec::new();

            if let Some(listen) = period.listen {
                let deadline = tokio::time::Instant::now() + listen;

                for name in &targets {
                    match self.metering.acquire(name) {
                        Ok(reference) => references.push(reference),
                        Err(error) => unavailable.push(json!({
                            "device": name,
                            "reason": error.to_string(),
                        })),
                    }
                }

                tokio::select! {
                    _ = self.cancellation.cancelled() => {
                        return Err(CallError::invalid("daemon is stopping"));
                    }
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }

            let mut result = views::signal_history::signal_history(
                &self.registry.metering.history,
                &records,
                selected,
                &period,
                arguments["detail"] == "debug",
            )?;
            drop(references);

            if !unavailable.is_empty() {
                result["unavailable"] = json!(unavailable);
            }

            return Ok(result);
        }

        let mut snapshots = stream::iter(targets.into_iter().map(|name| {
            let metering = Arc::clone(&self.metering);
            async move {
                let result = metering.snapshot(&name).await;
                (name, result)
            }
        }))
        .buffer_unordered(METER_SNAPSHOT_CONCURRENCY);
        let mut unavailable = Vec::new();

        while let Some((name, result)) = snapshots.next().await {
            match result {
                Ok(Some(_)) => {}
                Ok(None) => {
                    unavailable.push(json!({"device": name, "reason": "no recent metering data"}))
                }
                Err(error) => {
                    unavailable.push(json!({"device": name, "reason": error.to_string()}))
                }
            }
        }

        let mut result = views::signal_levels(
            &self.registry.metering.cached()?,
            &records,
            selected,
            arguments["detail"] == "debug",
        );

        if !unavailable.is_empty() {
            result["unavailable"] = json!(unavailable);
        }

        if arguments["device"].is_null()
            && arguments["detail"] != "debug"
            && let Some(signal) = &self.signal
        {
            match signal.now_playing(0.5).await {
                Ok(playing) => {
                    result["playing_now"] = playing["computers"].clone();
                    result["playing_notes"] = playing["notes"].clone();
                }
                Err(error) => result["playing_notes"] = json!([error.to_string()]),
            }
        }

        Ok(result)
    }

    async fn resource(&self, parameters: &Map<String, Value>) -> Result<Value, CallError> {
        let uri = parameters
            .get("uri")
            .and_then(Value::as_str)
            .ok_or_else(|| CallError::invalid("uri is required"))?;
        if uri == apps::ROUTING_VIEW_URI {
            return Ok(apps::read_resource());
        }

        let payload = if let Some(selector) = uri
            .strip_prefix("netaudio://devices/")
            .filter(|selector| !selector.is_empty())
        {
            let selector = percent_encoding::percent_decode_str(selector)
                .decode_utf8()
                .map_err(|_| CallError::invalid("invalid device URI"))?;
            self.model
                .device(&selector)?
                .ok_or_else(|| CallError::invalid("device not found"))?
        } else if let Some(resource) = self
            .catalog
            .resources
            .iter()
            .find(|resource| resource.uri == uri)
        {
            match resource.path.as_str() {
                "/external-flows" => Arc::new(self.model.external_flow_records()?),
                "/issues" => Arc::new(
                    serde_json::to_value(self.journal.issues(Default::default()).await?)
                        .map_err(anyhow::Error::from)?,
                ),
                "/event-journal" => Arc::new(
                    serde_json::to_value(self.journal.page(Default::default()).await?)
                        .map_err(anyhow::Error::from)?,
                ),
                "/devices" => Arc::new(
                    serde_json::to_value(self.model.records()?).map_err(anyhow::Error::from)?,
                ),
                "/server-info" => Arc::clone(&self.information),
                "/metering/cache" => Arc::new(self.registry.metering.cached()?),
                "/metering/status" => Arc::new(self.metering.status()?),
                "/ddm/status" => Arc::new(self.managed.status()?),
                "/ddm/connections" => Arc::new(self.managed.connections()?),
                "/ddm/domains" => Arc::new(json!(self.managed.domains(None)?)),
                _ => return Err(CallError::unavailable(&resource.tool_name)),
            }
        } else {
            return Err(CallError::invalid(format!("unknown resource {uri}")));
        };
        Ok(
            json!({"contents": [{"uri": uri, "mimeType": "application/json", "text": payload.to_string()}]}),
        )
    }

    fn unknown_tool(&self, name: &str) -> CallError {
        let suggestions = self.catalog.discover(&name.replace('_', " "), 3, 0, false);
        let suggestions = suggestions["operations"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|operation| operation["name"].as_str())
            .collect::<Vec<_>>();
        let hint = if suggestions.is_empty() {
            "; use discover_tools to find it".to_owned()
        } else {
            format!("; did you mean {}?", suggestions.join(" or "))
        };
        CallError::invalid(format!("unknown operation {name:?}{hint}"))
    }
}

fn require_device<'a>(records: &'a Records, selector: &Value) -> Result<&'a Value, CallError> {
    let selector = selector
        .as_str()
        .ok_or_else(|| CallError::invalid("device is required"))?;
    views::resolve(records, selector)?
        .ok_or_else(|| CallError::invalid(format!("device {selector} not found")))
}
use super::apps::{self, Apps};
