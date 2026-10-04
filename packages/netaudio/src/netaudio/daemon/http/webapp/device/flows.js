import { api } from "../api.js";
import { AsyncButton, Panel } from "../components.js";
import * as format from "../format.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";

function parseChannels(text) {
  const values = String(text)
    .split(",")
    .map((value) => Number(value.trim()));
  if (
    !values.length ||
    values.some(
      (value) => !Number.isInteger(value) || value < 1 || value > 65535,
    )
  ) {
    throw new Error(
      "Enter comma-separated positive transmitter channel numbers.",
    );
  }
  return values;
}

export function canonicalFlowRequest({
  authoring,
  channels,
  encoding,
  flowId,
  flowName = "",
  framesPerPacket = "",
  mediaMode = "native_dante",
  primaryAddress = "",
  primaryPort = "",
  sampleRate,
  secondaryAddress = "",
  secondaryPort = "",
}) {
  const channelNumbers = Array.isArray(channels)
    ? channels
    : parseChannels(channels);
  if (!authoring)
    throw new Error("Flow authoring capabilities are unavailable.");

  if (
    !Number.isInteger(Number(flowId)) ||
    Number(flowId) < 1 ||
    Number(flowId) > authoring.identifier_max
  ) {
    throw new Error(
      `Enter a flow identifier from 1 through ${authoring.identifier_max}.`,
    );
  }
  if (!authoring.media_modes.includes(mediaMode))
    throw new Error("This audio mode is not available for flow creation.");
  const socket = (address, port, label) => {
    if (!address && !port) return null;
    if (
      !address ||
      !Number.isInteger(Number(port)) ||
      Number(port) < 1 ||
      Number(port) > 65535
    )
      throw new Error(
        `${label} needs an IPv4 address and UDP port from 1 through 65535.`,
      );
    return { address, port: Number(port), interface: null };
  };
  const primaryDestination = socket(
    primaryAddress.trim(),
    primaryPort,
    "Primary destination",
  );
  const secondaryDestination = socket(
    secondaryAddress.trim(),
    secondaryPort,
    "Secondary destination",
  );
  const fpp = framesPerPacket === "" ? null : Number(framesPerPacket);
  if (fpp !== null && (!Number.isInteger(fpp) || fpp < 1 || fpp > 65535))
    throw new Error("Frames per packet must be from 1 through 65535.");
  return {
    schema_version: 1,
    media_mode: mediaMode,
    flow_type: "multicast",
    name: flowName.trim() || null,
    channel_slots: channelNumbers.map((transmitter_channel, index) => ({
      slot: index + 1,
      transmitter_channel,
    })),
    sample_rate_hz:
      Number.isInteger(Number(sampleRate)) && Number(sampleRate) > 0
        ? Number(sampleRate)
        : null,
    encoding_bits:
      Number.isInteger(Number(encoding)) && Number(encoding) > 0
        ? Number(encoding)
        : null,
    frames_per_packet: fpp,
    primary_destination: primaryDestination,
    secondary_destination: secondaryDestination,
    redundancy: "device_default",
    identity: {
      global_flow_id: null,
      media_type_code: null,
      media_local_flow_id: null,
      [authoring.identity_field]: Number(flowId),
    },
    protocol: {
      protocol_id: null,
      protocol_version: null,
      cohort: null,
      required_capabilities: [],
    },
    raw_fields: {},
  };
}

export function flowEvidenceRows(result) {
  const acknowledgement = result?.request_acknowledgement;
  let acknowledgementLabel = "Not received";
  if (acknowledgement?.accepted === true) {
    acknowledgementLabel = "Accepted";
  } else if (acknowledgement?.parseable === true) {
    acknowledgementLabel = "Rejected";
  } else if (acknowledgement?.received === true) {
    acknowledgementLabel = "Received but unparseable";
  }
  const confirmationLabel = (value, unavailable) =>
    value === true
      ? "Confirmed"
      : value === false
        ? "Contradicted"
        : unavailable;
  return [
    ["Request acknowledgement", acknowledgementLabel],
    [
      "Device confirmation",
      confirmationLabel(
        result?.device_confirmation,
        "No separate signal from this ARC transport",
      ),
    ],
    [
      "Effective state",
      confirmationLabel(result?.effective_state_confirmation, "Unverified"),
    ],
    [
      "Persistence",
      confirmationLabel(result?.persistence_confirmation, "Not verified"),
    ],
  ];
}

function socketLabel(socket) {
  if (!socket) return "";
  return `${socket.address}:${socket.port}${socket.interface ? ` via ${socket.interface}` : ""}`;
}

function channelLabel(specification) {
  return (specification.channel_slots || [])
    .map((entry) => `${entry.slot}:${entry.transmitter_channel}`)
    .join(", ");
}

function capitalized(value) {
  return value ? `${value.charAt(0).toUpperCase()}${value.slice(1)}` : "";
}

function receiverFlowEndpoints(flow) {
  const endpoints = Array.isArray(flow.interface_endpoints)
    ? flow.interface_endpoints
    : flow.destination_user_datagram_port != null
      ? [
          {
            ipv4_address:
              flow.destination_internet_protocol_version_four_address,
            udp_port: flow.destination_user_datagram_port,
          },
        ]
      : [];
  return endpoints
    .filter((endpoint) => endpoint.ipv4_address && endpoint.udp_port != null)
    .map((endpoint) => `${endpoint.ipv4_address}:${endpoint.udp_port}`)
    .join(", ");
}

function receiverFlowChannels(flow) {
  const slots = flow.receiver_channel_numbers_by_flow_channel;
  if (Array.isArray(slots)) {
    return slots
      .map((channels, index) => `${index + 1}:${channels.join(",") || "none"}`)
      .join("; ");
  }
  return "";
}

function externalIdentityLabel(flow) {
  const identity = flow.external_identity;
  if (!identity) return "";
  return [identity.source_ipv4, identity.session_id].filter((value) => value != null).join("/");
}

export function ReceiverFlows({ device }) {
  const flows = Array.isArray(device.receiver_flows)
    ? device.receiver_flows
    : [];
  if (!flows.length) return null;
  const external = flows.some((flow) => flow.external_identity);
  const typed = flows.some((flow) => flow.flow_type);
  const mapped = flows.some((flow) => receiverFlowChannels(flow));
  const addressed = flows.some((flow) => receiverFlowEndpoints(flow));
  if (!typed && !mapped && !addressed && !external) return null;
  return html`<${Panel} title=${`Receiver flows (${flows.length})`}>
    <div class="table-wrapper">
      <table class="data">
        <thead>
          <tr>
            <th>Flow</th>
            ${typed ? html`<th>Type</th>` : null}
            ${mapped ? html`<th>Slot:receiver channels</th>` : null}
            ${addressed ? html`<th>Destination</th>` : null}
            ${external ? html`<th>Source</th><th>SDP</th>` : null}
          </tr>
        </thead>
        <tbody>
          ${flows.map(
            (flow) =>
              html`<tr key=${flow.flow_number}>
                <td data-label="Flow">${flow.flow_number ?? ""}</td>
                ${typed ? html`<td data-label="Type">${capitalized(flow.flow_type)}</td>` : null}
                ${mapped ? html`<td data-label="Slot:receiver channels">${receiverFlowChannels(flow)}</td>` : null}
                ${addressed ? html`<td data-label="Destination">${receiverFlowEndpoints(flow)}</td>` : null}
                ${external
                  ? html`<td data-label="Source">${externalIdentityLabel(flow)}</td>
                      <td data-label="SDP">
                        ${flow.sdp_correlation?.matched === true
                          ? "Matched"
                          : flow.external_identity
                            ? "Not matched"
                            : ""}
                      </td>`
                  : null}
              </tr>`,
          )}
        </tbody>
      </table>
    </div>
  <//>`;
}

function flowTypeLabel(entry) {
  return [entry.media_mode === "rtp_aes67" ? "AES67" : null, capitalized(entry.flow_type)]
    .filter(Boolean)
    .join(" ");
}

function FlowRow({ entry, onDelete, requestName }) {
  const flowId = entry.identity?.global_flow_id;
  return html`<tr>
    <td data-label="Flow">${flowId ?? ""}</td>
    <td data-label="Type">${flowTypeLabel(entry)}</td>
    <td data-label="Name">${entry.name || ""}</td>
    <td data-label="Slot:channel">${channelLabel(entry)}</td>
    <td data-label="Sample rate">${format.sampleRate(entry.sample_rate_hz)}</td>
    <td data-label="Encoding">${entry.encoding_bits == null ? "" : `PCM ${entry.encoding_bits}`}</td>
    <td data-label="Destination">${socketLabel(entry.primary_destination)}</td>
    <td data-label="">
      ${
        flowId == null
          ? null
          : html`<${AsyncButton}
              small
              variant="danger"
              description=${`delete transmit flow ${flowId} on ${requestName}`}
              onRun=${() => onDelete(flowId)}
              >Delete<//
            >`
      }
    </td>
  </tr>`;
}

function FlowField({ children, label }) {
  return html`<label class="flow-field"><span>${label}</span>${children}</label>`;
}

export function TransmitFlows({ device }) {
  const requestName = deviceRequestName(device);
  const [inventory, setInventory] = useState(null);
  const [error, setError] = useState("");
  const [channels, setChannels] = useState("");
  const [flowId, setFlowId] = useState("1");
  const [flowName, setFlowName] = useState("");
  const [framesPerPacket, setFramesPerPacket] = useState("");
  const [mediaMode, setMediaMode] = useState("native_dante");
  const [primaryAddress, setPrimaryAddress] = useState("");
  const [primaryPort, setPrimaryPort] = useState("");
  const [secondaryAddress, setSecondaryAddress] = useState("");
  const [secondaryPort, setSecondaryPort] = useState("");
  const [plan, setPlan] = useState(null);
  const [result, setResult] = useState(null);
  const authoring = device.transmit_flow_authoring;
  const authoringFacts = JSON.stringify([requestName, authoring, device.aes67_current,
    device.aes67_configuration_supported, device.is_locked, device.sample_rate, device.encoding]);
  useEffect(() => {
    setPlan(null);
    setResult(null);
    setFlowId("");
    setMediaMode("native_dante");
  }, [authoringFacts]);
  const refresh = async () => {
    try {
      setInventory(await api.getTransmitFlows(requestName));
    } catch (failure) {
      console.error(failure);
    }
  };
  useEffect(() => {
    setInventory(null);
    setPlan(null);
    setResult(null);
    setError("");
    void refresh();
  }, [requestName]);
  const specification = () =>
    canonicalFlowRequest({
      channels,
      encoding: device.encoding,
      flowId,
      flowName,
      framesPerPacket,
      mediaMode,
      primaryAddress,
      primaryPort,
      authoring,
      sampleRate: device.sample_rate,
      secondaryAddress,
      secondaryPort,
    });
  const preview = async () => {
    setResult(null);
    try {
      const response = await api.planTransmitFlow(requestName, specification());
      setPlan(response.plan);
      setError("");
    } catch (failure) {
      setPlan(null);
      setError(failure.message);
    }
  };
  const create = async () => {
    const response = await api.createTransmitFlow(requestName, specification());
    setResult(response);
    setPlan(null);
    await refresh();
  };
  const remove = async (identifier) => {
    const response = await api.deleteTransmitFlow(requestName, identifier);
    setResult(response);
    await refresh();
  };
  const flowEntries = inventory?.flows || [];
  if (!authoring && !flowEntries.length) return null;
  const supportsFlowOptions = authoring?.supports_flow_options === true;
  const edit = (setter) => (event) => {
    setter(event.target.value);
    setPlan(null);
  };
  return html`<${Panel} title=${`Transmit flows (${flowEntries.length})`}>
    ${
      flowEntries.length
        ? html`<div class="table-wrapper">
            <table class="data">
              <thead>
                <tr>
                  <th>Flow</th>
                  <th>Type</th>
                  <th>Name</th>
                  <th>Slot:channel</th>
                  <th>Sample rate</th>
                  <th>Encoding</th>
                  <th>Destination</th>
                  <th><span class="sr-only">Actions</span></th>
                </tr>
              </thead>
              <tbody>
                ${flowEntries.map((entry) => html`<${FlowRow} key=${entry.identity?.global_flow_id} entry=${entry} onDelete=${remove} requestName=${requestName} />`)}
              </tbody>
            </table>
          </div>`
        : null
    }
    ${
      authoring
        ? html`<form
            class="flow-form"
            onSubmit=${(event) => {
              event.preventDefault();
              void preview();
            }}
          >
            <h3 class="flow-form-title">New multicast flow</h3>
            <div class="flow-form-fields">
              <${FlowField} label="Channels">
                <input aria-label="Transmit flow channels" value=${channels} onInput=${edit(setChannels)} />
              <//>
              <${FlowField} label=${{ media_local_flow_id: "Media-local flow identifier", global_flow_id: "Global flow identifier" }[authoring.identity_field] || "Flow identifier"}>
                <input
                  type="number"
                  min="1"
                  max=${authoring.identifier_max}
                  aria-label="Transmit flow identifier"
                  value=${flowId}
                  onInput=${edit(setFlowId)}
                />
              <//>
              ${
                authoring.media_modes.length > 1
                  ? html`<${FlowField} label="Mode">
                      <select aria-label="Transmit flow media mode" value=${mediaMode} onChange=${edit(setMediaMode)}>
                        ${authoring.media_modes.map((mode) => html`<option value=${mode}
                          disabled=${mode === "rtp_aes67" && (device.aes67_configuration_supported !== true || device.aes67_current !== true)}
                          >${mode === "native_dante" ? "Native Dante" : "RTP/AES67"}</option>`)}
                      </select>
                    <//>`
                  : null
              }
              ${
                supportsFlowOptions
                  ? html`<${FlowField} label="Flow name">
                        <input aria-label="Transmit flow name" value=${flowName} onInput=${edit(setFlowName)} />
                      <//>
                      <${FlowField} label="Frames per packet">
                        <input
                          type="number"
                          min="1"
                          max="65535"
                          aria-label="Transmit flow frames per packet"
                          value=${framesPerPacket}
                          onInput=${edit(setFramesPerPacket)}
                        />
                      <//>`
                  : null
              }
              ${
                supportsFlowOptions && mediaMode === "rtp_aes67"
                  ? html`<${FlowField} label="Primary IPv4">
                        <input aria-label="Primary RTP destination address" value=${primaryAddress} onInput=${edit(setPrimaryAddress)} />
                      <//>
                      <${FlowField} label="Primary UDP port">
                        <input type="number" min="1" max="65535" aria-label="Primary RTP destination port" value=${primaryPort} onInput=${edit(setPrimaryPort)} />
                      <//>
                      <${FlowField} label="Secondary IPv4">
                        <input aria-label="Secondary RTP destination address" value=${secondaryAddress} onInput=${edit(setSecondaryAddress)} />
                      <//>
                      <${FlowField} label="Secondary UDP port">
                        <input type="number" min="1" max="65535" aria-label="Secondary RTP destination port" value=${secondaryPort} onInput=${edit(setSecondaryPort)} />
                      <//>`
                  : null
              }
            </div>
            <div class="flow-form-actions">
              <button class="btn btn-sm" type="submit">Plan</button>
              ${
                plan?.supported
                  ? html`<${AsyncButton}
                      variant="primary"
                      description=${`create the planned transmit flow on ${requestName}`}
                      onRun=${create}
                      >Create<//
                    >`
                  : null
              }
            </div>
            ${error ? html`<p role="alert" class="text-error">${error}</p>` : null}
            ${plan && !plan.supported ? html`<p role="alert" class="text-error">${plan.reasons.join("; ")}</p>` : null}
            ${plan?.supported ? html`<p role="status">Ready to create.</p>` : null}
            ${result ? html`<p role="status">${result.message || capitalized(result.state)}</p>` : null}
          </form>`
        : html`${result ? html`<p role="status">${result.message || capitalized(result.state)}</p>` : null}`
    }
  <//>`;
}
