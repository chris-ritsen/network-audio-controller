import { api } from "../api.js";
import { AsyncButton, Notice, Panel } from "../components.js";
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
  if (!socket) return "device allocated";
  return `${socket.address}:${socket.port}${socket.interface ? ` via ${socket.interface}` : ""}`;
}

function channelLabel(specification) {
  return (specification.channel_slots || [])
    .map((entry) => `${entry.slot}:${entry.transmitter_channel}`)
    .join(", ");
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
    .map(
      (endpoint) =>
        `${endpoint.ipv4_address || "address unavailable"}:${endpoint.udp_port ?? "port unavailable"}`,
    )
    .join(", ");
}

function receiverFlowChannels(flow) {
  const slots = flow.receiver_channel_numbers_by_flow_channel;
  if (Array.isArray(slots)) {
    return slots
      .map((channels, index) => `${index + 1}:${channels.join(",") || "none"}`)
      .join("; ");
  }
  return flow.receiver_mapping_descriptor_hexadecimal || "Not reported";
}

function externalIdentityLabel(flow) {
  const identity = flow.external_identity;
  if (!identity) return "Native Dante";
  return `${identity.source_ipv4 || "source unavailable"}/${identity.session_id ?? "session unavailable"}`;
}

export function ReceiverFlows({ device }) {
  const flows = Array.isArray(device.receiver_flows)
    ? device.receiver_flows
    : [];
  return html`<${Panel} title=${`Receiver flows (${flows.length})`}>
    <p class="text-sm">
      Inventory: ${device.receiver_flow_completeness || "unknown"}. ARC
      effective state, SDP correlation, RTP reception, clock lock, persistence,
      and decoded audio are separate observations.
    </p>
    ${
      device.receiver_flow_completeness !== "complete"
        ? html`<${Notice}
            >Complete fresh receiver-flow inventory is unavailable.<//
          >`
        : null
    }
    ${
      flows.length
        ? html`<div class="table-wrapper">
            <table class="data">
              <thead>
                <tr>
                  <th>Flow</th>
                  <th>Type / transport</th>
                  <th>Slot:receiver channels</th>
                  <th>Interface destinations</th>
                  <th>External identity</th>
                  <th>SDP</th>
                </tr>
              </thead>
              <tbody>
                ${flows.map(
                  (flow) =>
                    html`<tr key=${flow.flow_number}>
                      <td>${flow.flow_number ?? "unknown"}</td>
                      <td>
                        ${flow.flow_type || "unknown"} /
                        ${flow.transport ?? "unknown"}
                      </td>
                      <td>${receiverFlowChannels(flow)}</td>
                      <td>${receiverFlowEndpoints(flow) || "Not reported"}</td>
                      <td>${externalIdentityLabel(flow)}</td>
                      <td>
                        ${
                          flow.sdp_correlation?.matched === true
                            ? "Matched"
                            : flow.external_identity
                              ? "Not matched"
                              : "Not applicable"
                        }
                      </td>
                    </tr>`,
                )}
              </tbody>
            </table>
          </div>`
        : html`<${Notice}
            >No active receiver flows in the complete inventory.<//
          >`
    }
  <//>`;
}

function FlowRow({ entry, onDelete, requestName }) {
  const flowId = entry.identity?.global_flow_id;
  return html`<tr>
    <td>${flowId ?? "unknown"}</td>
    <td>${entry.media_mode || "unknown"} / ${entry.flow_type || "unknown"}</td>
    <td>${entry.name || "Unnamed"}</td>
    <td>${channelLabel(entry)}</td>
    <td>${format.sampleRate(entry.sample_rate_hz)}</td>
    <td>
      ${entry.encoding_bits == null ? "Not reported" : `PCM ${entry.encoding_bits}`}
    </td>
    <td>${socketLabel(entry.primary_destination)}</td>
    <td>
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
      setError("");
    } catch (failure) {
      setError(failure.message);
    }
  };
  useEffect(() => {
    setInventory(null);
    setPlan(null);
    setResult(null);
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
  const supportsFlowOptions = authoring?.supports_flow_options === true;
  return html`<${Panel} title=${`Transmit flows (${flowEntries.length})`}>
    <p class="text-sm">
      Fresh readback uses the canonical flow schema and retains raw fields.
      Control-plane confirmation does not confirm RTP reception, clock lock, or
      decoded audio.
    </p>
    ${error ? html`<${Notice}>${error}<//>` : null}
    ${inventory === null && !error ? html`<${Notice}>Reading transmitter flows…<//>` : null}
    ${
      inventory
        ? html`<div class="table-wrapper">
            <table class="data">
              <thead>
                <tr>
                  <th>Flow</th>
                  <th>Mode</th>
                  <th>Name</th>
                  <th>Slot:channel</th>
                  <th>Sample rate</th>
                  <th>Encoding</th>
                  <th>Destination</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                ${flowEntries.map((entry) => html`<${FlowRow} key=${entry.identity?.global_flow_id} entry=${entry} onDelete=${remove} requestName=${requestName} />`)}
              </tbody>
            </table>
          </div>`
        : null
    }
    ${inventory && !flowEntries.length ? html`<${Notice}>No active transmitter flows.<//>` : null}
    <form
      class="flex flex-col gap-3"
      onSubmit=${(event) => {
        event.preventDefault();
        void preview();
      }}
    >
      <h3 class="font-semibold">Plan multicast transmit flow</h3>
      <div class="flex flex-wrap gap-3">
        <label
          >Channels<input
            aria-label="Transmit flow channels"
            value=${channels}
            onInput=${(event) => {
              setChannels(event.target.value);
              setPlan(null);
            }}
            placeholder="1,2"
        /></label>
        <label
          >${{ media_local_flow_id: "Media-local flow identifier", global_flow_id: "Global flow identifier" }[authoring?.identity_field] || "Flow identifier"}<input
            type="number"
            min="1"
            max=${authoring?.identifier_max}
            aria-label="Transmit flow identifier"
            value=${flowId}
            onInput=${(event) => {
              setFlowId(event.target.value);
              setPlan(null);
            }}
        /></label>
        ${
          authoring?.media_modes.length > 1
            ? html`<label
                >Mode<select
                  aria-label="Transmit flow media mode"
                  value=${mediaMode}
                  onChange=${(event) => {
                    setMediaMode(event.target.value);
                    setPlan(null);
                  }}
                >
                  ${authoring.media_modes.map((mode) => html`<option value=${mode}
                    disabled=${mode === "rtp_aes67" && (device.aes67_configuration_supported !== true || device.aes67_current !== true)}
                    >${mode === "native_dante" ? "Native Dante" : "RTP/AES67"}</option>`)}
                </select></label
              >`
            : null
        }
        ${
          supportsFlowOptions
            ? html`<label
                >Flow name<input
                  aria-label="Transmit flow name"
                  value=${flowName}
                  onInput=${(event) => {
                    setFlowName(event.target.value);
                    setPlan(null);
                  }}
              /></label>`
            : null
        }
        ${
          supportsFlowOptions
            ? html`<label
                >Frames per packet<input
                  type="number"
                  min="1"
                  max="65535"
                  aria-label="Transmit flow frames per packet"
                  value=${framesPerPacket}
                  onInput=${(event) => {
                    setFramesPerPacket(event.target.value);
                    setPlan(null);
                  }}
              /></label>`
            : null
        }
      </div>
      ${
        supportsFlowOptions && mediaMode === "rtp_aes67"
          ? html`<div class="flex flex-wrap gap-3">
              <label
                >Primary IPv4<input
                  aria-label="Primary RTP destination address"
                  value=${primaryAddress}
                  onInput=${(event) => {
                    setPrimaryAddress(event.target.value);
                    setPlan(null);
                  }} /></label
              ><label
                >Primary UDP port<input
                  type="number"
                  min="1"
                  max="65535"
                  aria-label="Primary RTP destination port"
                  value=${primaryPort}
                  onInput=${(event) => {
                    setPrimaryPort(event.target.value);
                    setPlan(null);
                  }} /></label
              ><label
                >Secondary IPv4<input
                  aria-label="Secondary RTP destination address"
                  value=${secondaryAddress}
                  onInput=${(event) => {
                    setSecondaryAddress(event.target.value);
                    setPlan(null);
                  }} /></label
              ><label
                >Secondary UDP port<input
                  type="number"
                  min="1"
                  max="65535"
                  aria-label="Secondary RTP destination port"
                  value=${secondaryPort}
                  onInput=${(event) => {
                    setSecondaryPort(event.target.value);
                    setPlan(null);
                  }}
              /></label>
            </div>`
          : null
      }
      ${
        supportsFlowOptions
          ? html`<p class="text-sm">
              The media-local identifier is requested; the device allocates the
              global flow identifier. Sample rate and encoding are checked as
              fresh device-state preconditions and are not authored by this
              request.
            </p>`
          : null
      }
      <div>
        <button
          class="btn btn-sm"
          type="submit"
          disabled=${authoring == null || !channels.trim()}
        >
          Validate and plan
        </button>
      </div>
      ${
        plan
          ? html`<div
              role="status"
              class=${plan.supported ? "notice" : "alert alert-error"}
            >
              ${plan.supported ? `Ready: ${plan.serializer_cohort}` : `Unavailable: ${plan.reasons.join("; ")}`}
            </div>`
          : null
      }
      ${
        plan?.supported
          ? html`<div>
              <${AsyncButton}
                variant="primary"
                description=${`create the planned transmit flow on ${requestName}`}
                onRun=${create}
                >Create and verify<//
              >
            </div>`
          : null
      }
      ${
        result
          ? html`<div role="status" class="text-sm">
              <p>${result.state}: ${result.message}</p>
              <dl>
                ${flowEvidenceRows(result).map(
                  ([label, value]) => html`
                    <div>
                      <dt class="font-semibold inline">${label}:</dt>
                      <dd class="inline">${value}</dd>
                    </div>
                  `,
                )}
              </dl>
            </div>`
          : null
      }
    </form>
  <//>`;
}
