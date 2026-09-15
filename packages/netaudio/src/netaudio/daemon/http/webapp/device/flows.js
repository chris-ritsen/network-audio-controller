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
  if (new Set(values).size !== values.length)
    throw new Error("Transmitter channels must be unique.");
  return values;
}

export function canonicalFlowRequest({
  channels,
  encoding,
  flowId,
  flowName = "",
  framesPerPacket = "",
  mediaMode = "native_dante",
  primaryAddress = "",
  primaryPort = "",
  protocolId,
  sampleRate,
  secondaryAddress = "",
  secondaryPort = "",
}) {
  const channelNumbers = Array.isArray(channels)
    ? channels
    : parseChannels(channels);
  const modernAllocation = Number(protocolId) === 0x2809;
  if (
    !Number.isInteger(Number(flowId)) ||
    Number(flowId) < 1 ||
    Number(flowId) > (modernAllocation ? 65535 : 32)
  ) {
    throw new Error(
      modernAllocation
        ? "Modern creation needs a media-local flow identifier from 1 through 65535."
        : "Legacy creation needs a global flow identifier from 1 through 32.",
    );
  }
  if (!["native_dante", "rtp_aes67"].includes(mediaMode))
    throw new Error("Select native Dante or RTP/AES67 audio.");
  if (!modernAllocation && mediaMode !== "native_dante")
    throw new Error("RTP/AES67 authoring is scoped to ARC 0x2809.");
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
  if (
    mediaMode === "native_dante" &&
    (primaryDestination || secondaryDestination)
  )
    throw new Error(
      "Native Dante authoring does not accept explicit destinations.",
    );
  if (mediaMode === "rtp_aes67" && !primaryDestination)
    throw new Error("RTP/AES67 authoring needs a primary IPv4 destination.");
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
      global_flow_id: modernAllocation ? null : Number(flowId),
      media_type_code: 3,
      media_local_flow_id: modernAllocation ? Number(flowId) : null,
    },
    protocol: {
      protocol_id: Number(protocolId),
      protocol_version: null,
      cohort: modernAllocation ? "modern_2809" : "legacy_2729",
      required_capabilities: [],
    },
    raw_fields: modernAllocation ? { request_options_word: 0 } : {},
  };
}

export function flowEvidenceRows(result) {
  const acknowledgement = result?.request_acknowledgement;
  let acknowledgementLabel = "Not received";
  if (acknowledgement?.accepted === true) {
    acknowledgementLabel = `Accepted${acknowledgement.result_code == null ? "" : ` (result ${acknowledgement.result_code})`}`;
  } else if (acknowledgement?.parseable === true) {
    acknowledgementLabel = `Rejected${acknowledgement.result_code == null ? "" : ` (result ${acknowledgement.result_code})`}`;
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
      Inventory: ${device.receiver_flow_completeness || "unknown"}. ARC effective
      state, SDP correlation, RTP reception, clock lock, persistence, and decoded
      audio are separate observations.
    </p>
    ${device.receiver_flow_completeness !== "complete"
      ? html`<${Notice}>Complete fresh receiver-flow inventory is unavailable.<//>`
      : null}
    ${flows.length
      ? html`<div class="table-wrapper">
          <table class="data">
            <thead><tr>
              <th>Flow</th><th>Type / transport</th><th>Status</th>
              <th>Slot:receiver channels</th><th>Interface destinations</th>
              <th>External identity</th><th>SDP</th>
            </tr></thead>
            <tbody>${flows.map(
              (flow) => html`<tr key=${flow.flow_number ?? flow.global_flow_id}>
                <td>${flow.flow_number ?? flow.global_flow_id ?? "unknown"}</td>
                <td>${flow.flow_type || "unknown"} / ${flow.transport ?? "unknown"}</td>
                <td>${flow.subscription_status_code ?? flow.status_code ?? "unknown"}</td>
                <td>${receiverFlowChannels(flow)}</td>
                <td>${receiverFlowEndpoints(flow) || "Not reported"}</td>
                <td>${externalIdentityLabel(flow)}</td>
                <td>${flow.sdp_correlation?.matched === true
                  ? "Matched"
                  : flow.external_identity
                    ? "Not matched"
                    : "Not applicable"}</td>
              </tr>`,
            )}</tbody>
          </table>
        </div>`
      : html`<${Notice}>No active receiver flows in the complete inventory.<//>`}
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
  const protocolId = inventory?.flow_protocol_id;
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
      protocolId,
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
  const modernAllocation = Number(protocolId) === 0x2809;
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
          >${modernAllocation ? "Media-local flow identifier" : "Global flow identifier"}<input
            type="number"
            min="1"
            max=${modernAllocation ? "65535" : "32"}
            aria-label="Transmit flow identifier"
            value=${flowId}
            onInput=${(event) => {
              setFlowId(event.target.value);
              setPlan(null);
            }}
        /></label>
        ${
          modernAllocation
            ? html`<label
                >Mode<select
                  aria-label="Transmit flow media mode"
                  value=${mediaMode}
                  onChange=${(event) => {
                    setMediaMode(event.target.value);
                    setPlan(null);
                  }}
                >
                  <option value="native_dante">Native Dante</option>
                  <option value="rtp_aes67">RTP/AES67</option>
                </select></label
              >`
            : null
        }
        ${
          modernAllocation
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
          modernAllocation
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
        modernAllocation && mediaMode === "rtp_aes67"
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
        modernAllocation
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
          disabled=${protocolId == null || !channels.trim()}
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
