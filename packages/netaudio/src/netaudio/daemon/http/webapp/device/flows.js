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
  protocolId,
  sampleRate,
}) {
  const channelNumbers = Array.isArray(channels)
    ? channels
    : parseChannels(channels);
  const modernAllocation = Number(protocolId) === 0x2809;
  if (
    !modernAllocation &&
    (!Number.isInteger(Number(flowId)) ||
      Number(flowId) < 1 ||
      Number(flowId) > 32)
  ) {
    throw new Error(
      "Legacy creation needs a flow identifier from 1 through 32.",
    );
  }
  return {
    schema_version: 1,
    media_mode: "native_dante",
    flow_type: "multicast",
    name: null,
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
    frames_per_packet: null,
    primary_destination: null,
    secondary_destination: null,
    redundancy: "device_default",
    identity: {
      global_flow_id: modernAllocation ? null : Number(flowId),
      media_type_code: null,
      media_local_flow_id: null,
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
      protocolId,
      sampleRate: device.sample_rate_hz,
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
      <h3 class="font-semibold">Plan native multicast flow</h3>
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
        ${
          modernAllocation
            ? html`<p class="text-sm self-end">
                The device assigns the flow identifier.
              </p>`
            : html`<label
                >Flow identifier<input
                  type="number"
                  min="1"
                  max="32"
                  aria-label="Transmit flow identifier"
                  value=${flowId}
                  onInput=${(event) => {
            setFlowId(event.target.value);
            setPlan(null);
          }}
              /></label>`
        }
      </div>
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
