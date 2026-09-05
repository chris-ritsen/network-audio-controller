import { api } from "../api.js";
import { AsyncButton, DataTable, Fields, FieldRow, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { html, useCallback, useRef, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";
import { runAction } from "../toast.js";

const FLOW_HEADERS = ["Slot", "Type", "Channels", "Sample rate", "Encoding", "Destination", "Subscriber"];

function parseChannelNumbers(rawValue) {
  const numbers = String(rawValue)
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length)
    .map((part) => Number(part));
  if (!numbers.length || numbers.some((number) => !Number.isInteger(number) || number < 1)) {
    throw new Error("channels must be a comma separated list of channel numbers");
  }
  return numbers;
}

function flowRow(flow, index) {
  const channels = flow.populated_transmitter_channel_ids || flow.channels;
  const destinationAddress = flow.destination_internet_protocol_version_four_address;
  const destinationPort = flow.destination_user_datagram_port;
  return html`
    <tr key=${index}>
      <td class="numeric">${html`<${Value} value=${flow.global_flow_id ?? flow.flow_number} />`}</td>
      <td>${html`<${Value} value=${flow.flow_type ?? flow.flow_type_code} />`}</td>
      <td>
        ${Array.isArray(channels)
          ? channels.join(", ")
          : format.text(flow.populated_slot_count ?? flow.channel_count)}
      </td>
      <td>${format.sampleRate(flow.sample_rate)}</td>
      <td>${html`<${Value} value=${flow.encoding} />`}</td>
      <td>
        ${destinationAddress
          ? `${destinationAddress}${destinationPort ? `:${destinationPort}` : ""}`
          : format.ABSENT}
      </td>
      <td>
        ${flow.subscriber_device_name || flow.subscriber_flow_name
          ? `${html`<${Value} value=${flow.subscriber_device_name} />`} / ${html`<${Value} value=${flow.subscriber_flow_name} />`}`
          : format.ABSENT}
      </td>
    </tr>
  `;
}

export function FlowsSection({ device }) {
  const requestName = deviceRequestName(device);
  const [snapshot, setSnapshot] = useState(null);
  const slot = useRef(null);
  const channels = useRef(null);

  const loadFlows = useCallback(async () => {
    const outcome = await runAction(`read transmit flows on ${requestName}`, () => api.getTransmitFlows(requestName));
    if (outcome.ok) {
      setSnapshot(outcome.result);
    }
    return outcome.result;
  }, [requestName]);

  return html`
    <${Panel}
      title="Transmit flows"
      actions=${html`<${AsyncButton}
        small
        description=${`read transmit flows on ${requestName}`}
        onRun=${() => api.getTransmitFlows(requestName).then((result) => {
          setSnapshot(result);
          return result;
        })}
      >
        Load transmit flows
      <//>`}
    >
      <${FieldRow} label="Multicast flow">
        <label class="inline">
          slot
          <input key=${`flow-slot-${requestName}`} ref=${slot} type="number" min="1" size="5" />
        </label>
        <label class="inline">
          channels
          <input key=${`flow-channels-${requestName}`} ref=${channels} type="text" size="20" placeholder="1,2" />
        </label>
        <${AsyncButton}
          variant="primary"
          small
          description=${`create transmit flow on ${requestName}`}
          onRun=${async () => {
            const result = await api.createTransmitFlow(
              requestName,
              Number(slot.current.value),
              parseChannelNumbers(channels.current.value),
            );
            await loadFlows();
            return result;
          }}
        >
          Create
        <//>
        <${AsyncButton}
          variant="danger"
          small
          description=${`delete transmit flow on ${requestName}`}
          onRun=${async () => {
            const result = await api.deleteTransmitFlow(requestName, Number(slot.current.value));
            await loadFlows();
            return result;
          }}
        >
          Delete slot
        <//>
      <//>
      ${snapshot
        ? html`
            <${Fields}
              entries=${[
                ["Device", html`<${Value} value=${snapshot.device} />`],
                ["Flow protocol identifier", html`<${Value} value=${snapshot.flow_protocol_id} />`],
                ["Maximum flow slots", html`<${Value} value=${snapshot.max_flow_slots} />`],
                ["Flows reported", String((snapshot.flows || []).length)],
              ]}
            />
            <${DataTable} headers=${FLOW_HEADERS} rows=${(snapshot.flows || []).map(flowRow)} short />
          `
        : html`<div class="notice">
            No flow snapshot loaded. Reading transmit flows queries the device directly over ARC.
          </div>`}
      <${Fields}
        entries=${[
          ["Transmit flows reported by inventory", html`<${Value} value=${device.tx_flow_count} />`],
          ["Receive flows reported by inventory", html`<${Value} value=${device.rx_flow_count} />`],
          ["Receive flow details", html`<${Value} value=${device.receive_flows} />`],
        ]}
      />
    <//>
  `;
}
