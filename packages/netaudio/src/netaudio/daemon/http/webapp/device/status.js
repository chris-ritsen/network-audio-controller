import { api } from "../api.js";
import { AsyncButton, Disclosure, Fields, OnlineState, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";
import { navigate } from "../router.js";
import { deviceRequestName } from "../store.js";

export function StatusSection({ device }) {
  const requestName = deviceRequestName(device);
  const subscriptions = (device.subscriptions || []).filter((entry) => entry.tx_device);
  const problems = subscriptions.filter((entry) => entry.status && entry.status.severity !== "ok");
  return html`
    <div class="stack">
      <div class="split">
        <${Panel} title="Device">
          <${Fields}
            entries=${[
              ["State", html`<${OnlineState} online=${device.online} />`],
              ["Manufacturer", html`<${Value} value=${device.manufacturer} />`],
              ["Model", html`<${Value} value=${device.model} />`],
              ["Dante model", html`<${Value} value=${device.dante_model} />`],
              ["Product version", html`<${Value} value=${device.product_version} />`],
              ["Dante firmware", html`<${Value} value=${device.firmware_version} />`],
              ["Dante software", html`<${Value} value=${device.software_version} />`],
              ["Primary address", html`<${Value} value=${device.ipv4} />`],
              ["MAC address", html`<${Value} value=${device.mac_address} />`],
              ["Device lock", device.is_locked ? "locked" : "unlocked"],
              ["Last seen", format.timestamp(device.last_seen)],
            ]}
          />
        <//>
        <${Panel} title="Audio and clock">
          <${Fields}
            entries=${[
              ["Sample rate", format.sampleRate(device.sample_rate_hz)],
              ["Encoding", device.encoding ? `PCM ${device.encoding}` : format.ABSENT],
              ["Latency", format.latency(device.latency_ms)],
              ["Clock role", html`<${Value} value=${device.clock_role} />`],
              ["Preferred leader", format.preferredLeader(device.preferred_leader)],
              ["Clock source", format.clockSourceCode(device.clock_source_code)],
              ["Clock subdomain", format.clockSubdomain(device.clock_subdomain)],
              ["AES67", device.aes67_supported === false ? "not supported" : html`<${Value} value=${device.aes67_current} />`],
            ]}
          />
        <//>
      </div>
      <${Panel} title="Channels and subscriptions">
        <${Fields}
          entries=${[
            ["Transmitters", html`<${Value} value=${device.tx_count} />`],
            ["Receivers", html`<${Value} value=${device.rx_count} />`],
            ["Subscriptions", String(subscriptions.length)],
            [
              "Subscription problems",
              problems.length
                ? html`<span class="state-warn">${problems.length} — ${problems.map((entry) => `${entry.rx_channel}: ${format.subscriptionStatusText(entry)}`).join("; ")}</span>`
                : "none",
            ],
            ["Transmit flows", html`<${Value} value=${device.tx_flow_count} />`],
            ["Receive flows", html`<${Value} value=${device.rx_flow_count} />`],
          ]}
        />
      <//>
      <${Panel}
        title="Inventory"
        actions=${html`
          <${AsyncButton} small description=${`report ${requestName} unresponsive`} onRun=${() => api.reportUnresponsive(requestName)}>
            Report unresponsive
          <//>
          <${AsyncButton}
            small
            variant="danger"
            description=${`forget ${requestName}`}
            onRun=${async () => {
              const result = await api.forgetDevice(requestName);
              navigate("/devices");
              return result;
            }}
          >
            Forget device
          <//>
        `}
      >
        <${Fields}
          entries=${[
            ["Server name", html`<${Value} value=${device.server_name} />`],
            ["Kind", html`<${Value} value=${device.kind} />`],
            ["Availability", html`<${Value} value=${device.availability_state} />`],
            ["Management", html`<${Value} value=${device.management_state} />`],
            ["Inventory sources", html`<${Value} value=${device.inventory_sources} />`],
            ["Control transports", html`<${Value} value=${device.control_transports} />`],
          ]}
        />
        <${Disclosure} summary="All device fields">
          <${Fields}
            entries=${[
              ["Model identifier", html`<${Value} value=${device.model_id} />`],
              ["Dante model identifier", html`<${Value} value=${device.dante_model_id} />`],
              ["Board name", html`<${Value} value=${device.board_name} />`],
              ["Inventory identifier", html`<${Value} value=${device.inventory_id} />`],
              ["Direct control available", html`<${Value} value=${device.direct_control_available} />`],
              ["Media types", html`<${Value} value=${device.media_types} />`],
              ["Networks", html`<${Value} value=${device.num_networks} />`],
              ["Link speed", device.link_speed_mbps ? `${device.link_speed_mbps} Mbps` : format.ABSENT],
              ["Routing capacity transmit", html`<${Value} value=${device.routing_capacity_transmit_channel_count} />`],
              ["Routing capacity receive", html`<${Value} value=${device.routing_capacity_receive_channel_count} />`],
              ["Licensed transmit channels", html`<${Value} value=${device.licensed_transmit_channel_count} />`],
              ["Licensed receive channels", html`<${Value} value=${device.licensed_receive_channel_count} />`],
              ["Licensed redundancy", html`<${Value} value=${device.licensed_redundancy_enabled} />`],
              ["Licensed", html`<${Value} value=${device.is_licensed} />`],
              ["Routing ready", html`<${Value} value=${device.routing_ready} />`],
              ["Frequency offset", device.clock_frequency_offset_parts_per_billion === null || device.clock_frequency_offset_parts_per_billion === undefined ? format.ABSENT : `${device.clock_frequency_offset_parts_per_billion} ppb`],
              ["Services", html`<${Value} value=${device.services} />`],
              ["Field sources", html`<${Value} value=${device.field_sources} />`],
              ["Settings properties", html`<${Value} value=${device.settings_properties} />`],
            ]}
          />
        <//>
      <//>
    </div>
  `;
}
