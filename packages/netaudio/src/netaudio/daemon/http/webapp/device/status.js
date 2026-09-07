import { Fields, OnlineState, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";

export function StatusSection({ device }) {
  const subscriptions = (device.subscriptions || []).filter((entry) => entry.tx_device);
  const problems = subscriptions.filter((entry) => entry.status && entry.status.severity !== "ok");
  return html`
    <div class="flex flex-col gap-4">
      <div class="split">
        <${Panel} title="Device">
          <${Fields}
            entries=${[
              ["State", html`<${OnlineState} online=${device.online} />`],
              ["Manufacturer", html`<${Value} value=${device.manufacturer} />`],
              ["Model", html`<${Value} value=${format.deviceModelName(device)} />`],
              ["Dante model", html`<${Value} value=${device.board_name} />`],
              ["Product version", html`<${Value} value=${device.product_version} />`],
              ["Dante firmware", html`<${Value} value=${device.firmware_version} />`],
              ["Dante software", html`<${Value} value=${device.software_version} />`],
              ["Primary address", html`<${Value} value=${device.ipv4} />`],
              ["MAC address", format.macAddress(device)],
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
          ]}
        />
      <//>
    </div>
  `;
}
