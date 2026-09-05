import { api } from "../api.js";
import { AsyncButton, DataTable, Fields, FieldRow, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { html, useCallback, useRef, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";
import { runAction } from "../toast.js";

const INTERFACE_HEADERS = ["Index", "Mode", "Address", "Netmask", "Gateway", "DNS", "MAC address", "Link speed"];

function interfaceRows(interfaces) {
  return (interfaces || []).map(
    (entry, index) => html`
      <tr key=${index}>
        <td class="numeric">${index}</td>
        <td>${html`<${Value} value=${entry.mode} />`}</td>
        <td>${html`<${Value} value=${entry.ip_address} />`}</td>
        <td>${html`<${Value} value=${entry.netmask} />`}</td>
        <td>${html`<${Value} value=${entry.gateway} />`}</td>
        <td>${html`<${Value} value=${entry.dns_server} />`}</td>
        <td>${html`<${Value} value=${entry.mac_address} />`}</td>
        <td>${html`<${Value} value=${entry.link_speed_mbps ?? entry.speed} />`}</td>
      </tr>
    `,
  );
}

export function NetworkSection({ device }) {
  const requestName = deviceRequestName(device);
  const [probe, setProbe] = useState(null);
  const address = useRef(null);
  const netmask = useRef(null);
  const gateway = useRef(null);
  const dns = useRef(null);

  const probeInterfaces = useCallback(async () => {
    const outcome = await runAction(`probe interfaces on ${requestName}`, () => api.getInterfaces(requestName));
    if (outcome.ok) {
      setProbe(outcome.result);
    }
    return outcome.result;
  }, [requestName]);

  return html`
    <${Panel}
      title="Network config"
      actions=${html`
        <${AsyncButton} small description=${`probe interfaces on ${requestName}`} onRun=${() => api.getInterfaces(requestName).then((result) => {
          setProbe(result);
          return result;
        })}>
          Probe interface status
        <//>
        <${AsyncButton}
          small
          description=${`set ${requestName} to DHCP`}
          onRun=${async () => {
            const result = await api.setInterface({ device: requestName, mode: "dhcp" });
            await probeInterfaces();
            return result;
          }}
        >
          Set DHCP
        <//>
      `}
    >
      <${FieldRow} label="Static address">
        <input key=${`interface-ip-${requestName}`} ref=${address} type="text" size="16" placeholder="address" />
        <input key=${`interface-netmask-${requestName}`} ref=${netmask} type="text" size="16" placeholder="netmask" />
        <input key=${`interface-gateway-${requestName}`} ref=${gateway} type="text" size="16" placeholder="gateway" />
        <input key=${`interface-dns-${requestName}`} ref=${dns} type="text" size="16" placeholder="dns" />
        <${AsyncButton}
          variant="primary"
          small
          description=${`set static address on ${requestName}`}
          onRun=${async () => {
            const result = await api.setInterface({
              device: requestName,
              dns: dns.current.value,
              gateway: gateway.current.value,
              ip: address.current.value,
              mode: "static",
              netmask: netmask.current.value,
            });
            await probeInterfaces();
            return result;
          }}
        >
          Apply
        <//>
      <//>
      <div class="section-label">Cached interfaces</div>
      <${DataTable} headers=${INTERFACE_HEADERS} rows=${interfaceRows(device.interfaces)} short />
      ${probe
        ? html`
            <div class="section-label">Last probe</div>
            <${Fields}
              entries=${[
                ["Reboot required", html`<${Value} value=${probe.reboot_required} />`],
                ["Pending configuration", html`<${Value} value=${probe.pending_config} />`],
                ["Link speed", probe.link_speed_mbps ? `${probe.link_speed_mbps} Mbps` : format.ABSENT],
              ]}
            />
            <${DataTable} headers=${INTERFACE_HEADERS} rows=${interfaceRows(probe.interfaces)} short />
          `
        : null}
      <${Fields}
        entries=${[
          ["Reboot required", html`<${Value} value=${device.interface_reboot_required} />`],
          ["Pending configuration", html`<${Value} value=${device.interface_pending_config} />`],
          ["Network interface traffic", html`<${Value} value=${device.network_interface_traffic} />`],
          ["Receiver flow connection health", html`<${Value} value=${device.receiver_flow_connection_health} />`],
          [
            "Receiver flow latency",
            device.receiver_flow_latency_ns ? `${device.receiver_flow_latency_ns} ns` : format.ABSENT,
          ],
        ]}
      />
    <//>
  `;
}
