import { Notice, Panel } from "../components.js";
import { html } from "../lib/preact.js";
import { MeterBank } from "../meters.js";
import { DetailedMonitoring } from "../monitoring.js";
import { isEnrolled } from "./managed.js";
import { deviceRequestName } from "../store.js";

export function DeviceMeters({ device }) {
  if (!device.online) return html`<${Notice}>Monitoring is unavailable while this device is offline.<//>`;
  return html`<div class="flex flex-col gap-3">
    ${isEnrolled(device) ? null : html`<${DetailedMonitoring} requestName=${deviceRequestName(device)} online=${device.online} />`}
    <div class="split">
      <${Panel} title="Receive levels"><${MeterBank} device=${device} direction="rx" serverName=${device.server_name} /><//>
      <${Panel} title="Transmit levels"><${MeterBank} device=${device} direction="tx" serverName=${device.server_name} /><//>
    </div>
  </div>`;
}
