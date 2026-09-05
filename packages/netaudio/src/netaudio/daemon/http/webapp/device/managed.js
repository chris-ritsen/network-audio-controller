import { Fields, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";

export function isManaged(device) {
  return Boolean(device.ddm_device_id) || (device.inventory_sources || []).includes("ddm");
}

export function ManagedSection({ device }) {
  return html`
    <${Panel} title="Domain">
      <${Fields}
        entries=${[
          ["Managed device identifier", html`<${Value} value=${device.ddm_device_id} />`],
          ["Managed context", html`<${Value} value=${device.ddm_context} />`],
          ["Domain identifier", html`<${Value} value=${device.ddm_domain_id} />`],
          ["Domain name", html`<${Value} value=${device.ddm_domain_name} />`],
          ["Enrolment state", html`<${Value} value=${device.ddm_enrolment_state} />`],
          ["Connection state", html`<${Value} value=${device.ddm_connection_state} />`],
          ["Connection last changed", format.timestamp(device.ddm_connection_last_changed)],
          ["Last sync", format.timestamp(device.ddm_last_sync)],
          ["Status", html`<${Value} value=${device.ddm_status} />`],
          ["Identity", html`<${Value} value=${device.ddm_identity} />`],
          ["Server profile", html`<${Value} value=${device.ddm_server_profile} />`],
          ["Capabilities", html`<${Value} value=${device.ddm_capabilities} />`],
          ["Clocking state", html`<${Value} value=${device.ddm_clocking_state} />`],
          ["Clock preferences", html`<${Value} value=${device.ddm_clock_preferences} />`],
          ["Inputs", html`<${Value} value=${device.ddm_inputs} />`],
          ["Outputs", html`<${Value} value=${device.ddm_outputs} />`],
          ["Parameters", html`<${Value} value=${device.ddm_parameters} />`],
        ]}
      />
    <//>
  `;
}
