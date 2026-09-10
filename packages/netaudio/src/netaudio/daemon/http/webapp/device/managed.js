import { Fields, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";

export function isEnrolled(device) {
  return device.ddm_enrolment_state === "ENROLLED" && Boolean(device.ddm_domain_id);
}

export function ManagedSection({ device }) {
  return html`
    <${Panel} title="Domain">
      <${Fields}
        entries=${[
          ["Managed context", html`<${Value} value=${device.ddm_context} />`],
          ["Domain name", html`<${Value} value=${device.ddm_domain_name} />`],
          ["Enrollment state", html`<${Value} value=${device.ddm_enrolment_state} />`],
          ["Connection state", html`<${Value} value=${device.ddm_connection_state} />`],
          ["Connection last changed", format.timestamp(device.ddm_connection_last_changed)],
          ["Last sync", format.timestamp(device.ddm_last_sync)],
          ["Status", html`<${Value} value=${device.ddm_status} />`],
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
