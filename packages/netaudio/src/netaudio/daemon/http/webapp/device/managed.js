import { Fields, Panel } from "../components.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";

const STATUS_AREAS = [
  ["clocking", "Clocking"],
  ["connectivity", "Connectivity"],
  ["latency", "Latency"],
  ["subscriptions", "Subscriptions"],
];

export function isEnrolled(device) {
  return device.ddm_enrolment_state === "ENROLLED" && Boolean(device.ddm_domain_id);
}

function words(value) {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const text = value.trim().replaceAll("_", " ").toLowerCase();
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function when(value) {
  const text = format.timestamp(value);
  return text === format.ABSENT ? undefined : text;
}

function domainStatus(status) {
  if (!status || typeof status !== "object") return undefined;
  const problems = STATUS_AREAS.filter(([key]) => status[key] && status[key] !== "OK").map(
    ([key, label]) => `${label}: ${words(status[key])}${status.alert_message?.[key] ? ` (${status.alert_message[key]})` : ""}`,
  );
  if (problems.length) return html`<span class="state-warn">${problems.join(" · ")}</span>`;
  return status.summary === "OK" ? "OK" : words(status.summary);
}

export function ManagedSection({ device }) {
  const context = device.ddm_context && device.ddm_context !== device.ddm_domain_name ? device.ddm_context : undefined;
  return html`
    <${Panel} title="Domain">
      <${Fields}
        entries=${[
          ["Domain", device.ddm_domain_name || undefined],
          ["Managed context", context],
          ["Enrollment", words(device.ddm_enrolment_state)],
          ["Connection", words(device.ddm_connection_state)],
          ["Status", domainStatus(device.ddm_status)],
          ["Connection changed", when(device.ddm_connection_last_changed)],
          ["Last sync", when(device.ddm_last_sync)],
          ["Server profile", device.ddm_server_profile || undefined],
        ]}
      />
    <//>
  `;
}
