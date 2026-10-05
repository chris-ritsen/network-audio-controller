import { Fields, Panel } from "../components.js";
import * as format from "../format.js";
import { t } from "../i18n.js";
import { html } from "../lib/preact.js";

const STATUS_AREAS = [
  ["clocking", (values) => (values.message ? t("Clocking: {state} ({message})", values) : t("Clocking: {state}", values))],
  ["connectivity", (values) => (values.message ? t("Connectivity: {state} ({message})", values) : t("Connectivity: {state}", values))],
  ["latency", (values) => (values.message ? t("Latency: {state} ({message})", values) : t("Latency: {state}", values))],
  ["subscriptions", (values) => (values.message ? t("Subscriptions: {state} ({message})", values) : t("Subscriptions: {state}", values))],
];

export function isEnrolled(device) {
  return device.ddm_enrolment_state === "ENROLLED" && Boolean(device.ddm_domain_id);
}

function words(value) {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const text = value.trim().replaceAll("_", " ").toLowerCase();
  return t(text.charAt(0).toUpperCase() + text.slice(1));
}

function when(value) {
  const text = format.timestamp(value);
  return text === format.ABSENT ? undefined : text;
}

function domainStatus(status) {
  if (!status || typeof status !== "object") return undefined;
  const problems = STATUS_AREAS.filter(([key]) => status[key] && status[key] !== "OK").map(
    ([key, describe]) => describe({ state: words(status[key]), message: status.alert_message?.[key] ? t(status.alert_message[key]) : "" }),
  );
  if (problems.length) return html`<span class="state-warn">${problems.join(" · ")}</span>`;
  return status.summary === "OK" ? t("OK") : words(status.summary);
}

export function ManagedSection({ device }) {
  const context = device.ddm_context && device.ddm_context !== device.ddm_domain_name ? device.ddm_context : undefined;
  return html`
    <${Panel} title=${t("Domain")}>
      <${Fields}
        entries=${[
          [t("Domain"), device.ddm_domain_name || undefined],
          [t("Managed context"), context],
          [t("Enrollment"), words(device.ddm_enrolment_state)],
          [t("Connection"), words(device.ddm_connection_state)],
          [t("Status"), domainStatus(device.ddm_status)],
          [t("Connection changed"), when(device.ddm_connection_last_changed)],
          [t("Last sync"), when(device.ddm_last_sync)],
          [t("Server profile"), device.ddm_server_profile || undefined],
        ]}
      />
    <//>
  `;
}
