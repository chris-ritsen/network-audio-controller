import { Fields, Panel } from "../components.js";
import { aes67Status } from "../aes67.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";
import { scopedDevices } from "../store.js";
import { DiagnosticsSection } from "./diagnostics.js";

const HIDDEN_WORDS = new Set(["", "unknown", "none", "unset", "unavailable", "null", "undefined", "—"]);

const PORT_STATES = {
  master: "Leader",
  leader: "Leader",
  slave: "Follower",
  follower: "Follower",
  listening: "Listening",
  passive: "Passive",
  initializing: "Starting",
  faulty: "Faulty",
  uncalibrated: "Calibrating",
  pre_master: "Becoming leader",
};

const AES67_STATES = new Set([
  "Enabled",
  "Disabled",
  "Enable pending",
  "Disable pending",
  "Configured enabled",
  "Configured disabled",
]);

function shown(value) {
  if (value === null || value === undefined) return undefined;
  const text = String(value).trim();
  return HIDDEN_WORDS.has(text.toLowerCase()) ? undefined : text;
}

function words(value) {
  const text = shown(value);
  if (text === undefined) return undefined;
  const spaced = text.replaceAll("_", " ");
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

function yesNo(value) {
  return typeof value === "boolean" ? (value ? "Yes" : "No") : undefined;
}

function count(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : 0;
}

function versionOf(...candidates) {
  return candidates.map(shown).find(Boolean);
}

function identity(value) {
  return typeof value === "string" ? value.replace(/[:-]/g, "").toLowerCase() : "";
}

export function clockLeaderLabel(device, inventory) {
  const leader = identity(device.ptpv1_master_uuid);
  if (!leader || /^0+$/.test(leader)) return "";
  const matches = Object.values(inventory || {}).filter((candidate) => identity(candidate.ptpv1_device_uuid) === leader);
  const sameDomain = matches.filter((candidate) => (candidate.ddm_domain_id || "") === (device.ddm_domain_id || ""));
  const chosen = sameDomain.length === 1 ? sameDomain : matches;
  return chosen.length === 1 ? format.deviceLabel(chosen[0]) : "";
}

function clockLeader(device, clock) {
  if (String(device.clock_role || clock.clock_role || "").toLowerCase() !== "follower") return undefined;
  return clockLeaderLabel(device, scopedDevices.value) || undefined;
}

function synchronization(device, clock, fresh) {
  const locked = device.ddm_clocking_state?.locked;
  const synchronized = fresh && clock.synchronization === "synchronized" ? true
    : fresh && clock.synchronization === "lost" ? false
    : locked === true || locked === "LOCKED" ? true
    : locked === false || locked === "UNLOCKED" ? false
    : null;
  if (synchronized === null) return undefined;
  return synchronized ? "Synchronized" : html`<span class="state-bad">Not synchronized</span>`;
}

function frequencyOffset(clock, fresh) {
  const value = clock.clock_frequency_offset_parts_per_billion;
  if (!fresh || value == null || !Number.isFinite(Number(value))) return undefined;
  return `${Number((Number(value) / 1000).toFixed(3))} ppm`;
}

function clockMute(clock, fresh) {
  if (!fresh || clock.mute_state !== "muted") return undefined;
  const reasons = (clock.mute_reasons || []).filter(Boolean).join(", ");
  return html`<span class="state-bad">${reasons ? `Muted: ${reasons}` : "Muted"}</span>`;
}

function portEntries(clock, fresh) {
  if (!fresh) return [];
  const ports = (clock.clock_port_records || []).filter(
    (port) => port.user_disabled !== true && String(port.state || "").toLowerCase() !== "disabled",
  );
  const versions = ports.map((port) => port.ptp_version);
  return ports.map((port) => {
    const repeated = versions.filter((version) => version === port.ptp_version).length > 1;
    const label = port.ptp_version == null ? `Clock port ${port.record_number}` : `PTPv${port.ptp_version}${repeated ? ` port ${port.record_number}` : ""}`;
    const state = PORT_STATES[String(port.state || "").toLowerCase()] || words(port.state);
    const details = [
      state,
      words(port.transport_path),
      port.network_interface_index === 1 ? "Secondary network" : undefined,
      port.link_down === true ? "Link down" : undefined,
    ].filter(Boolean);
    return [label, details.length ? details.join(" · ") : undefined];
  });
}

function subscriptionProblems(problems) {
  if (!problems.length) return undefined;
  return html`<ul class="status-problems">
    ${problems.map(
      (entry) => html`<li key=${entry.rx_channel} class="state-warn">${entry.rx_channel}: ${format.subscriptionStatusText(entry)}</li>`,
    )}
  </ul>`;
}

export function StatusSection({ device }) {
  const subscriptions = (device.subscriptions || []).filter((entry) => entry.tx_device);
  const problems = subscriptions.filter(
    (entry) => entry.status && entry.status.severity !== "ok",
  );
  const clock = device.clock_status || {};
  const fresh = format.clockStatusFresh(device);
  const model = shown(format.deviceModelName(device));
  const platform = shown(device.platform_model_name);
  const transmitters = count(device.tx_count);
  const receivers = count(device.rx_count);
  const mac = format.macAddress(device);
  const aes67 = aes67Status(device).label;
  return html`
    <div class="flex flex-col gap-4">
      <div class="split">
        <${Panel} title="Device">
          <${Fields}
            entries=${[
              ["Model", model],
              ["Manufacturer", shown(device.manufacturer)],
              ["Dante platform", platform && platform !== model ? platform : undefined],
              ["Product version", versionOf(device.friendly_product_version, device.product_version, device.ddm_product_version)],
              [
                device.platform_hardware_version ? "Dante firmware" : "Dante software",
                versionOf(device.platform_software_version, device.ddm_dante_version),
              ],
              ["Hardware version", versionOf(device.platform_hardware_version, device.ddm_dante_hardware_version)],
              ["IP address", shown(device.ipv4)],
              ["MAC address", mac === format.ABSENT ? undefined : mac],
              ["Lock", device.is_locked == null ? undefined : device.is_locked ? "Locked" : "Unlocked"],
              ["Domain", device.ddm_enrolment_state === "ENROLLED" ? shown(device.ddm_domain_name) : undefined],
            ]}
          />
        <//>
        <${Panel} title="Audio">
          <${Fields}
            entries=${[
              ["Sample rate", device.sample_rate_hz ? format.sampleRate(device.sample_rate_hz) : undefined],
              ["Encoding", device.encoding ? `PCM ${device.encoding}` : undefined],
              ["Latency", device.latency_ms == null ? undefined : format.latency(device.latency_ms)],
              ["Transmit channels", transmitters || undefined],
              ["Receive channels", receivers || undefined],
              ["Subscriptions", receivers ? subscriptions.length : undefined],
              ["Subscription problems", subscriptionProblems(problems)],
              ["AES67", AES67_STATES.has(aes67) ? aes67 : undefined],
            ]}
          />
        <//>
        <${Panel} title="Clock">
          <${Fields}
            entries=${[
              ["Sync", synchronization(device, clock, fresh)],
              ["Muted", clockMute(clock, fresh)],
              ["Role", words(device.clock_role)],
              ["Leader", clockLeader(device, clock)],
              ["Preferred leader", yesNo(device.preferred_leader)],
              ["Source", words(device.clock_source)],
              ["Subdomain", device.ddm_enrolment_state === "ENROLLED" ? undefined : shown(device.clock_subdomain_presentation?.label)],
              ["Frequency offset", String(device.clock_role).toLowerCase() === "leader" ? undefined : frequencyOffset(clock, fresh)],
              ["Word clock", words(fresh ? clock.word_clock_state : undefined)],
              ...portEntries(clock, fresh),
            ]}
          />
        <//>
      </div>
      <${DiagnosticsSection} device=${device} />
    </div>
  `;
}
