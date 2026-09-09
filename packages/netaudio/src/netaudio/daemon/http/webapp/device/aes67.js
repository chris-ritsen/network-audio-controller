import { aes67Status } from "../aes67.js";
import { api } from "../api.js";
import { AsyncButton, Fields, FieldRow, Panel } from "../components.js";
import { html, useRef } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";

const modeLabel = (value) => value ? "Enabled" : "Disabled";

function ManagedReadiness({ status, device }) {
  const availability = status.rebootRequired === true
    ? "Reboot required"
    : status.rtpAvailable === true
      ? status.rebootRequired === false ? "Available" : "Support reported"
      : status.rtpAvailable === false ? "Unavailable" : "Not reported";
  return html`
    <${Fields} entries=${[
      ["Domain", device.ddm_domain_name || undefined],
      ["RTP flows", availability],
    ]} />
    ${status.rebootRequired === true
      ? html`<p class="state-warn">DDM reports that a reboot is required before this device can accept RTP flows.</p>`
      : null}
  `;
}

export function Aes67Section({ device }) {
  const status = aes67Status(device);
  const requestName = deviceRequestName(device);
  const prefix = useRef(null);
  const selectedMode = status.configured ?? status.current;
  const knownPrefix = typeof device.aes67_multicast_prefix === "string" && device.aes67_multicast_prefix.length > 0;
  return html`
    <${Panel} title="AES67 config">
      <p role="status" class=${status.pending ? "state-warn" : undefined}>${status.label}</p>
      ${status.managed ? html`<${ManagedReadiness} status=${status} device=${device} />` : null}
      ${!status.managed && status.supported !== false && status.pending
        ? html`<${Fields} entries=${[
            ["Current mode", modeLabel(status.current)],
            ["Configured mode", modeLabel(status.configured)],
          ]} />`
        : null}
      ${status.canConfigure ? html`
        <${FieldRow} label="AES67 mode">
          <${AsyncButton}
            small disabled=${selectedMode === true}
            description=${`enable AES67 on ${device.name || "device"}`}
            onRun=${() => api.setAes67(requestName, true)}
          >Enable<//>
          <${AsyncButton}
            small disabled=${selectedMode === false}
            description=${`disable AES67 on ${device.name || "device"}`}
            onRun=${() => api.setAes67(requestName, false)}
          >Disable<//>
        <//>
        ${knownPrefix ? html`
          <${FieldRow} label="Multicast address prefix">
            <input key=${`aes67-prefix-${requestName}`} ref=${prefix} type="text" size="16"
              aria-label="AES67 multicast address prefix" defaultValue=${device.aes67_multicast_prefix} />
            <${AsyncButton}
              variant="primary" small description=${`set AES67 multicast prefix on ${device.name || "device"}`}
              onRun=${() => api.setAes67MulticastPrefix(requestName, prefix.current.value)}
            >Apply<//>
          <//>
        ` : null}
      ` : null}
    <//>
  `;
}
