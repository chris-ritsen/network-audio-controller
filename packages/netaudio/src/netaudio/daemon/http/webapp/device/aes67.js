import { aes67Status } from "../aes67.js";
import { api } from "../api.js";
import { AsyncButton, Fields, FieldRow, Panel } from "../components.js";
import { t } from "../i18n.js";
import { html, useRef } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";

function stateLabel(status) {
  if (status.pending) return t(status.label);
  if (status.current !== null) return status.current ? t("Enabled") : t("Disabled");
  if (status.configured !== null) return status.configured ? t("Enabled") : t("Disabled");
  return undefined;
}

function rtpLabel(status) {
  if (!status.managed) return undefined;
  if (status.rebootRequired === true) return t("Reboot required");
  if (status.rtpAvailable === true) return t("Available");
  return undefined;
}

export function Aes67Section({ device }) {
  const status = aes67Status(device);
  const requestName = deviceRequestName(device);
  const prefix = useRef(null);
  if (status.supported === false) return null;
  const enabled = status.configured ?? status.current;
  const knownPrefix = typeof device.aes67_multicast_prefix === "string" && device.aes67_multicast_prefix.length > 0;
  const entries = [
    ["AES67", status.canConfigure ? undefined : stateLabel(status)],
    [t("DDM domain"), status.managed ? device.ddm_domain_name || undefined : undefined],
    [t("RTP flows"), rtpLabel(status)],
    [t("Multicast address prefix"), knownPrefix && !status.canConfigure ? device.aes67_multicast_prefix : undefined],
  ].filter(([, value]) => value !== undefined);
  if (!entries.length && !status.canConfigure) return null;
  return html`
    <${Panel} title="AES67">
      ${entries.length ? html`<${Fields} entries=${entries} />` : null}
      ${status.canConfigure ? html`
        <${FieldRow} label="AES67">
          ${stateLabel(status) ? html`<span>${stateLabel(status)}</span>` : null}
          <${AsyncButton}
            small
            description=${enabled
              ? device.name ? t("disable AES67 on {device}", { device: device.name }) : t("disable AES67 on device")
              : device.name ? t("enable AES67 on {device}", { device: device.name }) : t("enable AES67 on device")}
            onRun=${() => api.setAes67(requestName, !enabled)}
          >${enabled ? t("Disable") : t("Enable")}<//>
        <//>
        ${!status.managed && knownPrefix ? html`
          <${FieldRow} label=${t("Multicast address prefix")}>
            <input key=${`aes67-prefix-${requestName}`} ref=${prefix} type="text" size="16"
              aria-label=${t("AES67 multicast address prefix")} defaultValue=${device.aes67_multicast_prefix} />
            <${AsyncButton}
              variant="primary" small description=${device.name ? t("set AES67 multicast prefix on {device}", { device: device.name }) : t("set AES67 multicast prefix on device")}
              onRun=${() => api.setAes67MulticastPrefix(requestName, prefix.current.value)}
            >${t("Apply")}<//>
          <//>
        ` : null}
      ` : null}
    <//>
  `;
}
