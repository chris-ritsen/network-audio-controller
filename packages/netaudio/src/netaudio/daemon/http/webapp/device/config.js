import { api } from "../api.js";
import { AsyncButton, FieldRow, Panel } from "../components.js";
import * as format from "../format.js";
import { html, useRef, useState } from "../lib/preact.js";
import { deviceRequestName, inventoryReady } from "../store.js";
import { isEnrolled } from "./managed.js";


function RenameControl({ device, requestName }) {
  const input = useRef(null);
  return html`
    <${FieldRow} label="Device name">
      <input key=${`rename-${requestName}`} ref=${input} type="text" size="28" defaultValue=${device.name} />
      <${AsyncButton}
        variant="primary"
        small
        description=${`rename ${device.name || "device"}`}
        onRun=${() => api.renameDevice(requestName, input.current.value)}
      >
        Apply
      <//>
    <//>
  `;
}

function SampleRateControl({ device, requestName }) {
  const select = useRef(null);
  const [pending, setPending] = useState(null);
  const supported = device.supported_sample_rates_hz || [];
  const apply = async () => {
    setPending(null);
    const rate = Number(select.current.value);
    try {
      return await api.setSampleRate(requestName, rate, false);
    } catch (error) {
      const preflight = error.payload?.preflight;
      if (error.status !== 409 || preflight?.requires_destructive_confirmation !== true
        || preflight?.is_classified !== true || preflight?.topology_characterized !== true
        || preflight?.target_sample_rate_hertz !== rate) throw error;
      setPending({ requestName, rate, losses: preflight.destructive_transmitter_membership_loss });
    }
  };
  if (!supported.length) {
    return html`<${FieldRow} label="Sample rate">
      <span>${format.sampleRate(device.sample_rate_hz)}${inventoryReady.value ? " — supported values unavailable" : ""}</span>
    <//>`;
  }
  return html`
    <${FieldRow} label="Sample rate">
      <select key=${`sample-rate-${requestName}`} ref=${select} aria-label="Sample rate" onChange=${() => setPending(null)}>
        ${supported.map(
          (value) => html`<option key=${value} value=${value} selected=${Number(value) === Number(device.sample_rate_hz)}>
            ${format.sampleRate(value)}
          </option>`,
        )}
      </select>
      <${AsyncButton}
        variant="primary"
        small
        description=${`set sample rate on ${device.name || "device"}`}
        onRun=${apply}
      >
        Apply
      <//>
      ${pending?.requestName === requestName ? html`
        <div class="flex flex-col gap-2" role="alert">
          <span>Changing to ${format.sampleRate(pending.rate)} removes these transmitter channels from existing flows:</span>
          <ul>${pending.losses.map((loss) => html`<li>Flow ${loss.flow_number}: channels ${loss.removed_channel_members.join(", ")}</li>`)}</ul>
          <${AsyncButton} small variant="danger" description=${`change sample rate and remove flow channels on ${device.name || "device"}`}
            onRun=${async () => {
              const result = await api.setSampleRate(requestName, pending.rate, true);
              setPending(null);
              return result;
            }}>Change rate and remove channels<//>
          <button type="button" class="btn btn-sm" onClick=${() => setPending(null)}>Cancel<//>
        </div>
      ` : null}
    <//>
  `;
}

function EncodingControl({ device, requestName }) {
  const select = useRef(null);
  const supported = device.supported_encodings || [];
  if (!supported.length) {
    return html`<${FieldRow} label="Encoding">
      <span>${format.text(device.encoding)}${inventoryReady.value ? " — supported values unavailable" : ""}</span>
    <//>`;
  }
  return html`
    <${FieldRow} label="Encoding">
      <select key=${`encoding-${requestName}`} ref=${select} aria-label="Encoding">
        ${supported.map(
          (value) => html`<option key=${value} value=${value} selected=${Number(value) === Number(device.encoding)}>
            PCM ${value}
          </option>`,
        )}
      </select>
      <${AsyncButton}
        variant="primary"
        small
        description=${`set encoding on ${device.name || "device"}`}
        onRun=${() => api.setEncoding(requestName, Number(select.current.value))}
      >
        Apply
      <//>
    <//>
  `;
}

function PullupControl({ device, requestName }) {
  const select = useRef(null);
  const labels = new Map([[0, "None"], [1, "+4.1667%"], [2, "+0.1%"], [3, "−0.1%"], [4, "−4.0%"]]);
  const supported = (device.supported_sample_rate_pullup_raw_values || []).filter((value) => labels.has(value));
  if (!supported.length) {
    return null;
  }
  return html`
    <${FieldRow} label="Sample rate pull-up">
      <select key=${`pullup-${requestName}`} ref=${select}>
        ${supported.map(
          (value) => html`<option key=${value} value=${value} selected=${Number(value) === Number(device.sample_rate_pullup_raw_value)}>
            ${labels.get(value)}
          </option>`,
        )}
      </select>
      <${AsyncButton}
        variant="primary"
        small
        description=${`set sample rate pull-up on ${device.name || "device"}`}
        onRun=${() => api.setSampleRatePullup(requestName, Number(select.current.value))}
      >
        Apply
      <//>
    <//>
  `;
}

function ClockingControls({ device, requestName }) {
  const subdomain = useRef(null);
  return html`
    <${Panel}
      title="Clocking"
      actions=${html`<${AsyncButton}
        small
        description=${`refresh clock status for ${device.name || "device"}`}
        onRun=${() => api.refreshClock(requestName)}
      >
        Refresh clock status
      <//>`}
    >
      <${FieldRow} label="Preferred leader">
        <span>${format.preferredLeader(device.preferred_leader)}</span>
        <${AsyncButton}
          small
          description=${`set ${device.name || "device"} as preferred leader`}
          onRun=${() => api.setPreferredLeader(requestName, true)}
        >
          Enable
        <//>
        <${AsyncButton}
          small
          description=${`clear preferred leader on ${device.name || "device"}`}
          onRun=${() => api.setPreferredLeader(requestName, false)}
        >
          Disable
        <//>
      <//>
      <${FieldRow} label="Clock subdomain">
        ${isEnrolled(device) ? html`<span>Managed by DDM</span>` : html`
        <span>${format.clockSubdomain(device.clock_subdomain)}</span>
        <input
          key=${`clock-subdomain-${requestName}`}
          ref=${subdomain}
          type="text"
          size="18"
          placeholder="Subdomain name"
          defaultValue=${format.clockSubdomainInputValue(device.clock_subdomain)}
        />
        <${AsyncButton}
          variant="primary"
          small
          description=${`set clock subdomain on ${device.name || "device"}`}
          onRun=${() => api.setClockSubdomain(requestName, subdomain.current.value)}
        >
          Apply
        <//>
        `}
      <//>
    <//>
  `;
}

export function DeviceConfigSection({ device }) {
  const requestName = deviceRequestName(device);
  return html`
    <div class="flex flex-col gap-4">
      <${Panel}
        title="Device config"
        actions=${html`
          <${AsyncButton} small description=${`refresh settings for ${device.name || "device"}`} onRun=${() => api.refresh(requestName)}>Refresh settings<//>
          <${AsyncButton} small description=${`identify ${device.name || "device"}`} onRun=${() => api.identify(requestName)}>Identify<//>
          <${AsyncButton} small variant="danger" description=${`reboot ${device.name || "device"}`} onRun=${() => api.reboot(requestName)}>Reboot<//>
        `}
      >
        <${RenameControl} device=${device} requestName=${requestName} />
        <${SampleRateControl} device=${device} requestName=${requestName} />
        <${EncodingControl} device=${device} requestName=${requestName} />
        <${LatencyControl} device=${device} />
        <${PullupControl} device=${device} requestName=${requestName} />
      <//>
      <${ClockingControls} device=${device} requestName=${requestName} />
    </div>
  `;
}

function LatencyControl({ device }) {
  const requestName = deviceRequestName(device);
  const control = useRef(null);
  const customInput = useRef(null);
  const [custom, setCustom] = useState(false);
  const minimum = device.min_latency_ms;
  const maximum = device.max_latency_ms;
  const hasRange = Number.isFinite(minimum) && Number.isFinite(maximum) && minimum >= 0 && maximum >= minimum;
  const values = new Set((device.standard_latency_choices_ms || []).map(Number));
  for (const candidate of [device.latency_ms, device.configured_latency_ms, device.default_latency_ms]) {
    if (candidate !== null && candidate !== undefined) {
      values.add(Number(candidate));
    }
  }
  const choices = [...values].sort((first, second) => first - second);
  const current = device.configured_latency_ms ?? device.latency_ms;
  if (!choices.length && !hasRange) {
    return html`<${FieldRow} label="Latency"><span>Settings unavailable</span><//>`;
  }
  return html`
        <${FieldRow} label="Latency">
          ${choices.length
            ? html`<select key=${`latency-${requestName}`} ref=${control} aria-label="Latency" onChange=${(event) => setCustom(event.currentTarget.value === "custom")}>
                ${choices.map(
                  (value) => html`<option key=${value} value=${value} selected=${Number(value) === Number(current)}>${value} ms</option>`,
                )}
                ${hasRange ? html`<option value="custom">Custom…</option>` : null}
              </select>`
            : null}
          ${custom || !choices.length ? html`
            <input key=${`custom-latency-${requestName}`} ref=${customInput} aria-label="Custom latency in milliseconds"
              type="number" min=${minimum} max=${maximum} step="any" required defaultValue=${current ?? minimum} />
            <span>ms (${minimum}–${maximum})</span>
          ` : null}
          <${AsyncButton}
            variant="primary"
            small
            description=${`set latency on ${device.name || "device"}`}
            onRun=${() => {
              const input = custom || !choices.length ? customInput.current : control.current;
              if (!input.checkValidity()) throw new Error(`Enter a latency from ${minimum} to ${maximum} ms`);
              return api.setLatency(requestName, Number(input.value));
            }}
          >
            Apply
          <//>
        <//>
  `;
}
