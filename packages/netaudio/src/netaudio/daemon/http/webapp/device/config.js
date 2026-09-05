import { api } from "../api.js";
import { AsyncButton, Disclosure, Fields, FieldRow, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { html, useRef } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";

function parseClockSource(rawValue) {
  const trimmed = String(rawValue).trim();
  const parsed = trimmed.toLowerCase().startsWith("0x") ? Number.parseInt(trimmed.slice(2), 16) : Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 0xffff) {
    throw new Error("clock source must be an integer from 0 through 65535 (decimal or 0x hex)");
  }
  return parsed;
}

function RenameControl({ device, requestName }) {
  const input = useRef(null);
  return html`
    <${FieldRow} label="Device name">
      <input key=${`rename-${requestName}`} ref=${input} type="text" size="28" defaultValue=${device.name} />
      <${AsyncButton}
        variant="primary"
        small
        description=${`rename ${requestName}`}
        onRun=${() => api.renameDevice(requestName, input.current.value)}
      >
        Apply
      <//>
    <//>
  `;
}

function SampleRateControl({ device, requestName }) {
  const select = useRef(null);
  const confirm = useRef(null);
  const supported = device.supported_sample_rates_hz || [];
  if (!supported.length) {
    return html`<${FieldRow} label="Sample rate">
      <span>${format.sampleRate(device.sample_rate_hz)} — not configurable on this device</span>
    <//>`;
  }
  return html`
    <${FieldRow} label="Sample rate">
      <select key=${`sample-rate-${requestName}`} ref=${select}>
        ${supported.map(
          (value) => html`<option key=${value} value=${value} selected=${Number(value) === Number(device.sample_rate_hz)}>
            ${format.sampleRate(value)}
          </option>`,
        )}
      </select>
      <label class="inline">
        <input type="checkbox" ref=${confirm} key=${`sample-rate-confirm-${requestName}`} />
        I understand this interrupts audio and may drop subscriptions
      </label>
      <${AsyncButton}
        variant="primary"
        small
        description=${`set sample rate on ${requestName}`}
        onRun=${() => api.setSampleRate(requestName, Number(select.current.value), confirm.current.checked)}
      >
        Apply
      <//>
    <//>
  `;
}

function EncodingControl({ device, requestName }) {
  const select = useRef(null);
  const supported = device.supported_encodings || [];
  if (!supported.length) {
    return html`<${FieldRow} label="Encoding">
      <span>${format.text(device.encoding)} — not configurable on this device</span>
    <//>`;
  }
  return html`
    <${FieldRow} label="Encoding">
      <select key=${`encoding-${requestName}`} ref=${select}>
        ${supported.map(
          (value) => html`<option key=${value} value=${value} selected=${Number(value) === Number(device.encoding)}>
            PCM ${value}
          </option>`,
        )}
      </select>
      <${AsyncButton}
        variant="primary"
        small
        description=${`set encoding on ${requestName}`}
        onRun=${() => api.setEncoding(requestName, Number(select.current.value))}
      >
        Apply
      <//>
    <//>
  `;
}

function PullupControl({ device, requestName }) {
  const select = useRef(null);
  const supported = device.supported_sample_rate_pullup_raw_values || [];
  if (!supported.length) {
    return null;
  }
  return html`
    <${FieldRow} label="Sample rate pull-up">
      <select key=${`pullup-${requestName}`} ref=${select}>
        ${supported.map(
          (value) => html`<option key=${value} value=${value} selected=${Number(value) === Number(device.sample_rate_pullup_raw_value)}>
            ${value}
          </option>`,
        )}
      </select>
      <${AsyncButton}
        variant="primary"
        small
        description=${`set sample rate pull-up on ${requestName}`}
        onRun=${() => api.setSampleRatePullup(requestName, Number(select.current.value))}
      >
        Apply
      <//>
    <//>
  `;
}

function ClockingControls({ device, requestName }) {
  const source = useRef(null);
  const subdomain = useRef(null);
  return html`
    <${Panel}
      title="Clocking"
      actions=${html`<${AsyncButton}
        small
        description=${`refresh clock status for ${requestName}`}
        onRun=${() => api.refreshClock(requestName)}
      >
        Refresh clock status
      <//>`}
    >
      <${FieldRow} label="Preferred leader">
        <span>${format.preferredLeader(device.preferred_leader)}</span>
        <${AsyncButton}
          small
          description=${`set ${requestName} as preferred leader`}
          onRun=${() => api.setPreferredLeader(requestName, true)}
        >
          Enable
        <//>
        <${AsyncButton}
          small
          description=${`clear preferred leader on ${requestName}`}
          onRun=${() => api.setPreferredLeader(requestName, false)}
        >
          Disable
        <//>
      <//>
      <${FieldRow} label="Clock source">
        <span>${format.clockSourceCode(device.clock_source_code)}</span>
        <input key=${`clock-source-${requestName}`} ref=${source} type="text" size="8" defaultValue=${device.clock_source_code} />
        <${AsyncButton}
          variant="primary"
          small
          description=${`set clock source on ${requestName}`}
          onRun=${() => api.setClockSource(requestName, parseClockSource(source.current.value))}
        >
          Apply
        <//>
      <//>
      <${FieldRow} label="Clock subdomain">
        <span>${format.clockSubdomain(device.clock_subdomain)}</span>
        <input
          key=${`clock-subdomain-${requestName}`}
          ref=${subdomain}
          type="text"
          size="18"
          placeholder="name, hex:00ff, or unset"
          defaultValue=${format.clockSubdomainInputValue(device.clock_subdomain)}
        />
        <${AsyncButton}
          variant="primary"
          small
          description=${`set clock subdomain on ${requestName}`}
          onRun=${() => api.setClockSubdomain(requestName, subdomain.current.value)}
        >
          Apply
        <//>
      <//>
    <//>
  `;
}

export function DeviceConfigSection({ device }) {
  const requestName = deviceRequestName(device);
  return html`
    <div class="stack">
      <${Panel}
        title="Device config"
        actions=${html`
          <${AsyncButton} small description=${`identify ${requestName}`} onRun=${() => api.identify(requestName)}>Identify<//>
          <${AsyncButton} small description=${`refresh ${requestName}`} onRun=${() => api.refresh(requestName)}>Re-read device<//>
          <${AsyncButton} small variant="danger" description=${`reboot ${requestName}`} onRun=${() => api.reboot(requestName)}>Reboot<//>
        `}
      >
        <${RenameControl} device=${device} requestName=${requestName} />
        <${SampleRateControl} device=${device} requestName=${requestName} />
        <${EncodingControl} device=${device} requestName=${requestName} />
        <${PullupControl} device=${device} requestName=${requestName} />
      <//>
      <${ClockingControls} device=${device} requestName=${requestName} />
      <${Disclosure} summary="Advanced audio and clock fields">
        <${Fields}
          entries=${[
            ["Supported sample rates", (device.supported_sample_rates_hz || []).map(format.sampleRate).join(", ") || format.ABSENT],
            ["Sample rate channel capacities", html`<${Value} value=${device.sample_rate_channel_capacities} />`],
            ["Supported encodings", html`<${Value} value=${device.supported_encodings} />`],
            ["Encoding configurable", html`<${Value} value=${device.encoding_configurable} />`],
            ["Bit depth", html`<${Value} value=${device.bit_depth} />`],
            ["Requested sample rate pull-up", html`<${Value} value=${device.requested_sample_rate_pullup_raw_value} />`],
            ["Clock identity", html`<${Value} value=${device.clock_identity} />`],
            ["Leader clock identity", html`<${Value} value=${device.leader_clock_identity} />`],
            ["Clock port state code", html`<${Value} value=${device.clock_port_state_code} />`],
            ["Clock port records", html`<${Value} value=${device.clock_port_records} />`],
            ["Domain clocking state", html`<${Value} value=${device.ddm_clocking_state} />`],
            ["Domain clock preferences", html`<${Value} value=${device.ddm_clock_preferences} />`],
          ]}
        />
      <//>
    </div>
  `;
}

export function LatencySection({ device }) {
  const requestName = deviceRequestName(device);
  const control = useRef(null);
  const values = new Set((device.standard_latency_choices_ms || []).map(Number));
  for (const candidate of [device.latency_ms, device.configured_latency_ms, device.default_latency_ms]) {
    if (candidate !== null && candidate !== undefined) {
      values.add(Number(candidate));
    }
  }
  const choices = [...values].sort((first, second) => first - second);
  const current = device.configured_latency_ms ?? device.latency_ms;
  const health = device.receiver_flow_connection_health;
  return html`
    <div class="stack">
      <${Panel} title="Device latency">
        <${FieldRow} label="Latency">
          ${choices.length
            ? html`<select key=${`latency-${requestName}`} ref=${control}>
                ${choices.map(
                  (value) => html`<option key=${value} value=${value} selected=${Number(value) === Number(current)}>${value} ms</option>`,
                )}
              </select>`
            : html`<input key=${`latency-${requestName}`} ref=${control} type="number" min="0" defaultValue=${current} />`}
          <${AsyncButton}
            variant="primary"
            small
            description=${`set latency on ${requestName}`}
            onRun=${() => api.setLatency(requestName, Number(control.current.value))}
          >
            Apply
          <//>
        <//>
        <${Fields}
          entries=${[
            ["Current latency", format.latency(device.latency_ms)],
            ["Configured latency", format.latency(device.configured_latency_ms)],
            ["Active latency", format.latency(device.active_latency_ms)],
            ["Default latency", format.latency(device.default_latency_ms)],
            ["Minimum latency", format.latency(device.min_latency_ms)],
            ["Maximum latency", format.latency(device.max_latency_ms)],
          ]}
        />
      <//>
      ${health
        ? html`<${Panel} title="Receive flow latency">
            <${Value} value=${health} />
          <//>`
        : null}
    </div>
  `;
}

export function Aes67Section({ device }) {
  const requestName = deviceRequestName(device);
  const prefix = useRef(null);
  if (device.aes67_supported === false) {
    return html`<${Panel} title="AES67 config">
      <div class="notice">This device does not support AES67.</div>
    <//>`;
  }
  return html`
    <${Panel} title="AES67 config">
      <${FieldRow} label="AES67 mode">
        <span>${format.text(device.aes67_current)}</span>
        <${AsyncButton} small description=${`enable AES67 on ${requestName}`} onRun=${() => api.setAes67(requestName, true)}>
          Enable
        <//>
        <${AsyncButton} small description=${`disable AES67 on ${requestName}`} onRun=${() => api.setAes67(requestName, false)}>
          Disable
        <//>
      <//>
      <${FieldRow} label="Multicast address prefix">
        <input
          key=${`aes67-prefix-${requestName}`}
          ref=${prefix}
          type="text"
          size="16"
          defaultValue=${device.aes67_multicast_prefix || ""}
        />
        <${AsyncButton}
          variant="primary"
          small
          description=${`set AES67 multicast prefix on ${requestName}`}
          onRun=${() => api.setAes67MulticastPrefix(requestName, prefix.current.value)}
        >
          Apply
        <//>
      <//>
      <${Fields}
        entries=${[
          ["Supported", html`<${Value} value=${device.aes67_supported} />`],
          ["Configured", html`<${Value} value=${device.aes67_configured} />`],
          ["Current", html`<${Value} value=${device.aes67_current} />`],
          ["Multicast prefix", html`<${Value} value=${device.aes67_multicast_prefix} />`],
        ]}
      />
    <//>
  `;
}
