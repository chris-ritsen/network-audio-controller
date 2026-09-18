import { api } from "../api.js";
import { AsyncButton, FieldRow, Panel } from "../components.js";
import * as format from "../format.js";
import { html, useRef, useState } from "../lib/preact.js";
import { deviceRequestName, inventoryReady } from "../store.js";
import {
  operationReasonText,
  operationWritable,
  performanceOperationReasonText,
  performanceOperationWritable,
} from "./availability.js";
import { isEnrolled } from "./managed.js";

function RenameControl({ device, requestName }) {
  const input = useRef(null);
  return html`
    <${FieldRow} label="Device name">
      <input
        key=${`rename-${requestName}`}
        ref=${input}
        type="text"
        size="28"
        defaultValue=${device.name}
      />
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
  const writable = operationWritable(device, "sample_rate");
  const apply = async () => {
    setPending(null);
    const rate = Number(select.current.value);
    try {
      return await api.setSampleRate(requestName, rate, false);
    } catch (error) {
      const preflight = error.payload?.preflight;
      if (
        error.status !== 409 ||
        preflight?.requires_destructive_confirmation !== true ||
        preflight?.is_classified !== true ||
        preflight?.topology_characterized !== true ||
        preflight?.target_sample_rate_hertz !== rate
      )
        throw error;
      setPending({
        requestName,
        rate,
        losses: preflight.destructive_transmitter_membership_loss,
      });
    }
  };
  if (!writable || !supported.length) {
    return html`<${FieldRow} label="Sample rate">
      <span>${format.sampleRate(device.sample_rate_hz)}</span>
      <span class="text-sm opacity-70"
        >${
          writable
            ? inventoryReady.value
              ? "Supported values are unavailable."
              : "Loading supported values."
            : operationReasonText(device, "sample_rate")
        }</span
      >
    <//>`;
  }
  return html`
    <${FieldRow} label="Sample rate">
      <select
        key=${`sample-rate-${requestName}`}
        ref=${select}
        aria-label="Sample rate"
        onChange=${() => setPending(null)}
      >
        ${supported.map(
          (value) =>
            html`<option
              key=${value}
              value=${value}
              selected=${Number(value) === Number(device.sample_rate_hz)}
            >
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
      ${
        pending?.requestName === requestName
          ? html`
              <div class="flex flex-col gap-2" role="alert">
                <span
                  >Changing to ${format.sampleRate(pending.rate)} removes these
                  transmitter channels from existing flows:</span
                >
                <ul>
                  ${pending.losses.map((loss) => html`<li>Flow ${loss.flow_number}: channels ${loss.removed_channel_members.join(", ")}</li>`)}
                </ul>
                <${AsyncButton}
                  small
                  variant="danger"
                  description=${`change sample rate and remove flow channels on ${device.name || "device"}`}
                  onRun=${async () => {
                    const result = await api.setSampleRate(
                      requestName,
                      pending.rate,
                      true,
                    );
                    setPending(null);
                    return result;
                  }}
                  >Change rate and remove channels<//
                >
                <button
                  type="button"
                  class="btn btn-sm"
                  onClick=${() => setPending(null)}
                >
                  Cancel
                <//>
              </div>
            `
          : null
      }
    <//>
  `;
}

function EncodingControl({ device, requestName }) {
  const select = useRef(null);
  const supported = device.supported_encodings || [];
  const writable = operationWritable(device, "encoding");
  if (!writable || !supported.length) {
    return html`<${FieldRow} label="Encoding">
      <span>${format.text(device.encoding)}</span>
      <span class="text-sm opacity-70"
        >${
          writable
            ? inventoryReady.value
              ? "Supported values are unavailable."
              : "Loading supported values."
            : operationReasonText(device, "encoding")
        }</span
      >
    <//>`;
  }
  return html`
    <${FieldRow} label="Encoding">
      <select
        key=${`encoding-${requestName}`}
        ref=${select}
        aria-label="Encoding"
      >
        ${supported.map(
          (value) =>
            html`<option
              key=${value}
              value=${value}
              selected=${Number(value) === Number(device.encoding)}
            >
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
  const labels = new Map([
    [0, "None"],
    [1, "+4.1667%"],
    [2, "+0.1%"],
    [3, "−0.1%"],
    [4, "−4.0%"],
  ]);
  const supported = (
    device.supported_sample_rate_pullup_raw_values || []
  ).filter((value) => labels.has(value));
  const writable = operationWritable(device, "sample_rate_pullup");
  if (!writable || !supported.length) {
    const current =
      labels.get(Number(device.sample_rate_pullup_raw_value)) ??
      format.text(device.sample_rate_pullup_raw_value);
    return html`<${FieldRow} label="Sample rate pull-up">
      <span>${current}</span>
      <span class="text-sm opacity-70"
        >${
          writable
            ? "Supported values are unavailable."
            : operationReasonText(device, "sample_rate_pullup")
        }</span
      >
    <//>`;
  }
  return html`
    <${FieldRow} label="Sample rate pull-up">
      <select key=${`pullup-${requestName}`} ref=${select}>
        ${supported.map(
          (value) =>
            html`<option
              key=${value}
              value=${value}
              selected=${Number(value) === Number(device.sample_rate_pullup_raw_value)}
            >
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
  const preferred = useRef(null);
  const source = useRef(null);
  const subdomain = useRef(null);
  const unicast = useRef(null);
  const [changeSubdomain, setChangeSubdomain] = useState(false);
  const status = device.clock_status || {};
  const caps = status.clock_capabilities;
  const fresh = format.clockStatusFresh(device);
  const managed = isEnrolled(device);
  const preferredAllowed =
    fresh &&
    caps != null &&
    !(caps & 0x120) &&
    (status.record_revision < 0x072e ||
      status.preferred_leader_locked === false);
  const named = fresh && !!(caps & 4);
  const perPort = !!(caps & 0x200);
  const unicastAllowed = fresh && !!(caps & 0x208);
  const apply = async () => {
    const changes = {};
    if (preferredAllowed && preferred.current?.value !== "keep")
      changes.preferred_leader = preferred.current.value === "true";
    if (source.current?.value !== "keep")
      changes.clock_source = Number(source.current.value);
    if (named && changeSubdomain) changes.subdomain = subdomain.current.value;
    if (unicastAllowed && unicast.current?.value !== "keep")
      changes[
        perPort
          ? "aggregate_ptpv1_unicast_delay_requests"
          : "global_unicast_delay_requests"
      ] = unicast.current.value === "true";
    const result = await api.setClockConfiguration(requestName, changes);
    return result;
  };
  return html`
    <${Panel}
      title="Clocking"
      actions=${html`<${AsyncButton} small onRun=${() => api.refreshClock(requestName)}>Refresh clock status<//>`}
    >
      ${
        managed
          ? html`<p>Clock settings are managed by DDM.</p>
              <${AsyncButton}
                small
                onRun=${() => api.setPreferredLeader(requestName, true)}
                >Prefer this device<//
              >
              <${AsyncButton}
                small
                onRun=${() => api.setPreferredLeader(requestName, false)}
                >Clear preference<//
              >`
          : html`
              ${!fresh && html`<p>Refresh clock status to check which settings this device allows.</p>`}
              <${FieldRow} label="Preferred leader">
                <select ref=${preferred} disabled=${!preferredAllowed}>
                  <option value="keep">Keep current setting</option>
                  <option value="true">On</option>
                  <option value="false">Off</option>
                </select>
                ${!preferredAllowed && fresh && html`<span>Unavailable or locked</span>`}
              <//>
              <${FieldRow} label="Clock source">
                <select ref=${source} disabled=${!fresh}>
                  <option value="keep">Keep current setting</option>
                  <option value="0">Internal</option>
                  ${(device.supported_clock_sources || []).filter((value) => value === 1 || value === 2).map((value) => html`<option value=${value}>${value === 1 ? "External/BNC" : "AES"}</option>`)}
                </select>
              <//>
              <${FieldRow} label="Clock subdomain">
                <label
                  ><input
                    type="checkbox"
                    disabled=${!named}
                    checked=${changeSubdomain}
                    onChange=${(event) => setChangeSubdomain(event.target.checked)}
                  />
                  Change</label
                >
                <input
                  ref=${subdomain}
                  disabled=${!named || !changeSubdomain}
                  maxlength="15"
                  defaultValue=${format.clockSubdomainInputValue(device.clock_subdomain)}
                />
              <//>
              <${FieldRow}
                label=${perPort ? "PTPv1 unicast delay requests" : "Unicast delay requests"}
              >
                <select ref=${unicast} disabled=${!unicastAllowed}>
                  <option value="keep">Keep current setting</option>
                  <option value="true">On</option>
                  <option value="false">Off</option>
                </select>
              <//>
              <${AsyncButton}
                variant="primary"
                disabled=${!fresh}
                onRun=${apply}
                >Apply clock settings<//
              >
            `
      }
    <//>
  `;
}

function PerformancePairControl({
  device,
  requestName,
  operation,
  label,
  run,
}) {
  const latency = useRef(null);
  const frames = useRef(null);
  if (!performanceOperationWritable(device, operation)) {
    return html`<${FieldRow} label=${label}>
      <span class="text-sm opacity-70"
        >${performanceOperationReasonText(device, operation)}</span
      >
    <//>`;
  }
  return html`<${FieldRow} label=${label}>
    <input
      ref=${latency}
      aria-label=${`${label} latency in microseconds`}
      type="number"
      min="0"
      max="4294967"
      step="1"
      required
      placeholder="Latency µs"
    />
    <input
      ref=${frames}
      aria-label=${`${label} frames per packet`}
      type="number"
      min="0"
      max="65535"
      step="1"
      required
      placeholder="Frames/packet"
    />
    <${AsyncButton}
      variant="primary"
      small
      description=${`set ${label.toLowerCase()} on ${device.name || "device"}`}
      onRun=${() => {
        if (
          !latency.current.checkValidity() ||
          !frames.current.checkValidity()
        ) {
          throw new Error(
            "Enter integer latency and frames-per-packet values in range",
          );
        }
        return run(
          requestName,
          Number(latency.current.value),
          Number(frames.current.value),
        );
      }}
      >Apply<//
    >
  <//>`;
}

function PerformanceControls({ device, requestName }) {
  const slots = useRef(null);
  const slotsWritable = performanceOperationWritable(
    device,
    "receive_flow_default_slots",
  );
  const storeWritable = performanceOperationWritable(
    device,
    "store_current_configuration",
  );
  return html`<${Panel}
    title="Flow performance"
    actions=${
      storeWritable
        ? html`<${AsyncButton}
            small
            description=${`request configuration storage on ${device.name || "device"}`}
            onRun=${() => api.storeCurrentConfiguration(requestName)}
            >Store current configuration<//
          >`
        : html`<span class="text-sm opacity-70"
            >Storage unavailable:
            ${performanceOperationReasonText(device, "store_current_configuration")}</span
          >`
    }
  >
    <${PerformancePairControl}
      device=${device}
      requestName=${requestName}
      operation="receive_flow_performance"
      label="Receive flow"
      run=${api.setReceiveFlowPerformance}
    />
    <${PerformancePairControl}
      device=${device}
      requestName=${requestName}
      operation="transmit_flow_performance"
      label="Transmit flow"
      run=${api.setTransmitFlowPerformance}
    />
    <${PerformancePairControl}
      device=${device}
      requestName=${requestName}
      operation="unicast_performance"
      label="Unicast"
      run=${api.setUnicastPerformance}
    />
    <${FieldRow} label="Receive default slots">
      ${
        slotsWritable
          ? html`
              <input
                ref=${slots}
                aria-label="Receive flow default slots"
                type="number"
                min="0"
                max="65535"
                step="1"
                required
              />
              <${AsyncButton}
                variant="primary"
                small
                description=${`set receive-flow default slots on ${device.name || "device"}`}
                onRun=${() => {
                  if (!slots.current.checkValidity())
                    throw new Error("Enter default slots from 0 through 65535");
                  return api.setReceiveFlowDefaultSlots(
                    requestName,
                    Number(slots.current.value),
                  );
                }}
                >Apply<//
              >
            `
          : html`<span class="text-sm opacity-70"
              >${performanceOperationReasonText(device, "receive_flow_default_slots")}</span
            >`
      }
    <//>
    <p class="text-sm opacity-70">
      Storage acknowledgement does not confirm persistence. Persistence requires
      an independent signal or post-reboot readback.
    </p>
  <//>`;
}

export function DeviceConfigSection({ device }) {
  const requestName = deviceRequestName(device);
  const identifyWritable = operationWritable(device, "identify");
  return html`
    <div class="flex flex-col gap-4">
      <${Panel}
        title="Device config"
        actions=${html`
          <${AsyncButton}
            small
            description=${`refresh settings for ${device.name || "device"}`}
            onRun=${() => api.refresh(requestName)}
            >Refresh settings<//
          >
          ${
            identifyWritable
              ? html`<${AsyncButton}
                  small
                  description=${`identify ${device.name || "device"}`}
                  onRun=${() => api.identify(requestName)}
                  >Identify<//
                >`
              : html`<span class="text-sm opacity-70"
                  >Identify unavailable:
                  ${operationReasonText(device, "identify")}</span
                >`
          }
          <${AsyncButton}
            small
            variant="danger"
            description=${`reboot ${device.name || "device"}`}
            onRun=${() => api.reboot(requestName)}
            >Reboot<//
          >
        `}
      >
        <${RenameControl} device=${device} requestName=${requestName} />
        <${SampleRateControl} device=${device} requestName=${requestName} />
        <${EncodingControl} device=${device} requestName=${requestName} />
        <${LatencyControl} device=${device} />
        <${PullupControl} device=${device} requestName=${requestName} />
      <//>
      <${PerformanceControls} device=${device} requestName=${requestName} />
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
  const hasRange =
    Number.isFinite(minimum) &&
    Number.isFinite(maximum) &&
    minimum >= 0 &&
    maximum >= minimum;
  const values = new Set(
    (device.standard_latency_choices_ms || []).map(Number),
  );
  for (const candidate of [
    device.latency_ms,
    device.configured_latency_ms,
    device.default_latency_ms,
  ]) {
    if (candidate !== null && candidate !== undefined) {
      values.add(Number(candidate));
    }
  }
  const choices = [...values].sort((first, second) => first - second);
  const current = device.configured_latency_ms ?? device.latency_ms;
  if (!choices.length && !hasRange) {
    return html`<${FieldRow} label="Latency"
      ><span>Settings unavailable</span><//
    >`;
  }
  return html`
    <${FieldRow} label="Latency">
      ${
        choices.length
          ? html`<select
              key=${`latency-${requestName}`}
              ref=${control}
              aria-label="Latency"
              onChange=${(event) => setCustom(event.currentTarget.value === "custom")}
            >
              ${choices.map(
                (value) =>
                  html`<option
                    key=${value}
                    value=${value}
                    selected=${Number(value) === Number(current)}
                  >
                    ${value} ms
                  </option>`,
              )}
              ${hasRange ? html`<option value="custom">Custom…</option>` : null}
            </select>`
          : null
      }
      ${
        custom || !choices.length
          ? html`
              <input
                key=${`custom-latency-${requestName}`}
                ref=${customInput}
                aria-label="Custom latency in milliseconds"
                type="number"
                min=${minimum}
                max=${maximum}
                step="any"
                required
                defaultValue=${current ?? minimum}
              />
              <span>ms (${minimum}–${maximum})</span>
            `
          : null
      }
      <${AsyncButton}
        variant="primary"
        small
        description=${`set latency on ${device.name || "device"}`}
        onRun=${() => {
          const input =
            custom || !choices.length ? customInput.current : control.current;
          if (!input.checkValidity())
            throw new Error(`Enter a latency from ${minimum} to ${maximum} ms`);
          return api.setLatency(requestName, Number(input.value));
        }}
      >
        Apply
      <//>
    <//>
  `;
}
