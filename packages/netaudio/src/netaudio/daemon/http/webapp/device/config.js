import { DeviceControls } from "./controls.js";
import { sampleRatePullupChoices } from "../core-metadata.js";
import { api } from "../api.js";
import { AsyncButton, FieldRow, Panel } from "../components.js";
import * as format from "../format.js";
import { html, useRef, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";
import {
  operationWritable,
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
        aria-label="Device name"
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
    if (device.sample_rate_hz == null) return null;
    return html`<${FieldRow} label="Sample rate">
      <span>${format.sampleRate(device.sample_rate_hz)}</span>
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
    if (!device.encoding) return null;
    return html`<${FieldRow} label="Encoding">
      <span>PCM ${device.encoding}</span>
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
  const labels = new Map(
    sampleRatePullupChoices.map(({ value, label }) => [
      value,
      label.charAt(0).toUpperCase() + label.slice(1),
    ]),
  );
  const supported = (
    device.supported_sample_rate_pullup_raw_values || []
  ).filter((value) => labels.has(value));
  const writable = operationWritable(device, "sample_rate_pullup");
  if (!writable || !supported.length) {
    const current = labels.get(Number(device.sample_rate_pullup_raw_value));
    if (!current) return null;
    return html`<${FieldRow} label="Sample rate pull-up">
      <span>${current}</span>
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

const EXTENDED_CLOCK_FIELDS = [
  ["follower_only", "Follower only", null],
  ["ptpv2_domain", "PTPv2 domain", 255],
  ["ptpv2_priority1", "PTPv2 priority 1", 255],
  ["ptpv2_priority2", "PTPv2 priority 2", 255],
  ["multicast_dscp", "Multicast DSCP", 63],
];

const PORT_CLOCK_FIELDS = [
  ["ttl", "Multicast TTL", 0, 255],
  ["sync_interval", "Sync interval (log seconds)", -128, 127],
  ["announce_interval", "Announce interval (log seconds)", -128, 127],
  ["delay_request_interval", "Delay-request interval (log seconds)", -128, 127],
  ["peer_delay_interval", "Peer-delay interval (log seconds)", -128, 127],
];

function OnOffSelect({ reference, onChange, label }) {
  return html`<select ref=${reference} aria-label=${label} onChange=${onChange}>
    <option value="keep">Keep current setting</option>
    <option value="true">On</option>
    <option value="false">Off</option>
  </select>`;
}

function ClockingControls({ device, requestName }) {
  const preferred = useRef(null);
  const source = useRef(null);
  const subdomain = useRef(null);
  const unicast = useRef(null);
  const [extended, setExtended] = useState({});
  const [portChanges, setPortChanges] = useState({});
  const allowed = device.clock_control_availability || {};
  const fresh = format.clockStatusFresh(device);
  const managed = isEnrolled(device);
  if (managed) {
    return html`<${Panel} title="Clocking">
      <${FieldRow} label="Preferred leader">
        <${AsyncButton}
          small
          description=${`prefer ${device.name || "device"} as clock leader`}
          onRun=${() => api.setPreferredLeader(requestName, true)}
          >On<//
        >
        <${AsyncButton}
          small
          description=${`clear the clock leader preference on ${device.name || "device"}`}
          onRun=${() => api.setPreferredLeader(requestName, false)}
          >Off<//
        >
      <//>
    <//>`;
  }
  if (!fresh) return null;
  const sourceChoices = device.clock_source_choices || [];
  const preferredAllowed = allowed.preferred_leader === true;
  const sourceAllowed = allowed.clock_source === true && sourceChoices.length > 0;
  const named = allowed.subdomain === true;
  const perPort = allowed.aggregate_ptpv1_unicast_delay_requests === true;
  const unicastAllowed = perPort || allowed.global_unicast_delay_requests === true;
  const extendedFields = EXTENDED_CLOCK_FIELDS.filter(
    ([name]) => allowed[name] === true && device.clock_status?.[name] != null,
  );
  const ports = allowed.ports === true
    ? (device.clock_status?.extended_ports || []).filter((port) => port.port_id >= 1 && port.port_id <= 64)
    : [];
  if (!preferredAllowed && !sourceAllowed && !named && !unicastAllowed && !extendedFields.length && !ports.length) {
    return null;
  }
  const apply = async () => {
    const changes = Object.fromEntries(Object.entries(extended).filter(([name]) => allowed[name] === true));
    if (ports.length) {
      const changedPorts = Object.entries(portChanges).map(([port_id, fields]) => ({
        port_id: Number(port_id),
        ...Object.fromEntries(Object.entries(fields).filter(([, value]) => value != null)),
      })).filter((port) => Object.keys(port).length > 1);
      if (changedPorts.length) changes.ports = changedPorts;
    }
    if (preferredAllowed && preferred.current?.value !== "keep")
      changes.preferred_leader = preferred.current.value === "true";
    if (sourceAllowed && source.current?.value !== "keep")
      changes.clock_source = Number(source.current.value);
    const currentSubdomain = device.clock_subdomain_presentation?.text ?? "";
    if (named && subdomain.current && subdomain.current.value !== currentSubdomain)
      changes.subdomain = subdomain.current.value;
    if (unicastAllowed && unicast.current?.value !== "keep")
      changes[
        perPort
          ? "aggregate_ptpv1_unicast_delay_requests"
          : "global_unicast_delay_requests"
      ] = unicast.current.value === "true";
    return api.setClockConfiguration(requestName, changes);
  };
  return html`
    <${Panel} title="Clocking">
      ${preferredAllowed ? html`<${FieldRow} label="Preferred leader"><${OnOffSelect} reference=${preferred} label="Preferred leader" /><//>` : null}
      ${sourceAllowed ? html`<${FieldRow} label="Clock source">
        <select ref=${source} aria-label="Clock source">
          <option value="keep">Keep current setting</option>
          ${sourceChoices.map((choice) => html`<option value=${choice.code}>${format.stateLabel(choice.label)}</option>`)}
        </select>
      <//>` : null}
      ${named ? html`<${FieldRow} label="Clock subdomain">
        <input ref=${subdomain} aria-label="Clock subdomain" defaultValue=${device.clock_subdomain_presentation?.text ?? ""} />
      <//>` : null}
      ${unicastAllowed ? html`<${FieldRow} label=${perPort ? "PTPv1 unicast delay requests" : "Unicast delay requests"}>
        <${OnOffSelect} reference=${unicast} label=${perPort ? "PTPv1 unicast delay requests" : "Unicast delay requests"} />
      <//>` : null}
      ${extendedFields.length || ports.length ? html`<details><summary>Advanced clock settings</summary>
        ${extendedFields.map(([name, label, maximum]) => html`<${FieldRow} label=${label}>
          ${maximum == null
            ? html`<${OnOffSelect} label=${label} onChange=${(event) => setExtended({ ...extended, [name]: event.target.value === "keep" ? null : event.target.value === "true" })} />`
            : html`<input type="number" aria-label=${label} min="0" max=${maximum} step="1" defaultValue=${device.clock_status[name]}
                onChange=${(event) => setExtended({ ...extended, [name]: event.target.value === "" ? null : Number(event.target.value) })} />`}
        <//>`)}
        ${ports.map((port) => html`
          <fieldset><legend>Port ${port.port_id}</legend>
            ${PORT_CLOCK_FIELDS.filter(([name]) => port[name] != null).map(([name, label, minimum, maximum]) => html`
              <${FieldRow} label=${label}><input type="number" aria-label=${`Port ${port.port_id} ${label}`} min=${minimum} max=${maximum} step="1" defaultValue=${port[name]}
                onChange=${(event) => setPortChanges({ ...portChanges, [port.port_id]: {
                  ...portChanges[port.port_id], [name]: event.target.value === "" ? null : Number(event.target.value),
                } })} /><//>`)}
            ${port.follower_only != null ? html`<${FieldRow} label="Follower only"><${OnOffSelect}
              label=${`Port ${port.port_id} follower only`}
              onChange=${(event) => setPortChanges({ ...portChanges, [port.port_id]: {
                ...portChanges[port.port_id], follower_only: event.target.value === "keep" ? null : event.target.value === "true",
              } })} /><//>` : null}
          </fieldset>`)}
      </details>` : null}
      <${AsyncButton}
        variant="primary"
        small
        description=${`apply clock settings on ${device.name || "device"}`}
        onRun=${apply}
        >Apply clock settings<//
      >
    <//>
  `;
}


export function DeviceConfigSection({ device }) {
  const requestName = deviceRequestName(device);
  const identifyWritable = operationWritable(device, "identify");
  return html`
    <div class="flex flex-col gap-4">
      <${Panel}
        title="Device config"
        headerActions=${html`
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
              : null
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
      <${ClockingControls} device=${device} requestName=${requestName} />
      <${DeviceControls} device=${device} />
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
  if (!choices.length && !hasRange) return null;
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
