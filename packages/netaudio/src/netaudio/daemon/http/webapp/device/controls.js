import { api } from "../api.js";
import { AsyncButton, FieldRow, Panel } from "../components.js";
import { html, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";

function fresh(observation) {
  const age = Date.now() / 1000 - observation.observed_at_unix;
  return observation.fresh && age >= 0 && age <= 10;
}

function Setting({ device, category, observation, editor, title }) {
  const [value, setValue] = useState(editor.initial);
  const [fields, setFields] = useState(editor.initial_fields);
  const [plan, setPlan] = useState(null);
  const writable =
    !editor.reason && device.device_controls?.writable && fresh(observation);
  const update = (next) => {
    setValue(next);
    setPlan(null);
  };
  const select = (label, name) => {
    const choices = new Map();

    for (const variant of editor.variants) {
      const matches = Object.entries(fields).every(
        ([other, field]) =>
          other === name || variant.fields[other]?.key === field.key,
      );

      if (matches && variant.fields[name])
        choices.set(variant.fields[name].key, variant);
    }

    const selected = fields[name];
    return html`<${FieldRow} label=${label}>
      <select
        aria-label=${label}
        value=${selected?.key ?? ""}
        disabled=${!writable || !choices.size}
        onChange=${(event) => {
          const variant = choices.get(event.target.value);
          if (!variant) return;
          setFields(variant.fields);
          update(
            category === "bluetooth_identification"
              ? { ...variant.requested, custom_name: value.custom_name }
              : variant.requested,
          );
        }}
      >
        ${selected && !choices.has(selected.key) && html`<option value=${selected.key}>${selected.label}</option>`}
        ${[...choices].map(([key, variant]) => html`<option value=${key}>${variant.fields[name].label}</option>`)}
      </select>
    <//>`;
  };
  let inputs;

  if (category === "bluetooth_identification") {
    inputs = html`${select("Name source", "name_source")}
      <${FieldRow} label="Custom Bluetooth name">
        <input
          aria-label="Custom Bluetooth name"
          value=${value.custom_name}
          disabled=${!writable || value.name_source !== editor.custom_name_source}
          onInput=${(event) => update({ ...value, custom_name: event.target.value })}
        />
        <span>Up to ${editor.custom_name_limit} characters</span>
      <//>`;
  }

  if (category === "bluetooth_discovery") {
    inputs = html`<label
      ><input
        type="checkbox"
        checked=${value}
        disabled=${!writable}
        onChange=${(event) => update(event.target.checked)}
      />
      Discoverable</label
    >`;
  }

  if (category === "serial") {
    inputs = html`${select("Baud rate", "baud_rate")}${select("Data bits", "data_bits")}
    ${select("Parity", "parity")}${select("Stop bits", "stop_bits")}`;
  }

  if (category === "bandwidth" && editor.bandwidth) {
    const limits = editor.bandwidth;
    inputs = html`<label
        ><input
          type="checkbox"
          checked=${value.enabled}
          disabled=${!writable}
          onChange=${(event) => update(event.target.checked ? limits.enable : limits.disable)}
        />
        Enable user bandwidth control</label
      >
      <${FieldRow} label="Target bandwidth">
        <input
          aria-label="Target bandwidth"
          type="number"
          min=${limits.minimum}
          max=${limits.maximum}
          value=${value.target}
          disabled=${!writable || !value.enabled}
          onInput=${(event) => update({ ...value, target: Number(event.target.value) })}
        />
        <span>Mbit/s (${limits.minimum}–${limits.maximum})</span>
      <//>`;
  }

  if (category === "hdcp") inputs = select("HDCP mode", "mode");
  if (category === "codec_format") inputs = select("Codec", "codec");

  if (category === "video_format") {
    inputs = html`<p>Configured: ${editor.details.configured}</p>
      <p>Actual: ${editor.details.actual}</p>
      <p>Direction: ${editor.details.direction}</p>
      ${select("Resolution", "resolution")}${select("Bit depth", "bit_depth")}${select("Color space", "color_space")}`;
  }

  return html`<${Panel} title=${title}>
    ${!writable && html`<p>${editor.reason || device.device_controls?.write_unavailable_reason || "Refresh status to check whether this setting is available."}</p>`}
    ${inputs}
    <div class="flex gap-2">
      <${AsyncButton}
        small
        disabled=${!writable}
        onRun=${async () => {
        const result = await api.deviceControls(
          deviceRequestName(device),
          "plan",
          category,
          value,
        );
        setPlan(result);
        return result;
      }}
        >Preview<//
      >
      <${AsyncButton}
        small
        disabled=${!writable || plan?.action !== "change"}
        onRun=${() => api.deviceControls(deviceRequestName(device), "apply", category, value)}
        >Apply<//
      >
    </div>
    ${plan && html`<p role="status">${plan.action}${plan.reason ? ": " + plan.reason : ""}</p>`}
  <//>`;
}

export function DeviceControls({ device }) {
  const state = device.device_controls || {};
  const [confirm, setConfirm] = useState(false);
  if (!state.family) return null;
  const observations = state.observations || {};
  const presentation = state.presentation;
  const connection = observations.bluetooth_connection?.value;
  const labels = {
    bluetooth_identification: "Bluetooth name",
    bluetooth_discovery: "Bluetooth discovery",
    video_format: "Video format",
    codec_format: "Video codec",
    bandwidth: "Encoder bandwidth",
    hdcp: "HDCP",
    serial: "Serial port",
  };
  return html`<${Panel}
      title=${state.family === "bluetooth" ? "Bluetooth" : "Video and serial"}
      actions=${html`<${AsyncButton}
      small
      disabled=${!state.readable}
      onRun=${() => api.deviceControls(deviceRequestName(device), "inspect")}
      >Refresh device controls<//
    >`}
    >
      ${state.write_unavailable_reason && html`<p>${state.write_unavailable_reason}</p>`}
      ${connection && html`<p>Connection: ${presentation?.summary.connection || "Unknown"}${connection.peer_name ? " — " + connection.peer_name : ""}</p>`}
      ${
      observations.video_channel &&
      html`<p>Signal: ${presentation?.summary.signal || "Unknown"}</p>
        <p>
          Observed HDCP: ${presentation?.summary.observed_hdcp || "Unavailable"}
        </p>`
    }
      ${
      observations.bluetooth_pairing &&
      html`<p>Remembered devices: ${observations.bluetooth_pairing.value}</p>
        <label
          ><input
            type="checkbox"
            checked=${confirm}
            onChange=${(event) => setConfirm(event.target.checked)}
          />
          Confirm forgetting all paired devices</label
        >
        <${AsyncButton}
          variant="danger"
          disabled=${!state.writable || !confirm || !fresh(observations.bluetooth_pairing)}
          onRun=${async () => {
          const result = await api.deviceControls(
            deviceRequestName(device),
            "apply",
            "bluetooth_pairing",
            "clear",
            true,
          );
          setConfirm(false);
          return result;
        }}
          >Clear pairing list<//
        >`
    }
    <//>
    ${Object.entries(labels)
    .filter(
      ([category]) => presentation?.editors[category] && observations[category],
    )
    .map(
      ([category, title]) =>
        html`<${Setting}
          key=${`${device.server_name}:${category}:${observations[category].observed_at_unix}`}
          device=${device}
          category=${category}
          observation=${observations[category]}
          editor=${presentation.editors[category]}
          title=${title}
        />`,
    )}`;
}
