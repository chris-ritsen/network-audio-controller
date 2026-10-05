import { api } from "../api.js";
import { AsyncButton, FieldRow, Fields, Panel } from "../components.js";
import { t } from "../i18n.js";
import { html, useEffect, useRef, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";

function Setting({ device, category, editor, title }) {
  const [value, setValue] = useState(editor.initial);
  const [fields, setFields] = useState(editor.initial_fields);
  const [dirty, setDirty] = useState(false);
  const [plan, setPlan] = useState(null);
  const initial = JSON.stringify([editor.initial, editor.initial_fields]);
  useEffect(() => {
    if (dirty) return;
    setValue(editor.initial);
    setFields(editor.initial_fields);
  }, [initial]);
  const writable = !editor.reason && device.device_controls?.writable === true;
  const update = (next) => {
    setValue(next);
    setDirty(true);
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
        ${selected && !choices.has(selected.key) && html`<option value=${selected.key}>${t(selected.label)}</option>`}
        ${[...choices].map(([key, variant]) => html`<option value=${key}>${t(variant.fields[name].label)}</option>`)}
      </select>
    <//>`;
  };
  let inputs;

  if (category === "bluetooth_identification") {
    inputs = html`${select(t("Name source"), "name_source")}
      <${FieldRow} label=${t("Custom Bluetooth name")}>
        <input
          aria-label=${t("Custom Bluetooth name")}
          value=${value.custom_name}
          disabled=${!writable || value.name_source !== editor.custom_name_source}
          onInput=${(event) => update({ ...value, custom_name: event.target.value })}
        />
        <span>${t("{used}/{limit} bytes", { used: new TextEncoder().encode(value.custom_name || "").length, limit: editor.custom_name_limit })}</span>
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
      ${t("Discoverable")}</label
    >`;
  }

  if (category === "serial") {
    inputs = html`${select(t("Baud rate"), "baud_rate")}${select(t("Data bits"), "data_bits")}
    ${select(t("Parity"), "parity")}${select(t("Stop bits"), "stop_bits")}`;
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
        ${t("Enable user bandwidth control")}</label
      >
      <${FieldRow} label=${t("Target bandwidth")}>
        <input
          aria-label=${t("Target bandwidth")}
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

  if (category === "hdcp") inputs = select(t("HDCP mode"), "mode");
  if (category === "codec_format") inputs = select(t("Codec"), "codec");

  if (category === "video_format") {
    inputs = html`<${Fields} entries=${[
        [t("Configured"), editor.details.configured ? t(editor.details.configured) : undefined],
        [t("Actual"), editor.details.actual ? t(editor.details.actual) : undefined],
        [t("Direction"), editor.details.direction ? t(editor.details.direction) : undefined],
      ]} />
      ${select(t("Resolution"), "resolution")}${select(t("Bit depth"), "bit_depth")}${select(t("Color space"), "color_space")}`;
  }

  if (device.device_controls?.writable !== true) return null;
  return html`<${Panel} title=${title}>
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
        >${t("Preview")}<//
      >
      <${AsyncButton}
        small
        disabled=${!writable || plan?.action !== "change"}
        onRun=${async () => {
          const result = await api.deviceControls(deviceRequestName(device), "apply", category, value);
          setDirty(false);
          setPlan(null);
          return result;
        }}
        >${t("Apply")}<//
      >
    </div>
    ${plan && html`<p role="status">${plan.reason ? t("{action}: {reason}", { action: t(plan.action), reason: t(plan.reason) }) : t(plan.action)}</p>`}
  <//>`;
}

export function DeviceControls({ device }) {
  const state = device.device_controls || {};
  const [confirm, setConfirm] = useState(false);
  const lastInspection = useRef(0);
  const inspect = () => {
    if (!state.family || !state.readable || Date.now() - lastInspection.current < 5000) return;
    lastInspection.current = Date.now();
    api.deviceControls(deviceRequestName(device), "inspect").catch((error) => console.error(error));
  };
  useEffect(() => {
    lastInspection.current = 0;
    inspect();
  }, [deviceRequestName(device), state.family]);
  if (!state.family) return null;
  const observations = state.observations || {};
  const presentation = state.presentation;
  const connection = observations.bluetooth_connection?.value;
  const labels = {
    bluetooth_identification: t("Bluetooth name"),
    bluetooth_discovery: t("Bluetooth discovery"),
    video_format: t("Video format"),
    codec_format: t("Video codec"),
    bandwidth: t("Encoder bandwidth"),
    hdcp: "HDCP",
    serial: t("Serial port"),
  };
  return html`<div class="device-controls" onPointerEnter=${inspect} onPointerDown=${inspect} onFocusIn=${inspect}><${Panel}
      title=${state.family === "bluetooth" ? "Bluetooth" : t("Video and serial")}
      headerActions=${state.readable ? html`<${AsyncButton}
      small
      description=${device.name ? t("refresh device controls on {device}", { device: device.name }) : t("refresh device controls on device")}
      onRun=${() => api.deviceControls(deviceRequestName(device), "inspect")}
      >${t("Refresh")}<//
    >` : null}
    >
      <${Fields} entries=${[
        [t("Connection"), connection ? [presentation?.summary.connection && t(presentation.summary.connection), connection.peer_name].filter(Boolean).join(" · ") || undefined : undefined],
        [t("Signal"), observations.video_channel && presentation?.summary.signal ? t(presentation.summary.signal) : undefined],
        ["HDCP", observations.video_channel && presentation?.summary.observed_hdcp ? t(presentation.summary.observed_hdcp) : undefined],
        [t("Remembered devices"), observations.bluetooth_pairing?.value > 0 ? observations.bluetooth_pairing.value : undefined],
      ]} />
      ${
      observations.bluetooth_pairing?.value > 0 &&
      state.writable &&
      html`<label
          ><input
            type="checkbox"
            checked=${confirm}
            onChange=${(event) => setConfirm(event.target.checked)}
          />
          ${t("Confirm forgetting all paired devices")}</label
        >
        <${AsyncButton}
          variant="danger"
          disabled=${!confirm}
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
          >${t("Clear pairing list")}<//
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
          key=${`${device.server_name}:${category}`}
          device=${device}
          category=${category}
          editor=${presentation.editors[category]}
          title=${title}
        />`,
    )}</div>`;
}
