import { api } from "../api.js";
import { AsyncButton, FieldRow, Panel } from "../components.js";
import { html, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";

const depths = {
  1: "6-bit",
  2: "8-bit",
  4: "10-bit",
  8: "12-bit",
  16: "14-bit",
  32: "16-bit",
};
const colors = {
  1: "RGB 4:4:4",
  2: "YCbCr 4:0:0",
  4: "YCbCr 4:2:0",
  8: "YCbCr 4:2:2",
  16: "YCbCr 4:4:4",
  32: "YCbCr 4:2:2:4",
};
const signals = {
  0: "Unknown",
  16: "Valid unprotected signal",
  17: "Valid protected signal",
  32: "Disconnected",
  33: "Invalid unprotected signal",
  34: "HDCP negotiation failed",
  35: "Negotiating",
  36: "Incompatible HDCP",
  48: "HDCP unsupported",
  49: "HDMI failure",
  50: "Invalid video format",
  51: "No video",
  52: "Codec failure",
  53: "Sink does not support the video",
  54: "Source-format mismatch",
};
const resolutions = {
  256: "800×480 60 Hz",
  257: "800×600 60 Hz",
  258: "1024×768 60 Hz",
  259: "1280×1024 60 Hz",
  260: "1600×1200 60 Hz",
  261: "1920×1200 60 Hz",
  262: "1920×1200 60 Hz reduced blanking",
};
const resolution = (v) =>
  resolutions[v] || (v === 0 ? "Automatic" : "Video mode " + v);
const formatText = (v) =>
  v
    ? [
        resolution(v.resolution),
        depths[v.bit_depth] || "Depth mask " + v.bit_depth,
        colors[v.color_space] || "Color mask " + v.color_space,
      ].join(", ")
    : "Unavailable";
function initial(category, v) {
  if (category === "bluetooth_discovery") return v === 1;
  if (category === "bandwidth")
    return { target: v.target, enabled: v.enabled === 1 };
  if (category === "hdcp") return { mode: v.configured_mode };
  if (category === "video_format")
    return { format: { ...v.configured }, selection: { ...v.selection } };
  if (category === "codec_format") return { ...v.current };
  return { ...v };
}
function Setting({ device, category, observation, title }) {
  const [value, setValue] = useState(() =>
    initial(category, observation.value),
  );
  const [plan, setPlan] = useState(null);
  const current = observation.value;
  const observations = device.device_controls?.observations || {};
  const formatStatus = observations.video_format?.value;
  const reason =
    category === "serial" &&
    (current.hardware_flow_control || current.software_flow_control)
      ? "The reported flow-control mode is unsupported; serial settings are read-only."
      : category === "bandwidth" &&
          (formatStatus?.direction !== 0 ||
            !current.minimum ||
            !current.maximum)
        ? "Bandwidth control is unavailable on this device."
        : category === "video_format" && current.direction !== 0
          ? "Receiver format settings are read-only."
          : null;
  const writable =
    !reason &&
    device.device_controls?.writable &&
    observation.fresh &&
    Date.now() / 1000 - observation.observed_at_unix <= 10;
  const update = (next) => {
    setValue(next);
    setPlan(null);
  };
  const set = (key, v) => update({ ...value, [key]: v });
  const select = (label, key, choices) =>
    html`<${FieldRow} label=${label}
      ><select
        aria-label=${label}
        value=${value[key]}
        disabled=${!writable}
        onChange=${(e) => set(key, Number(e.target.value))}
      >
        ${choices.map(([id, name]) => html`<option value=${id} disabled=${category === "hdcp" && ![1, 2, 3].includes(id)}>${name}</option>`)}
      </select><//
    >`;
  let inputs;
  if (category === "bluetooth_identification")
    inputs = html`${select("Name source", "name_source", [
        [1, "Dante device name"],
        [2, "Custom name"],
      ])}<${FieldRow} label="Custom Bluetooth name"
        ><input
          aria-label="Custom Bluetooth name"
          value=${value.custom_name}
          disabled=${!writable || value.name_source !== 2}
          onInput=${(e) => set("custom_name", e.target.value)}
        /><span>Up to 32 characters</span><//
      >`;
  if (category === "bluetooth_discovery")
    inputs = html`<label
      ><input
        type="checkbox"
        checked=${value}
        disabled=${!writable}
        onChange=${(e) => update(e.target.checked)}
      />
      Discoverable</label
    >`;
  if (category === "serial")
    inputs = html`${select(
        "Baud rate",
        "baud_rate",
        [1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200, 230400].map(
          (v) => [v, v],
        ),
      )}${select("Data bits", "data_bits", [
        [7, 7],
        [8, 8],
      ])}${select("Parity", "parity", [
        [0, "None"],
        [1, "Even"],
        [2, "Odd"],
      ])}${select("Stop bits", "stop_bits", [
        [1, 1],
        [2, 2],
      ])}
      <p>
        Hardware flow control: ${current.hardware_flow_control}; software flow
        control: ${current.software_flow_control}
      </p>`;
  if (category === "bandwidth")
    inputs = html`<label
        ><input
          type="checkbox"
          checked=${value.enabled}
          disabled=${!writable}
          onChange=${(e) => update({ enabled: e.target.checked, target: e.target.checked ? current.target || current.minimum : 0 })}
        />
        Enable user bandwidth control</label
      ><${FieldRow} label="Target bandwidth"
        ><input
          aria-label="Target bandwidth"
          type="number"
          min=${current.minimum}
          max=${Math.min(current.maximum, 700)}
          value=${value.target}
          disabled=${!writable || !value.enabled}
          onInput=${(e) => set("target", Number(e.target.value))}
        /><span
          >Mbit/s (${current.minimum}–${Math.min(current.maximum, 700)})</span
        ><//
      >`;
  if (category === "hdcp")
    inputs = select(
      "HDCP mode",
      "mode",
      current.supported_modes.map((v) => [
        v,
        { 1: "None", 2: "HDCP 1.x", 3: "Automatic" }[v] ||
          "Unknown (" + v + ")",
      ]),
    );
  if (category === "codec_format")
    inputs = html`<${FieldRow} label="Codec"
      ><select
        aria-label="Codec"
        disabled=${!writable}
        value=${JSON.stringify(value)}
        onChange=${(e) => update(JSON.parse(e.target.value))}
      >
        ${current.supported.flatMap((f) => [1, 2, 4, 8, 16].filter((bit) => f.level & bit).map((bit) => html`<option value=${JSON.stringify({ ...f, level: bit })}>${f.codec_type === 1 ? "JPEG 2000" : "Codec " + f.codec_type}, ${{ 1: "Broadcast", 2: "Ultra-low-latency" }[f.profile] || "Profile " + f.profile}, level ${Math.log2(bit) + 1}</option>`))}
      </select><//
    >`;
  if (category === "video_format") {
    const candidates = current.supported.filter(
      (f) =>
        !value.selection.manual_resolution ||
        f.resolution === value.format.resolution,
    );
    const options = (name, labels) =>
      Object.entries(labels).filter(([bit]) =>
        candidates.some((f) => f[name] & Number(bit)),
      );
    const field = (label, name, choices) =>
      html`<${FieldRow} label=${label}
        ><select
          aria-label=${label}
          value=${value.selection["manual_" + name] ? value.format[name] : 0}
          disabled=${!writable || current.direction !== 0 || (name !== "resolution" && device.device_controls.observations.visca?.value?.capability !== 1)}
          onChange=${(e) => {
            const v = Number(e.target.value);
            update({
              format: { ...value.format, [name]: v },
              selection: { ...value.selection, ["manual_" + name]: v !== 0 },
            });
          }}
        >
          <option value="0">Automatic</option>
          ${choices.map(([id, label]) => html`<option value=${id}>${label}</option>`)}
        </select><//
      >`;
    inputs = html`<p>Configured: ${formatText(current.configured)}</p>
      <p>Actual: ${formatText(current.actual)}</p>
      <p>
        ${"Direction: "}
        ${current.direction === 0 ? "Transmitter" : current.direction === 1 ? "Receiver" : current.direction}
      </p>
      ${field(
        "Resolution",
        "resolution",
        [...new Set(current.supported.map((f) => f.resolution))].map((v) => [
          v,
          resolution(v),
        ]),
      )}${field("Bit depth", "bit_depth", options("bit_depth", depths))}${field("Color space", "color_space", options("color_space", colors))}`;
  }
  return html`<${Panel} title=${title}
    >${!writable && html`<p>${reason || device.device_controls?.write_unavailable_reason || "Refresh status to check whether this setting is available."}</p>`}${inputs}
    <div class="flex gap-2">
      <${AsyncButton}
        small
        disabled=${!writable}
        onRun=${async () => {
          const p = await api.deviceControls(
            deviceRequestName(device),
            "plan",
            category,
            value,
          );
          setPlan(p);
          return p;
        }}
        >Preview<//
      ><${AsyncButton}
        small
        disabled=${!writable || plan?.action !== "change"}
        onRun=${() => api.deviceControls(deviceRequestName(device), "apply", category, value)}
        >Apply<//
      >
    </div>
    ${plan && html`<p role="status">${plan.action}${plan.reason ? ": " + plan.reason : ""}</p>`}<//
  >`;
}
export function DeviceControls({ device }) {
  const state = device.device_controls || {};
  const [confirm, setConfirm] = useState(false);
  if (!state.family) return null;
  const observations = state.observations || {};
  const connection = observations.bluetooth_connection?.value;
  const video = observations.video_channel?.value;
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
      actions=${html`<${AsyncButton} small disabled=${!state.readable} onRun=${() => api.deviceControls(deviceRequestName(device), "inspect")}>Refresh device controls<//>`}
      >${state.write_unavailable_reason && html`<p>${state.write_unavailable_reason}</p>`}${connection && html`<p>Connection: ${{ 0: "Unknown", 1: "Connected", 2: "Disconnected", 3: "Link lost" }[connection.state] || "Unknown (" + connection.state + ")"}${connection.peer_name ? " — " + connection.peer_name : ""}</p>`}${
        video &&
        html`<p>
            ${"Signal: "}
            ${signals[video.status_code] || "Unknown (" + video.status_code + ")"}
          </p>
          <p>
            ${"Observed HDCP: "}
            ${{ 0: "Undefined", 1: "None", 2: "1.x", 3: "2.x" }[video.observed_hdcp_version] || "Unavailable"}
          </p>`
      }${
        observations.bluetooth_pairing &&
        html`<p>Remembered devices: ${observations.bluetooth_pairing.value}</p>
          <label
            ><input
              type="checkbox"
              checked=${confirm}
              onChange=${(e) => setConfirm(e.target.checked)}
            />
            Confirm forgetting all paired devices</label
          ><${AsyncButton}
            variant="danger"
            disabled=${!state.writable || !confirm}
            onRun=${async () => {
              const r = await api.deviceControls(
                deviceRequestName(device),
                "apply",
                "bluetooth_pairing",
                "clear",
                true,
              );
              setConfirm(false);
              return r;
            }}
            >Clear pairing list<//
          >`
      }<//
    >${Object.entries(labels)
      .filter(([c]) => observations[c]?.value != null)
      .map(
        ([category, title]) =>
          html`<${Setting}
            key=${`${device.server_name}:${category}`}
            device=${device}
            category=${category}
            observation=${observations[category]}
            title=${title}
          />`,
      )}`;
}
