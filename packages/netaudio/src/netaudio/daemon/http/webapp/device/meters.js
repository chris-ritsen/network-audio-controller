import { ChannelStrips } from "../channel-strips.js";
import { Notice, Panel } from "../components.js";
import * as format from "../format.js";
import { t } from "../i18n.js";
import { html, useCallback, useMemo } from "../lib/preact.js";
import { MeterBank } from "../meters.js";
import { DetailedMonitoring } from "../monitoring.js";
import { formatDecibels } from "../peak-meter.js";
import { isEnrolled } from "./managed.js";
import { deviceRequestName, meterValuesFor } from "../store.js";
import { meterLayout, setMeterLayout } from "../ui-preferences.js";

const STRIP_READOUTS = {
  clipping: t("Clip"),
  muted: "-∞",
  mute_or_floor: "-∞",
  below_threshold: t("Low"),
};

function stripChannels(device, direction) {
  const declared = (device.channels && device.channels[direction === "tx" ? "transmitters" : "receivers"]) || {};
  return Object.keys(declared)
    .map(Number)
    .sort((first, second) => first - second)
    .map((number) => {
      const name = declared[number].name || "";
      return { key: `${direction}:${number}`, channel: number, number, name: name === String(number) ? "" : name, tracks: 1 };
    });
}

function DeviceStrips({ device, direction }) {
  const strips = useMemo(() => stripChannels(device, direction), [device, direction]);
  const read = useCallback(
    (strip) => {
      const latest = meterValuesFor(device.server_name);
      const raw = latest ? (latest[direction] || {})[strip.channel] : undefined;
      const source = latest ? latest.metering_source : undefined;
      const decibels = format.meteringDecibelsFullScale(raw, source);
      const presence = format.meteringSignalPresence(raw, source);
      const clipping = presence === "clipping";
      return {
        levels: [decibels !== null ? decibels : clipping ? 0 : null],
        clipping,
        readout: decibels !== null ? formatDecibels(decibels) : raw === undefined || raw === null ? "" : STRIP_READOUTS[presence] || "",
      };
    },
    [device.server_name, direction],
  );
  if (!strips.length) return null;
  return html`<${ChannelStrips} strips=${strips} read=${read} label=${direction === "tx" ? t("Transmit levels") : t("Receive levels")} />`;
}

function LayoutSwitch() {
  const current = meterLayout.value;
  return html`<div class="join" role="group" aria-label=${t("Meter layout")}>
    ${[
      ["strips", t("Strips")],
      ["bars", t("Bars")],
    ].map(
      ([id, label]) => html`<button
        key=${id}
        type="button"
        class=${`btn btn-xs join-item${current === id ? " btn-primary" : ""}`}
        aria-pressed=${current === id ? "true" : "false"}
        onClick=${() => setMeterLayout(id)}
      >
        ${label}
      </button>`,
    )}
  </div>`;
}

export function DeviceMeters({ device }) {
  if (!device.online) return html`<${Notice}>${t("Monitoring is unavailable while this device is offline.")}<//>`;
  const strips = meterLayout.value === "strips";
  const directions = [
    ["rx", t("Receive levels")],
    ["tx", t("Transmit levels")],
  ].filter(([direction]) => stripChannels(device, direction).length);
  const bank = (direction) =>
    strips
      ? html`<${DeviceStrips} device=${device} direction=${direction} />`
      : html`<${MeterBank} device=${device} direction=${direction} serverName=${device.server_name} />`;
  return html`<div class="flex flex-col gap-3">
    ${isEnrolled(device) ? null : html`<${DetailedMonitoring} requestName=${deviceRequestName(device)} online=${device.online} />`}
    <div class="flex items-center justify-end gap-2">
      <span class="text-sm">${t("Meters")}</span>
      <${LayoutSwitch} />
    </div>
    <div class=${strips || directions.length < 2 ? "flex flex-col gap-3" : "split"}>
      ${directions.map(([direction, title]) => html`<${Panel} key=${direction} title=${title}>${bank(direction)}<//>`)}
    </div>
  </div>`;
}
