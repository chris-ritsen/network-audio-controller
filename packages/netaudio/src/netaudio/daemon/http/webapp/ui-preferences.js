import { signal } from "./lib/preact.js";

const METER_LAYOUT_KEY = "netaudio.meter-layout";
function readMeterLayout() {
  try { return window.localStorage.getItem(METER_LAYOUT_KEY) === "bars" ? "bars" : "strips"; } catch { return "strips"; }
}
export const meterLayout = signal(readMeterLayout());
export function setMeterLayout(value) {
  meterLayout.value = value === "bars" ? "bars" : "strips";
  try { window.localStorage.setItem(METER_LAYOUT_KEY, meterLayout.value); } catch {}
}
