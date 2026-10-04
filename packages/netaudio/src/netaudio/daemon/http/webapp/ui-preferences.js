import { signal } from "./lib/preact.js";

const TEXT_SELECTION_KEY = "netaudio.allow-text-selection";
function readTextSelection() {
  try { return window.localStorage.getItem(TEXT_SELECTION_KEY) === "true"; } catch { return false; }
}
export const allowTextSelection = signal(readTextSelection());
function applyTextSelection() {
  globalThis.document?.documentElement?.setAttribute("data-allow-text-selection", String(allowTextSelection.value));
}
export function setAllowTextSelection(value) {
  allowTextSelection.value = value;
  try { window.localStorage.setItem(TEXT_SELECTION_KEY, String(value)); } catch {}
  applyTextSelection();
}
applyTextSelection();

const METER_LAYOUT_KEY = "netaudio.meter-layout";
function readMeterLayout() {
  try { return window.localStorage.getItem(METER_LAYOUT_KEY) === "bars" ? "bars" : "strips"; } catch { return "strips"; }
}
export const meterLayout = signal(readMeterLayout());
export function setMeterLayout(value) {
  meterLayout.value = value === "bars" ? "bars" : "strips";
  try { window.localStorage.setItem(METER_LAYOUT_KEY, meterLayout.value); } catch {}
}
