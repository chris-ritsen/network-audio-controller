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
