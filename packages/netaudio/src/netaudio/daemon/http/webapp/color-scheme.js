import { t } from "./i18n.js";
import { signal } from "./lib/preact.js";

const APPEARANCE_KEY = "netaudio.appearance";
const LIGHT_QUERY = "(prefers-color-scheme: light)";
export const APPEARANCES = [
  ["system", t("System")],
  ["light", t("Light")],
  ["dark", t("Dark")],
];

function storedAppearance() {
  try {
    const value = window.localStorage.getItem(APPEARANCE_KEY);
    return APPEARANCES.some(([id]) => id === value) ? value : "system";
  } catch {
    return "system";
  }
}

function systemScheme() {
  return window.matchMedia(LIGHT_QUERY).matches ? "light" : "dark";
}

function resolvedScheme() {
  return appearance.value === "system" ? systemScheme() : appearance.value;
}

export const appearance = signal(storedAppearance());
export const colorScheme = signal(resolvedScheme());

function applyColorScheme() {
  const scheme = resolvedScheme();
  colorScheme.value = scheme;
  document.documentElement.dataset.theme = scheme;
  document.documentElement.style.colorScheme = scheme;
}

export function setAppearance(value) {
  appearance.value = APPEARANCES.some(([id]) => id === value) ? value : "system";
  try { window.localStorage.setItem(APPEARANCE_KEY, appearance.value); } catch {}
  applyColorScheme();
}

export function startColorScheme() {
  applyColorScheme();
  window.matchMedia(LIGHT_QUERY).addEventListener("change", () => {
    if (appearance.value === "system") applyColorScheme();
  });
}

export function prefersLight() {
  return colorScheme.value === "light";
}

export function useColorScheme() {
  return colorScheme.value;
}
