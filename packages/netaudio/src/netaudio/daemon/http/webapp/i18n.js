const LANGUAGE_KEY = "netaudio.language";

export const LANGUAGES = [
  ["en", "English"],
  ["de", "Deutsch"],
  ["fr", "Français"],
  ["ja", "日本語"],
  ["ko", "한국어"],
  ["ms", "Bahasa Melayu"],
  ["nl", "Nederlands"],
  ["pt-BR", "Português (Brasil)"],
  ["ta", "தமிழ்"],
  ["zh-Hans", "简体中文"],
  ["zh-Hant", "繁體中文"],
];

let catalog = {};
let active = "en";
let pluralRules = new Intl.PluralRules("en");

export function languagePreference() {
  try {
    const value = globalThis.localStorage?.getItem(LANGUAGE_KEY);
    return LANGUAGES.some(([code]) => code === value) ? value : "system";
  } catch {
    return "system";
  }
}

export function matchLanguage(requested) {
  for (const tag of requested || []) {
    const lower = String(tag).toLowerCase();
    if (lower.startsWith("zh")) return /hant|-tw|-hk|-mo/.test(lower) ? "zh-Hant" : "zh-Hans";
    if (lower === "pt" || lower.startsWith("pt-")) return "pt-BR";
    const match = LANGUAGES.find(([code]) => lower === code.toLowerCase() || lower.startsWith(`${code.toLowerCase()}-`));
    if (match) return match[0];
  }
  return "en";
}

export function currentLanguage() {
  return active;
}

export async function loadLanguage() {
  const preference = languagePreference();
  const browser = globalThis.navigator?.languages?.length ? globalThis.navigator.languages : [globalThis.navigator?.language || "en"];
  const code = preference === "system" ? matchLanguage(browser) : preference;
  active = code;
  pluralRules = new Intl.PluralRules(code);
  if (globalThis.document?.documentElement) globalThis.document.documentElement.lang = code;
  if (code === "en") return;
  try {
    catalog = (await import(`./i18n/${code}.js`)).default;
  } catch (error) {
    console.warn(`Translations for ${code} could not be loaded`, error);
    catalog = {};
  }
}

export function setLanguagePreference(value) {
  try {
    if (value === "system") globalThis.localStorage?.removeItem(LANGUAGE_KEY);
    else globalThis.localStorage?.setItem(LANGUAGE_KEY, value);
  } catch (error) {
    console.warn("The language choice could not be saved", error);
  }
  globalThis.location?.reload();
}

function fill(text, values) {
  if (!values) return text;
  return text.replace(/\{(\w+)\}/g, (match, name) => (name in values ? String(values[name]) : match));
}

export function t(text, values) {
  const translated = catalog[text];
  return fill(typeof translated === "string" && translated ? translated : text, values);
}

export function tn(count, one, other, values = {}) {
  const entry = catalog[other];
  const all = { count, ...values };
  if (entry && typeof entry === "object") {
    const form = entry[pluralRules.select(count)] ?? entry.other;
    if (form) return fill(form, all);
  }
  return fill(count === 1 ? one : other, all);
}
