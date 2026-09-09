import * as format from "./format.js";
import { aes67Status } from "./aes67.js";
import { signal } from "./lib/preact.js";

const UNKNOWN = "Not reported";
const text = (value) => typeof value === "string" && value.trim() ? value.trim() : UNKNOWN;
const boolean = (value, yes, no) => value === true ? yes : value === false ? no : UNKNOWN;
const number = (value, label) => typeof value === "number" && Number.isFinite(value) ? label(value) : UNKNOWN;

function subscriptionStates(device) {
  if (!Array.isArray(device.subscriptions)) return [UNKNOWN];
  const routed = device.subscriptions.filter((entry) => entry.tx_device && entry.tx_channel);
  if (!routed.length) return ["None"];
  return [...new Set(routed.map((entry) => ({ good: "Has successes", warn: "Has warnings", bad: "Has errors" })[format.subscriptionTone(entry)] || UNKNOWN))];
}

function multicastState(device) {
  if (!Array.isArray(device.transmitter_flows)) return UNKNOWN;
  if (device.transmitter_flows.some((flow) => flow.flow_type?.toLowerCase() === "multicast")) return "Active";
  if (device.transmitter_flows.some((flow) => !["unicast", "multicast"].includes(flow.flow_type?.toLowerCase()))) return UNKNOWN;
  return "None";
}

export const DEVICE_FILTERS = [
  { id: "manufacturer", label: "Manufacturer", values: (device) => [text(device.manufacturer)] },
  { id: "model", label: "Model", values: (device) => [text(format.deviceModelName(device))] },
  { id: "domain", label: "Domain", values: (device) => [text(device.ddm_domain_name)] },
  { id: "lock", label: "Device lock", values: (device) => [boolean(device.is_locked, "Locked", "Unlocked")] },
  { id: "sample-rate", label: "Sample rate", values: (device) => [number(device.sample_rate_hz, format.sampleRate)] },
  { id: "latency", label: "Latency", values: (device) => [number(device.latency_ms, format.latency)] },
  { id: "subscription", label: "Subscription", values: subscriptionStates },
  { id: "tx-multicast", label: "Tx multicast flows", values: (device) => [multicastState(device)] },
  { id: "aes67", label: "AES67", values: (device) => [aes67Status(device).label] },
  { id: "sample-rate-pullup", label: "Sample rate pull-up", values: (device) => {
    const raw = device.sample_rate_pullup_raw_value;
    return [Number.isInteger(raw) ? ["None", "+4.1667%", "+0.1%", "-0.1%", "-4.0%"][raw] || UNKNOWN : UNKNOWN];
  } },
  { id: "media", label: "Media type", values: (device) => {
    const types = new Set();
    for (const channels of Object.values(device.channels || {})) {
      for (const channel of Object.values(channels || {})) {
        const type = (channel.ddm_media_type || channel.media_type || "").toUpperCase();
        if (["AUDIO", "VIDEO", "ANCILLARY"].includes(type)) types.add({ AUDIO: "Audio", VIDEO: "Video", ANCILLARY: "Ancillary" }[type]);
      }
    }
    return types.size ? [...types] : [UNKNOWN];
  } },
  { id: "external-clock", label: "External clock", values: (device) => [boolean(device.ddm_clock_preferences?.external_word_clock, "Enabled", "Disabled")] },
];

const STORAGE_KEY = "netaudio.routing.filters";

export function readRoutingFilters() {
  try {
    const saved = JSON.parse(window.localStorage.getItem(STORAGE_KEY));
    return {
      search: typeof saved?.search === "string" ? saved.search : "",
      receiverSearch: typeof saved?.receiverSearch === "string" ? saved.receiverSearch : "",
      transmitterSearch: typeof saved?.transmitterSearch === "string" ? saved.transmitterSearch : "",
      expandedGroups: DEVICE_FILTERS.filter(({ id }) => Array.isArray(saved?.expandedGroups) && saved.expandedGroups.includes(id)).map(({ id }) => id),
      listMode: saved?.listMode === true,
      panelOpen: typeof saved?.panelOpen === "boolean" ? saved.panelOpen : null,
      values: Object.fromEntries(DEVICE_FILTERS.flatMap(({ id }) => {
        const selected = saved?.values?.[id];
        if (!Array.isArray(selected)) return [];
        const values = [...new Set(selected.filter((value) => typeof value === "string" && value.length))];
        return values.length ? [[id, values]] : [];
      })),
    };
  } catch {
    return { search: "", receiverSearch: "", transmitterSearch: "", expandedGroups: [], listMode: false, panelOpen: null, values: {} };
  }
}

export function saveRoutingFilters(filters) {
  inventoryFilters.value = filters;
  try { window.localStorage.setItem(STORAGE_KEY, JSON.stringify(filters)); } catch {}
}

export const inventoryFilters = signal(readRoutingFilters());

export function matchesDeviceFilters(device, filters, except) {
  const query = (filters.search || "").trim().toLowerCase();
  if (query && !format.deviceHaystack(device).includes(query)) return false;
  return DEVICE_FILTERS.every((group) => {
    const selected = filters.values?.[group.id] || [];
    return group.id === except || !selected.length || group.values(device).some((value) => selected.includes(value));
  });
}

export function deviceFilterOptions(devices, filters) {
  return DEVICE_FILTERS.map((group) => {
    const counts = new Map((filters.values?.[group.id] || []).map((value) => [value, 0]));
    for (const device of devices) {
      for (const value of new Set(group.values(device))) {
        if (!counts.has(value)) counts.set(value, 0);
        if (matchesDeviceFilters(device, filters, group.id)) counts.set(value, counts.get(value) + 1);
      }
    }
    return { ...group, options: [...counts].sort(([a], [b]) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" })) };
  });
}

export function toggleDeviceFilter(filters, group, value) {
  const selected = filters.values?.[group] || [];
  const next = selected.includes(value) ? selected.filter((entry) => entry !== value) : [...selected, value];
  const values = { ...filters.values };
  if (next.length) values[group] = next;
  else delete values[group];
  return { ...filters, values };
}
