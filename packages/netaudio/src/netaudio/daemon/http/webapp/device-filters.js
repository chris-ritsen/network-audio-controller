import * as format from "./format.js";

const UNKNOWN = "Not reported";
const text = (value) => typeof value === "string" && value.trim() ? value.trim() : UNKNOWN;
const boolean = (value, yes, no) => value === true ? yes : value === false ? no : UNKNOWN;
const number = (value, label) => typeof value === "number" && Number.isFinite(value) ? label(value) : UNKNOWN;

export const DEVICE_FILTERS = [
  { id: "availability", label: "Availability", values: (device) => [boolean(device.online, "Online", "Offline")] },
  { id: "manufacturer", label: "Manufacturer", values: (device) => [text(device.manufacturer)] },
  { id: "model", label: "Model", values: (device) => [text(format.deviceModelName(device))] },
  { id: "domain", label: "Domain", values: (device) => [text(device.ddm_domain_name)] },
  { id: "lock", label: "Device lock", values: (device) => [boolean(device.is_locked, "Locked", "Unlocked")] },
  { id: "sample-rate", label: "Sample rate", values: (device) => [number(device.sample_rate_hz, format.sampleRate)] },
  { id: "latency", label: "Latency", values: (device) => [number(device.latency_ms, format.latency)] },
  { id: "media", label: "Media type", values: (device) => {
    const types = new Set();
    for (const channels of Object.values(device.channels || {})) {
      for (const channel of Object.values(channels || {})) {
        const type = channel.ddm_media_type;
        if (["AUDIO", "VIDEO", "ANCILLARY"].includes(type)) types.add({ AUDIO: "Audio", VIDEO: "Video", ANCILLARY: "Ancillary" }[type]);
      }
    }
    return types.size ? [...types] : [UNKNOWN];
  } },
  { id: "external-clock", label: "External clock", values: (device) => [boolean(device.ddm_clock_preferences?.external_word_clock, "Enabled", "Disabled")] },
];

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
