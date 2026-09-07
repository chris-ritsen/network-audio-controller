export const ABSENT = "—";

export function text(value) {
  if (value === null || value === undefined || value === "") {
    return ABSENT;
  }
  if (typeof value === "boolean") {
    return value ? "yes" : "no";
  }
  if (Array.isArray(value)) {
    return value.length ? value.map((entry) => text(entry)).join(", ") : ABSENT;
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export function sampleRate(value) {
  if (value === null || value === undefined) {
    return ABSENT;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return text(value);
  }
  return `${numeric / 1000} kHz`;
}

export function latency(value) {
  if (value === null || value === undefined) {
    return ABSENT;
  }
  return `${value} ms`;
}

export function timestamp(value) {
  if (value === null || value === undefined || value === "") {
    return ABSENT;
  }
  const epochSeconds =
    typeof value === "number" ? value : /^\d+(\.\d+)?$/.test(String(value).trim()) ? Number(value) : null;
  const parsed = epochSeconds === null ? new Date(value) : new Date(epochSeconds * 1000);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  const pad = (part) => String(part).padStart(2, "0");
  const date = `${parsed.getFullYear()}-${pad(parsed.getMonth() + 1)}-${pad(parsed.getDate())}`;
  const time = `${pad(parsed.getHours())}:${pad(parsed.getMinutes())}:${pad(parsed.getSeconds())}`;
  return `${date} ${time}`;
}

export function deviceLabel(device) {
  return device.name || device.server_name;
}

export function channelLabel(channel, number) {
  const name = channel && channel.name ? channel.name : `channel ${number}`;
  return `${number}: ${name}`;
}

export function severityClass(severity) {
  if (severity === "ok" || severity === "success" || severity === "good") {
    return "state-good";
  }
  if (severity === "warning" || severity === "warn") {
    return "state-warn";
  }
  if (severity === "error" || severity === "critical" || severity === "bad") {
    return "state-bad";
  }
  return "";
}

export function subscriptionStatusText(subscription) {
  const status = subscription?.status;
  if (!status) {
    return ABSENT;
  }
  const identifier = status.status || status.label;
  const connected = {
    DYNAMIC: "Connected — unicast",
    STATIC: "Connected — multicast",
    SUBSCRIBE_SELF: "Connected — local loopback",
    MANUAL: "Connected — manually configured",
    CONNECTED: "Connected",
  };
  if (status.state === "connected" && statusTone(status.severity) === "good") {
    return connected[identifier] || "Connected";
  }
  if (identifier === "NONE" || status.state === "none") return "Not subscribed";
  const detail = typeof status.detail === "string" ? status.detail.trim() : "";
  if (detail && !/\b0x[\da-f]+\b|\b[\da-f]{16,}\b/i.test(detail)) {
    return detail.replace(/^(Error|Warning):\s*/i, "");
  }
  return {
    pending: "Subscription pending",
    resolving: "Finding source channel",
    unresolved: "Source channel not found",
    error: "Subscription failed",
  }[status.state] || "Subscription status unavailable";
}

export function subscriptionSource(subscription) {
  if (!subscription || !subscription.tx_device || !subscription.tx_channel) {
    return ABSENT;
  }
  return `${subscription.tx_channel}@${subscription.tx_device}`;
}

export const METER_FLOOR_DBFS = -61;

export function meteringDecibelsFullScale(value) {
  const raw = Number(value);
  if (!Number.isInteger(raw) || raw < 0 || raw > 0xff) {
    return null;
  }
  if (raw === 0x01) {
    return 0;
  }
  if (raw >= 0x02 && raw <= 0xfd) {
    return -((raw - 1) / 2);
  }
  return null;
}

export function meteringLabel(value) {
  if (value === null || value === undefined) {
    return ABSENT;
  }
  const raw = Number(value);
  if (!Number.isInteger(raw) || raw < 0 || raw > 0xff) {
    return text(value);
  }
  if (raw === 0x00) {
    return "clipping";
  }
  if (raw === 0xfe) {
    return "muted";
  }
  if (raw === 0xff) {
    return "invalid";
  }
  return `${meteringDecibelsFullScale(raw).toFixed(1)} dBFS`;
}

export function macAddress(device) {
  const reported = (device.interfaces || []).map((entry) => entry.mac_address).find(Boolean) || device.mac_address;
  let mac = typeof reported === "string" ? reported.replace(/[:-]/g, "").toLowerCase() : "";
  if (/^[0-9a-f]{16}$/.test(mac)) {
    if (mac.slice(6, 10) === "fffe") mac = mac.slice(0, 6) + mac.slice(10);
    else if (mac.endsWith("0000")) mac = mac.slice(0, 12);
  }
  return /^[0-9a-f]{12}$/.test(mac) ? mac.match(/../g).join(":").toUpperCase() : ABSENT;
}

export function stateLabel(value) {
  if (value === null || value === undefined || value === "") return ABSENT;
  if (value === "OK") return "OK";
  const words = String(value).replaceAll("_", " ").toLowerCase();
  return words.charAt(0).toUpperCase() + words.slice(1);
}

export function clockLeaderName(device, inventory) {
  const normalize = (value) => typeof value === "string" ? value.replace(/[:-]/g, "").toLowerCase() : "";
  const identity = normalize(device.leader_clock_identity);
  if (!identity) return ABSENT;
  const matches = Object.values(inventory).filter((candidate) => {
    if ((candidate.ddm_server_profile || "") !== (device.ddm_server_profile || "")) return false;
    if ((candidate.ddm_domain_id || "") !== (device.ddm_domain_id || "")) return false;
    return normalize(candidate.clock_identity) === identity || normalize(macAddress(candidate)) === identity;
  });
  return matches.length === 1 ? deviceLabel(matches[0]) : text(device.leader_clock_identity);
}

export function meterFraction(value) {
  const raw = Number(value);
  if (raw === 0x00) {
    return 1;
  }
  const decibelsFullScale = meteringDecibelsFullScale(raw);
  if (decibelsFullScale === null || decibelsFullScale <= METER_FLOOR_DBFS) {
    return 0;
  }
  return Math.min(1, (decibelsFullScale - METER_FLOOR_DBFS) / -METER_FLOOR_DBFS);
}

export function sortedDevices(devices) {
  return Object.values(devices).sort((first, second) =>
    deviceLabel(first).localeCompare(deviceLabel(second), undefined, { numeric: true, sensitivity: "base" }),
  );
}

export function sortedChannelNumbers(channels) {
  return Object.keys(channels || {})
    .map((key) => Number(key))
    .sort((first, second) => first - second);
}

export function deviceModelName(device) {
  return device.model || device.dante_model || "";
}

export function deviceHaystack(device) {
  return [
    device.name,
    device.server_name,
    device.ipv4,
    device.mac_address,
    device.model,
    device.dante_model,
    device.board_name,
    device.manufacturer,
    device.ddm_domain_name,
  ]
    .filter((value) => typeof value === "string")
    .join(" ")
    .toLowerCase();
}

export function deviceSummaryLine(device) {
  const parts = [];
  if (deviceModelName(device)) {
    parts.push(deviceModelName(device));
  }
  if (device.ipv4) {
    parts.push(device.ipv4);
  }
  if (!parts.length && device.server_name) {
    parts.push(device.server_name);
  }
  return parts.join("  ");
}

export function sortedShureDevices(shureDevices) {
  return Object.values(shureDevices).sort((first, second) =>
    String(first.name || first.mac).localeCompare(String(second.name || second.mac), undefined, {
      numeric: true,
      sensitivity: "base",
    }),
  );
}

export function channelCountLabel(device) {
  return `${text(device.tx_count)} × ${text(device.rx_count)}`;
}

export function statusTone(severity) {
  if (severity === "ok" || severity === "success" || severity === "good") {
    return "good";
  }
  if (severity === "warning" || severity === "warn") {
    return "warn";
  }
  if (severity === "error" || severity === "critical" || severity === "bad") {
    return "bad";
  }
  return "";
}

export function subscriptionTone(subscription) {
  const status = subscription?.status;
  if ((status?.status || status?.label) === "UNRESOLVED") return "warn";
  return statusTone(status?.severity);
}

const CLOCK_SUBDOMAIN_SIZE = 16;

function clockSubdomainBytes(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    return [...value].map((character) => character.charCodeAt(0) & 0xff);
  }
  if (!Array.isArray(value)) {
    return null;
  }
  if (value.some((entry) => !Number.isInteger(entry) || entry < 0 || entry > 0xff)) {
    return null;
  }
  return value.slice(0, CLOCK_SUBDOMAIN_SIZE);
}

export function clockSubdomain(value) {
  const raw = clockSubdomainBytes(value);
  if (raw === null) {
    return ABSENT;
  }
  const terminator = raw.indexOf(0);
  const content = terminator < 0 ? raw : raw.slice(0, terminator);
  if (!content.length) {
    return "unset (default subdomain)";
  }
  const printable = content.every((entry) => entry >= 0x20 && entry <= 0x7e);
  if (printable) {
    return content.map((entry) => String.fromCharCode(entry)).join("");
  }
  return "Custom subdomain";
}

export function clockSubdomainInputValue(value) {
  const formatted = clockSubdomain(value);
  if (formatted === ABSENT || formatted === "unset (default subdomain)" || formatted === "Custom subdomain") {
    return "";
  }
  return formatted;
}

export function clockSourceCode(value) {
  if (value === null || value === undefined || typeof value !== "number" || !Number.isInteger(value)) {
    return ABSENT;
  }
  return "Device-defined";
}

export function preferredLeader(value) {
  if (value === null || value === undefined) {
    return ABSENT;
  }
  return value ? "enabled" : "disabled";
}
