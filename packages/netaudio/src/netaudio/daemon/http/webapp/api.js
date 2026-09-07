import { inventoryReady, removeForgottenDevices } from "./store.js";

export class ApiError extends Error {
  constructor(message, status, payload) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.payload = payload;
  }
}

async function request(method, path, body) {
  if (method !== "GET" && !["/presets/preview", "/metering/stop", "/ddm/login", "/ddm/logout", "/ddm/context", "/ddm/domains", "/settings/monitoring"].includes(path) && !inventoryReady.value) {
    throw new ApiError("Waiting for live device inventory. Try again when connected.", 409, null);
  }
  const options = { method, headers: { Accept: "application/json" } };
  if (body !== undefined) {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(body);
  }
  const response = await fetch(path, options);
  const raw = await response.text();
  let payload = null;
  if (raw) {
    try {
      payload = JSON.parse(raw);
    } catch (error) {
      throw new ApiError(`${method} ${path} returned invalid JSON: ${raw}`, response.status, raw);
    }
  }
  if (!response.ok) {
    const message = payload && payload.error ? payload.error : `${method} ${path} failed with ${response.status}`;
    throw new ApiError(message, response.status, payload);
  }
  return payload;
}

function get(path) {
  return request("GET", path);
}

function post(path, body = {}) {
  return request("POST", path, body);
}

async function remove(path) {
  const result = await request("DELETE", path);
  removeForgottenDevices(result.forgotten || []);
  return result;
}

export const api = {
  savePreset: (body) => post("/presets/save", body),
  previewPreset: (body) => post("/presets/preview", body),
  loadPreset: (body) => post("/presets/load", body),
  getDevices: (context) => get(`/devices${context ? `?context=${encodeURIComponent(context)}` : ""}`),
  getDevice: (name, context) =>
    get(`/devices/${encodeURIComponent(name)}${context ? `?context=${encodeURIComponent(context)}` : ""}`),
  getInterfaces: (name) => get(`/interfaces/${encodeURIComponent(name)}`),
  getRedundancy: (name) => get(`/redundancy/${encodeURIComponent(name)}`),
  setRedundancy: (body) => post("/redundancy", body),
  getLockStatus: (name) => get(`/lock-status/${encodeURIComponent(name)}`),
  getShureDevices: () => get("/shure/devices"),
  getShureDevice: (identifier) => get(`/shure/devices/${encodeURIComponent(identifier)}`),
  getManagedDevices: (context) => get(`/ddm/devices${context ? `?context=${encodeURIComponent(context)}` : ""}`),
  getManagedDomains: (context) => get(`/ddm/domains${context ? `?context=${encodeURIComponent(context)}` : ""}`),
  getManagedStatus: () => get("/ddm/status"),
  getConnections: () => get("/ddm/connections"),
  getSettings: () => get("/settings"),
  setMonitoringPort: (port) => post("/settings/monitoring", { port }),
  loginDdm: (body) => post("/ddm/login", body),
  logoutDdm: (server) => post("/ddm/logout", { server }),
  setDdmEnrollment: (body) => post("/ddm/enrollment", body),
  createDdmDomain: (server, name) => post("/ddm/domains", { server, name }),
  selectDdmContext: (body) => post("/ddm/context", body),
  getMeteringCache: () => get("/metering/cache"),
  getMeteringSnapshot: (name) => get(`/metering/snapshot/${encodeURIComponent(name)}`),

  forgetDevice: (name) => remove(`/devices/${encodeURIComponent(name)}`),

  subscribe: (body) => post("/subscribe", body),
  unsubscribe: (body) => post("/unsubscribe", body),
  identify: (device) => post("/identify", { device }),
  renameDevice: (device, name) => post("/rename-device", { device, name }),
  renameChannel: (device, channelType, channelNumber, name) =>
    post("/rename-channel", { device, channel_type: channelType, channel_number: channelNumber, name }),
  setLatency: (device, latency) => post("/set-latency", { device, latency }),
  lock: (device, pin) => post("/lock", { device, pin }),
  unlock: (device, pin) => post("/unlock", { device, pin }),
  refresh: (device) => post("/refresh", device ? { device } : {}),
  setSampleRate: (device, sampleRate, confirmDestructive) =>
    post("/set-sample-rate", { device, sample_rate: sampleRate, confirm_destructive: confirmDestructive === true }),
  setEncoding: (device, encoding) => post("/set-encoding", { device, encoding }),
  setGain: (device, channelNumber, gainLevel, deviceType) =>
    post("/set-gain", { device, channel_number: channelNumber, gain_level: gainLevel, device_type: deviceType || "" }),
  setAes67: (device, enabled) => post("/set-aes67", { device, enabled }),
  setAes67MulticastPrefix: (device, prefix) => post("/set-aes67-multicast-prefix", { device, prefix }),
  setSampleRatePullup: (device, rawValue) => post("/set-sample-rate-pullup", { device, raw_value: rawValue }),
  setPreferredLeader: (device, preferred) => post("/set-preferred-leader", { device, preferred }),
  setClockSource: (device, clockSource) => post("/set-clock-source", { device, clock_source: clockSource }),
  setClockSubdomain: (device, subdomain) => post("/set-clock-subdomain", { device, subdomain }),
  refreshClock: (device) => post("/refresh-clock", { device }),
  reboot: (device) => post("/reboot", { device }),
  setInterface: (body) => post("/interface", body),
  startMetering: (device, clientId) => post("/metering/start", { device, client_id: clientId }),
  stopMetering: (device, clientId) => post("/metering/stop", { device, client_id: clientId }),
  reportUnresponsive: (device) => post("/report-unresponsive", { device }),
  managedGraphql: (query, variables, operationName, context) =>
    post("/ddm/graphql", { query, variables, operation_name: operationName, context }),
  managedRefresh: (context) => post("/ddm/refresh", context ? { context } : {}),
  shutdown: () => post("/shutdown", {}),
};
