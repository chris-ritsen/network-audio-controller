import { batch, signal } from "./lib/preact.js";

const EVENT_LOG_LIMIT = 400;
const METER_TABLE_INTERVAL_MILLISECONDS = 250;
const RECONNECT_DELAY_MILLISECONDS = 2000;

const PENDING_SUBSCRIPTION_TIMEOUT_MILLISECONDS = 8000;

export const connectionState = signal("connecting");
export const devices = signal({});
export const events = signal([]);
export const meterRevision = signal(0);
export const pendingSubscriptions = signal({});
export const shureDevices = signal({});
export const shureMeters = signal({});

export const meterCache = new Map();

const meterHandlers = new Set();
let meterTableTimer = null;

export function onMeterValues(handler) {
  meterHandlers.add(handler);
  return () => meterHandlers.delete(handler);
}

export function meterValuesFor(serverName) {
  return meterCache.get(serverName) || null;
}

function scheduleMeterTableUpdate() {
  if (meterTableTimer !== null) {
    return;
  }
  meterTableTimer = setTimeout(() => {
    meterTableTimer = null;
    meterRevision.value += 1;
  }, METER_TABLE_INTERVAL_MILLISECONDS);
}

function recordEvent(payload) {
  const entry = { payload, received: new Date().toISOString() };
  const next = [entry, ...events.value];
  if (next.length > EVENT_LOG_LIMIT) {
    next.length = EVENT_LOG_LIMIT;
  }
  events.value = next;
}

function applyMeterValues(payload) {
  const previous = meterCache.get(payload.server_name);
  const continues = previous && previous.metering_source === payload.metering_source;
  const values = {
    metering_source: payload.metering_source,
    rx: { ...(continues ? previous.rx : {}), ...(payload.rx || {}) },
    rx_signal_presence: {
      ...(continues ? previous.rx_signal_presence : {}),
      ...(payload.rx_signal_presence || {}),
    },
    source_ip: payload.source_ip,
    source_port: payload.source_port,
    tx: { ...(continues ? previous.tx : {}), ...(payload.tx || {}) },
    tx_signal_presence: {
      ...(continues ? previous.tx_signal_presence : {}),
      ...(payload.tx_signal_presence || {}),
    },
    wall_time: payload.wall_time,
  };
  meterCache.set(payload.server_name, values);
  for (const handler of meterHandlers) {
    handler(payload.server_name, values);
  }
  scheduleMeterTableUpdate();
}

export function pendingKey(receiverName, receiveChannelNumber) {
  return `${receiverName}\u0000${receiveChannelNumber}`;
}

function markPending(payload) {
  const key = pendingKey(payload.rx_device, payload.rx_channel);
  const entry = {
    action: payload.action,
    since: Date.now(),
    tx_channel: payload.tx_channel,
    tx_device: payload.tx_device,
  };
  pendingSubscriptions.value = { ...pendingSubscriptions.value, [key]: entry };
  setTimeout(() => {
    const current = pendingSubscriptions.value;
    if (current[key] === entry) {
      const next = { ...current };
      delete next[key];
      pendingSubscriptions.value = next;
    }
  }, PENDING_SUBSCRIPTION_TIMEOUT_MILLISECONDS);
}

function clearPendingForDevice(device) {
  const current = pendingSubscriptions.value;
  const names = new Set([device.name, device.server_name].filter(Boolean));
  let changed = false;
  const next = {};
  for (const [key, entry] of Object.entries(current)) {
    const receiverName = key.split("\u0000")[0];
    if (names.has(receiverName)) {
      changed = true;
      continue;
    }
    next[key] = entry;
  }
  if (changed) {
    pendingSubscriptions.value = next;
  }
}

function applyEvent(payload) {
  const kind = payload.event;
  if (kind === "subscription_pending") {
    markPending(payload);
    return;
  }
  if (kind === "snapshot") {
    batch(() => {
      devices.value = payload.devices || {};
      shureDevices.value = payload.shure_devices || {};
    });
    for (const [serverName, values] of Object.entries(payload.metering || {})) {
      meterCache.set(serverName, values);
    }
    scheduleMeterTableUpdate();
    return;
  }
  if (kind === "device_discovered" || kind === "device_updated") {
    devices.value = { ...devices.value, [payload.server_name]: payload.device };
    if (payload.device) {
      clearPendingForDevice(payload.device);
    }
    return;
  }
  if (kind === "device_removed") {
    const next = { ...devices.value };
    delete next[payload.server_name];
    devices.value = next;
    meterCache.delete(payload.server_name);
    return;
  }
  if (kind === "meter_values") {
    applyMeterValues(payload);
    return;
  }
  if (kind === "shure_device_discovered" || kind === "shure_device_updated") {
    shureDevices.value = { ...shureDevices.value, [payload.mac]: payload.device };
    return;
  }
  if (kind === "shure_device_removed") {
    const next = { ...shureDevices.value };
    delete next[payload.mac];
    shureDevices.value = next;
    return;
  }
  if (kind === "shure_meter_values") {
    const existing = shureMeters.value[payload.mac] || {};
    const channel = { ...(existing[payload.channel] || {}), [payload.key]: payload.value };
    shureMeters.value = { ...shureMeters.value, [payload.mac]: { ...existing, [payload.channel]: channel } };
  }
}

export function connect() {
  const source = new EventSource("/events");

  source.addEventListener("open", () => {
    connectionState.value = "open";
  });

  source.addEventListener("message", (event) => {
    let payload = null;
    try {
      payload = JSON.parse(event.data);
    } catch (error) {
      recordEvent({ data: event.data, event: "parse_error", message: String(error) });
      return;
    }
    applyEvent(payload);
    if (payload.event !== "meter_values" && payload.event !== "shure_meter_values") {
      recordEvent(payload);
    }
  });

  source.addEventListener("error", () => {
    connectionState.value = "closed";
    source.close();
    setTimeout(connect, RECONNECT_DELAY_MILLISECONDS);
  });
}

export function deviceRequestName(device) {
  return device.server_name || device.name;
}

export function deviceByName(deviceName) {
  if (!deviceName) {
    return null;
  }
  for (const device of Object.values(devices.value)) {
    if (device.name === deviceName || device.server_name === deviceName) {
      return device;
    }
  }
  return null;
}

export function deviceKey(device) {
  return device.server_name || device.name;
}
