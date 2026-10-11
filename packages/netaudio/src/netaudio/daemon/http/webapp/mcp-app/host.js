const PROTOCOL_VERSION = "2026-01-26";
const REQUEST_TIMEOUT_MILLISECONDS = 60000;

export class HostError extends Error {
  constructor(message, code) {
    super(message);
    this.name = "HostError";
    this.code = code;
  }
}

let nextIdentifier = 1;
const pending = new Map();
const notificationHandlers = new Map();
const requestHandlers = new Map();

function post(message) {
  window.parent.postMessage({ jsonrpc: "2.0", ...message }, "*");
}

function answer(identifier, method, params) {
  const handler = requestHandlers.get(method);
  Promise.resolve()
    .then(() => {
      if (!handler) throw new HostError(`unknown method ${method}`, -32601);
      return handler(params);
    })
    .then(
      (result) => post({ id: identifier, result: result ?? {} }),
      (error) => post({ id: identifier, error: { code: error.code ?? -32000, message: error.message || String(error) } }),
    );
}

window.addEventListener("message", (event) => {
  if (event.source !== window.parent) return;
  const message = event.data;
  if (!message || message.jsonrpc !== "2.0") return;
  if (typeof message.method !== "string") {
    const entry = pending.get(message.id);
    if (!entry) return;
    pending.delete(message.id);
    clearTimeout(entry.timer);
    if (message.error) entry.reject(new HostError(message.error.message || "host error", message.error.code));
    else entry.resolve(message.result);
    return;
  }
  if (message.id !== undefined && message.id !== null) {
    answer(message.id, message.method, message.params || {});
    return;
  }
  const handler = notificationHandlers.get(message.method);
  if (handler) handler(message.params || {});
});

export function request(method, params = {}) {
  const identifier = nextIdentifier;
  nextIdentifier += 1;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(identifier);
      reject(new HostError(`${method} timed out`, -32001));
    }, REQUEST_TIMEOUT_MILLISECONDS);
    pending.set(identifier, { resolve, reject, timer });
    post({ id: identifier, method, params });
  });
}

export function notify(method, params = {}) {
  post({ method, params });
}

export function onNotification(method, handler) {
  notificationHandlers.set(method, handler);
}

export function onRequest(method, handler) {
  requestHandlers.set(method, handler);
}

export async function callTool(name, args) {
  const result = await request("tools/call", { name, arguments: args });
  if (result?.isError) {
    const text = (result.content || []).map((block) => block.text).filter(Boolean).join("\n");
    throw new HostError(text || `${name} failed`);
  }
  return result?.structuredContent ?? {};
}

function reportSize() {
  let scheduled = false;
  let lastWidth = 0;
  let lastHeight = 0;
  const send = () => {
    if (scheduled) return;
    scheduled = true;
    requestAnimationFrame(() => {
      scheduled = false;
      const root = document.documentElement;
      const original = root.style.height;
      root.style.height = "max-content";
      const height = Math.ceil(root.getBoundingClientRect().height);
      root.style.height = original;
      const width = Math.ceil(window.innerWidth);
      if (width === lastWidth && height === lastHeight) return;
      lastWidth = width;
      lastHeight = height;
      notify("ui/notifications/size-changed", { width, height });
    });
  };
  send();
  const observer = new ResizeObserver(send);
  observer.observe(document.documentElement);
  observer.observe(document.body);
}

export async function connect({ name, version, displayModes }) {
  onRequest("ping", () => ({}));
  const result = await request("ui/initialize", {
    appInfo: { name, version },
    appCapabilities: { availableDisplayModes: displayModes },
    protocolVersion: PROTOCOL_VERSION,
  });
  notify("ui/notifications/initialized");
  reportSize();
  return result || {};
}
