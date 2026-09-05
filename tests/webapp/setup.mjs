import { readFileSync } from "node:fs";

const storage = new Map();

globalThis.localStorage = {
  getItem: (key) => (storage.has(key) ? storage.get(key) : null),
  removeItem: (key) => storage.delete(key),
  setItem: (key, value) => storage.set(key, String(value)),
};

globalThis.window = {
  addEventListener() {},
  devicePixelRatio: 1,
  history: {
    pushState(_state, _title, url) {
      setLocation(url);
    },
    replaceState(_state, _title, url) {
      setLocation(url);
    },
  },
  innerHeight: 900,
  innerWidth: 1440,
  localStorage: globalThis.localStorage,
  location: { origin: "http://127.0.0.1:9000", pathname: "/devices", search: "" },
  matchMedia: () => ({ matches: false }),
  removeEventListener() {},
  requestAnimationFrame: () => 0,
  cancelAnimationFrame() {},
};

globalThis.document = {
  addEventListener() {},
  body: {},
  createElement: () => ({ appendChild() {}, remove() {}, style: {} }),
  getElementById: () => null,
  querySelector: () => null,
  removeEventListener() {},
};

Object.defineProperty(globalThis, "navigator", { configurable: true, value: { platform: "MacIntel", userAgent: "node" } });

globalThis.ResizeObserver = class {
  observe() {}
  disconnect() {}
};

export function setLocation(url) {
  const parsed = new URL(url, "http://127.0.0.1:9000");
  globalThis.window.location.pathname = parsed.pathname;
  globalThis.window.location.search = parsed.search;
}

export function fixture(name) {
  return JSON.parse(readFileSync(new URL(`./fixtures/${name}.json`, import.meta.url), "utf8"));
}

export const WEBAPP = new URL("../../packages/netaudio/src/netaudio/daemon/http/webapp/", import.meta.url).href;
