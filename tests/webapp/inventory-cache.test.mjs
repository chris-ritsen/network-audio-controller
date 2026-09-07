import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";
const { readInventoryCache, writeInventoryCache } = await import(`${WEBAPP}inventory-cache.js`);
const key = "netaudio.inventory.v1";

test("last-known backend link speed remains visible during browser startup", () => {
  writeInventoryCache({ device: { server_name: "device", link_speed_mbps: 1000 } });
  assert.equal(readInventoryCache().device.link_speed_mbps, 1000);
  window.localStorage.removeItem(key);
});

test("inventory cache preserves display data without copying unrelated settings", () => {
  writeInventoryCache({ device: { server_name: "device", name: "Receiver", online: true, rx_count: 2, credentials: "excluded", ddm_parameters: { secret: "excluded" } } });
  assert.deepEqual(readInventoryCache(), { device: { server_name: "device", name: "Receiver", online: true, rx_count: 2 } });
  writeInventoryCache({});
  assert.deepEqual(readInventoryCache(), {});
});

test("corrupt and malformed cached inventory never prevents startup", () => {
  for (const value of ["not json", "null", JSON.stringify({ savedAt: Date.now(), devices: [] }), JSON.stringify({ savedAt: Date.now(), devices: { broken: null } })]) {
    window.localStorage.setItem(key, value);
    assert.deepEqual(readInventoryCache(), {});
  }
  window.localStorage.removeItem(key);
});

test("saved inventory remains available after a long absence", () => {
  const devices = { old: { server_name: "old" } };
  window.localStorage.setItem(key, JSON.stringify({ savedAt: 1, devices }));
  assert.deepEqual(readInventoryCache(), devices);
  window.localStorage.removeItem(key);
});
