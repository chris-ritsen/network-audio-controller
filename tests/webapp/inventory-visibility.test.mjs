import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";
const { visibleInventory } = await import(`${WEBAPP}inventory-visibility.js`);
const store = await import(`${WEBAPP}store.js`);

const records = {
  online: { online: true },
  offline: { online: false },
  unenrolled: { online: false, management_state: "unenrolled", ddm_device_id: "old" },
  enrolled: { online: false, management_state: "managed", ddm_domain_id: "domain", ddm_context: "studio" },
};

test("offline devices are excluded from inventory regardless of enrollment", () => {
  assert.deepEqual(Object.keys(visibleInventory(records)), ["online"]);
  assert.deepEqual(visibleInventory({ gone: { availability_state: "offline" } }), {});
});

test("offline exclusion composes with context filtering and applies to Shure", () => {
  const previous = store.devices.value;
  try {
    store.devices.value = records;
    store.selectContext("studio");
    assert.deepEqual(store.scopedDevices.value, {});
    store.selectContext("all");
    assert.deepEqual(Object.keys(store.scopedDevices.value), ["online"]);
    store.shureDevices.value = records;
    assert.deepEqual(Object.keys(store.visibleShureDevices.value), ["online"]);
  } finally {
    store.devices.value = previous;
    store.shureDevices.value = {};
    store.selectContext("all");
  }
});
