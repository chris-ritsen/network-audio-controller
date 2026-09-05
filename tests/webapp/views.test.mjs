import assert from "node:assert/strict";
import { test } from "node:test";

import { fixture, setLocation, WEBAPP } from "./setup.mjs";

const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const store = await import(`${WEBAPP}store.js`);
const router = await import(`${WEBAPP}router.js`);

const devices = fixture("devices");
store.devices.value = devices;
const names = Object.values(devices).map((device) => device.name);

function renderView(view, path) {
  setLocation(path);
  const location = router.resolve(window.location.pathname, window.location.search);
  return render(h(view.component, { location }));
}

const VIEWS = [
  ["clock-status", "clockStatusView", "/clock-status"],
  ["ddm", "ddmView", "/ddm"],
  ["devices", "devicesView", "/devices"],
  ["events", "eventsView", "/events"],
  ["flows", "flowsView", "/flows"],
  ["metering", "meteringView", "/metering"],
  ["network-status", "networkStatusView", "/network-status"],
  ["routing", "routingView", "/routing"],
  ["shure", "shureView", "/shure"],
];

for (const [identifier, exportName, path] of VIEWS) {
  test(`${identifier} index renders`, async () => {
    const module = await import(`${WEBAPP}views/${identifier}.js`);
    const view = module[exportName];
    const markup = renderView(view, path);
    assert.ok(markup.length > 0);
    assert.doesNotMatch(markup, /\[object Object\]/);
    assert.doesNotMatch(markup, /\{"[a-z_]+":/);
    assert.doesNotMatch(markup, /\bundefined\b/);
    assert.doesNotMatch(markup, /\bNaN\b/);
  });
}

const DEVICE_SECTIONS = ["receive", "transmit", "status", "latency", "device-config", "network-config", "aes67-config", "flows", "lock", "domain"];

for (const name of names) {
  for (const section of DEVICE_SECTIONS) {
    test(`device ${name} ${section} renders`, async () => {
      const { devicesView } = await import(`${WEBAPP}views/devices.js`);
      const markup = renderView(devicesView, `/devices/${encodeURIComponent(name)}/${section}`);
      assert.ok(markup.includes(name), `expected ${name} in markup`);
      assert.doesNotMatch(markup, /\[object Object\]/);
      assert.doesNotMatch(markup, /\{"[a-z_]+":/);
      assert.doesNotMatch(markup, /\bundefined\b/);
      assert.doesNotMatch(markup, /\bNaN\b/);
    });
  }
  test(`metering for ${name} renders`, async () => {
    const { meteringView } = await import(`${WEBAPP}views/metering.js`);
    const markup = renderView(meteringView, `/metering/${encodeURIComponent(name)}`);
    assert.ok(markup.includes(name));
  });
}
