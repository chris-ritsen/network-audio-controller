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

test("source picker offers disconnect only for a configured source", async () => {
  const { RoutePicker } = await import(`${WEBAPP}route-picker.js`);
  const receiver = Object.values(devices)[0];
  for (const subscription of [null, {}, { tx_device: "", tx_channel: "" }, { tx_device: "source", tx_channel: "" }]) {
    assert.doesNotMatch(render(h(RoutePicker, { receiver, receiveChannelNumber: 1, subscription })), />Disconnect</);
  }
  assert.match(render(h(RoutePicker, { receiver, receiveChannelNumber: 1, subscription: { tx_device: "source", tx_channel: "left" } })), />Disconnect</);
});

function renderView(view, path) {
  setLocation(path);
  const location = router.resolve(window.location.pathname, window.location.search);
  return render(h(view.component, { location }));
}

const VIEWS = [
  ["ddm", "ddmView", "/ddm"],
  ["devices", "devicesView", "/devices"],
  ["events", "eventsView", "/events"],
  ["routing", "routingView", "/routing"],
  ["subscriptions", "subscriptionsView", "/subscriptions"],
  ["presets", "presetsView", "/presets"],
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

const DEVICE_SECTIONS = ["receive", "transmit", "metering", "status", "device-config", "network-config", "aes67-config", "lock", "domain"];

test("subscription table has its own route and is absent from the routing matrix page", async () => {
  const { routingView } = await import(`${WEBAPP}views/routing.js`);
  const { subscriptionsView } = await import(`${WEBAPP}views/subscriptions.js`);
  assert.doesNotMatch(renderView(routingView, "/routing"), /Subscription details|Subscriptions \(/);
  assert.match(renderView(subscriptionsView, "/subscriptions"), /Subscriptions \(/);
  assert.equal(router.resolve("/subscriptions", "").view, "subscriptions");
});

test("offline unmanaged device sections are excluded from the displayed inventory", async () => {
  const { devicesView } = await import(`${WEBAPP}views/devices.js`);
  const device = Object.values(devices).find((entry) => entry.online === false);
  for (const section of DEVICE_SECTIONS) {
    const markup = renderView(devicesView, `/devices/${encodeURIComponent(device.name)}/${section}`);
    assert.match(markup, /No device/);
    assert.doesNotMatch(markup, /Dismiss offline/);
    assert.doesNotMatch(markup, /Reboot|Apply|Identify|Start detailed|type="password"/);
  }
});

test("structured display values use readable summaries, not protocol dumps", async () => {
  const { Value } = await import(`${WEBAPP}components.js`);
  const markup = render(h(Value, { value: { summary: "Connected", raw_hex: "deadbeef", opcode: "0x1002", nested: { packet: "cafebabe" } } }));
  assert.match(markup, /Connected/);
  assert.doesNotMatch(markup, /deadbeef|0x1002|cafebabe|Raw hex|Opcode/);
});

for (const port of [4321, null]) {
  test(`metering hides transport metadata with port ${port}`, async () => {
    const { DeviceMeters } = await import(`${WEBAPP}device/meters.js`);
    const device = Object.values(devices)[0];
    store.meterCache.set(device.server_name, {
      source_ip: "192.0.2.37",
      source_port: port,
      metering_source: "detailed",
      tx: {},
      rx: {},
    });
    try {
      const markup = render(h(DeviceMeters, { device }));
      assert.doesNotMatch(markup, /\[object Object\]/);
      assert.match(markup, /Receive levels/);
      assert.doesNotMatch(markup, /192\.0\.2\.37/);
      assert.doesNotMatch(markup, /:null|:undefined/);
    } finally {
      store.meterCache.delete(device.server_name);
    }
  });
}

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
    const { DeviceMeters } = await import(`${WEBAPP}device/meters.js`);
    const device = Object.values(devices).find((entry) => entry.name === name);
    const markup = render(h(DeviceMeters, { device }));
    assert.match(markup, device.online ? /Receive levels/ : /Monitoring is unavailable/);
  });
}
