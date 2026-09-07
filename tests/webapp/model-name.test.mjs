import assert from "node:assert/strict";
import { test } from "node:test";
import { fixture, setLocation, WEBAPP } from "./setup.mjs";

const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const format = await import(`${WEBAPP}format.js`);
const store = await import(`${WEBAPP}store.js`);
const router = await import(`${WEBAPP}router.js`);
const { devicesView } = await import(`${WEBAPP}views/devices.js`);
const { StatusSection } = await import(`${WEBAPP}device/status.js`);

test("product model and Dante platform name remain distinct", () => {
  const device = { name: "a32", model: "A32 Dante AD/DA Converter", dante_model: "A32 Dante AD/DA Converter",
    board_name: "Brooklyn II", dante_model_id: "Bklyn2" };
  const markup = render(h(StatusSection, { device }));
  assert.match(markup, /Brooklyn II/);
  assert.equal(markup.match(/A32 Dante AD\/DA Converter/g).length, 1);
  assert.match(format.deviceHaystack(device), /brooklyn ii/);
  const unavailable = render(h(StatusSection, { device: { ...device, board_name: undefined } }));
  assert.equal(unavailable.match(/A32 Dante AD\/DA Converter/g).length, 1);
});

test("reported Bluetooth product name appears in device list, header and search", () => {
  const device = { ...Object.values(fixture("devices"))[0], name: "avio-bt-1", server_name: "avio-bt-1.local.",
    model: "", dante_model: "Audinate Dante AVIO Bluetooth", online: true };
  store.devices.value = { [device.server_name]: device };
  assert.equal(format.deviceModelName(device), device.dante_model);
  assert.match(format.deviceHaystack(device), /audinate dante avio bluetooth/);
  assert.match(format.deviceSummaryLine(device), /Audinate Dante AVIO Bluetooth/);
  for (const path of ["/devices", "/devices/avio-bt-1/receive"]) {
    setLocation(path);
    const markup = render(h(devicesView.component, { location: router.resolve(path, "") }));
    assert.match(markup, /Audinate Dante AVIO Bluetooth/);
    if (path === "/devices") {
      assert.match(markup, /^<div class="flex flex-col gap-4 pb-6">/);
      assert.match(markup, /class="table-wrapper">/);
      assert.doesNotMatch(markup, />Device Info</);
      assert.doesNotMatch(markup, /devices online|>Device name</);
      assert.match(markup, /placeholder="Find a device…"/);
    }
  }
  assert.equal(format.deviceModelName({ model: "AD4D", dante_model: "other" }), "AD4D");
  assert.equal(format.deviceModelName({}), "");
});
