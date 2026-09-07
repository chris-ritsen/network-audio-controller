import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP, fixture, setLocation } from "./setup.mjs";

const { DEVICE_FILTERS, matchesDeviceFilters, deviceFilterOptions, toggleDeviceFilter, readRoutingFilters, saveRoutingFilters } = await import(`${WEBAPP}device-filters.js`);
const { DeviceFilterPanel } = await import(`${WEBAPP}filter-panel.js`);
const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const inventory = [
  { name: "Alpha", manufacturer: "Maker A", sample_rate_hz: 48000, latency_ms: 0, is_locked: false, online: true,
    ddm_clock_preferences: { external_word_clock: false }, channels: { receivers: { 1: { ddm_media_type: "AUDIO" } } } },
  { name: "Beta", manufacturer: "Maker B", sample_rate_hz: 96000, latency_ms: 2, is_locked: true, online: true },
  { name: "Gamma", manufacturer: "Maker A", sample_rate_hz: 96000, online: false },
];

test("device filters OR choices within a group and AND across groups", () => {
  const filters = { search: "", values: { manufacturer: ["Maker A", "Maker B"], "sample-rate": ["96 kHz"] } };
  assert.deepEqual(inventory.filter((d) => matchesDeviceFilters(d, filters)).map((d) => d.name), ["Beta", "Gamma"]);
  assert.deepEqual(inventory.filter((d) => matchesDeviceFilters(d, { ...filters, search: "gamma" })).map((d) => d.name), ["Gamma"]);
  assert.equal(matchesDeviceFilters(inventory[0], { values: { latency: ["0 ms"], lock: ["Unlocked"], "external-clock": ["Disabled"] } }), true);
});

test("missing state is not invented from absent fields or raw identifiers", () => {
  for (const group of DEVICE_FILTERS) assert.deepEqual(group.values({}), ["Not reported"]);
  assert.deepEqual(DEVICE_FILTERS.find((g) => g.id === "media").values({ media_types: 8 }), ["Not reported"]);
  assert.deepEqual(DEVICE_FILTERS.find((g) => g.id === "external-clock").values({ clock_source_code: 0 }), ["Not reported"]);
});

test("facet counts respect other groups and retain selected values after inventory changes", () => {
  const filters = { values: { manufacturer: ["Maker A"], "sample-rate": ["96 kHz"] } };
  const options = deviceFilterOptions(inventory, filters);
  assert.deepEqual(options.find((g) => g.id === "manufacturer").options, [["Maker A", 1], ["Maker B", 1]]);
  assert.deepEqual(options.find((g) => g.id === "sample-rate").options, [["48 kHz", 1], ["96 kHz", 1]]);
  assert.deepEqual(deviceFilterOptions([], filters).find((g) => g.id === "manufacturer").options, [["Maker A", 0]]);
  const next = toggleDeviceFilter(filters, "manufacturer", "Maker A");
  assert.equal(next.values.manufacturer, undefined);
  assert.deepEqual(filters.values.manufacturer, ["Maker A"]);
});

test("filter panel uses checkboxes without added dismiss buttons or selection badges", () => {
  const filters = { search: "Alpha", values: { lock: ["Unlocked"] } };
  const markup = render(h(DeviceFilterPanel, { all: inventory, filters, onChange() {} }));
  assert.match(markup, /aria-label="Device filters"/);
  assert.match(markup, /Clear all/);
  assert.match(markup, /type="checkbox"/);
  assert.doesNotMatch(markup, /Active device filters|Remove Device lock|routing-filter-chips|badge/);
  assert.equal((markup.match(/<button/g) || []).length, 1);
  assert.doesNotMatch(markup, /<details[^>]*\bopen\b/);
  for (const group of DEVICE_FILTERS) assert.ok(markup.includes(group.label));
});

test("routing filters persist searches and selections, including absent devices", () => {
  const filters = { search: "Desk", receiverSearch: "left", transmitterSearch: "right", expandedGroups: ["manufacturer"], listMode: true, panelOpen: false, values: { manufacturer: ["Absent maker"], availability: ["Online"] } };
  try {
    saveRoutingFilters(filters);
    assert.deepEqual(readRoutingFilters(), filters);
    saveRoutingFilters({ search: "", values: {} });
    assert.deepEqual(readRoutingFilters().values, {});
  } finally { window.localStorage.removeItem("netaudio.routing.filters"); }
});

test("invalid stored filters are ignored and unavailable storage is harmless", () => {
  const original = window.localStorage;
  try {
    for (const saved of ["{", "null", JSON.stringify({ search: [], values: { model: [3, "A", "A"], unknown: ["B"] } })]) {
      original.setItem("netaudio.routing.filters", saved);
      const filters = readRoutingFilters();
      assert.equal(filters.search, "");
      assert.deepEqual(filters.values, saved.startsWith('{"') ? { model: ["A"] } : {});
    }
    window.localStorage = { getItem() { throw Error("denied"); }, setItem() { throw Error("denied"); } };
    assert.deepEqual(readRoutingFilters().values, {});
    assert.doesNotThrow(() => saveRoutingFilters({ values: {} }));
  } finally {
    window.localStorage = original;
    original.removeItem("netaudio.routing.filters");
  }
});

test("routing keeps labeled navigation and axis searches alongside the filter panel", async () => {
  const store = await import(`${WEBAPP}store.js`);
  const { routingView } = await import(`${WEBAPP}views/routing.js`);
  const previous = store.devices.value;
  try {
    store.devices.value = fixture("devices");
    setLocation("/routing");
    const markup = render(h(routingView.component));
    assert.match(markup, /Device filters/);
    assert.match(markup, /aria-controls="routing-device-filters"/);
    assert.match(markup, /Receivers/);
    assert.match(markup, /Transmitters/);
    assert.match(markup, /matrix-viewport/);
  } finally { store.devices.value = previous; }
});

test("filtered channel-list source picker excludes devices outside the chosen inventory", async () => {
  const { RoutePicker } = await import(`${WEBAPP}route-picker.js`);
  const markup = render(h(RoutePicker, { receiver: { name: "Receiver", online: true }, receiveChannelNumber: 1,
    onClose() {}, sourceDevices: [{ name: "Included source", online: true, channels: { transmitters: { 1: { name: "Included channel" } } } }],
  }));
  assert.match(markup, /Included source/);
  assert.match(markup, /Included channel/);
  assert.doesNotMatch(markup, /lx-dante|Windows-PC/);
});
