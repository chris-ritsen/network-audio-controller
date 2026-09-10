import assert from "node:assert/strict";
import { test } from "node:test";

import { fixture, WEBAPP } from "./setup.mjs";

const matrix = await import(`${WEBAPP}matrix.js`);
const store = await import(`${WEBAPP}store.js`);
const devices = fixture("devices");

test("matrix column headers grow to fit labels and subscription text never affects layout", () => {
  const context = { measureText: (text) => ({ width: text.length * 8 }) };
  const theme = { dataFont: "monospace" };
  const channel = { kind: "channel", number: 1, name: "Main" };
  const baseline = matrix.measureMatrixLayout(context, [channel], [channel], theme);
  const subscribed = { ...channel, subscription: { tx_device: "source".repeat(100), tx_channel: "channel".repeat(100) } };
  assert.deepEqual(matrix.measureMatrixLayout(context, [subscribed], [channel], theme), baseline);
  const long = { ...channel, name: "long-name".repeat(100) };
  assert.deepEqual(matrix.measureMatrixLayout(context, [long], [long], theme), { gutter: 340, header: 78 + long.name.length * 8 });
});
const { groupChannels } = await import(`${WEBAPP}channel-groups.js`);

test("channel groups partition by channel number, expand independently, and retain scoped subscriptions", () => {
  const channels = Object.fromEntries(Array.from({ length: 33 }, (_, index) => [index + 1, { name: `ch${index + 1}` }]));
  const receiver = { name: "Receiver", server_name: "rx", channels: { receivers: channels }, subscriptions: [
    { rx_channel: "ch1", tx_device: "Source", tx_channel: "ch17", status: { severity: "ok" } },
    { rx_channel: "ch17", tx_device: "Source", tx_channel: "ch1", status: { severity: "warning" } },
  ] };
  const transmitter = { name: "Source", server_name: "tx", channels: { transmitters: channels } };
  const options = { devices: { rx: receiver, tx: transmitter }, expandedReceivers: new Set(["Receiver"]),
    expandedTransmitters: new Set(["Source"]), receiverFilter: "", transmitterFilter: "",
    groups: { enabled: true, receivers: new Set(), transmitters: new Set() } };
  const collapsed = matrix.buildMatrixModel(options);
  assert.deepEqual(collapsed.rows.slice(1).map((row) => row.name), ["1–16", "17–32", "33–33"]);
  assert.equal(collapsed.rows[0].activity, undefined);
  assert.equal(collapsed.rows[1].activity.count, 1);
  assert.equal(collapsed.rows[2].activity.severity, "warning");
  assert.equal(matrix.cellState(collapsed.rows[1], collapsed.columns[1], collapsed.subscriptionIndex, {}).kind, "empty");
  assert.equal(matrix.cellState(collapsed.rows[1], collapsed.columns[2], collapsed.subscriptionIndex, {}).count, 1);
  options.groups.receivers.add(collapsed.rows[1].key);
  const expanded = matrix.buildMatrixModel(options);
  assert.equal(expanded.rows.filter((row) => row.kind === "channel").length, 16);
  const channel = expanded.rows.find((row) => row.kind === "channel");
  assert.equal(channel.activity.count, 1);
  assert.ok(expanded.rows.filter((row) => row.kind !== "channel" && row.expanded).every((row) => row.activity === undefined));
  assert.equal(matrix.cellState(channel, expanded.columns[1], expanded.subscriptionIndex, {}).kind, "empty");
  assert.equal(matrix.cellState(channel, expanded.columns[2], expanded.subscriptionIndex, {}).kind, "partial");
  for (const row of expanded.rows) for (const column of expanded.columns) {
    assert.deepEqual(matrix.cellState(column, row, expanded.subscriptionIndex, {}, true),
      matrix.cellState(row, column, expanded.subscriptionIndex, {}));
  }
  assert.deepEqual(groupChannels("sparse", [{ number: 1 }, { number: 17 }, { number: 33 }]).map((group) => group.channels.length), [1, 1, 1]);
});

function model(options = {}) {
  return matrix.buildMatrixModel({
    devices,
    expandedReceivers: new Set(),
    expandedTransmitters: new Set(),
    receiverFilter: "",
    transmitterFilter: "",
    ...options,
  });
}

test("receiver status indicators follow channels and collapsed summaries, including hidden sources", () => {
  const receivers = model({ transmitterFilter: "Windows-PC", expandedReceivers: new Set(["avio-usb-1"]) });
  assert.equal(receivers.rows.find((row) => row.label === "avio-usb-1" && row.kind === "device").activity, undefined);
  assert.equal(receivers.rows.find((row) => row.label === "avio-usb-1" && row.kind === "channel").activity.count, 1);
  const transmitters = model({ receiverFilter: "avio-bt-1" });
  const source = transmitters.columns.find((column) => column.label === "lx-dante");
  const baseline = model().columns.find((column) => column.label === "lx-dante");
  assert.equal(source.activity, undefined);
  assert.equal(baseline.activity, undefined);
});

test("collapsed model has one row and one column per device with channels", () => {
  const { columns, rows } = model();
  assert.ok(rows.every((row) => row.kind === "device"));
  assert.ok(columns.every((column) => column.kind === "device"));
  const receivers = Object.values(devices).filter((device) => Object.keys(device.channels.receivers || {}).length);
  assert.equal(rows.length, receivers.length);
});

test("flipping axes preserves every subscription cell and activity marker", () => {
  const names = new Set(Object.values(devices).map((device) => device.name));
  const { rows, columns, subscriptionIndex } = model({ expandedReceivers: names, expandedTransmitters: names });
  for (const receiver of rows) {
    for (const transmitter of columns) {
      assert.deepEqual(
        matrix.cellState(transmitter, receiver, subscriptionIndex, {}, true),
        matrix.cellState(receiver, transmitter, subscriptionIndex, {}),
      );
    }
  }
});

test("expanding a receiver adds its channel rows after the device row", () => {
  const { rows } = model({ expandedReceivers: new Set(["avio-usb-1"]) });
  const index = rows.findIndex((row) => row.kind === "device" && row.label === "avio-usb-1");
  assert.ok(index >= 0);
  assert.equal(rows[index + 1].kind, "channel");
  assert.equal(rows[index + 1].label, "avio-usb-1");
  assert.equal(rows[index + 1].number, 1);
});

test("channel filter shows only matching channels and auto-expands", () => {
  const { rows } = model({ receiverFilter: "mic-mix-2" });
  const labels = rows.map((row) => `${row.kind}:${row.label}:${row.name || ""}`);
  assert.deepEqual(labels, ["device:avio-usb-1:", "channel:avio-usb-1:mic-mix-2"]);
});

test("device-level cell aggregates subscriptions between two devices", () => {
  const { columns, rows, subscriptionIndex } = model();
  const receiver = rows.find((row) => row.label === "avio-usb-1");
  const transmitter = columns.find((column) => column.label === "lx-dante");
  const state = matrix.cellState(receiver, transmitter, subscriptionIndex, {});
  assert.equal(state.kind, "aggregate");
  assert.equal(state.count, 2);
  assert.equal(state.severity, "ok");
});

test("unrelated device pair is empty", () => {
  const { columns, rows, subscriptionIndex } = model();
  const receiver = rows.find((row) => row.label === "avio-usb-1");
  const transmitter = columns.find((column) => column.label === "Windows-PC");
  assert.equal(matrix.cellState(receiver, transmitter, subscriptionIndex, {}).kind, "empty");
});

test("channel cell reports subscription severity", () => {
  const { columns, rows, subscriptionIndex } = model({
    expandedReceivers: new Set(["Windows-PC"]),
    expandedTransmitters: new Set(["lx-dante"]),
  });
  const receiver = rows.find((row) => row.kind === "channel" && row.label === "Windows-PC" && row.subscription);
  assert.ok(receiver, "expected a subscribed Windows-PC channel row");
  const transmitter = columns.find(
    (column) =>
      column.kind === "channel" && column.label === "lx-dante" && column.name === receiver.subscription.tx_channel,
  );
  assert.ok(transmitter);
  assert.equal(matrix.cellState(receiver, transmitter, subscriptionIndex, {}).kind, "ok");
  const tooltip = matrix.describeHover({ rowIndex: rows.indexOf(receiver), columnIndex: columns.indexOf(transmitter) }, rows, columns, subscriptionIndex, {}, false);
  assert.match(tooltip, /Connected/);
  assert.ok(tooltip.includes(receiver.name));
  assert.ok(tooltip.includes(transmitter.name));
  assert.ok(tooltip.startsWith(`${receiver.name}@${receiver.label} ← ${transmitter.name}@${transmitter.label}\n`));
  assert.doesNotMatch(tooltip, /›/);
});

test("a pending change renders as pending on the targeted cell only", () => {
  const { columns, rows, subscriptionIndex } = model({
    expandedReceivers: new Set(["avio-usb-1"]),
    expandedTransmitters: new Set(["lx-dante"]),
  });
  const receiver = rows.find((row) => row.kind === "channel" && row.label === "avio-usb-1" && row.number === 1);
  const target = columns.find((column) => column.kind === "channel" && column.label === "lx-dante" && column.number === 5);
  const other = columns.find((column) => column.kind === "channel" && column.label === "lx-dante" && column.number === 6);
  const pending = {
    [store.pendingKey(receiver.device.server_name, receiver.number)]: {
      action: "add",
      tx_channel: target.name,
      tx_device: "lx-dante",
    },
  };
  assert.equal(matrix.cellState(receiver, target, subscriptionIndex, pending).kind, "pending");
  assert.equal(matrix.cellState(receiver, other, subscriptionIndex, pending).kind, "empty");
  assert.match(matrix.describeHover({ rowIndex: rows.indexOf(receiver), columnIndex: columns.indexOf(target) }, rows, columns, subscriptionIndex, pending, false), /change pending/);
});

test("collapsed receiver shows a partial marker against a transmitter device it is subscribed to", () => {
  const { columns, rows, subscriptionIndex } = model({ expandedReceivers: new Set(["avio-usb-1"]) });
  const receiver = rows.find((row) => row.kind === "channel" && row.label === "avio-usb-1" && row.number === 1);
  const transmitter = columns.find((column) => column.kind === "device" && column.label === "lx-dante");
  assert.equal(matrix.cellState(receiver, transmitter, subscriptionIndex, {}).kind, "partial");
});

test("receiver-reported connection progress remains pending until connected", () => {
  const receiver = { name: "Receiver", server_name: "receiver", channels: { receivers: { 1: { name: "Input" } } }, subscriptions: [
    { rx_channel: "Input", tx_device: "Source", tx_channel: "Output", status: { state: "in_progress", severity: "progress" } },
  ] };
  const source = { name: "Source", server_name: "source", channels: { transmitters: { 1: { name: "Output" } } } };
  const options = { devices: { receiver, source }, expandedReceivers: new Set(["Receiver"]), expandedTransmitters: new Set(["Source"]), receiverFilter: "", transmitterFilter: "" };
  for (const [state, severity, expected] of [["in_progress", "progress", "pending"], ["resolved", "progress", "pending"], ["connected", "ok", "ok"], ["error", "error", "error"]]) {
    receiver.subscriptions[0].status = { state, severity };
    const { rows, columns, subscriptionIndex } = matrix.buildMatrixModel(options);
    const row = rows.find((entry) => entry.kind === "channel");
    const column = columns.find((entry) => entry.kind === "channel");
    assert.equal(matrix.cellState(row, column, subscriptionIndex, {}).kind, expected);
    assert.equal(row.activity.severity, expected);
  }
});

test("pending subscriptions update channel, collapsed group, and crosspoint status together", () => {
  for (const action of ["add", "remove"]) {
    const subscription = { rx_channel: "Input", tx_device: "Source", tx_channel: "Output", status: { state: "connected", severity: "ok" } };
    const receiver = { name: "Receiver", server_name: "receiver", channels: { receivers: { 1: { name: "Input" } } }, subscriptions: action === "remove" ? [subscription] : [] };
    const source = { name: "Source", server_name: "source", channels: { transmitters: { 1: { name: "Output" } } } };
    const options = { devices: { receiver, source }, expandedReceivers: new Set(["Receiver"]), expandedTransmitters: new Set(["Source"]), receiverFilter: "", transmitterFilter: "",
      pending: { [store.pendingKey("receiver", 1)]: { action, tx_device: "Source", tx_channel: "Output" } } };
    const pending = matrix.buildMatrixModel(options);
    const row = pending.rows.find((entry) => entry.kind === "channel");
    const column = pending.columns.find((entry) => entry.kind === "channel");
    assert.equal(row.activity.severity, "pending");
    assert.equal(matrix.cellState(row, column, pending.subscriptionIndex, options.pending).kind, "pending");
    const grouped = matrix.buildMatrixModel({ ...options, groups: { enabled: true, receivers: new Set(), transmitters: new Set() } });
    assert.equal(grouped.rows.find((entry) => entry.kind === "group").activity.severity, "pending");
    receiver.subscriptions = action === "add" ? [subscription] : [];
    const confirmed = matrix.buildMatrixModel({ ...options, pending: {} });
    const confirmedRow = confirmed.rows.find((entry) => entry.kind === "channel");
    assert.equal(confirmedRow.activity?.severity, action === "add" ? "ok" : undefined);
    assert.equal(matrix.cellState(confirmedRow, column, confirmed.subscriptionIndex, {}).kind, action === "add" ? "ok" : "empty");
  }
});

test("pending routes survive unrelated readbacks and clear only for matching channels", () => {
  const first = store.pendingKey("Receiver", 1);
  const second = store.pendingKey("Receiver", 2);
  store.pendingSubscriptions.value = {
    [first]: { action: "add", tx_device: "Source", tx_channel: "New" },
    [second]: { action: "remove" },
  };
  const device = { name: "Receiver", channels: { receivers: { 1: { name: "One" }, 2: { name: "Two" } } },
    subscriptions: [{ rx_channel: "One", tx_device: "Source", tx_channel: "Old" },
      { rx_channel: "Two", tx_device: "Source", tx_channel: "Old" }] };
  store.clearPendingForDevice(device);
  assert.equal(Object.keys(store.pendingSubscriptions.value).length, 2);
  device.subscriptions[0].tx_channel = "New";
  store.clearPendingForDevice(device);
  assert.deepEqual(Object.keys(store.pendingSubscriptions.value), [second]);
  device.subscriptions.pop();
  store.clearPendingForDevice(device);
  assert.deepEqual(store.pendingSubscriptions.value, {});
});
