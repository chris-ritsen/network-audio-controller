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
  assert.deepEqual(matrix.measureMatrixLayout(context, [long], [long], theme), { gutter: 340, header: 78 + `1  ${long.name}`.length * 8 });
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
  assert.equal(collapsed.rows[1].activity.count, 1);
  assert.equal(collapsed.rows[2].activity.severity, "warning");
  assert.equal(matrix.cellState(collapsed.rows[1], collapsed.columns[1], collapsed.subscriptionIndex, {}).kind, "empty");
  assert.equal(matrix.cellState(collapsed.rows[1], collapsed.columns[2], collapsed.subscriptionIndex, {}).count, 1);
  options.groups.receivers.add(collapsed.rows[1].key);
  const expanded = matrix.buildMatrixModel(options);
  assert.equal(expanded.rows.filter((row) => row.kind === "channel").length, 16);
  const channel = expanded.rows.find((row) => row.kind === "channel");
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

test("axis indicators include subscriptions hidden by the opposite filter", () => {
  const receivers = model({ transmitterFilter: "Windows-PC", expandedReceivers: new Set(["avio-usb-1"]) });
  assert.equal(receivers.rows.find((row) => row.label === "avio-usb-1" && row.kind === "device").activity.count, 2);
  assert.equal(receivers.rows.find((row) => row.label === "avio-usb-1" && row.kind === "channel").activity.count, 1);
  const transmitters = model({ receiverFilter: "avio-bt-1" });
  const source = transmitters.columns.find((column) => column.label === "lx-dante");
  const baseline = model().columns.find((column) => column.label === "lx-dante");
  assert.deepEqual(source.activity, baseline.activity);
  assert.ok(source.activity.count >= 3);
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
