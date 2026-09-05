import assert from "node:assert/strict";
import { test } from "node:test";

import { fixture, WEBAPP } from "./setup.mjs";

const matrix = await import(`${WEBAPP}matrix.js`);
const store = await import(`${WEBAPP}store.js`);
const devices = fixture("devices");

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

test("collapsed model has one row and one column per device with channels", () => {
  const { columns, rows } = model();
  assert.ok(rows.every((row) => row.kind === "device"));
  assert.ok(columns.every((column) => column.kind === "device"));
  const receivers = Object.values(devices).filter((device) => Object.keys(device.channels.receivers || {}).length);
  assert.equal(rows.length, receivers.length);
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
});

test("collapsed receiver shows a partial marker against a transmitter device it is subscribed to", () => {
  const { columns, rows, subscriptionIndex } = model({ expandedReceivers: new Set(["avio-usb-1"]) });
  const receiver = rows.find((row) => row.kind === "channel" && row.label === "avio-usb-1" && row.number === 1);
  const transmitter = columns.find((column) => column.kind === "device" && column.label === "lx-dante");
  assert.equal(matrix.cellState(receiver, transmitter, subscriptionIndex, {}).kind, "partial");
});
