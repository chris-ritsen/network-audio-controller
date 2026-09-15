import assert from "node:assert/strict";
import { test } from "node:test";

import { WEBAPP } from "./setup.mjs";

const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const { RoutePicker } = await import(`${WEBAPP}route-picker.js`);
const { sameCanonicalDevice, selfConnectionTargetState } = await import(
  `${WEBAPP}self-connection.js`
);

function receiver(capability) {
  return {
    name: "Renamed receiver",
    server_name: "receiver.local.",
    mac_address: "00:1d:c1:50:36:8b",
    online: true,
    channels: {
      receivers: { 1: { name: "Input", can_subscribe_self: capability } },
      transmitters: { 1: { name: "Own output" } },
    },
  };
}

test("canonical identity does not use display-name equality", () => {
  const original = receiver(false);
  const renamed = { ...original, name: "New name" };
  const sameName = {
    ...original,
    server_name: "other.local.",
    mac_address: "00:1d:c1:00:00:01",
  };

  assert.equal(sameCanonicalDevice(original, renamed), true);
  assert.equal(sameCanonicalDevice(original, sameName), false);
});

test("source picker disables only same-device sources for a blocked receiver channel", () => {
  const target = receiver(false);
  const remote = {
    name: "Remote source",
    server_name: "remote.local.",
    online: true,
    channels: { transmitters: { 1: { name: "Remote output" } } },
  };
  const markup = render(
    h(RoutePicker, {
      receiver: target,
      receiveChannelNumber: 1,
      onClose() {},
      sourceDevices: [target, remote],
    }),
  );

  assert.match(markup, /disabled[^>]*source-picker-entry self-unsupported/);
  assert.match(markup, /Self blocked/);
  assert.match(markup, /Remote output/);
  assert.equal(
    selfConnectionTargetState(target, target.channels.receivers[1], remote)
      .allowed,
    true,
  );

  target.channels.receivers[1].can_subscribe_self = true;
  const supported = render(
    h(RoutePicker, {
      receiver: target,
      receiveChannelNumber: 1,
      onClose() {},
      sourceDevices: [target],
    }),
  );
  assert.doesNotMatch(
    supported,
    /self-unsupported|self-unavailable|Self blocked/,
  );
  assert.doesNotMatch(supported, /disabled/);
});
