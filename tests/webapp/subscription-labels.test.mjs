import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { subscriptionStatusText } = await import(`${WEBAPP}format.js`);
const { subscriptionTone } = await import(`${WEBAPP}format.js`);

test("source not yet found is a warning while other errors retain their severity", () => {
  assert.equal(subscriptionTone({ status: { status: "UNRESOLVED", severity: "error" } }), "warn");
  assert.equal(subscriptionTone({ status: { label: "UNRESOLVED", severity: "error" } }), "warn");
  for (const status of ["RESOLVE_FAIL", "RESOLVED_NONE", "CHANNEL_FORMAT", "NO_CONNECTION"]) {
    assert.equal(subscriptionTone({ status: { status, severity: "error" } }), "bad");
  }
});

test("connected labels describe transport and loopback instead of API identifiers", () => {
  for (const [status, expected] of [
    ["DYNAMIC", "Connected — unicast"], ["STATIC", "Connected — multicast"],
    ["SUBSCRIBE_SELF", "Connected — local loopback"], ["CONNECTED", "Connected"],
    ["MANUAL", "Connected — manually configured"],
  ]) {
    assert.equal(subscriptionStatusText({ status: { status, label: status, state: "connected", severity: "ok",
      detail: "Active subscription to a configured source flow" } }), expected);
  }
});

test("failure and warning descriptions retain their meaning without enum prefixes", () => {
  for (const [status, severity, detail, expected] of [
    ["UNRESOLVED", "error", "Error: Channel Name not yet found on network", "Channel Name not yet found on network"],
    ["TX_NOT_READY", "warning", "Warning: There is an external issue with Tx Channel", "There is an external issue with Tx Channel"],
    ["CHANNEL_FORMAT", "error", "Error: Channel formats do not match", "Channel formats do not match"],
  ]) {
    assert.equal(subscriptionStatusText({ status: { status, label: status, severity, detail } }), expected);
  }
  assert.equal(subscriptionStatusText({ status: { status: "NONE", state: "none" } }), "Not subscribed");
  assert.equal(subscriptionStatusText({ status: { label: "UNKNOWN_ENUM", detail: "Status 0xabcd" } }), "Subscription status unavailable");
  assert.equal(subscriptionStatusText({ status: { status: "STATIC", state: "unresolved", severity: "error", detail: "Source unavailable" } }), "Source unavailable");
});
