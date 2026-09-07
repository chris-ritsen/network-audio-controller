import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { subscriptionIndicator } = await import(`${WEBAPP}device/receiver-status.js`);
const { signalIndicator } = await import(`${WEBAPP}signal-presence.js`);

test("connected transport icons distinguish dynamic and static subscriptions without guessing other states", async () => {
  const { h } = await import("preact");
  const { render } = await import("preact-render-to-string");
  const { SubscriptionTransport } = await import(`${WEBAPP}device/receiver-status.js`);
  for (const [status, label] of [["DYNAMIC", "Unicast"], ["STATIC", "Multicast"]]) {
    const markup = render(h(SubscriptionTransport, { subscription: { status: { status, state: "connected", severity: "ok" } } }));
    assert.ok(markup.includes(`aria-label="${label}"`));
    assert.match(markup, /<svg/);
  }
  for (const status of [undefined, "SUBSCRIBE_SELF", "CONNECTED", "UNRESOLVED"]) {
    const markup = render(h(SubscriptionTransport, { subscription: { status: { status, state: "connected", severity: "ok" } } }));
    assert.match(markup, /size-5 shrink-0/);
    assert.match(markup, /aria-hidden="true"/);
    assert.doesNotMatch(markup, /<svg/);
  }
  assert.doesNotMatch(render(h(SubscriptionTransport, { subscription: { status: { status: "STATIC", state: "unresolved", severity: "error" } } })), /<svg/);
});

test("receiver status uses icons and readable tooltips without enum labels", async () => {
  const { h } = await import("preact");
  const { render } = await import("preact-render-to-string");
  const { SubscriptionStatus } = await import(`${WEBAPP}device/receiver-status.js`);
  for (const [label, detail] of [
    ["DYNAMIC", "Active subscription to an automatically configured source flow"],
    ["STATIC", "Active subscription to a manually configured source flow"],
    ["SUBSCRIBE_SELF", "Channel is successfully subscribed to own TX channels (local loopback mode)"],
    ["UNRESOLVED", "Error: Channel Name not yet found on network"],
  ]) {
    const markup = render(h(SubscriptionStatus, { subscription: { status: { label, detail, state: "connected", severity: "ok" } } }));
    assert.doesNotMatch(markup, new RegExp(label));
    assert.ok(markup.includes('title="Subscription successful"'));
    assert.doesNotMatch(markup, /<span>[^<]+<\/span>/);
  }
  const failure = render(h(SubscriptionStatus, { subscription: { status: { label: "UNRESOLVED", detail: "Error: Channel Name not yet found on network", state: "unresolved", severity: "error" } } }));
  assert.ok(failure.includes('title="Error: Channel Name not yet found on network"'));
  assert.doesNotMatch(failure, /UNRESOLVED/);
  const unknown = render(h(SubscriptionStatus, { subscription: { status: { label: "Unknown 0x1234", detail: "Status 0xabcd" } } }));
  assert.doesNotMatch(unknown, /0x1234|0xabcd/);
});

test("receiver indicators distinguish connection success, failure, pending and unavailable", () => {
  const cases = [
    [null, "Not subscribed"],
    [{ tx_device: "Source" }, "Subscription status unavailable"],
    [{ status: { state: "connected", severity: "ok" } }, "Subscribed"],
    [{ status: { state: "unresolved", severity: "none" } }, "Subscription pending"],
    [{ status: { severity: "warning" } }, "Subscription warning"],
    [{ status: { severity: "error" } }, "Subscription failed"],
    [{ status: { status: "UNRESOLVED", state: "unresolved", severity: "error" } }, "Subscription warning"],
    [{ status: { state: "uncharacterized", label: "Unknown 0x1234" } }, "Subscription status unavailable"],
  ];
  for (const [subscription, label] of cases) assert.equal(subscriptionIndicator(subscription).label, label);
});

test("signal indicators use reported presence and expire stale samples without claiming silence", () => {
  const now = 100_000;
  for (const [presence, label] of [
    ["signal_present", "Signal present"], ["below_threshold", "No signal"],
    ["muted", "Muted"], ["clipping", "Clipping"], ["unknown", "Signal unavailable"],
  ]) {
    const values = { wall_time: 99, rx_signal_presence: { 1: presence } };
    assert.equal(signalIndicator(values, 1, now).label, label);
    assert.equal(signalIndicator(values, 2, now).label, "Signal unavailable");
    assert.equal(signalIndicator(values, 1, now + 6000).label, "Signal unavailable");
  }
  assert.equal(signalIndicator(null, 1, now).label, "Signal unavailable");
  assert.equal(signalIndicator({ wall_time: 99, rx_updated_at: { 1: 90 }, rx: { 1: 20 } }, 1, now).label, "Signal unavailable");
});

test("detailed level samples provide signal indication without exposing raw values", () => {
  const now = 100_000;
  for (const [raw, label] of [[20, "Signal present"], [253, "No signal"], [0, "Clipping"], [254, "Muted"], [255, "Signal unavailable"], [null, "Signal unavailable"]]) {
    assert.equal(signalIndicator({ wall_time: 99, rx: { 1: raw } }, 1, now).label, label);
  }
});

test("receive and transmit preserve every detailed half-decibel step", () => {
  for (const direction of ["rx", "tx"]) {
    let previous = Infinity;
    for (let raw = 1; raw <= 253; raw++) {
      const result = signalIndicator({ wall_time: 99, [direction]: { 1: raw } }, 1, 100_000, direction);
      assert.equal(result.dbfs, raw === 1 ? 0 : -(raw - 1) / 2);
      assert.ok(result.level < previous);
      assert.ok(result.level >= 0 && result.level <= 1);
      previous = result.level;
    }
    for (const [raw, state] of [[0, "clipping"], [254, "muted"], [255, "unknown"]]) {
      assert.equal(signalIndicator({ wall_time: 99, [direction]: { 1: raw } }, 1, 100_000, direction).state, state);
    }
  }
  assert.equal(signalIndicator({ wall_time: 99, tx_updated_at: { 1: 90 }, tx: { 1: 20 } }, 1, 100_000, "tx").state, "unknown");
});

test("partial streamed updates do not freshen untouched receive or transmit channels", async () => {
  const store = await import(`${WEBAPP}store.js`);
  const previous = globalThis.EventSource;
  const handlers = new Map();
  globalThis.EventSource = class {
    addEventListener(name, callback) { handlers.set(name, callback); }
  };
  try {
    store.connect();
    const send = (sample) => handlers.get("message")({ data: JSON.stringify({ event: "meter_values", server_name: "signal-test.local.", metering_source: "detailed", ...sample }) });
    send({ wall_time: 90, rx: { 1: 20, 2: 40 }, tx: { 1: 60, 2: 80 } });
    send({ wall_time: 99, rx: { 2: 22 }, tx: { 2: 24 } });
    const values = store.meterValuesFor("signal-test.local.");
    for (const direction of ["rx", "tx"]) {
      assert.equal(signalIndicator(values, 1, 100_000, direction).state, "unknown");
      assert.equal(signalIndicator(values, 2, 100_000, direction).state, "present");
    }
  } finally {
    globalThis.EventSource = previous;
    store.meterCache.delete("signal-test.local.");
  }
});
