import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const { subscriptionsView } = await import(`${WEBAPP}views/subscriptions.js`);
const store = await import(`${WEBAPP}store.js`);
const { iconPaths } = await import(`${WEBAPP}icon-paths.js`);

test("subscriptions show severity icons and expand without a table height limit", () => {
  const previous = store.devices.value;
  try {
    store.devices.value = { "test.local.": { name: "test", server_name: "test.local.", online: true,
      subscriptions: ["ok", "warning", "error"].map((severity, index) => ({
        rx_channel: String(index), tx_device: "source", tx_channel: "one", status: { severity, label: severity },
      })),
    } };
    const markup = render(h(subscriptionsView.component));
    assert.match(markup, /^<div class="flex flex-col gap-4 pb-6">/);
    for (const name of ["subscription-ok", "warning", "subscription-blocked"]) {
      assert.ok(iconPaths[name]);
      assert.ok(markup.includes(iconPaths[name]), name);
    }
    assert.match(markup, /class="table-wrapper">/);
    for (const tone of ["good", "warn", "bad"]) assert.ok(markup.includes(`state-${tone}`));
  } finally {
    store.devices.value = previous;
  }
});
