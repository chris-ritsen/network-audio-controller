import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const { settingsView } = await import(`${WEBAPP}views/settings.js`);

test("Metering heading has no empty icon or extra spacing wrapper", () => {
  const markup = render(h(settingsView.component));
  assert.match(markup, /<h2[^>]*>Metering<\/h2>/);
});
