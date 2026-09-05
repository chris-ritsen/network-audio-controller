import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { WEBAPP } from "./setup.mjs";

const stylesheet = readFileSync(new URL("app.css", WEBAPP), "utf8");

function ruleFor(selector) {
  const bodies = [];
  for (const match of stylesheet.matchAll(/(^|\n)([^{}@]+)\{([^}]*)\}/g)) {
    const selectors = match[2].split(",").map((entry) => entry.trim());
    if (selectors.includes(selector)) {
      bodies.push(match[3]);
    }
  }
  return bodies.length ? bodies.join("\n") : null;
}

const REQUIRED_RULES = {
  ".btn": ["cursor"],
  ".content": ["overflow"],
  ".device-list": ["gap"],
  ".fields": ["grid-template-columns"],
  ".matrix-canvas": ["position: absolute"],
  ".matrix-shell": ["border"],
  ".matrix-stage": ["position: relative"],
  ".matrix-status": ["display"],
  ".matrix-viewport": ["overflow: auto", "height", "position: relative"],
  ".meter-bank": ["overflow-x"],
  ".nav-list": ["gap"],
  ".panel": ["border"],
  ".sidebar": ["grid-area"],
  ".table-wrapper": ["overflow"],
  ".toast-stack": ["position: fixed"],
  ".topbar": ["grid-area"],
  "dialog.palette": ["margin"],
  "table.data th": ["position: sticky"],
};

for (const [selector, declarations] of Object.entries(REQUIRED_RULES)) {
  test(`stylesheet defines ${selector}`, () => {
    const body = ruleFor(selector);
    assert.ok(body !== null, `missing rule for ${selector}`);
    for (const declaration of declarations) {
      assert.ok(body.includes(declaration), `${selector} lacks ${declaration}`);
    }
  });
}

test("no text is dimmed or truncated", () => {
  assert.doesNotMatch(stylesheet, /text-overflow:\s*ellipsis/);
  assert.doesNotMatch(stylesheet, /\n\s*opacity:\s*0?\.\d/);
});

test("no press transform moves buttons", () => {
  const active = ruleFor(".btn:active");
  assert.ok(active !== null);
  assert.doesNotMatch(active, /transform/);
});

test("mobile layout collapses the sidebar into a drawer", () => {
  assert.match(stylesheet, /@media \(max-width: 900px\)[\s\S]*\.sidebar\.open \{[^}]*transform: translateX\(0\)/);
});

test("every static class used by the application has a stylesheet rule", () => {
  const root = new URL(WEBAPP).pathname;
  const files = [];
  const walk = (directory) => {
    for (const entry of readdirSync(directory)) {
      const full = join(directory, entry);
      if (statSync(full).isDirectory()) {
        if (entry !== "vendor") {
          walk(full);
        }
      } else if (entry.endsWith(".js")) {
        files.push(full);
      }
    }
  };
  walk(root);
  const defined = new Set([...stylesheet.matchAll(/\.([a-zA-Z][\w-]*)/g)].map((match) => match[1]));
  const dynamic = new Set(["good", "warn", "bad", "accent", "ok", "warning", "error", "pending", "open", "closed", "connecting", "info", "success", "state-"]);
  const missing = new Set();
  for (const file of files) {
    const text = readFileSync(file, "utf8");
    for (const match of text.matchAll(/class=(?:"|\$\{`|`)([^"`$]*)/g)) {
      for (const token of match[1].split(/\s+/)) {
        if (/^[a-zA-Z][\w-]*$/.test(token) && !defined.has(token) && !dynamic.has(token)) {
          missing.add(`${token} (${file.slice(root.length)})`);
        }
      }
    }
  }
  assert.deepEqual([...missing].sort(), []);
});
