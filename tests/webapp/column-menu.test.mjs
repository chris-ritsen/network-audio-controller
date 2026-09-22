import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const { ConfigurableTable } = await import(`${WEBAPP}table.js`);

test("column chooser is alphabetical independently of saved table order", () => {
  const columns = [
    { id: "z", label: "Zebra" },
    { id: "b", label: "beta" },
    { id: "a", label: "Alpha" },
    { id: "fixed", label: "Fixed", configurable: false },
  ];
  const saved = JSON.stringify({ order: ["z", "a", "b", "fixed"], hidden: ["b"] });
  localStorage.setItem("netaudio.columns.chooser-test", saved);
  try {
    const markup = render(h(ConfigurableTable, { columns, rows: [], rowKey: (row) => row.id, tableId: "chooser-test" }));
    const target = markup.match(/popovertarget="([^"]+)"/)[1];
    assert.match(markup, /popovertargetaction="toggle"/);
    assert.ok(markup.includes(`id="${target}" popover="auto"`));
    const options = [...markup.matchAll(/<label class="column-option">([\s\S]*?)<\/label>/g)];
    assert.deepEqual(options.map((match) => match[1].match(/<span>(.*?)<\/span>/)[1]), ["Alpha", "beta", "Zebra"]);
    assert.doesNotMatch(options[1][1], /checked/);
    const headings = markup.match(/<thead>([\s\S]*?)<\/thead>/)[1];
    assert.ok(headings.indexOf("Zebra") < headings.indexOf("Alpha"));
    assert.doesNotMatch(headings, /beta/);
    assert.equal(localStorage.getItem("netaudio.columns.chooser-test"), saved);
    assert.deepEqual(columns.map((column) => column.id), ["z", "b", "a", "fixed"]);
  } finally {
    localStorage.removeItem("netaudio.columns.chooser-test");
  }
});
