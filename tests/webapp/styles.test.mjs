import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import prettier from "prettier";

const root = new URL("../../scripts/webapp/", import.meta.url);

test("component styles are imported, valid and written as multiline rules", async () => {
  const entry = readFileSync(new URL("styles.css", root), "utf8");
  const imports = [...entry.matchAll(/@import "\.\/([^\"]+)"/g)].map((match) => match[1]);
  for (const name of ["layout.css", "tables.css", "routing.css", "filters.css", "tooltips.css", "channels.css"]) assert.ok(imports.includes(name));
  for (const name of imports) {
    const source = readFileSync(new URL(name, root), "utf8");
    const { ast } = await prettier.__debug.parse(source, { parser: "css" });
    function check(nodes) {
      for (const node of nodes) {
        if (node.nodes) {
          assert.ok(node.source.end.line > node.source.start.line, `${name}: multiline block required`);
          const lines = node.nodes.filter((child) => child.type === "css-decl").map((child) => child.source.start.line);
          assert.equal(new Set(lines).size, lines.length, `${name}: one declaration per line`);
          check(node.nodes);
        }
      }
    }
    check(ast.nodes);
  }
  const layout = readFileSync(new URL("layout.css", root), "utf8");
  assert.doesNotMatch(layout, /\.table-wrapper|\.matrix-tooltip|\.routing-filter|\.channel-name/);
});

test("table header has one separator instead of a border plus inset line", () => {
  const source = readFileSync(new URL("tables.css", root), "utf8");
  const header = source.match(/table\.data th \{([^}]+)\}/)[1];
  assert.match(header, /box-shadow: inset 0 -1px 0 var\(--line-strong\)/);
  assert.doesNotMatch(header, /border-bottom:/);
});
