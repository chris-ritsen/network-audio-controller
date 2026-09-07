import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { sortRows } = await import(`${WEBAPP}table-sort.js`);
const { html } = await import(`${WEBAPP}lib/preact.js`);
const { Value } = await import(`${WEBAPP}components.js`);

test("numeric cells sort numerically, keep missing values last, and leave input untouched", () => {
  const rows = [{ n: 128 }, { n: null }, { n: 2 }, { n: 64 }];
  const column = { cell: (row) => html`<${Value} value=${row.n} />` };
  assert.deepEqual(sortRows(rows, column, "ascending").map((row) => row.n), [2, 64, 128, null]);
  assert.deepEqual(sortRows(rows, column, "descending").map((row) => row.n), [128, 64, 2, null]);
  assert.deepEqual(rows.map((row) => row.n), [128, null, 2, 64]);
});

test("names sort naturally and ties preserve inventory order", () => {
  const rows = [{ id: 1, name: "Device 10" }, { id: 2, name: "device 2" }, { id: 3, name: "Device 2" }];
  const column = { cell: (row) => row.name };
  assert.deepEqual(sortRows(rows, column, "ascending").map((row) => row.id), [2, 3, 1]);
  assert.deepEqual(sortRows(rows, column, "descending").map((row) => row.id), [1, 2, 3]);
});
