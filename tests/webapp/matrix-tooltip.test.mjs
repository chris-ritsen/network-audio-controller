import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { tooltipPosition } = await import(`${WEBAPP}matrix-tooltip.js`);

test("matrix tooltip follows the pointer and stays inside the matrix edges", () => {
  const bounds = { width: 800, height: 500 };
  const tooltip = { width: 400, height: 90 };
  assert.deepEqual(tooltipPosition({ x: 100, y: 100 }, bounds, tooltip), { left: 116, top: 116 });
  assert.deepEqual(tooltipPosition({ x: 790, y: 490 }, bounds, tooltip), { left: 388, top: 384 });
  for (const point of [{ x: 0, y: 0 }, { x: 400, y: 480 }, { x: 799, y: 0 }]) {
    const result = tooltipPosition(point, bounds, tooltip);
    assert.ok(result.left >= 12 && result.top >= 12);
    assert.ok(result.left + tooltip.width <= bounds.width - 12);
    assert.ok(result.top + tooltip.height <= bounds.height - 12);
  }
});
