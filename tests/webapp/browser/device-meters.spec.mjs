import { test, expect } from "@playwright/test";
import { deviceFixture, serveWebapp } from "./fixture.mjs";

test("Metering tab fits mobile and releases monitoring when leaving the tab", async ({ page }) => {
  const device = Object.values(deviceFixture).find((entry) => entry.name === "avio-bt-1");
  await serveWebapp(page, { devices: { [device.server_name]: device }, metering: { [device.server_name]: { wall_time: Date.now() / 1000, tx: { 1: 30, 2: 254 }, rx: { 1: 20 } } } });
  const writes = [];
  await page.route("**/metering/*", (route) => {
    if (route.request().method() !== "POST") return route.fallback();
    writes.push({ path: new URL(route.request().url()).pathname, body: route.request().postDataJSON() });
    return route.fulfill({ contentType: "application/json", body: "{}" });
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("http://netaudio.test/devices/avio-bt-1/metering");
  await expect.poll(() => writes.length).toBe(1);
  const meters = page.locator("#content .split").filter({ has: page.getByRole("heading", { name: "Receive levels", exact: true }) });
  await expect(meters).toBeVisible();
  const box = await meters.boundingBox();
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(390);
  expect(await meters.evaluate((node) => node.scrollWidth <= node.clientWidth + 1)).toBe(true);
  await page.screenshot({ path: `test-results/device-metering-tab-${test.info().project.name}.png` });
  await page.getByRole("combobox", { name: "Device section" }).selectOption("status");
  await expect(meters).toHaveCount(0);
  await expect.poll(() => writes.length).toBe(2);
  expect(writes[1]).toEqual({ path: "/metering/stop", body: writes[0].body });
  expect(await page.evaluate(async () => {
    const { resolve } = await import("/router.js");
    return [resolve("/metering", "").found, resolve("/metering/avio-bt-1", "").found];
  })).toEqual([false, false]);
});

test("breadcrumbs reuse display labels without changing device names", async ({ page }) => {
  await serveWebapp(page);
  for (const [section, label] of [["receive", "Receive"], ["metering", "Metering"], ["aes67-config", "AES67 config"], ["device-config", "Device config"], ["lock", "Device lock"]]) {
    await page.goto(`http://netaudio.test/devices/Windows-PC/${section}`);
    const breadcrumb = page.getByRole("navigation", { name: "Breadcrumb" });
    await expect(breadcrumb.locator('[aria-current="page"]')).toHaveText(label);
    await expect(breadcrumb.getByRole("link", { name: "Windows-PC", exact: true })).toBeVisible();
  }
});
