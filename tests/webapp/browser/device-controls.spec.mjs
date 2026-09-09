import { test, expect } from "@playwright/test";
import { deviceFixture, serveWebapp } from "./fixture.mjs";

test("flow pages, device controls and diagnostic fields are absent", async ({ page }) => {
  await serveWebapp(page);
  const requests = [];
  page.on("request", (request) => requests.push(new URL(request.url()).pathname));
  for (const path of ["/devices", "/devices/avio-bt-1/receive", "/devices/avio-bt-1/status", "/devices/avio-bt-1/network-config"]) {
    await page.goto(`http://netaudio.test${path}`);
    await expect(page.getByRole("link", { name: /flows/i })).toHaveCount(0);
    await expect(page.locator("#content")).not.toContainText(/\bflows?\b/i);
  }
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await expect(page.getByText("Flows", { exact: true })).toHaveCount(0);
  expect(requests.some((path) => path.startsWith("/flows"))).toBe(false);
  expect(await page.evaluate(async () => {
    const { resolve } = await import("/router.js");
    return [resolve("/flows", "").found, resolve("/flows/avio-bt-1", "").found];
  })).toEqual([false, false]);
});

test("network views have focused defaults while every inventory column remains available", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/devices");
  const navigation = page.getByRole("navigation", { name: "Network views" });
  for (const name of ["Routing", "Device Info", "Clock Status", "Network Status"]) {
    await expect(navigation.getByRole("link", { name, exact: true })).toBeVisible();
  }
  await page.getByRole("button", { name: /^Columns/ }).click();
  const menu = page.locator(".column-menu");
  const labels = await menu.locator(".column-option span").allTextContents();
  expect(labels.every((label) => label.trim().length > 0)).toBe(true);
  expect(labels).not.toContain("Dismiss");
  expect(new Set(labels).size).toBe(labels.length);
  for (const label of ["Clock source", "Secondary link speed", "Gateway"]) {
    await menu.getByRole("checkbox", { name: label, exact: true }).check();
    await expect(page.getByRole("columnheader", { name: label, exact: true })).toBeVisible();
  }
  await page.keyboard.press("Escape");
  await navigation.getByRole("link", { name: "Clock Status", exact: true }).click();
  await expect(page.getByRole("columnheader", { name: "Clock role", exact: true })).toBeVisible();
  await expect(page.getByRole("columnheader", { name: "Primary address", exact: true })).toHaveCount(0);
  await navigation.getByRole("link", { name: "Network Status", exact: true }).click();
  await expect(page.getByRole("columnheader", { name: "Primary address", exact: true })).toBeVisible();
  await expect(page.getByRole("columnheader", { name: "Clock role", exact: true })).toHaveCount(0);
});

test("latency is in device config and status contains no inventory actions", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/devices/avio-bt-1/device-config");
  await expect(page.locator(".tabs").getByRole("link", { name: "Latency", exact: true })).toHaveCount(0);
  await expect(page.locator("#content")).toContainText("Latency");
  await page.goto("http://netaudio.test/devices/avio-bt-1/status");
  await expect(page.locator("#content")).not.toContainText("Inventory");
  await expect(page.getByRole("button", { name: "Report unresponsive" })).toHaveCount(0);
});

test("metering omits timestamp and source address metadata", async ({ page }) => {
  const [id, device] = Object.entries(deviceFixture).find(([, record]) => record.online);
  await serveWebapp(page, { metering: { [id]: { wall_time: Date.now() / 1000, source_ip: "192.0.2.34", source_port: 1026 } } });
  await page.goto(`http://netaudio.test/devices/${encodeURIComponent(device.name)}/metering`);
  const updated = page.getByText(/^Last updated /);
  const source = page.getByText("Source address 192.0.2.34:1026", { exact: true });
  await expect(updated).toHaveCount(0);
  await expect(source).toHaveCount(0);
  await expect(page.getByText("Receive levels", { exact: true })).toBeVisible();
  await expect(page.getByText("Transmit levels", { exact: true })).toBeVisible();
  await expect(page.locator("#content")).not.toContainText("last sample");
});

test("lock controls show relevant state without internal diagnostics and clear the PIN", async ({ page }) => {
  await serveWebapp(page);
  let locked = false;
  const writes = [];
  await page.route("**/lock-status/**", (route) => route.fulfill({ contentType: "application/json", body: JSON.stringify({
    is_locked: locked, observed_at: "2026-09-05T19:35:15Z", observation_source: "observed_after_0x1008",
  }) }));
  await page.route("**/lock", (route) => {
    if (route.request().method() !== "POST") return route.fallback();
    writes.push(route.request().postDataJSON()); locked = true;
    return route.fulfill({ contentType: "application/json", body: '{"success":true}' });
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("http://netaudio.test/devices/avio-bt-1/lock");
  await expect(page.getByRole("status").filter({ hasText: "Unlocked" })).toBeVisible();
  const lock = page.getByRole("button", { name: "Lock", exact: true });
  await expect(lock).toBeDisabled();
  await page.getByLabel("Device PIN", { exact: true }).fill("1234");
  await lock.click();
  await expect(page.getByRole("button", { name: "Unlock", exact: true })).toBeDisabled();
  await expect(page.getByLabel("Device PIN", { exact: true })).toHaveValue("");
  expect(writes).toHaveLength(1);
  for (const text of ["Lock reset status", "Clear configuration status", "Diagnostic log export supported", "Observation source", "Probed lock state", "0x1008"]) {
    await expect(page.locator("#content")).not.toContainText(text);
  }
  await page.screenshot({ path: "test-results/mobile-device-lock.png" });
});

test("an unknown lock state is not presented as unlocked", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/devices/Windows-PC/lock");
  await expect(page.getByText("Lock state unavailable", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Lock", exact: true })).toBeDisabled();
});

test("receivers show subscription icons on desktop and collapsed mobile cards", async ({ page }) => {
  const inventory = structuredClone(deviceFixture);
  const device = Object.values(inventory).find((entry) => entry.name === "Windows-PC");
  device.subscriptions[0].status = { state: "connected", severity: "ok", label: "DYNAMIC", detail: "Active subscription to an automatically configured source flow" };
  device.subscriptions[1].status = { state: "unresolved", severity: "warning" };
  device.subscriptions[2].status = { state: "uncharacterized", severity: "error", label: "Error 0x1234" };
  await serveWebapp(page, { devices: inventory, metering: { [device.server_name]: {
    wall_time: Date.now() / 1000, metering_source: "signal_presence",
    rx_signal_presence: { 1: "signal_present", 2: "below_threshold", 3: "clipping", 4: "muted" },
  } } });
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto("http://netaudio.test/devices/Windows-PC/receive");
  const rows = page.locator("table.data tbody tr");
  await expect(rows.nth(0).getByRole("img", { name: "Subscribed", exact: true })).toBeVisible();
  await expect(rows.nth(1).getByRole("img", { name: "Subscription warning", exact: true })).toBeVisible();
  await expect(rows.nth(2).getByRole("img", { name: "Subscription failed", exact: true })).toBeVisible();
  await expect(rows.nth(3).getByRole("img", { name: "Subscribed", exact: true })).toBeVisible();
  await expect(page.locator("#content")).not.toContainText("0x1234");
  await expect(page.locator("#content")).not.toContainText("DYNAMIC");
  await expect(rows.nth(0).locator(".receiver-subscription:visible")).toHaveText("");
  await expect(rows.nth(0).locator(".receiver-subscription:visible")).toHaveAttribute("title", /Subscription successful$/);
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("dialog", { name: "Application navigation" })).toBeHidden();
  await expect(rows.nth(0).getByRole("button")).toHaveAttribute("aria-expanded", "false");
  await expect(rows.nth(0).getByRole("img", { name: "Subscribed", exact: true })).toBeVisible();
  await expect(rows.nth(3).getByRole("img", { name: "Subscribed", exact: true })).toBeVisible();
  await page.screenshot({ path: "test-results/mobile-receiver-indicators.png" });
  await expect(page.locator(".signal-presence")).toHaveCount(0);
  expect(writes).toEqual([]);
});

test("header controls are labeled and desktop sorting never shows the mobile direction control", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/devices");
  for (const label of ["Show navigation", "Search"]) {
    const control = page.getByRole("button", { name: label, exact: true });
    await expect(control).toBeVisible();
    expect(await control.innerText()).toBe("");
  }
  await page.getByRole("columnheader", { name: "Name", exact: true }).getByRole("button").click();
  await expect(page.locator(".mobile-table-order")).toBeHidden();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.locator(".mobile-table-order")).toBeVisible();
  const labels = await page.getByRole("combobox", { name: "Sort by", exact: true }).locator('option:not([value=""])').allTextContents();
  expect(labels).toEqual([...labels].sort((first, second) => first.localeCompare(second, undefined, { numeric: true, sensitivity: "base" })));
  const card = page.locator(".expandable-row").first();
  expect(await card.evaluate((node) => getComputedStyle(node).overflow)).toBe("hidden");
});
