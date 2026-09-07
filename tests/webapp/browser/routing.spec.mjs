import { test, expect } from "@playwright/test";
import { readFile } from "node:fs/promises";

const root = new URL(
  "../../../packages/netaudio/src/netaudio/daemon/http/webapp/",
  import.meta.url,
);
const devices = JSON.parse(
  await readFile(new URL("../fixtures/devices.json", import.meta.url), "utf8"),
);

test.beforeEach(async ({ page }) => {
  await page.route("**/*", async (route) => {
    const url = new URL(route.request().url());
    if (url.hostname !== "netaudio.test") return route.abort();
    if (url.pathname === "/events" && route.request().resourceType() !== "document")
      return route.fulfill({
        contentType: "text/event-stream",
        body: `data: ${JSON.stringify({ event: "snapshot", devices })}\n\n`,
      });
    if (route.request().method() !== "GET")
      return route.fulfill({
        status: 503,
        contentType: "application/json",
        body: '{"error":"Test receiver unavailable"}',
      });
    const path =
      ["/routing", "/subscriptions", "/devices", "/events", "/shure"].includes(url.pathname) || url.pathname.startsWith("/devices/") ? "index.html" : url.pathname.slice(1);
    try {
      const body = await readFile(new URL(path, root));
      const contentType = path.endsWith(".js")
        ? "text/javascript"
        : path.endsWith(".css")
          ? "text/css"
          : path.endsWith(".svg")
            ? "image/svg+xml"
            : "text/html";
      return route.fulfill({ contentType, body });
    } catch {
      return route.fulfill({ status: 404, body: "Not found" });
    }
  });
});

test("phone views use the available width without horizontal scrolling", async ({ page }) => {
  const name = Object.values(devices).find((device) => device.online)?.name;
  for (const width of [320, 390]) {
    await page.setViewportSize({ width, height: 844 });
    for (const path of ["/devices", "/routing", "/events", "/shure", `/devices/${encodeURIComponent(name)}/receive`, `/devices/${encodeURIComponent(name)}/device-config`]) {
      await page.goto(`http://netaudio.test${path}`);
      await expect(page.locator("#content"), path).toBeVisible();
      const overflowing = await page.locator("#content, .topbar, .table-wrapper, .tabs, .card-body").evaluateAll((nodes) => nodes.filter((node) => node.clientWidth && node.scrollWidth > node.clientWidth + 1).map((node) => ({ class: node.className, width: node.clientWidth, scroll: node.scrollWidth })));
      expect(overflowing, `${width}px ${path}`).toEqual([]);
      expect(await page.evaluate(() => document.documentElement.scrollWidth), path).toBeLessThanOrEqual(width);
    }
  }
  await page.goto("http://netaudio.test/devices");
  const sort = page.getByRole("combobox", { name: "Sort by", exact: true });
  await expect(sort).toBeVisible();
  const firstOption = await sort.locator('option:not([value=""])').first().getAttribute("value");
  await sort.selectOption(firstOption);
  await expect(page.getByRole("button", { name: "Sort descending", exact: true })).toBeVisible();
  const firstCard = page.locator(".mobile-card-toggle").first();
  await expect(firstCard).toHaveAttribute("aria-expanded", "false");
  await firstCard.click();
  const firstCell = page.locator('table.data tbody td[data-label]:not(.mobile-card-heading)').first();
  await expect(firstCell).toBeVisible();
  expect(await firstCell.evaluate((node) => getComputedStyle(node, "::before").content)).not.toBe('""');
  await page.screenshot({ path: "test-results/phone-device-cards.png" });
});

test("routing reserves space for cells and moves subscription details to a separate page", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://netaudio.test/routing");
  await page.locator(".routing-options summary").click();
  await page.getByRole("button", { name: "Expand all devices and groups", exact: true }).click();
  await page.locator(".routing-options summary").click();
  await expect(page.getByRole("navigation", { name: "Network views" }).getByRole("link").first()).toHaveText("Routing");
  await expect(page.locator(".matrix-canvas")).toBeVisible();
  await expect(page.getByRole("heading", { name: /^Subscriptions \(/ })).toHaveCount(0);
  const geometry = await page.locator(".matrix-stage").evaluate((node) => {
    const viewport = node.querySelector(".matrix-viewport");
    return { height: node.clientHeight, gutter: Number(viewport.dataset.gutterWidth), header: Number(viewport.dataset.headerHeight) };
  });
  expect(geometry.gutter).toBeLessThanOrEqual(340);
  expect(geometry.header).toBeLessThanOrEqual(280);
  expect(geometry.height - geometry.header).toBeGreaterThan(350);
  await page.getByRole("button", { name: "Show navigation", exact: true }).click();
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "Subscriptions", exact: true }).click();
  await expect(page).toHaveURL("http://netaudio.test/subscriptions");
  await expect(page.getByRole("heading", { name: /^Subscriptions \(/ })).toBeVisible();
  await expect(page.locator(".matrix-canvas")).toHaveCount(0);
});

test("mobile device and channel cards expand independently and the section menu navigates", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto("http://netaudio.test/devices");
  const cards = page.locator(".mobile-card-toggle");
  await expect(cards).toHaveCount(Object.values(devices).filter((device) => device.online !== false).length);
  await expect(page.locator('.expandable-row > td:not(.mobile-card-heading):visible')).toHaveCount(0);
  expect((await cards.first().boundingBox()).height).toBeLessThanOrEqual(80);
  await page.screenshot({ path: "test-results/mobile-device-list.png" });
  await cards.first().focus();
  await page.keyboard.press("Enter");
  await expect(cards.first()).toHaveAttribute("aria-expanded", "true");
  await expect(cards.nth(1)).toHaveAttribute("aria-expanded", "false");
  await cards.first().click();
  await expect(cards.first()).toHaveAttribute("aria-expanded", "false");
  await page.goto("http://netaudio.test/devices/Windows-PC/receive");
  await expect(page.locator(".device-header .pill")).toHaveCount(0);
  await expect(page.getByRole("navigation", { name: "Device sections" })).toBeHidden();
  const menu = page.getByRole("combobox", { name: "Device section", exact: true });
  await expect(menu).toBeVisible();
  await expect(cards).toHaveCount(64);
  await expect(page.getByRole("button", { name: "Set", exact: true })).toHaveCount(0);
  await page.screenshot({ path: "test-results/mobile-receiver-cards.png" });
  await cards.first().click();
  await expect(page.getByRole("button", { name: "Set", exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "Edit receive channel 1 name", exact: true }).click();
  await expect(page.getByRole("textbox", { name: "Receive channel 1 name", exact: true })).toBeVisible();
  expect((await page.getByRole("button", { name: "Save", exact: true }).boundingBox()).height).toBe(32);
  await expect(cards.nth(1)).toHaveAttribute("aria-expanded", "false");
  await page.screenshot({ path: "test-results/mobile-receiver-expanded.png" });
  await menu.selectOption("transmit");
  await expect(page).toHaveURL(/\/transmit$/);
  await expect(menu).toHaveValue("transmit");
  await expect(cards.first()).toHaveAttribute("aria-expanded", "false");
  await page.goBack();
  await expect(menu).toHaveValue("receive");
  expect(writes).toEqual([]);
});

for (const width of [1200, 390]) {
  test(`offline dismissal controls are absent throughout the app at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    const offline = Object.values(devices).find((device) => device.online === false);
    for (const path of ["/devices", "/routing", `/devices/${encodeURIComponent(offline.name)}/receive`]) {
      await page.goto(`http://netaudio.test${path}`);
      await expect(page.locator("#content")).not.toBeEmpty();
      await expect(page.getByRole("button", { name: /dismiss/i })).toHaveCount(0);
      await expect(page.getByRole("columnheader", { name: /dismiss/i })).toHaveCount(0);
    }
  });
}

test("breadcrumbs share the wordmark text baseline", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://netaudio.test/devices/avio-bt-1/receive");
  const baselines = await page.locator(".brand-name, .breadcrumb a").evaluateAll((nodes) => nodes.map((node) => {
    const marker = document.createElement("span");
    marker.style.display = "inline-block";
    marker.style.width = "0";
    marker.style.height = "0";
    marker.style.verticalAlign = "baseline";
    node.append(marker);
    const baseline = marker.getBoundingClientRect().bottom;
    marker.remove();
    return baseline;
  }));
  expect(baselines.length).toBeGreaterThan(1);
  for (const baseline of baselines) expect(Math.abs(baseline - baselines[0])).toBeLessThan(1);
});

test("search is compact on mobile and dismisses only when clicked outside", async ({ page }) => {
  await page.goto("http://netaudio.test/routing");
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 });
    const search = page.getByRole("button", { name: "Search", exact: true });
    await expect(search.locator("span")).toHaveCount(0);
    expect((await search.boundingBox()).width).toBe(32);
    await search.click();
    const dialog = page.getByRole("dialog", { name: "Search", exact: true });
    await expect(dialog).toBeVisible();
    await dialog.locator("input").click();
    await expect(dialog).toBeVisible();
    await page.mouse.click(2, 2);
    await expect(dialog).not.toBeVisible();
    await search.click();
    await expect(dialog).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  }
});

test("routing filter search has a visible gap below its label", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://netaudio.test/routing");
  for (const name of ["Search devices"]) {
    const input = page.getByRole("searchbox", { name, exact: true });
    await expect(input).toBeVisible();
    const gap = await input.evaluate((node) => {
      const label = node.closest("label");
      const text = [...label.childNodes].find((child) => child.nodeType === Node.TEXT_NODE && child.textContent.trim());
      const range = document.createRange();
      range.selectNodeContents(text);
      return node.getBoundingClientRect().top - range.getBoundingClientRect().bottom;
    });
    expect(gap).toBeGreaterThanOrEqual(5);
  }
});

test("tools menu scrolls when its navigation exceeds the available height", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://netaudio.test/routing");
  await page.getByRole("button", { name: "Show navigation", exact: true }).click();
  const sidebar = page.getByRole("dialog", { name: "Application navigation" });
  await expect(sidebar).toBeVisible();
  const navigation = page.getByRole("navigation", { name: "Main navigation" });
  await expect(navigation.getByRole("link", { name: "Device Info", exact: true })).toHaveCount(0);
  await expect(navigation.getByRole("link", { name: "Shure", exact: true })).toHaveText("Shure");
  expect(await sidebar.evaluate((node) => node.scrollHeight <= node.clientHeight)).toBe(true);
  expect(await sidebar.locator("nav").evaluate((node) => node.scrollHeight <= node.clientHeight)).toBe(true);
  await page.setViewportSize({ width: 1440, height: 200 });
  expect(await sidebar.evaluate((node) => node.scrollHeight > node.clientHeight)).toBe(true);
  await sidebar.evaluate((node) => { node.scrollTop = node.scrollHeight; });
  expect(await sidebar.evaluate((node) => node.scrollTop)).toBeGreaterThan(0);
  const lastLink = sidebar.getByRole("link").last();
  const panelBox = await sidebar.boundingBox();
  const linkBox = await lastLink.boundingBox();
  expect(linkBox.y + linkBox.height).toBeLessThanOrEqual(panelBox.y + panelBox.height);
});

test("tools menu labels fit without truncation", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://netaudio.test/routing");
  await page.getByRole("button", { name: "Show navigation", exact: true }).click();
  const nav = page.getByRole("navigation", { name: "Main navigation" });
  const measure = () => nav.locator("a").evaluateAll((links) => links.map((link) => {
    const style = getComputedStyle(link);
    return {
      width: link.getBoundingClientRect().width,
      needed: [...link.children].reduce((total, child) => total + child.getBoundingClientRect().width, 0)
        + parseFloat(style.columnGap) + parseFloat(style.paddingLeft) + parseFloat(style.paddingRight)
        + parseFloat(style.borderLeftWidth) + parseFloat(style.borderRightWidth),
    };
  }));
  const before = await measure();
  for (const link of before) expect(link.width).toBeGreaterThanOrEqual(link.needed);
});

test("tools menu closes on Escape and reload without moving the workspace", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://netaudio.test/routing");
  const collapsed = await page.locator("#content").boundingBox();
  await expect(page.getByRole("dialog", { name: "Application navigation" })).toBeHidden();
  await expect(page.getByRole("button", { name: "Show navigation", exact: true })).toHaveAttribute("aria-expanded", "false");
  expect((await page.locator("#content").boundingBox()).x).toBe(collapsed.x);
  expect((await page.locator("#content").boundingBox()).width).toBe(collapsed.width);
  await page.getByRole("button", { name: "Show navigation", exact: true }).click();
  await expect(page.getByRole("dialog", { name: "Application navigation" })).toBeVisible();
  expect(await page.locator("#content").boundingBox()).toEqual(collapsed);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "Show navigation", exact: true })).toBeFocused();
  await page.getByRole("button", { name: "Show navigation", exact: true }).click();
  await page.reload();
  await expect(page.getByRole("dialog", { name: "Application navigation" })).toBeHidden();
  expect(await page.locator("#content").boundingBox()).toEqual(collapsed);
});

test("each axis expands and collapses all devices and their groups together and persists", async ({ page }) => {
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.setViewportSize({ width: 1600, height: 1100 });
  await page.addInitScript(() => localStorage.setItem("netaudio.matrix.flipped", "false"));
  await page.goto("http://netaudio.test/routing");
  const dimensions = () => page.locator(".matrix-spacer").evaluate((node) => {
    const viewport = node.closest(".matrix-viewport");
    return {
      width: parseFloat(node.style.width) - Number(viewport.dataset.gutterWidth),
      height: parseFloat(node.style.height) - Number(viewport.dataset.headerHeight),
    };
  });
  await page.getByRole("button", { name: "Collapse all receiver devices and groups", exact: true }).click();
  await page.getByRole("button", { name: "Collapse all transmitter devices and groups", exact: true }).click();
  const initial = await dimensions();
  await page.getByRole("button", { name: "Expand all receiver devices and groups", exact: true }).click();
  await expect.poll(async () => (await dimensions()).height).toBeGreaterThan(initial.height);
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem("netaudio.matrix.expanded")).transmitters)).toEqual([]);
  await page.getByRole("button", { name: "Collapse all receiver devices and groups", exact: true }).click();
  await expect.poll(async () => (await dimensions()).height).toBe(initial.height);
  await page.locator(".routing-options summary").click();
  await page.getByRole("checkbox", { name: "Channel groups", exact: true }).check();
  await expect(page.getByRole("button", { name: "Expand all channel groups", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Expand all devices and groups", exact: true }).click();
  await expect.poll(async () => (await dimensions()).height).toBeGreaterThan(initial.height);
  await expect.poll(async () => (await dimensions()).width).toBeGreaterThan(initial.width);
  const expanded = await dimensions();
  const expandedDevices = await page.evaluate(() => localStorage.getItem("netaudio.matrix.expanded"));
  await page.getByRole("button", { name: "Collapse all channel groups", exact: true }).click();
  await expect(page.locator(".routing-options-panel")).toBeVisible();
  await expect.poll(async () => (await dimensions()).height).toBeLessThan(expanded.height);
  await expect.poll(async () => (await dimensions()).width).toBeLessThan(expanded.width);
  expect(await page.evaluate(() => localStorage.getItem("netaudio.matrix.expanded"))).toBe(expandedDevices);
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem("netaudio.matrix.groups")).receivers)).toEqual([]);
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem("netaudio.matrix.groups")).transmitters)).toEqual([]);
  await page.getByRole("button", { name: "Expand all channel groups", exact: true }).click();
  await expect(page.locator(".routing-options-panel")).toBeVisible();
  await expect.poll(dimensions).toEqual(expanded);
  const savedGroups = await page.evaluate(() => JSON.parse(localStorage.getItem("netaudio.matrix.groups")));
  expect(savedGroups.receivers.length).toBeGreaterThan(0);
  expect(savedGroups.transmitters.length).toBeGreaterThan(0);
  await expect(page.locator(".matrix-axis-controls button")).toHaveCount(4);
  await page.locator(".routing-options summary").click();
  await page.getByRole("button", { name: "Collapse all receiver devices and groups", exact: true }).click();
  await expect.poll(async () => (await dimensions()).height).toBe(initial.height);
  expect((await dimensions()).width).toBe(expanded.width);
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem("netaudio.matrix.groups")).receivers)).toEqual([]);
  await page.getByRole("button", { name: "Collapse all transmitter devices and groups", exact: true }).click();
  await expect.poll(async () => (await dimensions()).width).toBe(initial.width);
  await page.reload();
  await page.locator(".routing-options summary").click();
  await expect(page.getByRole("checkbox", { name: "Channel groups", exact: true })).toBeChecked();
  await expect.poll(async () => (await dimensions()).height).toBe(initial.height);
  await page.locator(".routing-options summary").click();
  const header = Number(await page.locator(".matrix-viewport").getAttribute("data-header-height"));
  await page.locator(".matrix-viewport").click({ position: { x: 15, y: header + 15 } });
  await expect.poll(async () => (await dimensions()).height).toBeGreaterThan(initial.height);
  const grouped = await dimensions();
  await page.locator(".matrix-viewport").click({ position: { x: 65, y: header + 45 } });
  await expect.poll(async () => (await dimensions()).height).toBeGreaterThan(grouped.height);
  await page.locator(".matrix-viewport").click({ position: { x: 65, y: header + 45 } });
  await expect.poll(async () => (await dimensions()).height).toBe(grouped.height);
  await page.locator(".routing-options summary").click();
  await page.getByRole("button", { name: "Flip axes", exact: true }).click();
  const flipped = await dimensions();
  await page.locator(".routing-options summary").click();
  await page.getByRole("button", { name: "Expand all receiver devices and groups", exact: true }).click();
  await expect.poll(async () => (await dimensions()).width).toBeGreaterThan(flipped.width);
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem("netaudio.matrix.groups")).transmitters)).toEqual([]);
  await page.locator(".routing-options summary").click();
  await page.getByRole("button", { name: "Collapse all devices and groups", exact: true }).click();
  expect((await dimensions()).width).toBeLessThan(flipped.width);
  expect(writes).toEqual([]);
});

test("table headers sort by click and keyboard; device names are real navigable links", async ({ page }) => {
  await page.goto("http://netaudio.test/devices");
  const table = page.locator("#content table");
  const header = table.getByRole("columnheader", { name: "Name", exact: true });
  await header.getByRole("button").click();
  await expect(header).toHaveAttribute("aria-sort", "ascending");
  const ascending = await table.locator("tbody a").allTextContents();
  await header.getByRole("button").focus();
  await page.keyboard.press("Enter");
  await expect(header).toHaveAttribute("aria-sort", "descending");
  expect(await table.locator("tbody a").allTextContents()).toEqual([...ascending].reverse());
  const numericHeader = table.getByRole("columnheader").filter({ hasText: "Rx channels" });
  await numericHeader.getByRole("button").click();
  const index = await numericHeader.evaluate((node) => node.cellIndex);
  const counts = await table.locator("tbody tr").evaluateAll((rows, index) => rows.map((row) => Number(row.cells[index].textContent)), index);
  expect(counts).toEqual([...counts].sort((a, b) => a - b));
  const link = table.getByRole("link", { name: "Windows-PC", exact: true });
  await expect(link).toHaveAttribute("href", /\/devices\/Windows-PC\/receive/);
  await link.focus();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/devices\/Windows-PC\/receive$/);
  await expect(page.locator("#content header").getByRole("button", { name: "Unsubscribe all", exact: true })).toBeVisible();
  const status = page.locator("#content td.status-cell").first();
  expect((await status.boundingBox()).width).toBeLessThanOrEqual(280);
  expect(await status.evaluate((node) => getComputedStyle(node).whiteSpace)).toBe("normal");
  await expect(page.getByRole("button", { name: "Subscribe…", exact: true })).toHaveCount(0);
  await page.getByRole("navigation", { name: "Breadcrumb" }).getByRole("link", { name: "Device Info" }).click();
  await expect(page).toHaveURL(/\/devices$/);
});

test("routing has no offline dismissal or floating notifications", async ({ page }) => {
  await page.goto("http://netaudio.test/routing");
  await expect(page.locator(".toast-stack, .app-toast")).toHaveCount(0);
  await expect(page.getByText("Review offline devices", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Dismiss offline", exact: true })).toHaveCount(0);
  await expect(page.locator(".topbar .connection-pill")).toHaveCount(0);
  await page.getByRole("button", { name: "Show navigation", exact: true }).click();
  const shureLink = page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "Shure", exact: true });
  await expect(shureLink.getByText("Shure", { exact: true })).toBeVisible();
  await expect(shureLink.locator("svg")).toHaveAttribute("width", "20");
});

test("saved devices survive connection loss but an empty snapshot replaces them", async ({ page }) => {
  await page.goto("http://netaudio.test/devices");
  await expect.poll(() => page.evaluate(() => localStorage.getItem("netaudio.inventory.v1"))).not.toBeNull();
  const before = await page.locator("#content table").boundingBox();
  await page.route("**/events", (route) => route.abort());
  await page.reload();
  await expect(page.getByText("Showing saved devices.", { exact: false })).toHaveCount(0);
  await expect(page.locator("#content table").getByRole("link", { name: "Windows-PC", exact: true })).toBeVisible();
  const after = await page.locator("#content table").boundingBox();
  expect(after.y).toBe(before.y);
  expect(after.height).toBe(before.height);
  await page.route("**/events", (route) => route.fulfill({ contentType: "text/event-stream", body: 'data: {"event":"snapshot","devices":{}}\n\n' }));
  await page.reload();
  await expect(page.locator("#content").getByRole("link", { name: "Windows-PC", exact: true })).toHaveCount(0);
  await expect(page.getByText("Showing saved devices.", { exact: false })).toHaveCount(0);
  expect(await page.evaluate(() => Object.keys(JSON.parse(localStorage.getItem("netaudio.inventory.v1")).devices).length)).toBe(0);
});

test("device Metering tab starts and releases its own session", async ({ page }) => {
  const writes = [];
  await page.route("**/metering/*", (route) => {
    if (route.request().method() !== "POST") return route.fallback();
    writes.push({ path: new URL(route.request().url()).pathname, body: route.request().postDataJSON() });
    return route.fulfill({ contentType: "application/json", body: "{}" });
  });
  const device = Object.values(devices).find((entry) => entry.online);
  await page.goto("http://netaudio.test/devices");
  expect(writes).toEqual([]);
  await expect(page.getByRole("link", { name: "Metering", exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: /^Show meters/ })).toHaveCount(0);
  await page.goto(`http://netaudio.test/devices/${encodeURIComponent(device.name)}/metering`);
  await expect(page.locator("#content")).toContainText("Receive levels");
  await expect(page.locator("#content")).toContainText("Transmit levels");
  await expect.poll(() => writes.length).toBe(1);
  expect(writes[0].path).toBe("/metering/start");
  expect(writes[0].body.device).toBe(device.server_name);
  await page.getByRole("link", { name: "Status", exact: true }).click();
  await expect.poll(() => writes.length).toBe(2);
  expect(writes[1]).toEqual({ path: "/metering/stop", body: writes[0].body });
});

test("hidden offline devices do not request a detailed session through a direct URL", async ({ page }) => {
  const writes = [];
  await page.route("**/metering/*", (route) => {
    if (route.request().method() !== "POST") return route.fallback();
    writes.push(route.request().url());
    return route.fulfill({ contentType: "application/json", body: "{}" });
  });
  const device = Object.values(devices).find((entry) => entry.online === false);
  await page.goto(`http://netaudio.test/devices/${encodeURIComponent(device.name)}/metering`);
  await expect(page.getByText(`No device named ${device.name}`, { exact: false })).toBeVisible();
  await page.getByRole("link", { name: "Routing", exact: true }).click();
  expect(writes).toEqual([]);
});

test("channel tables have no signal placeholders and do not start detailed monitoring", async ({ page }) => {
  const requests = [];
  await page.route("**/metering/*", (route) => {
    requests.push(route.request().url());
    return route.fulfill({ status: 409, contentType: "application/json", body: '{"error":"Unsupported"}' });
  });
  const device = Object.values(devices).find((entry) => entry.online && Object.keys(entry.channels?.receivers || {}).length);
  for (const section of ["receive", "transmit"]) {
    await page.goto(`http://netaudio.test/devices/${encodeURIComponent(device.name)}/${section}`);
    await expect(page.locator("#content")).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Signal", exact: true })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Retry monitoring" })).toHaveCount(0);
    await expect(page.getByText("Detailed monitoring could not start", { exact: false })).toHaveCount(0);
    await expect(page.locator(".signal-presence")).toHaveCount(0);
  }
  expect(requests).toEqual([]);
});

test("network settings load once without a refresh button and stay a compact form", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  let reads = 0;
  const snapshot = { interfaces: [{ interface: "primary", mode: "dynamic", ip_address: "192.0.2.34",
    netmask: "255.255.255.0", configured: { mode: "dynamic" } }],
    redundancy: { current: "switched", configured: "switched", supported: ["switched", "redundant"] } };
  await page.route("**/interfaces/*", (route) => {
    reads += 1;
    return route.fulfill({ contentType: "application/json", body: JSON.stringify(snapshot) });
  });
  const device = Object.values(devices).find((entry) => entry.online);
  await page.goto(`http://netaudio.test/devices/${encodeURIComponent(device.name)}/network-config`);
  await expect(page.getByRole("button", { name: "Save primary settings" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Refresh network settings" })).toHaveCount(0);
  const geometry = await page.locator(".network-config").evaluate((node) => {
    const sections = node.querySelectorAll(".network-section");
    return { width: node.getBoundingClientRect().width,
      gap: sections[1].getBoundingClientRect().top - sections[0].getBoundingClientRect().bottom };
  });
  expect(geometry.width).toBeLessThanOrEqual(704);
  expect(geometry.gap).toBeGreaterThanOrEqual(24);
  expect(reads).toBe(1);
});

test("phone routing fits and a failed write keeps the source sheet open", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("http://netaudio.test/routing");
  const controls = page.getByRole("region", {
    name: "Route receiver channels",
  });
  await expect(controls).toBeVisible();
  await expect(page.locator(".matrix-canvas")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Search", exact: true }),
  ).toBeVisible();
  await controls.getByRole("combobox").selectOption({ label: "avio-usb-1" });
  const channel = controls.getByRole("button").first();
  await page.screenshot({ path: "test-results/phone-channels.png" });
  await expect(channel.locator("svg")).toHaveCount(2);
  await channel.click();
  const sheet = page.getByRole("dialog", { name: "Choose source" });
  await expect(sheet).toBeVisible();
  await sheet.getByRole("textbox").fill("lx-dante");
  await sheet
    .locator("button:not(:disabled)")
    .filter({ hasText: "lx-dante" })
    .first()
    .click();
  await expect(sheet.getByRole("alert")).toContainText(
    "Test receiver unavailable",
  );
  await expect(sheet).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({ path: "test-results/phone-routing.png" });
});

test("tools menu leaves routing intact and axis preferences survive reload", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://netaudio.test/routing");
  await expect(page.locator(".matrix-canvas")).toBeVisible();
  await page.setViewportSize({ width: 1440, height: 1400 });
  await expect.poll(async () => (await page.locator(".matrix-viewport").boundingBox()).height).toBeGreaterThan(720);
  const content = await page.locator("#content").boundingBox();
  const grid = await page.locator(".routing-grid").boundingBox();
  expect(Math.abs(content.y + content.height - grid.y - grid.height)).toBeLessThan(2);
  await expect(page.locator(".matrix-status")).toHaveCount(0);
  await page.locator(".routing-options summary").click();
  const flip = page.locator(".routing-options button[aria-pressed]");
  await flip.click();
  await expect(flip).toHaveAttribute("aria-pressed", "false");
  await page.reload();
  await page.locator(".routing-options summary").click();
  await expect(flip).toHaveAttribute("aria-pressed", "false");
  await flip.click();
  await expect(flip).toHaveAttribute("aria-pressed", "true");
  await expect(page.locator(".routing-options-panel")).toBeVisible();
  await page.locator(".routing-options summary").click();
  await expect(page.locator(".routing-options-panel")).toBeHidden();
  const before = await page.locator("#content").boundingBox();
  await page.getByRole("button", { name: "Show navigation" }).click();
  const sidebar = page.getByRole("dialog", { name: "Application navigation" });
  await expect(sidebar.locator(".device-list, details")).toHaveCount(0);
  for (const link of await sidebar.getByRole("link").all()) {
    await expect(link).toBeVisible();
    await expect(link.locator("svg")).toBeVisible();
  }
  await page.keyboard.press("Escape");
  await expect(sidebar).toBeHidden();
  const after = await page.locator("#content").boundingBox();
  expect(after).toEqual(before);
  await page.screenshot({ path: "test-results/desktop-routing.png" });
});

test("phone source selection and disconnect use the selected receiver channel", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const writes = [];
  for (const action of ["subscribe", "unsubscribe"]) {
    await page.route(`**/${action}`, (route) => {
      writes.push({ action, body: route.request().postDataJSON() });
      return route.fulfill({ contentType: "application/json", body: "{}" });
    });
  }
  await page.goto("http://netaudio.test/routing");
  const controls = page.getByRole("region", {
    name: "Route receiver channels",
  });
  await controls.getByRole("combobox").selectOption({ label: "avio-usb-1" });
  await controls.getByRole("button").first().click();
  const sheet = page.getByRole("dialog", { name: "Choose source" });
  await sheet.getByRole("textbox").fill("lx-dante");
  await sheet
    .locator("button:not(:disabled)")
    .filter({ hasText: "lx-dante" })
    .first()
    .click();
  await expect(sheet).toHaveCount(0);
  expect(writes[0]).toMatchObject({
    action: "subscribe",
    body: { rx_channel: 1, tx_device: "lx-dante", tx_channel: "01" },
  });
  expect(writes[0].body.rx_device).toBe(
    Object.values(devices).find((device) => device.name === "avio-usb-1")
      .server_name,
  );
  await controls.getByRole("button").first().click();
  await sheet.getByRole("button", { name: "Disconnect" }).click();
  await expect(sheet).toHaveCount(0);
  expect(writes[1]).toEqual({
    action: "unsubscribe",
    body: { rx_channel: 1, rx_device: writes[0].body.rx_device },
  });
});
