import { test, expect } from "@playwright/test";
import { serveWebapp, deviceFixture } from "./fixture.mjs";

test("long device tables remain reachable through page scrolling", async ({ page }, testInfo) => {
  const template = Object.values(deviceFixture).find((device) => device.online);
  const devices = Object.fromEntries(Array.from({ length: 40 }, (_, index) => {
    const name = `Receiver ${String(index + 1).padStart(2, "0")}`;
    const server_name = `receiver-${index + 1}.local.`;
    return [server_name, { ...template, name, server_name }];
  }));
  await page.setViewportSize({ width: 1600, height: 600 });
  await serveWebapp(page, { devices });
  await page.goto("http://netaudio.test/devices");
  const table = page.locator("table.data").first();
  await expect(table.locator("tbody tr")).toHaveCount(40);
  const geometry = await table.evaluate((node) => {
    const wrapper = node.closest(".table-wrapper");
    return { table: node.getBoundingClientRect().height, wrapper: wrapper.getBoundingClientRect().height };
  });
  expect(geometry.table).toBeGreaterThan(600);
  expect(geometry.wrapper).toBeGreaterThanOrEqual(geometry.table - 1);
  await page.locator("#content").hover();
  await page.mouse.wheel(0, 5000);
  await expect(table.locator("tbody tr").last()).toBeInViewport();
  const artifact = testInfo.outputPath("device-table-last-row.png");
  await page.screenshot({ path: artifact });
  await testInfo.attach("device-table-last-row", { path: artifact, contentType: "image/png" });
});

test("details, tables, notices and errors remain selectable with UI selection disabled", async ({ page }) => {
  await serveWebapp(page);
  await page.route("**/settings", (route) => route.request().headers().accept === "application/json"
    ? route.fulfill({ contentType: "application/json", body: JSON.stringify({ monitoring_port: 8752, active_monitoring_port: 8752 }) })
    : route.fallback());
  await page.goto("http://netaudio.test/devices");
  const selection = (locator) => locator.evaluate((node) => getComputedStyle(node).userSelect);
  expect(await selection(page.locator("tbody td").first())).not.toBe("none");
  await page.locator(".device-table-link").first().click();
  expect(await selection(page.locator(".content-title"))).not.toBe("none");
  expect(await selection(page.locator(".device-address"))).not.toBe("none");
  await page.goto("http://netaudio.test/settings");
  await page.getByRole("spinbutton").fill("8751");
  expect(await selection(page.getByRole("alert"))).not.toBe("none");
  await expect(page.getByText("Video format", { exact: true })).toHaveCount(0);
});

test("mobile views use one selector and a reachable source picker", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await serveWebapp(page);
  await page.goto("http://netaudio.test/network-status");
  const selector = page.getByRole("combobox", { name: "View", exact: true });
  await expect(selector).toBeVisible();
  await expect(selector.locator("option")).toHaveCount(9);
  const header = page.locator(".topbar");
  expect(await header.evaluate((node) => node.scrollWidth <= node.clientWidth)).toBe(true);
  await page.locator(".mobile-card-toggle").first().click();
  const field = page.locator('td[data-label="Primary address"]').first();
  await field.hover();
  await selector.focus();
  await selector.dispatchEvent("pointerdown", { pointerType: "touch" });
  await selector.selectOption("/routing");
  await expect(selector).not.toBeFocused();
  await page.screenshot({ path: testInfo.outputPath("mobile-selection.png") });
  await page.getByRole("region", { name: "Route receiver channels" }).getByRole("button").first().click();
  const picker = page.getByRole("dialog", { name: "Choose source" });
  await expect(picker).toBeVisible();
  await expect(picker.getByRole("button", { name: "Done", exact: true })).toBeVisible();
  await expect.poll(async () => (await page.locator(".source-picker-box").boundingBox()).y).toBeGreaterThanOrEqual(0);
  const box = await page.locator(".source-picker-box").boundingBox();
  expect(box.y + box.height).toBeLessThanOrEqual(844);
  await expect(page.locator(".source-picker-entry svg")).toHaveCount(0);
  await page.locator(".source-picker-results").evaluate((node) => { node.scrollTop = node.scrollHeight; });
  await expect(page.locator(".source-picker-entry").last()).toBeInViewport();
  await picker.getByRole("button", { name: "Done", exact: true }).click();
  await selector.focus();
  await selector.dispatchEvent("keydown", { key: "ArrowDown" });
  await expect(selector).toBeFocused();
  await selector.selectOption("/subscriptions");
  for (const slot of await page.locator(".subscription-transport").all()) await expect(slot).toBeHidden();
  await page.setViewportSize({ width: 1600, height: 1000 });
  await expect(selector).toBeHidden();
});

test("source picker and matrix reflect same-device source capability", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const receiver = { name: "A Receiver", server_name: "receiver.local.", online: true,
    channels: { receivers: {
      1: { name: "Blocked input", can_subscribe_self: false },
      2: { name: "Allowed input", can_subscribe_self: true },
    }, transmitters: { 1: { name: "Own output" } } }, subscriptions: [] };
  const remote = { name: "B Source", server_name: "source.local.", online: true,
    channels: { receivers: {}, transmitters: { 1: { name: "Remote output" } } }, subscriptions: [] };
  await serveWebapp(page, { devices: { receiver, remote } });
  await page.goto("http://netaudio.test/routing");
  const rows = page.getByRole("region", { name: "Route receiver channels" }).locator(".routing-channel-row");
  await rows.nth(0).click();
  let picker = page.getByRole("dialog", { name: "Choose source" });
  await expect(picker.getByRole("button", { name: /Remote output/ })).toBeEnabled();
  await picker.getByRole("button", { name: "Done", exact: true }).click();
  await rows.nth(1).click();
  picker = page.getByRole("dialog", { name: "Choose source" });
  await expect(picker.getByRole("button", { name: /Own output/ })).toBeEnabled();
  await picker.getByRole("button", { name: "Done", exact: true }).click();

  await page.setViewportSize({ width: 1200, height: 800 });
  await page.reload();
  const viewport = page.locator(".matrix-viewport");
  await expect(viewport).toBeVisible();
  const geometry = await viewport.evaluate((node) => ({
    cell: Number(node.dataset.cellSize),
    gutter: Number(node.dataset.gutterWidth),
    header: Number(node.dataset.headerHeight),
  }));
  const cell = (column, row) => ({
    x: geometry.gutter + column * geometry.cell + geometry.cell / 2,
    y: geometry.header + row * geometry.cell + geometry.cell / 2,
  });
  await viewport.hover({ position: cell(1, 1) });
  await expect(viewport).toHaveCSS("cursor", "not-allowed");
  await expect(page.getByRole("tooltip")).toContainText("cannot connect to an output on the same device");
  await viewport.hover({ position: cell(2, 1) });
  await expect(viewport).toHaveCSS("cursor", "crosshair");
  await expect(page.getByRole("tooltip")).not.toContainText("cannot connect to an output on the same device");
});

test("collapsed intersections expand channels without routing and names do not show tooltips", async ({ page }) => {
  await serveWebapp(page);
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto("http://netaudio.test/routing");
  await page.getByRole("button", { name: "Collapse all receiver devices and groups", exact: true }).click();
  await page.getByRole("button", { name: "Collapse all transmitter devices and groups", exact: true }).click();
  const viewport = page.locator(".matrix-viewport");
  const gutter = Number(await viewport.getAttribute("data-gutter-width"));
  const header = Number(await viewport.getAttribute("data-header-height"));
  await viewport.hover({ position: { x: 80, y: header + 15 } });
  await expect(page.locator(".matrix-tooltip")).toHaveCount(0);
  await viewport.hover({ position: { x: gutter + 15, y: 60 } });
  await expect(page.locator(".matrix-tooltip")).toHaveCount(0);
  await viewport.click({ position: { x: gutter + 15, y: header + 15 } });
  await expect.poll(() => page.evaluate(() => JSON.parse(localStorage.getItem("netaudio.matrix.expanded")).receivers.length)).toBe(1);
  await expect.poll(() => page.evaluate(() => JSON.parse(localStorage.getItem("netaudio.matrix.expanded")).transmitters.length)).toBe(1);
  expect(writes).toEqual([]);
});

test("shared filter panel follows every tab and filters device inventories persistently", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/routing");
  const panel = page.getByRole("complementary", { name: "Device filters" });
  await panel.getByRole("searchbox", { name: "Search devices" }).fill("avio-bt-1");
  for (const path of ["devices", "clock-status", "network-status", "subscriptions", "presets", "ddm", "routing"]) {
    await page.goto(`http://netaudio.test/${path}`);
    await expect(panel).toBeVisible();
    await expect(panel.getByRole("searchbox", { name: "Search devices" })).toHaveValue("avio-bt-1");
    if (["devices", "clock-status", "network-status"].includes(path)) {
      await expect(page.locator("#content tbody tr")).toHaveCount(1);
      await expect(page.locator("#content tbody")).toContainText("avio-bt-1");
    }
  }
  await panel.getByRole("button", { name: "Clear all", exact: true }).click();
  await page.goto("http://netaudio.test/devices");
  await expect.poll(() => page.locator("#content tbody tr").count()).toBeGreaterThan(1);
});

test("receiver row status sits next to the matrix and its tooltip follows the icon", async ({ page }) => {
  await serveWebapp(page);
  await page.addInitScript(() => localStorage.setItem("netaudio.matrix.flipped", "false"));
  await page.goto("http://netaudio.test/routing");
  const canvas = page.locator(".matrix-canvas");
  const geometry = await canvas.evaluate((node) => {
    const viewport = document.querySelector(".matrix-viewport");
    const gutter = Number(viewport.dataset.gutterWidth), header = Number(viewport.dataset.headerHeight);
    return { gutter, header };
  });
  await page.locator(".matrix-viewport").hover({ position: { x: geometry.gutter - 15, y: geometry.header + 45 } });
  await expect(page.getByRole("tooltip")).toBeVisible();
  await page.locator(".matrix-viewport").hover({ position: { x: 15, y: geometry.header + 45 } });
  await expect(page.getByRole("tooltip")).toBeHidden();
});

test("navigation controls toggle filters", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/routing");
  const header = page.locator(".topbar");
  const hide = header.getByRole("button", { name: "Hide filters", exact: true });
  await expect(hide).toHaveText("");
  await expect(hide).toHaveAttribute("aria-expanded", "true");
  await hide.click();
  await expect(page.locator("#inventory-filters")).toHaveCount(0);
  await header.getByRole("button", { name: "Show filters", exact: true }).click();
  await expect(page.locator("#inventory-filters")).toBeVisible();
  await page.getByRole("navigation", { name: "Views" }).getByRole("link", { name: "Settings", exact: true }).click();
  await expect(header.getByRole("button", { name: /filters/ })).toHaveCount(0);
});

test("receivers start across the top and a saved alternate orientation is respected", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/routing");
  await expect(page.locator(".column-axis .matrix-axis-title")).toContainText("Receivers");
  await expect(page.locator(".row-axis .matrix-axis-title")).toContainText("Transmitters");
  await page.getByRole("button", { name: "Flip axes", exact: true }).click();
  await page.reload();
  await expect(page.locator(".column-axis .matrix-axis-title")).toContainText("Transmitters");
});

test("desktop channel list supports search and selectable text", async ({ page }) => {
  await serveWebapp(page);
  await page.setViewportSize({ width: 1600, height: 1000 });
  await page.goto("http://netaudio.test/routing");
  await page.getByRole("button", { name: "Channel list", exact: true }).click();
  const region = page.getByRole("region", { name: "Route receiver channels" });
  await expect(region.locator(".routing-channel-head")).toBeVisible();
  const search = region.getByRole("searchbox");
  expect(await search.evaluate((node) => getComputedStyle(node).userSelect)).not.toBe("none");
  await search.fill("no match");
  await expect(region).toContainText("No channels match this search.");
});

test("network tabs and every tool remain reachable without changing devices", async ({ page }) => {
  await serveWebapp(page);
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto("http://netaudio.test/routing");
  const navigation = page.getByRole("navigation", { name: "Views" });
  for (const [label, path] of [["Device Info", "devices"], ["Clock", "clock-status"], ["Network", "network-status"], ["Events", "events"], ["Subscriptions", "subscriptions"], ["Presets", "presets"], ["Domains", "ddm"], ["Settings", "settings"], ["Routing", "routing"]]) {
    await navigation.getByRole("link", { name: label, exact: true }).click();
    await expect(page).toHaveURL(`http://netaudio.test/${path}`);
    await expect(navigation.getByRole("link", { name: label, exact: true })).toHaveAttribute("aria-current", "page");
  }
  expect(writes).toEqual([]);
});

test("switching devices preserves the section and shows the display name", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/devices/Windows-PC/status");
  const target = Object.values(deviceFixture).find((device) => device.name === "avio-bt-1");
  await page.getByRole("combobox", { name: "Switch device", exact: true }).selectOption(target.server_name);
  await expect(page).toHaveURL(new RegExp(`/devices/${target.server_name.replaceAll('.', '\\.')}\/status$`));
  await expect(page.locator(".device-heading h1")).toHaveText("avio-bt-1");
  await expect(page.getByRole("navigation", { name: "Device sections" }).getByRole("link", { name: "Status", exact: true })).toHaveAttribute("aria-current", "page");
});

test("small devices start expanded without overriding saved expansion choices", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/routing");
  await expect.poll(() => page.evaluate(async () => {
    const { expanded } = await import('/matrix.js');
    return expanded.value.receivers.has('avio-bt-1') && !expanded.value.receivers.has('Windows-PC');
  })).toBe(true);
  await page.getByRole("button", { name: "Collapse all receiver devices and groups", exact: true }).click();
  await page.reload();
  await expect.poll(() => page.evaluate(async () => [...(await import('/matrix.js')).expanded.value.receivers])).toEqual([]);
});

test("an enrolled offline device is absent from the routing workspace", async ({ page }) => {
  const devices = {
    source: { name: "Source", server_name: "source", online: true, channels: { transmitters: { 1: { name: "Out" } } }, subscriptions: [] },
    receiver: { name: "Receiver", server_name: "receiver", online: false, management_state: "managed", ddm_domain_id: "studio", channels: { receivers: { 1: { name: "In" } } }, subscriptions: [] },
  };
  await serveWebapp(page, { devices });
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto("http://netaudio.test/routing");
  await expect(page.locator(".matrix-viewport")).toHaveCount(0);
  await expect(page.locator("#content")).toContainText("No devices match the current filters.");
  expect(writes).toEqual([]);
});
