import { test, expect } from "@playwright/test";
import { serveWebapp, deviceFixture } from "./fixture.mjs";

test("flipping axes keeps routing options open", async ({ page }) => {
  await serveWebapp(page);
  await page.addInitScript(() => localStorage.setItem("netaudio.routing.filters", JSON.stringify({ panelOpen: false })));
  await page.goto("http://netaudio.test/routing");
  await page.locator(".routing-options summary").click();
  await page.getByRole("button", { name: "Flip axes", exact: true }).click();
  await expect(page.locator(".routing-options")).toHaveAttribute("open", "");
});

test("details, tables, notices and errors remain selectable with UI selection disabled", async ({ page }) => {
  await serveWebapp(page);
  await page.route("**/settings", (route) => route.request().headers().accept === "application/json"
    ? route.fulfill({ contentType: "application/json", body: JSON.stringify({ monitoring_port: 8752, active_monitoring_port: 8752 }) })
    : route.fallback());
  await page.goto("http://netaudio.test/devices");
  const selection = (locator) => locator.evaluate((node) => getComputedStyle(node).userSelect);
  expect(await selection(page.locator("tbody td").first())).toBe("text");
  expect(await selection(page.locator("thead th").first())).toBe("text");
  expect(await selection(page.getByRole("navigation", { name: "Network views" }).getByRole("link").first())).toBe("none");
  await page.locator(".device-table-link").first().click();
  expect(await selection(page.locator(".content-title"))).toBe("text");
  expect(await selection(page.locator(".device-address"))).toBe("text");
  await page.goto("http://netaudio.test/shure");
  await expect(page.locator(".notice")).toHaveText("No Shure devices found.");
  expect(await selection(page.locator(".notice"))).toBe("text");
  await page.goto("http://netaudio.test/settings");
  await page.getByRole("spinbutton").fill("8751");
  expect(await selection(page.getByRole("alert"))).toBe("text");
  await expect(page.getByText("Video format", { exact: true })).toHaveCount(0);
});

test("mobile views use one selector and a reachable source picker", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await serveWebapp(page);
  await page.goto("http://netaudio.test/network-status");
  const selector = page.getByRole("combobox", { name: "View", exact: true });
  await expect(selector).toBeVisible();
  await expect(selector.locator("option")).toHaveCount(10);
  await expect(page.getByRole("button", { name: "Show navigation", exact: true })).toBeHidden();
  const nav = page.getByRole("navigation", { name: "Network views" });
  expect(await nav.evaluate((node) => node.scrollWidth <= node.clientWidth)).toBe(true);
  await page.locator(".mobile-card-toggle").first().click();
  const field = page.locator('td[data-label="Primary address"]').first();
  await field.hover();
  await selector.selectOption("/routing");
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
  await selector.selectOption("/subscriptions");
  for (const slot of await page.locator(".subscription-transport").all()) await expect(slot).toBeHidden();
  await page.setViewportSize({ width: 1600, height: 1000 });
  await expect(selector).toBeHidden();
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
  for (const path of ["devices", "clock-status", "network-status", "subscriptions", "presets", "ddm", "events", "settings", "shure", "routing"]) {
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
  expect(await page.locator("#content tbody tr").count()).toBeGreaterThan(1);
});

test("text selection setting overrides all UI selection rules and survives reload", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/settings");
  const selection = page.getByRole("checkbox", { name: "Allow text selection", exact: true });
  const tab = page.getByRole("navigation", { name: "Network views" }).getByRole("link").first();
  await expect(selection).not.toBeChecked();
  await selection.check();
  expect(await tab.evaluate((node) => getComputedStyle(node).userSelect)).toBe("text");
  await page.reload();
  await expect(selection).toBeChecked();
  expect(await tab.evaluate((node) => getComputedStyle(node).userSelect)).toBe("text");
  await selection.uncheck();
  expect(await tab.evaluate((node) => getComputedStyle(node).userSelect)).toBe("none");
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
  const menu = header.getByRole("button", { name: "Show navigation", exact: true });
  await expect(menu).toHaveText("");
  const navigation = page.getByRole("navigation", { name: "Network views" });
  const hide = navigation.getByRole("button", { name: "Hide filters", exact: true });
  await expect(hide).toHaveText("");
  await expect(hide).toHaveAttribute("aria-expanded", "true");
  await hide.click();
  await expect(page.locator("#inventory-filters")).toHaveCount(0);
  await navigation.getByRole("button", { name: "Show filters", exact: true }).click();
  await expect(page.locator("#inventory-filters")).toBeVisible();
  const tabs = await page.getByRole("navigation", { name: "Network views" }).getByRole("link").all();
  await tabs[1].click();
  expect(await tabs[1].evaluate((node) => getComputedStyle(node).userSelect)).toBe("none");
  await expect(header.getByRole("button", { name: /filters/ })).toHaveCount(0);
});

test("receivers start across the top and a saved alternate orientation is respected", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/routing");
  await expect(page.locator(".column-axis .matrix-axis-title")).toContainText("Receivers");
  await expect(page.locator(".row-axis .matrix-axis-title")).toContainText("Transmitters");
  await page.locator(".routing-options summary").click();
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
  expect(await search.evaluate((node) => getComputedStyle(node).userSelect)).toBe("text");
  await search.fill("no match");
  await expect(region).toContainText("No channels match this search.");
});

test("view options dismiss outside and on Escape without closing on internal controls", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/routing");
  const trigger = page.locator(".routing-options summary");
  const panel = page.locator(".routing-options-panel");
  await trigger.click();
  await page.getByRole("checkbox", { name: "Channel groups", exact: true }).check();
  await expect(panel).toBeVisible();
  for (const name of ["Flip axes", "Expand all devices and groups", "Collapse all devices and groups"]) {
    await page.getByRole("button", { name, exact: true }).click();
    await expect(panel).toBeVisible();
  }
  await page.getByRole("searchbox", { name: "Search devices", exact: true }).click();
  await expect(panel).toBeHidden();
  await trigger.click();
  await page.keyboard.press("Escape");
  await expect(panel).toBeHidden();
  await expect(trigger).toBeFocused();
  await trigger.click();
  await page.getByRole("button", { name: "Show navigation", exact: true }).click();
  await expect(panel).toBeHidden();
  await expect(page.getByRole("dialog", { name: "Application navigation" })).toBeVisible();
});

test("Tools menu is nonmodal and dismisses on navigation", async ({ page }) => {
  await serveWebapp(page);
  await page.setViewportSize({ width: 1600, height: 1000 });
  await page.goto("http://netaudio.test/routing");
  const trigger = page.getByRole("button", { name: "Show navigation", exact: true });
  await trigger.click();
  const menu = page.getByRole("dialog", { name: "Application navigation" });
  await expect(menu).toBeVisible();
  await expect(menu.getByRole("link", { name: "Routing", exact: true })).toHaveCount(0);
  expect(await page.evaluate(() => document.querySelectorAll(":modal").length)).toBe(0);
  await page.getByRole("navigation", { name: "Network views" }).getByRole("link", { name: "Network Status", exact: true }).click();
  await expect(page).toHaveURL("http://netaudio.test/network-status");
  await expect(menu).toBeHidden();
});

test("network tabs and every tool remain reachable without changing devices", async ({ page }) => {
  await serveWebapp(page);
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto("http://netaudio.test/routing");
  const navigation = page.getByRole("navigation", { name: "Network views" });
  for (const [label, path] of [["Device Info", "devices"], ["Clock Status", "clock-status"], ["Network Status", "network-status"], ["Events", "events"], ["Routing", "routing"]]) {
    await navigation.getByRole("link", { name: label, exact: true }).click();
    await expect(page).toHaveURL(`http://netaudio.test/${path}`);
    await expect(navigation.getByRole("link", { name: label, exact: true })).toHaveAttribute("aria-current", "page");
  }
  for (const label of ["Subscriptions", "Presets", "Domains", "Shure", "Settings"]) {
    await page.getByRole("button", { name: "Show navigation", exact: true }).click();
    await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: label, exact: true }).click();
    await expect(page.getByRole("dialog", { name: "Application navigation" })).toBeHidden();
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
  await expect(page.getByRole("navigation", { name: "Breadcrumb" })).toContainText("avio-bt-1");
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
