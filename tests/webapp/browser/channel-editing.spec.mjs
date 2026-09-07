import { test, expect } from "@playwright/test";
import { deviceFixture, serveWebapp } from "./fixture.mjs";

for (const width of [1200, 390]) {
  test(`channel name glyphs stay fixed when editing at ${width}px`, async ({ page }) => {
    await serveWebapp(page);
    await page.setViewportSize({ width, height: 900 });
    await page.goto("http://netaudio.test/devices/Windows-PC/receive");
    if (width < 900) await page.locator(".mobile-card-toggle").first().click();
    const edit = page.getByRole("button", { name: "Edit receive channel 1 name", exact: true });
    // Neutralize state colors so the pixel comparison isolates text placement.
    await page.addStyleTag({ content: `.channel-name-value, .channel-name-editor > input {
      color: #fff !important; background: #000 !important;
      border-color: transparent !important; outline: none !important; box-shadow: none !important;
    }` });
    const box = await edit.boundingBox();
    const clip = { x: box.x + 6, y: box.y + 4, width: 60, height: 24 };
    const before = await page.screenshot({ clip, path: test.info().outputPath("before-editing.png") });
    await edit.click();
    const input = page.getByRole("textbox", { name: "Receive channel 1 name", exact: true });
    await expect(input).toBeFocused();
    await input.evaluate((node) => { node.setSelectionRange(0, 0); node.blur(); });
    const after = await page.screenshot({ clip, caret: "hide", path: test.info().outputPath("during-editing.png") });
    if (!after.equals(before)) {
      console.log(await page.locator(".channel-name-value, .channel-name-editor > input").evaluateAll((nodes) => nodes.slice(0, 2).map((node) => {
        const style = getComputedStyle(node);
        return { tag: node.tagName, box: node.getBoundingClientRect().toJSON(), font: style.font,
          letterSpacing: style.letterSpacing, fontVariant: style.fontVariant, textIndent: style.textIndent,
          padding: style.padding, border: style.border, transform: style.transform };
      })));
      await test.info().attach("before-editing", { body: before, contentType: "image/png" });
      await test.info().attach("during-editing", { body: after, contentType: "image/png" });
    }
    expect(after).toEqual(before);
  });
}

for (const [section, direction, word] of [["receive", "rx", "Receive"], ["transmit", "tx", "Transmit"]]) {
  test(`${section} names edit in place without pencils or layout shifts`, async ({ page }) => {
    await serveWebapp(page);
    const writes = [];
    let fail = false;
    await page.route("**/rename-channel", (route) => {
      writes.push(route.request().postDataJSON());
      return route.fulfill({ status: fail ? 503 : 200, contentType: "application/json", body: fail ? '{"error":"Rename failed"}' : '{}' });
    });
    await page.goto(`http://netaudio.test/devices/Windows-PC/${section}`);
    const row = page.locator("table.data tbody tr").first();
    const edit = row.getByRole("button", { name: `Edit ${word.toLowerCase()} channel 1 name`, exact: true });
    await expect(row.getByRole("textbox")).toHaveCount(0);
    await expect(row.getByRole("button", { name: "Reset", exact: true })).toHaveCount(0);
    await expect(edit.locator("svg")).toHaveCount(0);
    const before = await row.boundingBox();
    await edit.click();
    const input = row.getByRole("textbox", { name: `${word} channel 1 name`, exact: true });
    await expect(input).toBeFocused();
    expect(await row.boundingBox()).toEqual(before);
    await input.fill("Cancelled name");
    await input.press("Escape");
    await expect(edit).toBeFocused();
    expect(writes).toEqual([]);
    await edit.click();
    await input.fill("New name");
    await input.press("Enter");
    await expect(input).toHaveCount(0);
    await expect(row.locator(".channel-name-display")).toContainText("New name");
    expect(writes[0]).toEqual({ device: "Windows-PC.local.", channel_type: direction, channel_number: 1, name: "New name" });
    await edit.click();
    fail = true;
    await input.fill("Try again");
    await row.getByRole("button", { name: "Save", exact: true }).click();
    await expect(row.getByRole("alert")).toHaveText("Rename failed");
    await expect(input).toHaveValue("Try again");
    await row.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(input).toHaveCount(0);
    fail = false;
    await edit.click();
    await input.fill("");
    await row.getByRole("button", { name: "Save", exact: true }).click();
    await expect(input).toHaveCount(0);
    expect(writes.at(-1).name).toBe("");
  });
}

test("column picker escapes short table clipping and stays within the viewport", async ({ page }) => {
  await serveWebapp(page);
  await page.setViewportSize({ width: 1200, height: 900 });
  await page.goto("http://netaudio.test/devices/avio-bt-1/receive");
  const trigger = page.getByRole("button", { name: /^Columns/ });
  await expect(page.locator("section.card > header").filter({ hasText: "Receivers" }).getByRole("button", { name: /^Columns/ })).toBeVisible();
  await expect(page.locator(".card-body .table-toolbar")).toBeHidden();
  await trigger.click();
  const menu = page.locator(".column-menu");
  await expect(menu).toBeVisible();
  expect(await menu.evaluate((node) => node.matches(":popover-open"))).toBe(true);
  const menuBox = await menu.boundingBox();
  const tableBox = await page.locator("table.data").boundingBox();
  expect(menuBox.y + menuBox.height).toBeGreaterThan(tableBox.y + tableBox.height);
  expect(menuBox.y + menuBox.height).toBeLessThanOrEqual(892);
  const reset = menu.getByRole("button", { name: "Reset to defaults", exact: true });
  await reset.scrollIntoViewIfNeeded();
  expect(await reset.evaluate((node) => {
    const box = node.getBoundingClientRect();
    return node.contains(document.elementFromPoint(box.x + box.width / 2, box.y + box.height / 2));
  })).toBe(true);
  await page.keyboard.press("Escape");
  await expect(menu).toBeHidden();
  await expect(trigger).toHaveAttribute("aria-expanded", "false");
  await page.setViewportSize({ width: 1200, height: 460 });
  await trigger.click();
  const smallBox = await menu.boundingBox();
  expect(smallBox.y).toBeGreaterThanOrEqual(8);
  expect(smallBox.y + smallBox.height).toBeLessThanOrEqual(452);
  await page.screenshot({ path: `test-results/column-popover-${test.info().project.name}.png` });
});

test("mobile editing has a visible Cancel and saves blank names as reset", async ({ page }) => {
  await serveWebapp(page);
  const writes = [];
  await page.route("**/rename-channel", (route) => {
    writes.push(route.request().postDataJSON());
    return route.fulfill({ contentType: "application/json", body: "{}" });
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("http://netaudio.test/devices/Windows-PC/receive");
  await page.locator(".mobile-card-toggle").first().click();
  const edit = page.getByRole("button", { name: "Edit receive channel 1 name", exact: true });
  await edit.click();
  const input = page.getByRole("textbox", { name: "Receive channel 1 name", exact: true });
  await input.fill("Discard this");
  const cancel = page.getByRole("button", { name: "Cancel", exact: true });
  await expect(cancel).toBeVisible();
  const box = await cancel.boundingBox();
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(390);
  await cancel.click();
  await expect(input).toHaveCount(0);
  expect(writes).toEqual([]);
  await edit.click();
  await input.fill("");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(input).toHaveCount(0);
  expect(writes[0].name).toBe("");
});

test("unenrolled devices never offer a Domain tab or render its direct URL", async ({ page }) => {
  const devices = structuredClone(deviceFixture);
  const device = Object.values(devices).find((entry) => entry.name === "Windows-PC");
  Object.assign(device, { inventory_sources: ["ddm"], ddm_device_id: "known-to-ddm", ddm_enrolment_state: "UNENROLLED", ddm_domain_id: null });
  await serveWebapp(page, { devices });
  await page.goto("http://netaudio.test/devices/Windows-PC/status");
  await expect(page.getByRole("link", { name: "Domain", exact: true })).toHaveCount(0);
  await expect(page.getByRole("combobox", { name: "Device section" }).locator('option[value="domain"]')).toHaveCount(0);
  await page.goto("http://netaudio.test/devices/Windows-PC/domain");
  await expect(page.getByText("This section is unavailable for this device.", { exact: true })).toBeVisible();
  await expect(page.getByText("Enrolment state", { exact: true })).toHaveCount(0);
  await page.evaluate(async () => {
    const store = await import("/store.js");
    const current = store.devices.value["Windows-PC.local."];
    store.devices.value = { ...store.devices.value, "Windows-PC.local.": { ...current, ddm_enrolment_state: "ENROLLED", ddm_domain_id: "test-domain" } };
  });
  await expect(page.getByRole("link", { name: "Domain", exact: true })).toBeVisible();
});
