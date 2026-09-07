import { test, expect } from "@playwright/test";
import { deviceFixture, serveWebapp } from "./fixture.mjs";

const [id, device] = Object.entries(deviceFixture).find(([, record]) => record.online);
const xml = '<preset><name>Show</name></preset>';
const preview = { name: "Show", digest: "not-shown-in-ui", devices: [{ name: device.name,
  settings: [{ label: "Preferred leader", value: "On" }], unsupported: [],
  targets: [{ id, name: device.name, address: device.ipv4, online: true }] }] };

async function setup(page, { data = preview, result } = {}) {
  await serveWebapp(page);
  const writes = [];
  await page.route("**/presets/*", async (route) => {
    const path = new URL(route.request().url()).pathname;
    writes.push({ path, body: route.request().postDataJSON() });
    return route.fulfill({ contentType: "application/json", body: JSON.stringify(path.endsWith("preview") ? data
      : path.endsWith("save") ? { filename: "Show.xml", xml }
        : result || { complete: true, report: { results: [[device.name, "preferred leader on (verified)"]], needs_reboot: [] } }) });
  });
  await page.goto("http://netaudio.test/presets");
  await expect(page.getByRole("heading", { name: "Presets", exact: true })).toBeVisible();
  return writes;
}

async function open(page) {
  await page.getByLabel("Preset XML file").setInputFiles({ name: "Show.xml", mimeType: "application/xml", buffer: Buffer.from(xml) });
  await expect(page.getByRole("heading", { name: "Show", exact: true })).toBeVisible();
}

test("save downloads XML with explicit devices and routing-only defaults", async ({ page }) => {
  const writes = await setup(page);
  const download = page.getByRole("button", { name: "Download XML" });
  await expect(download).toBeDisabled();
  await page.getByLabel("Preset name", { exact: true }).fill("Show");
  await page.getByRole("group", { name: "Devices in this view" }).getByRole("checkbox").first().check();
  const event = page.waitForEvent("download");
  await download.click();
  expect((await event).suggestedFilename()).toBe("Show.xml");
  expect(writes).toHaveLength(1);
  expect(writes[0].path).toBe("/presets/save");
  expect(writes[0].body.sections).toEqual(["routing"]);
  expect(writes[0].body.devices).toHaveLength(1);
});

test("opening previews only; apply requires confirmation and shows verification", async ({ page }) => {
  const writes = await setup(page);
  await open(page);
  const apply = page.getByRole("button", { name: "Apply to 1 device", exact: true });
  await expect(apply).toBeDisabled();
  expect(writes.map((write) => write.path)).toEqual(["/presets/preview"]);
  await page.getByLabel("I have reviewed", { exact: false }).check();
  await apply.click();
  await expect(page.getByText("Preset applied and verified", { exact: true })).toBeVisible();
  expect(writes[1].body).toEqual({ xml, digest: preview.digest, confirmed: true, confirm_destructive: false, targets: { [device.name]: id }, excluded: [] });
  await expect(apply).toBeDisabled();
  await expect(page.locator("#content")).not.toContainText(preview.digest);
});

test("missing entries must be explicitly skipped; partial results are honest", async ({ page }) => {
  const data = structuredClone(preview);
  data.devices.push({ name: "Missing", settings: [], unsupported: [], targets: [] });
  const writes = await setup(page, { data, result: { complete: false, report: { results: [[device.name, "Change requested; readback unavailable"]], needs_reboot: [device.name] } } });
  await open(page);
  await expect(page.getByLabel("I have reviewed", { exact: false })).toBeDisabled();
  await page.getByRole("checkbox", { name: "Missing", exact: true }).uncheck();
  await page.getByLabel("I have reviewed", { exact: false }).check();
  await page.getByRole("button", { name: "Apply to 1 device", exact: true }).click();
  await expect(page.getByText("Preset not fully applied or verified", { exact: true })).toBeVisible();
  expect(writes[1].body.excluded).toEqual(["Missing"]);
  await expect(page.getByText("Reboot pending:", { exact: false })).toBeVisible();
});

test("a context change clears the review and confirmation", async ({ page }) => {
  const writes = await setup(page);
  await open(page);
  await page.getByLabel("I have reviewed", { exact: false }).check();
  await page.evaluate(async () => { const store = await import("/store.js"); store.selectContext("local"); });
  await expect(page.getByRole("heading", { name: "Show", exact: true })).toHaveCount(0);
  expect(writes).toHaveLength(1);
});

test("mobile preset review fits the viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await setup(page);
  await open(page);
  const hide = page.getByRole("button", { name: "Hide navigation", exact: true });
  if (await hide.count()) await hide.click();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.getByRole("heading", { name: "Load preset", exact: true }).scrollIntoViewIfNeeded();
  await page.screenshot({ path: `test-results/preset-review-${test.info().project.name}.png`, fullPage: true });
});
