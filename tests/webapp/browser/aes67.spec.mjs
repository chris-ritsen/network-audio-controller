import { test, expect } from "@playwright/test";
import { deviceFixture, serveWebapp } from "./fixture.mjs";

function inventoryWith(state) {
  const source = structuredClone(Object.values(deviceFixture).find((device) => device.name === "avio-bt-1"));
  for (const key of Object.keys(source)) if (key.startsWith("aes67_") || key.startsWith("ddm_")) delete source[key];
  Object.assign(source, state);
  return { [source.server_name]: source };
}

test("AES67 readiness views make no writes and unknown support has no controls", async ({ page }) => {
  await serveWebapp(page, { devices: inventoryWith({}) });
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto("http://netaudio.test/devices/avio-bt-1/aes67-config");
  await expect(page.locator("#content").getByRole("status")).toHaveText("Not reported");
  await expect(page.locator("#content").getByRole("button")).toHaveCount(0);
  await expect(page.getByLabel("AES67 multicast address prefix")).toHaveCount(0);
  expect(writes).toEqual([]);
});

test("pending AES67 changes show current and configured modes on desktop and mobile", async ({ page }) => {
  await serveWebapp(page, { devices: inventoryWith({ aes67_supported: true, aes67_current: false, aes67_configured: true }) });
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto("http://netaudio.test/devices/avio-bt-1/aes67-config");
  await expect(page.locator("#content").getByRole("status")).toHaveText("Enable pending");
  await expect(page.locator("#content dl")).toContainText("Current modeDisabledConfigured modeEnabled");
  await expect(page.getByRole("button", { name: "Enable", exact: true })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Disable", exact: true })).toBeEnabled();
  await expect(page.getByRole("button", { name: "Reboot", exact: true })).toHaveCount(0);
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.locator("#content").getByRole("status")).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  expect(writes).toEqual([]);
});

test("enrolled devices show DDM RTP readiness without local configuration buttons", async ({ page }) => {
  await serveWebapp(page, { devices: inventoryWith({
    aes67_supported: true, aes67_current: true,
    ddm_enrolment_state: "ENROLLED", ddm_domain_id: "test-domain", ddm_domain_name: "Test domain",
    ddm_capabilities: { rtp_audio_supported: true, rtp_audio_support_suppressed: true },
  }) });
  const writes = [];
  page.on("request", (request) => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto("http://netaudio.test/devices/avio-bt-1/aes67-config");
  await expect(page.locator("#content").getByRole("status")).toHaveText("Managed by DDM");
  await expect(page.locator("#content dl")).toContainText("RTP flowsReboot required");
  await expect(page.locator("#content").getByRole("button")).toHaveCount(0);
  await expect(page.getByLabel("AES67 multicast address prefix")).toHaveCount(0);
  expect(writes).toEqual([]);
});

test("known direct devices retain explicit mode and multicast-prefix actions", async ({ page }) => {
  await serveWebapp(page, { devices: inventoryWith({ aes67_supported: true, aes67_current: false, aes67_configured: false, aes67_multicast_prefix: "239.69.0.0" }) });
  const writes = [];
  await page.route("**/set-aes67", (route) => {
    writes.push(route.request().postDataJSON());
    return route.fulfill({ contentType: "application/json", body: '{"success":true}' });
  });
  await page.route("**/set-aes67-multicast-prefix", (route) => {
    writes.push(route.request().postDataJSON());
    return route.fulfill({ contentType: "application/json", body: '{"success":true}' });
  });
  await page.goto("http://netaudio.test/devices/avio-bt-1/aes67-config");
  await expect(page.getByRole("button", { name: "Disable", exact: true })).toBeDisabled();
  expect(writes).toEqual([]);
  await page.getByRole("button", { name: "Enable", exact: true }).click();
  await page.getByLabel("AES67 multicast address prefix").fill("239.238.0.0");
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  expect(writes).toEqual([
    { device: "avio-bt-1.local.", enabled: true },
    { device: "avio-bt-1.local.", prefix: "239.238.0.0" },
  ]);
});
