import { test, expect } from "@playwright/test";
import { serveWebapp } from "./fixture.mjs";

const devices = Object.fromEntries(["Live", "Gone", "Enrolled"].map((name) => [name, {
  name, server_name: name, online: name === "Live", rx_count: 1, tx_count: 1,
  channels: { receivers: { 1: { name: "Input" } }, transmitters: { 1: { name: "Output" } } },
  subscriptions: [],
  ...(name === "Enrolled" ? { management_state: "managed", ddm_domain_id: "domain", inventory_sources: ["ddm"] } : {}),
}]));

test("offline devices stay excluded across views, search and reload without availability controls", async ({ page }) => {
  await serveWebapp(page, { devices });
  await page.goto("http://netaudio.test/devices");
  const toggle = page.getByRole("button", { name: "Hide offline devices", exact: true });
  await expect(toggle).toHaveCount(0);
  await expect(page.locator("#content")).toContainText("Live");
  await expect(page.locator("#content")).not.toContainText("Gone");
  await expect(page.locator("#content")).not.toContainText("Enrolled");
  await page.reload();
  for (const path of ["/routing", "/subscriptions", "/presets", "/ddm"]) {
    await page.goto(`http://netaudio.test${path}`);
    await expect(toggle).toHaveCount(0);
    await expect(page.locator("summary").filter({ hasText: /^Availability$/ })).toHaveCount(0);
    await expect(page.locator("#content")).not.toContainText("Gone");
    await expect(page.locator("#content")).not.toContainText("Enrolled");
  }
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await expect(page.getByRole("dialog")).not.toContainText("Gone");
  await expect(page.getByRole("dialog")).not.toContainText("Enrolled");
  await page.keyboard.press("Escape");
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(toggle).toHaveCount(0);
  await expect(page.locator("#content")).not.toContainText("Enrolled");
  await expect(page.locator("#content")).not.toContainText("Gone");
});

test("an empty authoritative snapshot clears remembered inventory", async ({ page }) => {
  await serveWebapp(page, { devices: {} });
  await page.addInitScript((records) => localStorage.setItem("netaudio.inventory.v1", JSON.stringify({ savedAt: Date.now(), devices: records })), devices);
  await page.goto("http://netaudio.test/devices");
  await expect.poll(() => page.evaluate(() => JSON.parse(localStorage.getItem("netaudio.inventory.v1")).devices)).toEqual({});
  await expect(page.locator("#content")).not.toContainText("Live");
});
