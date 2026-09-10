import { test, expect } from "@playwright/test";
import { serveWebapp } from "./fixture.mjs";

test("managed page omits the status statistics panel", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/ddm");
  await expect(page.getByText("Managed status", { exact: true })).toHaveCount(0);
  await expect(page.locator(".metric-row")).toHaveCount(0);
});

test("managed AVIO metering displays incoming signal levels without requesting detailed metering", async ({ page }) => {
  const device = {
    server_name: "ddm:fixture:input", name: "AVIO input", online: true,
    inventory_sources: ["ddm"], management_state: "managed", ddm_enrolment_state: "ENROLLED", ddm_domain_id: "fixture",
    channels: { receivers: {}, transmitters: { 1: { name: "CH1" }, 2: { name: "CH2" } } },
  };
  await serveWebapp(page, { devices: { [device.server_name]: device }, metering: {
    [device.server_name]: { tx: { 1: 163, 2: 42 }, rx: {}, wall_time: Date.now() / 1000, metering_source: "signal_presence" },
  } });
  const meteringRequests = [];
  page.on("request", (request) => { if (request.url().includes("/metering/start")) meteringRequests.push(request.url()); });
  await page.goto(`http://netaudio.test/devices/${encodeURIComponent(device.server_name)}/metering`);
  await expect(page.locator(".meter-bank canvas")).toHaveCount(1);
  await expect.poll(() => page.locator(".meter-bank canvas").evaluate((canvas) => canvas.width)).toBeGreaterThan(0);
  expect(meteringRequests).toEqual([]);
  await expect(page.getByRole("alert")).toHaveCount(0);
  await expect(page.getByText(/^Reported /)).toHaveCount(0);
});
