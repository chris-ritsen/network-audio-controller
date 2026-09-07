import { test, expect } from "@playwright/test";
import { serveWebapp } from "./fixture.mjs";

test("events stay readable as updates arrive and live mode pauses while filtering", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/events");
  await page.evaluate(async () => {
    const { events } = await import("/store.js");
    events.value = [{ received: 1000, payload: { event: "device_discovered", device_name: "First device" } }];
  });
  await page.getByRole("button", { name: "Show new events", exact: true }).click();
  await expect(page.getByRole("button", { name: "Show new events", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Show new events", exact: true })).toBeDisabled();
  await expect(page.locator(".event-log")).toContainText("First device");
  await page.evaluate(async () => {
    const { events } = await import("/store.js");
    events.value = Array.from({ length: 400 }, (_, index) => ({ received: 2000 + index, payload: { event: "device_updated", device_name: `New device ${index}` } }));
  });
  await expect(page.locator(".event-log")).toContainText("First device");
  await expect(page.locator(".event-log")).not.toContainText("New device");
  await expect(page.locator("#content")).not.toContainText("400 of 400");
  await page.getByRole("button", { name: "Show new events", exact: true }).click();
  await expect(page.locator(".event-log")).toContainText("New device 0");
  const live = page.getByRole("checkbox", { name: "Live updates", exact: true });
  const before = await live.boundingBox();
  await live.check();
  await expect(live).toBeChecked();
  await expect(page.getByRole("button", { name: "Show new events", exact: true })).toBeDisabled();
  expect((await live.boundingBox()).x).toBe(before.x);
  await page.getByRole("searchbox", { name: "Filter events" }).fill("no matching device");
  await expect(live).not.toBeChecked();
  await expect(page.getByText("No matching events.", { exact: true })).toBeVisible();
});

test("snapshot counts distinguish online devices from retained offline records", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/events");
  await page.evaluate(async () => {
    const { events } = await import("/store.js");
    events.value = [{ received: 1000, payload: { event: "snapshot",
      devices: { online: { online: true }, offline: { online: false }, unknown: {} },
      shure_devices: { first: { online: false }, second: { online: false } },
    } }];
  });
  await page.getByRole("button", { name: "Show new events", exact: true }).click();
  await expect(page.locator(".event-log")).toContainText("1 Dante devices online · 0 Shure devices online");
  await expect(page.locator(".event-log")).not.toContainText("2 Shure devices");
});
