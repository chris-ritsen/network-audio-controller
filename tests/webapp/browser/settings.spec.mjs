import { test, expect } from "@playwright/test";
import { readFile } from "node:fs/promises";

const root = new URL("../../../packages/netaudio/src/netaudio/daemon/http/webapp/", import.meta.url);

test("metering settings fit the viewport and save only an edited port", async ({ page }) => {
  const writes = [];
  let settings = { monitoring_port: 8752, active_monitoring_port: 8752 };
  await page.route("**/*", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (url.hostname !== "netaudio.test") return route.abort();
    const json = (value) => route.fulfill({ contentType: "application/json", body: JSON.stringify(value) });
    if (url.pathname === "/events") return route.fulfill({ contentType: "text/event-stream", body: 'data: {"event":"snapshot","devices":{}}\n\n' });
    if (url.pathname === "/settings/monitoring") {
      writes.push(request.postDataJSON());
      settings = { monitoring_port: writes.at(-1).port, active_monitoring_port: writes.at(-1).port };
      return json(settings);
    }
    if (url.pathname === "/settings" && request.headers().accept === "application/json") return json(settings);
    const path = url.pathname === "/settings" ? "index.html" : url.pathname.slice(1);
    try {
      return route.fulfill({ body: await readFile(new URL(path, root)), contentType: path.endsWith(".js") ? "text/javascript" : path.endsWith(".css") ? "text/css" : "text/html" });
    } catch { return route.fulfill({ status: 404 }); }
  });
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://netaudio.test/settings");
  await expect(page.getByRole("heading", { name: "Metering", exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Monitoring", exact: true })).toHaveCount(0);
  const port = page.getByRole("spinbutton", { name: "UDP port" });
  const save = page.getByRole("button", { name: "Save", exact: true });
  await expect(port).toHaveValue("8752");
  await expect(save).toBeDisabled();
  const fieldBox = await port.boundingBox();
  const saveBox = await save.boundingBox();
  expect(saveBox.x).toBe(fieldBox.x);
  expect(saveBox.y).toBeGreaterThan(fieldBox.y + fieldBox.height);
  expect(fieldBox.width).toBeLessThanOrEqual(160);
  await port.fill("8760");
  await expect(save).toBeEnabled();
  await save.click();
  await expect(save).toBeDisabled();
  expect(writes).toEqual([{ port: 8760 }]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect((await port.boundingBox()).x + (await port.boundingBox()).width).toBeLessThanOrEqual(390);
  expect((await save.boundingBox()).height).toBe(32);
  await page.screenshot({ path: "test-results/settings-mobile.png" });
});
