import { test, expect } from "@playwright/test";
import { serveWebapp } from "./fixture.mjs";

test("managed page omits the status statistics panel", async ({ page }) => {
  await serveWebapp(page);
  await page.goto("http://netaudio.test/ddm");
  await expect(page.getByText("Managed status", { exact: true })).toHaveCount(0);
  await expect(page.locator(".metric-row")).toHaveCount(0);
});
