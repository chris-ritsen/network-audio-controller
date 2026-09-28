import { test, expect } from "@playwright/test";
import { serveWebapp } from "./fixture.mjs";

test("failed startup shows a recoverable error and reload renders the app", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await serveWebapp(page);
  let release;
  const pending = new Promise((resolve) => { release = resolve; });
  const holdModule = async (route) => { await pending; await route.abort(); };
  await page.route("**/app.js", holdModule);
  await page.goto("http://netaudio.test/", { waitUntil: "domcontentloaded" });
  try {
    await expect(page.locator("#startup")).toBeHidden();
  } finally {
    release();
  }
  await expect(page.getByText("Unable to load NetAudio")).toBeVisible();
  await page.unroute("**/app.js", holdModule);
  const failModule = (route) => route.abort();
  await page.route("**/app.js", failModule);
  await page.goto("http://netaudio.test/");
  await expect(page.getByText("Unable to load NetAudio")).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("startup-error.png") });
  await page.unroute("**/app.js", failModule);
  await page.getByRole("button", { name: "Reload" }).click();
  await expect(page.locator(".topbar")).toBeVisible();
  await expect(page.locator("#startup")).toHaveCount(0);
  for (let reload = 0; reload < 3; reload += 1) {
    await page.reload();
    await expect(page.locator(".topbar")).toBeVisible();
    await expect(page.locator("#startup")).toHaveCount(0);
  }
  await page.screenshot({ path: testInfo.outputPath("reloaded.png") });
});
