import { test, expect } from "@playwright/test";
import { serveWebapp } from "./fixture.mjs";

function device(managed = true) {
  return {
    server_name: managed ? "ddm:manager:domain:device" : "device.local.",
    name: "Studio adapter",
    online: true,
    inventory_sources: managed ? ["ddm"] : ["direct"],
    management_state: managed ? "managed" : null,
    ddm_enrolment_state: managed ? "ENROLLED" : null,
    ddm_domain_id: managed ? "domain" : null,
    channels: { receivers: {}, transmitters: {} },
    sample_rate_hz: 48000,
    supported_sample_rates_hz: [44100, 48000, 88200, 96000],
    encoding: 24,
    supported_encodings: [16, 24, 32],
    configured_latency_ms: 1,
    standard_latency_choices_ms: [1, 2, 5],
    min_latency_ms: 1,
    max_latency_ms: 20.3125,
  };
}

for (const managed of [true, false]) {
  test(`${managed ? "enrolled" : "unenrolled"} configuration sends settings to the displayed device`, async ({ page }) => {
    const record = device(managed);
    await serveWebapp(page, { devices: { [record.server_name]: record } });
    const writes = [];
    for (const path of ["set-latency", "set-sample-rate", "set-encoding", "refresh"]) {
      await page.route(`**/${path}`, (route) => {
        writes.push({ path, ...route.request().postDataJSON() });
        return route.fulfill({ contentType: "application/json", body: '{"success":true}' });
      });
    }
    await page.goto(`http://netaudio.test/devices/${encodeURIComponent(record.server_name)}/device-config`);
    const row = (label) => page.locator(".field-row").filter({ has: page.getByText(label, { exact: true }) });
    await expect(page.getByLabel("Sample rate", { exact: true })).toHaveValue("48000");
    await expect(page.getByLabel("Encoding", { exact: true })).toHaveValue("24");
    await expect(page.getByLabel("Latency", { exact: true })).toHaveValue("1");
    if (managed) {
      await expect(row("Clock subdomain")).toContainText("Managed by DDM");
      await expect(row("Clock subdomain").getByRole("textbox")).toHaveCount(0);
    }
    await page.getByLabel("Latency", { exact: true }).selectOption("2");
    await row("Latency").getByRole("button", { name: "Apply", exact: true }).click();
    await page.getByLabel("Sample rate", { exact: true }).selectOption("96000");
    await row("Sample rate").getByRole("button", { name: "Apply", exact: true }).click();
    await page.getByLabel("Encoding", { exact: true }).selectOption("32");
    await row("Encoding").getByRole("button", { name: "Apply", exact: true }).click();
    await page.getByRole("button", { name: "Refresh settings", exact: true }).click();
    await expect.poll(() => writes.length).toBe(4);
    expect(writes).toEqual([
      { path: "set-latency", device: record.server_name, latency: 2 },
      { path: "set-sample-rate", device: record.server_name, sample_rate: 96000, confirm_destructive: false },
      { path: "set-encoding", device: record.server_name, encoding: 32 },
      { path: "refresh", device: record.server_name },
    ]);
  });
}

test("custom latency respects the reported range", async ({ page }) => {
  const record = device();
  await serveWebapp(page, { devices: { [record.server_name]: record } });
  const writes = [];
  await page.route("**/set-latency", (route) => {
    writes.push(route.request().postDataJSON());
    return route.fulfill({ contentType: "application/json", body: '{"success":true}' });
  });
  await page.goto(`http://netaudio.test/devices/${encodeURIComponent(record.server_name)}/device-config`);
  await page.getByLabel("Latency", { exact: true }).selectOption("custom");
  const input = page.getByLabel("Custom latency in milliseconds");
  await input.fill("21");
  const apply = page.locator(".field-row").filter({ has: input }).getByRole("button", { name: "Apply", exact: true });
  await apply.click();
  await expect(page.getByRole("alert")).toContainText("Enter a latency from 1 to 20.3125 ms");
  expect(writes).toEqual([]);
  await input.fill("10");
  await apply.click();
  await expect.poll(() => writes.length).toBe(1);
  expect(writes[0]).toEqual({ device: record.server_name, latency: 10 });
});

test("sample rate requires a separate action for reported channel loss", async ({ page }) => {
  const record = device();
  await serveWebapp(page, { devices: { [record.server_name]: record } });
  const writes = [];
  await page.route("**/set-sample-rate", (route) => {
    const body = route.request().postDataJSON();
    writes.push(body);
    return route.fulfill({
      contentType: "application/json",
      status: body.confirm_destructive ? 200 : 409,
      body: JSON.stringify(body.confirm_destructive ? { success: true } : {
        error: "Confirmation required",
        preflight: {
          requires_destructive_confirmation: true,
          is_classified: true,
          topology_characterized: true,
          target_sample_rate_hertz: 96000,
          destructive_transmitter_membership_loss: [{ flow_number: 1, removed_channel_members: [17, 18] }],
        },
      }),
    });
  });
  await page.goto(`http://netaudio.test/devices/${encodeURIComponent(record.server_name)}/device-config`);
  await page.getByLabel("Sample rate", { exact: true }).selectOption("96000");
  const row = page.locator(".field-row").filter({ has: page.getByText("Sample rate", { exact: true }) });
  await row.getByRole("button", { name: "Apply", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Flow 1: channels 17, 18");
  expect(writes).toHaveLength(1);
  expect(writes[0].confirm_destructive).toBe(false);
  await page.getByRole("button", { name: "Change rate and remove channels", exact: true }).click();
  await expect.poll(() => writes.length).toBe(2);
  expect(writes[1]).toEqual({ device: record.server_name, sample_rate: 96000, confirm_destructive: true });
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("pull-up choices have tuning labels and omit unknown values", async ({ page }) => {
  const record = { ...device(), supported_sample_rate_pullup_raw_values: [0, 1, 2, 3, 4, 99], sample_rate_pullup_raw_value: 0 };
  await serveWebapp(page, { devices: { [record.server_name]: record } });
  await page.goto(`http://netaudio.test/devices/${encodeURIComponent(record.server_name)}/device-config`);
  const row = page.locator(".field-row").filter({ has: page.getByText("Sample rate pull-up", { exact: true }) });
  await expect(row.locator("option")).toHaveText(["None", "+4.1667%", "+0.1%", "−0.1%", "−4.0%"]);
});

test("missing managed settings offer refresh without an invented latency", async ({ page }) => {
  const record = device();
  for (const field of ["configured_latency_ms", "standard_latency_choices_ms", "min_latency_ms", "max_latency_ms", "supported_sample_rates_hz", "supported_encodings"]) delete record[field];
  await serveWebapp(page, { devices: { [record.server_name]: record } });
  await page.goto(`http://netaudio.test/devices/${encodeURIComponent(record.server_name)}/device-config`);
  await expect(page.getByRole("button", { name: "Refresh settings", exact: true })).toBeVisible();
  await expect(page.getByLabel("Latency", { exact: true })).toHaveCount(0);
  await expect(page.getByText("Settings unavailable", { exact: true })).toBeVisible();
  await expect(page.locator("#content")).not.toContainText("not configurable on this device");
});
