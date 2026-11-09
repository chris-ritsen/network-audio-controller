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
    operation_availability: {
      identify: {
        supported: true,
        readable: false,
        writable: true,
        reasons: [],
      },
      sample_rate: {
        supported: true,
        readable: true,
        writable: true,
        reasons: [],
      },
      encoding: {
        supported: true,
        readable: true,
        writable: true,
        reasons: [],
      },
      sample_rate_pullup: {
        supported: true,
        readable: false,
        writable: true,
        reasons: [],
      },
    },
  };
}

test("custom latency respects the reported range", async ({ page }) => {
  const record = device();
  await serveWebapp(page, { devices: { [record.server_name]: record } });
  const writes = [];
  await page.route("**/set-latency", (route) => {
    writes.push(route.request().postDataJSON());
    return route.fulfill({
      contentType: "application/json",
      body: '{"success":true}',
    });
  });
  await page.goto(
    `http://netaudio.test/devices/${encodeURIComponent(record.server_name)}/device-config`,
  );
  await page.getByLabel("Latency", { exact: true }).selectOption("custom");
  const input = page.getByLabel("Custom latency in milliseconds");
  await input.fill("21");
  const apply = page
    .locator(".field-row")
    .filter({ has: input })
    .getByRole("button", { name: "Apply", exact: true });
  await apply.click();
  await expect(page.getByRole("alert")).toContainText(
    "Enter a latency from 1 to 20.3125 ms",
  );
  expect(writes).toEqual([]);
  await input.fill("10");
  await apply.click();
  await expect.poll(() => writes.length).toBe(1);
  expect(writes[0]).toEqual({ device: record.server_name, latency: 10 });
});

test("pull-up choices have tuning labels and omit unknown values", async ({
  page,
}) => {
  const record = {
    ...device(),
    supported_sample_rate_pullup_raw_values: [0, 1, 2, 3, 4, 99],
    sample_rate_pullup_raw_value: 0,
  };
  await serveWebapp(page, { devices: { [record.server_name]: record } });
  await page.goto(
    `http://netaudio.test/devices/${encodeURIComponent(record.server_name)}/device-config`,
  );
  const row = page
    .locator(".field-row")
    .filter({ has: page.getByText("Sample rate pull-up", { exact: true }) });
  await expect(row.locator("option")).toHaveText([
    "None",
    "+4.1667%",
    "+0.1%",
    "-0.1%",
    "-4.0%",
  ]);
});

test("missing managed settings offer refresh without an invented latency", async ({
  page,
}) => {
  const record = device();
  for (const field of [
    "configured_latency_ms",
    "standard_latency_choices_ms",
    "min_latency_ms",
    "max_latency_ms",
    "supported_sample_rates_hz",
    "supported_encodings",
  ])
    delete record[field];
  await serveWebapp(page, { devices: { [record.server_name]: record } });
  await page.goto(
    `http://netaudio.test/devices/${encodeURIComponent(record.server_name)}/device-config`,
  );
  await expect(
    page.getByRole("button", { name: "Refresh settings", exact: true }),
  ).toBeVisible();
  await expect(page.getByLabel("Latency", { exact: true })).toHaveCount(0);
  await expect(page.locator("#content")).not.toContainText(
    "not configurable on this device",
  );
});
