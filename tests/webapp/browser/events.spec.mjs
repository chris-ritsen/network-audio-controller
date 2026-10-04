import { test, expect } from "@playwright/test";
import { serveWebapp } from "./fixture.mjs";

const eventJournal = {
  schema_version: 1,
  retention_limit: 1000,
  count: 3,
  events: [
    {
      sequence: 3,
      timestamp: "2026-09-12T18:32:03Z",
      kind: "device_disappeared",
      severity: "error",
      device_identity: "stagebox-1",
      device_name: "Stagebox-1",
      server_name: "stagebox-1.local.",
      interface_identity: null,
      channel_identity: null,
      flow_identity: null,
      previous_value: true,
      current_value: false,
      raw: { current_observation: { online: false } },
      observation_source: "device_lifecycle",
      derivation_status: "observed",
    },
    {
      sequence: 2,
      timestamp: "2026-09-12T18:31:02Z",
      kind: "late_packet_count_increased",
      severity: "warning",
      device_identity: "desk-main",
      device_name: "Desk-Main",
      server_name: "desk-main.local.",
      interface_identity: null,
      channel_identity: null,
      flow_identity: "receive flow 1",
      previous_value: 2,
      current_value: 5,
      raw: { delta: 3 },
      observation_source: "connection_health",
      derivation_status: "observed",
    },
    {
      sequence: 1,
      timestamp: "2026-09-12T18:30:01Z",
      kind: "device_reappeared",
      severity: "info",
      device_identity: "desk-main",
      device_name: "Desk-Main",
      server_name: "desk-main.local.",
      interface_identity: null,
      channel_identity: null,
      flow_identity: null,
      previous_value: false,
      current_value: true,
      raw: { current_observation: { online: true } },
      observation_source: "device_lifecycle",
      derivation_status: "observed",
    },
  ],
};

test("event log uses Controller-style columns", async ({ page }) => {
  await serveWebapp(page, { eventJournal });
  await page.goto("http://netaudio.test/events");

  const table = page.getByRole("table", { name: "Event log" });
  await expect(table.getByRole("columnheader")).toHaveText([
    "Severity",
    "Timestamp",
    "Device Name",
    "Event",
  ]);
});

test("Clear removes only local history", async ({ page }) => {
  let clearRequests = 0;
  await serveWebapp(page, {
    eventJournal,
    onClearEventJournal: () => {
      clearRequests += 1;
    },
  });
  await page.goto("http://netaudio.test/events");

  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Clear", exact: true }).click();
  await expect.poll(() => clearRequests).toBe(1);
});
