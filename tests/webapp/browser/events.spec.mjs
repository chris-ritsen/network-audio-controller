import { readFile } from "node:fs/promises";

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

test("event log uses Controller-style columns and cumulative severity filters", async ({
  page,
}) => {
  await serveWebapp(page, { eventJournal });
  await page.goto("http://netaudio.test/events");

  const table = page.getByRole("table", { name: "Event log" });
  await expect(table.getByRole("columnheader")).toHaveText([
    "Severity",
    "Timestamp",
    "Device Name",
    "Event",
  ]);
  await expect(table.getByRole("row")).toHaveCount(4);
  await expect(table).toContainText("Device reappeared · no → yes");

  const severity = page.getByRole("combobox", { name: "Minimum severity" });
  await severity.selectOption("warning");
  await expect(table.getByRole("row")).toHaveCount(3);
  await expect(table).not.toContainText("Device reappeared");
  await expect(table).toContainText("Late packet count increased");
  await expect(table).toContainText("Device disappeared");

  await severity.selectOption("error");
  await expect(table.getByRole("row")).toHaveCount(2);
  await expect(table).not.toContainText("Late packet count increased");

  await severity.selectOption("info");
  await page
    .getByRole("searchbox", { name: "Search event log" })
    .fill("stagebox");
  await expect(table.getByRole("row")).toHaveCount(2);
  await expect(
    page.getByText("1 of 3 retained", { exact: true }),
  ).toBeVisible();
});

test("event rows expose the transition and supporting evidence", async ({
  page,
}) => {
  await serveWebapp(page, { eventJournal });
  await page.goto("http://netaudio.test/events");

  await page
    .getByRole("button", {
      name: "View details for Late packet count increased · receive flow 1 · 2 → 5",
    })
    .click();
  const details = page.getByRole("region", { name: "Event details" });
  await expect(details).toContainText("Desk-Main");
  await expect(details).toContainText("receive flow 1");
  await expect(details).toContainText("2 → 5");
  await expect(details).toContainText("Observed from Connection health");
  await details.getByText("Raw supporting evidence").click();
  await expect(details).toContainText('"delta": 3');
});

test("configuration operations show lifecycle and correlation evidence", async ({
  page,
}) => {
  const operation = {
    ...eventJournal,
    count: 1,
    events: [
      {
        sequence: 4,
        timestamp: "2026-09-12T18:33:04Z",
        kind: "configuration_operation",
        severity: "warning",
        device_identity: "desk-main.local.",
        device_name: "Desk-Main",
        server_name: "desk-main.local.",
        interface_identity: null,
        channel_identity: "rx:1",
        flow_identity: null,
        previous_value: null,
        current_value: {
          operation: "subscribe_external_rtp",
          phase: "partial_unobservable",
          state: "request_acknowledged",
        },
        raw: { acknowledgement: { accepted: true, result_code: 1 } },
        observation_source: "bounded_verification",
        derivation_status: "derived",
        operation_id: "child-1",
        correlation_id: "preset-1",
        parent_preset_run_id: "preset-1",
        operation_name: "subscribe_external_rtp",
        lifecycle_phase: "partial_unobservable",
        requested_values: { receiver_channel_ids: [1] },
        acknowledgement_result_code: 1,
        transport: "preset",
        effective_values: null,
        final_operation_state: "request_acknowledged",
        persistence_request_acknowledgement: null,
        persistence_confirmation: null,
      },
    ],
  };
  await serveWebapp(page, { eventJournal: operation });
  await page.goto("http://netaudio.test/events");

  await page
    .getByRole("button", {
      name: "View details for Subscribe external RTP · partial or unobservable · rx:1",
    })
    .click();
  const details = page.getByRole("region", { name: "Event details" });
  await expect(details).toContainText("child-1");
  await expect(details).toContainText("preset-1");
  await expect(details).toContainText("request acknowledged");
  await expect(details).toContainText('"receiver_channel_ids":[1]');
});

test("Save exports every retained event and Clear removes only local history", async ({
  page,
}) => {
  let clearRequests = 0;
  await serveWebapp(page, {
    eventJournal,
    onClearEventJournal: () => {
      clearRequests += 1;
    },
  });
  await page.goto("http://netaudio.test/events");
  await page
    .getByRole("combobox", { name: "Minimum severity" })
    .selectOption("error");

  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  const download = await downloadPromise;
  expect(download.suggestedFilename()).toMatch(/^netaudio-events-.*\.json$/);
  const exported = JSON.parse(await readFile(await download.path(), "utf8"));
  expect(exported.count).toBe(3);
  expect(exported.retention_limit).toBe(1000);
  expect(exported.events.map((entry) => entry.sequence)).toEqual([3, 2, 1]);

  page.once("dialog", async (dialog) => {
    expect(dialog.message()).toContain(
      "Device state and counters will not be changed.",
    );
    await dialog.accept();
  });
  await page.getByRole("button", { name: "Clear", exact: true }).click();
  await expect(
    page.getByText("No events match the current filters.", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("0 of 0 retained", { exact: true }),
  ).toBeVisible();
  expect(clearRequests).toBe(1);
});

test("issues remain available behind the secondary tab", async ({ page }) => {
  await serveWebapp(page, {
    eventJournal,
    issues: {
      schema_version: 1,
      count: 1,
      active_count: 1,
      issues: [
        {
          issue_id: "issue-1",
          title: "Receiver latency high",
          severity: "warning",
          state: "open",
          summary: "Peak latency crossed the configured threshold.",
          scope: { device_name: "Desk-Main", flow_identity: "receive flow 1" },
          evidence_class: "derived",
          evidence_source: "connection_health",
          observation_state: "current",
          occurrence_count: 2,
          suggested_remediation: "Inspect the network path.",
        },
      ],
    },
  });
  await page.goto("http://netaudio.test/events");
  await page.getByRole("button", { name: "Issues", exact: true }).click();
  await expect(
    page.getByText("Receiver latency high", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Inspect the network path.", { exact: false }),
  ).toBeVisible();
});
