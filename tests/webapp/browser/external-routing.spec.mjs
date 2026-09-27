import { test, expect } from "@playwright/test";
import { serveWebapp } from "./fixture.mjs";

test("SAP source slot two routes to receiver seven without losing session identity", async ({
  page,
}, testInfo) => {
  const source = {
    source_ipv4: "192.0.2.44",
    session_id: "9007199254740993",
    flow_name: "External program",
    channel_count: 2,
    routable: true,
    direction: "sendonly",
    encoding: "L24",
    sample_rate: 48000,
    primary_destination: { address: "239.69.1.10", port: 5004 },
    content_sha256: "test-announcement",
    expires_at: "2099-01-01T00:00:00Z",
  };
  const devices = {
    "receiver.local.": {
      server_name: "receiver.local.",
      name: "Receiver",
      online: true,
      availability_state: "online",
      management_state: "unmanaged",
      ipv4: "192.0.2.10",
      channels: { receivers: { 7: { name: "Program" } }, transmitters: {} },
      subscriptions: [],
    },
  };
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.addInitScript(() => {
    localStorage.setItem("netaudio.matrix.flipped", "false");
    localStorage.setItem(
      "netaudio.matrix.expanded",
      JSON.stringify({ receivers: ["Receiver"], transmitters: [] }),
    );
  });
  await serveWebapp(page, {
    devices,
    externalFlows: { "192.0.2.44/9007199254740993": source },
  });
  const writes = [];
  await page.route("**/external-flows/subscribe", (route) => {
    writes.push(route.request().postDataJSON());
    return route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        success: true,
        request_acknowledged: true,
        arc_effective_state_confirmed: null,
        decoded_audio_confirmed: null,
      }),
    });
  });
  await page.goto("http://netaudio.test/routing");
  await expect(page.locator(".matrix-canvas")).toBeVisible();
  const viewport = page.locator(".matrix-viewport");
  const geometry = await viewport.evaluate((node) => ({
    gutter: Number(node.dataset.gutterWidth),
    header: Number(node.dataset.headerHeight),
    cell: Number(node.dataset.cellSize),
  }));
  await viewport.click({
    position: {
      x: geometry.gutter + geometry.cell * 2.5,
      y: geometry.header + geometry.cell * 1.5,
    },
  });
  await expect.poll(() => writes.length).toBe(1);
  expect(writes[0]).toEqual({
    rx_device: "receiver.local.",
    source_ipv4: source.source_ipv4,
    session_id: source.session_id,
    content_sha256: source.content_sha256,
    receiver_channel_ids: [7],
    flow_slot_assignments: [2],
  });
  await testInfo.attach("request.json", {
    body: JSON.stringify(writes[0], null, 2),
    contentType: "application/json",
  });
  await page.screenshot({ path: testInfo.outputPath("external-routing.png") });
});
