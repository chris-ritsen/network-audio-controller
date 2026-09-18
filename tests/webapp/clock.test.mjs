import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP, fixture } from "./setup.mjs";
const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const { StatusSection } = await import(`${WEBAPP}device/status.js`);
const { DeviceConfigSection } = await import(`${WEBAPP}device/config.js`);
const { clockLeaderName } = await import(`${WEBAPP}format.js`);

function device() {
  return {
    ...Object.values(fixture("devices"))[0],
    management_state: "unmanaged",
    ddm_enrolled: false,
    online: true,
    clock_observed_at: new Date().toISOString(),
    clock_status: {
      status_supported: true,
      synchronization: "lost",
      servo_state_code: 2,
      servo_state: "synchronizing",
      mute_flags: 3,
      mute_reasons: ["synchronization loss", "external-clock problem"],
      clock_capabilities: 0x204,
      preferred_leader_locked: true,
      clock_port_records: [
        {
          record_number: 1,
          ptp_version: 1,
          state: "follower",
          state_code: 9,
          transport_path: "multicast",
          network_interface_index: null,
          link_down: null,
          user_disabled: null,
          unicast_delay_requests: null,
        },
      ],
    },
    ptpv1_device_uuid: "010203040506",
    ptpv1_master_uuid: "0708090a0b0c",
    ptpv1_grandmaster_uuid: "0d0e0f101112",
  };
}

test("clock details keep identities and unavailable flags separate", () => {
  const html = render(h(StatusSection, { device: device() }));
  for (const text of [
    "010203040506",
    "0708090a0b0c",
    "0d0e0f101112",
    "external-clock problem",
    "interface unavailable",
    "link unavailable",
  ])
    assert.ok(html.includes(text), text);
});

test("stale clock status is unavailable and disables direct configuration", () => {
  const stale = { ...device(), clock_observed_at: "2000-01-01T00:00:00Z" };
  const html = render(h(StatusSection, { device: stale }));
  assert.match(html, /Synchronization<[^]*?unavailable/);
  const form = render(h(DeviceConfigSection, { device: stale }));
  assert.match(form, /Refresh clock status to check/);
});

test("a MAC address cannot substitute for a reported PTPv1 UUID", () => {
  const clock = device();
  const candidate = { name: "Unrelated", mac_address: clock.ptpv1_master_uuid };
  assert.notEqual(clockLeaderName(clock, { candidate }), "Unrelated");
});
