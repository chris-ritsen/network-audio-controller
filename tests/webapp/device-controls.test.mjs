import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";
const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const { DeviceControls } = await import(`${WEBAPP}device/controls.js`);
function observation(value, fresh = true) {
  return { value, fresh, observed_at_unix: Date.now() / 1000 };
}
test("Bluetooth keeps link loss distinct and clearing is explicitly confirmed", () => {
  const device = {
    server_name: "bt.local.",
    device_controls: {
      family: "bluetooth",
      readable: true,
      writable: true,
      presentation: {
        summary: { connection: "Link lost" },
        editors: {
          bluetooth_identification: {
            initial: { name_source: 2, custom_name: "é" },
            initial_fields: { name_source: { key: "2", label: "Custom name" } },
            variants: [],
            details: {},
            custom_name_limit: 32,
            custom_name_source: 2,
          },
        },
      },
      observations: {
        bluetooth_connection: observation({ state: 3, peer_name: "" }),
        bluetooth_pairing: observation(2),
        bluetooth_identification: observation({
          name_source: 2,
          custom_name: "é",
        }),
      },
    },
  };
  const html = render(h(DeviceControls, { device }));
  assert.match(html, /Link lost/);
  assert.match(html, /Confirm forgetting all paired devices/);
  assert.match(html, /32 bytes/);
  assert.match(html, /disabled[^]*?Clear pairing list/);
});
test("video keeps configured format, actual format and observed HDCP separate", () => {
  const device = {
    server_name: "av.local.",
    device_controls: {
      family: "dante_av",
      readable: true,
      writable: true,
      presentation: {
        summary: { signal: "Valid protected signal", observed_hdcp: "2.x" },
        editors: {
          video_format: {
            initial: {},
            initial_fields: {},
            variants: [],
            reason: "Receiver format settings are read-only.",
            details: {
              configured: "Unknown, 8-bit, RGB 4:4:4",
              actual: "800×600 60 Hz, 10-bit, YCbCr 4:4:4",
              direction: "Receiver",
            },
          },
          hdcp: {
            initial: { mode: 3 },
            initial_fields: { mode: { key: "3", label: "Automatic" } },
            variants: [],
            details: {},
          },
        },
      },
      observations: {
        video_channel: observation({
          direction: 1,
          status_code: 17,
          observed_hdcp_version: 3,
        }),
        video_format: observation({
          direction: 1,
          configured: { resolution: 16, bit_depth: 2, color_space: 1 },
          actual: { resolution: 257, bit_depth: 4, color_space: 16 },
          selection: {
            manual_resolution: true,
            manual_bit_depth: true,
            manual_color_space: true,
          },
          supported: [{ resolution: 16, bit_depth: 14, color_space: 25 }],
        }),
        hdcp: observation({ configured_mode: 3, supported_modes: [1, 3] }),
      },
    },
  };
  const html = render(h(DeviceControls, { device }));
  assert.match(html, /Configured: Unknown/);
  assert.match(html, /Actual: 800×600 60 Hz/);
  assert.match(html, /Observed HDCP: 2.x/);
  assert.match(html, /Automatic/);
  assert.match(html, /aria-label="Resolution"[^>]*disabled/);
});
test("bandwidth uses device limits and meaningful units", () => {
  const html = render(
    h(DeviceControls, {
      device: {
        server_name: "av.local.",
        device_controls: {
          family: "dante_av",
          readable: true,
          writable: true,
          presentation: {
            summary: {},
            editors: {
              bandwidth: {
                initial: { target: 100, enabled: true },
                initial_fields: {},
                variants: [],
                details: {},
                bandwidth: {
                  minimum: 50,
                  maximum: 600,
                  enable: { target: 100, enabled: true },
                  disable: { target: 0, enabled: false },
                },
              },
            },
          },
          observations: {
            bandwidth: observation({
              target: 100,
              minimum: 50,
              maximum: 800,
              enabled: 1,
            }),
          },
        },
      },
    }),
  );
  assert.match(html, /Mbit\/s \(50–600\)/);
  assert.match(html, /max="600"/);
});
