import assert from "node:assert/strict";
import { test } from "node:test";

import { WEBAPP } from "./setup.mjs";

const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const {
  operationReasonText,
  operationWritable,
  performanceOperationReasonText,
  performanceOperationWritable,
} = await import(`${WEBAPP}device/availability.js`);
const { DeviceConfigSection } = await import(`${WEBAPP}device/config.js`);
const { ReceiveSection } = await import(`${WEBAPP}device/channels.js`);
const { LockSection } = await import(`${WEBAPP}device/security.js`);

const available = (writable, reasons = []) => ({
  supported: true,
  readable: true,
  writable,
  reasons,
});

test("availability helpers preserve backend restrictions as readable text", () => {
  const device = {
    operation_availability: {
      sample_rate: available(false, [
        "fixed",
        "device_locked",
        "managed_permission_denied",
      ]),
    },
  };
  assert.equal(operationWritable(device, "sample_rate"), false);
  assert.equal(
    operationReasonText(device, "sample_rate"),
    "The device reports that this setting is fixed. The device is locked. Permission for this managed operation was denied.",
  );
  assert.equal(
    operationReasonText({}, "sample_rate"),
    "Operation availability was not reported.",
  );
});

test("device configuration shows state and reasons without rejected actions", () => {
  const device = {
    name: "Adapter",
    server_name: "adapter.local.",
    sample_rate_hz: 48000,
    supported_sample_rates_hz: [48000, 96000],
    encoding: 24,
    supported_encodings: [16, 24],
    sample_rate_pullup_raw_value: 0,
    supported_sample_rate_pullup_raw_values: [0, 1],
    operation_availability: {
      identify: available(false, ["unsupported"]),
      sample_rate: available(false, ["device_locked"]),
      encoding: available(false, ["fixed"]),
      sample_rate_pullup: available(false, ["host_disabled"]),
    },
  };
  const markup = render(h(DeviceConfigSection, { device }));
  assert.match(markup, /48 kHz/);
  assert.match(markup, />24</);
  assert.match(markup, />None</);
  assert.match(
    markup,
    /device is locked|setting is fixed|host has disabled|Identify unavailable/,
  );
  assert.doesNotMatch(
    markup,
    /aria-label="Sample rate"|aria-label="Encoding"|set sample rate pull-up|>Identify</,
  );
});

test("codec and lock controls appear only when their operations are writable", () => {
  const base = {
    name: "Adapter",
    server_name: "adapter.local.",
    gain_device_type: "output",
    gain_level_choices: [{ value: 3, label: "+18 dB" }],
    channels: {
      receivers: {
        1: { name: "Output 1", gain_level: 3, gain_level_label: "+18 dB" },
      },
    },
    subscriptions: [],
  };
  const deniedGain = render(
    h(ReceiveSection, {
      device: {
        ...base,
        operation_availability: {
          codec_control: available(false, ["device_locked"]),
        },
      },
    }),
  );
  assert.match(deniedGain, /\+18 dB/);
  assert.match(deniedGain, /device is locked/);
  assert.doesNotMatch(deniedGain, />Set</);

  const writableGain = render(
    h(ReceiveSection, {
      device: {
        ...base,
        operation_availability: { codec_control: available(true) },
      },
    }),
  );
  assert.match(writableGain, />Set</);

  const lockMarkup = render(
    h(LockSection, {
      device: {
        ...base,
        is_locked: false,
        operation_availability: {
          locking: available(false, ["managed_transport_unavailable"]),
        },
      },
    }),
  );
  assert.match(lockMarkup, /Unlocked/);
  assert.match(lockMarkup, /unavailable through managed control/);
  assert.doesNotMatch(lockMarkup, /Device PIN|>Lock</);
});

test("flow performance controls follow typed backend availability", () => {
  const device = {
    name: "Adapter",
    server_name: "adapter.local.",
    operation_availability: { identify: available(false, ["unsupported"]) },
    performance_operation_availability: {
      receive_flow_performance: available(true),
      transmit_flow_performance: available(false, [
        "properties_not_advertised",
      ]),
      unicast_performance: available(false, ["software_version_unknown"]),
      receive_flow_default_slots: available(true),
      store_current_configuration: available(true),
    },
  };
  assert.equal(
    performanceOperationWritable(device, "receive_flow_performance"),
    true,
  );
  assert.match(
    performanceOperationReasonText(device, "unicast_performance"),
    /software version was not reported/,
  );
  const markup = render(h(DeviceConfigSection, { device }));
  assert.match(markup, /Flow performance/);
  assert.match(markup, /Receive flow latency in microseconds/);
  assert.match(markup, /Receive flow default slots/);
  assert.match(markup, /Store current configuration/);
  assert.match(markup, /does not advertise the required properties/);
  assert.doesNotMatch(markup, /Transmit flow latency in microseconds/);
});
