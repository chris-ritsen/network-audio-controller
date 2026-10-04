import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { aes67Status } = await import(`${WEBAPP}aes67.js`);
const { Aes67Section } = await import(`${WEBAPP}device/aes67.js`);
const { DEVICE_FILTERS } = await import(`${WEBAPP}device-filters.js`);
const { h } = await import("preact");
const { render } = await import("preact-render-to-string");

const managed = {
  ddm_enrolment_state: "ENROLLED",
  ddm_domain_id: "test-domain",
  ddm_domain_name: "Test domain",
};
const device = {
  name: "Test device",
  server_name: "test.local.",
  online: true,
};
function withAvailability(state) {
  const supported =
    typeof state.aes67_configuration_supported === "boolean"
      ? state.aes67_configuration_supported
      : null;
  const isManaged =
    state.ddm_enrolment_state === "ENROLLED" && Boolean(state.ddm_domain_id);
  const writable = supported === true && !isManaged;
  const reasons = writable
    ? []
    : supported === false
      ? ["unsupported"]
      : supported === null
        ? ["capability_unknown"]
        : ["managed_permission_missing"];
  return {
    ...state,
    operation_availability: {
      aes67: { supported, readable: true, writable, reasons },
    },
  };
}
const section = (state) =>
  render(
    h(Aes67Section, { device: { ...device, ...withAvailability(state) } }),
  );

const sectionStates = new Map([
  ["Configured enabled", "Enabled"],
  ["Configured disabled", "Disabled"],
  ["Enabled", "Enabled"],
  ["Disabled", "Disabled"],
  ["Enable pending", "Enable pending"],
  ["Disable pending", "Disable pending"],
  ["Managed by DDM", "Enabled"],
]);

for (const [state, label] of [
  [{}, "Not reported"],
  [
    { aes67_configuration_supported: "false", aes67_current: "true" },
    "Not reported",
  ],
  [
    { aes67_configuration_supported: false, aes67_current: false },
    "Unsupported",
  ],
  [
    { aes67_configuration_supported: false, aes67_current: true },
    "Unsupported",
  ],
  [{ aes67_configuration_supported: true }, "Supported, state unknown"],
  [{ aes67_configured: true }, "Configured enabled"],
  [{ aes67_configured: false }, "Configured disabled"],
  [{ aes67_current: true }, "Enabled"],
  [{ aes67_current: false }, "Disabled"],
  [{ aes67_current: true, aes67_configured: true }, "Enabled"],
  [{ aes67_current: false, aes67_configured: false }, "Disabled"],
  [{ aes67_current: false, aes67_configured: true }, "Enable pending"],
  [{ aes67_current: true, aes67_configured: false }, "Disable pending"],
  [
    { ...managed, aes67_configuration_supported: true, aes67_current: true },
    "Managed by DDM",
  ],
]) {
  test(`AES67 readiness ${JSON.stringify(state)} is ${label}`, () => {
    assert.equal(aes67Status(withAvailability(state)).label, label);
    assert.deepEqual(
      DEVICE_FILTERS.find(({ id }) => id === "aes67").values(state),
      [label],
    );
    const shown = sectionStates.get(label);
    if (shown) assert.match(section(state), new RegExp(`AES67</dt><dd>${shown}<`));
    else assert.doesNotMatch(section(state), /AES67<\/dt>/);
  });
}

test("unknown, unsupported and managed AES67 states have no local write controls", () => {
  for (const state of [
    {},
    { aes67_configuration_supported: false },
    { ...managed, aes67_configuration_supported: true },
  ]) {
    assert.doesNotMatch(section(state), /<button|<input|<select/);
    assert.equal(aes67Status(withAvailability(state)).canConfigure, false);
  }
});

test("an unenrolled DDM observation does not override direct AES67 state", () => {
  const state = {
    aes67_configuration_supported: true,
    aes67_current: false,
    ddm_enrolment_state: "UNENROLLED",
    ddm_capabilities: { rtp_audio_supported: false },
  };
  assert.equal(aes67Status(withAvailability(state)).label, "Disabled");
  assert.equal(aes67Status(withAvailability(state)).canConfigure, true);
  assert.match(section(state), />Enable</);
  assert.doesNotMatch(section(state), /Managed by DDM|RTP flows/);
});

test("pending mode keeps current and configured states distinct without inferring a reboot", () => {
  const markup = section({
    aes67_configuration_supported: true,
    aes67_current: false,
    aes67_configured: true,
  });
  assert.match(markup, /Enable pending/);
  assert.doesNotMatch(markup, /reboot/i);
});

test("multicast prefix controls appear only for a reported prefix", () => {
  assert.doesNotMatch(
    section({ aes67_configuration_supported: true }),
    /<input|Multicast address prefix/,
  );
  assert.match(
    section({
      aes67_configuration_supported: true,
      aes67_multicast_prefix: "239.69.0.0",
    }),
    /aria-label="AES67 multicast address prefix"/,
  );
});

test("an enrolled device exposes AES67 actions only when managed permission is explicit", () => {
  const state = {
    ...managed,
    aes67_configuration_supported: true,
    aes67_current: false,
    aes67_multicast_prefix: "239.69.0.0",
    operation_availability: {
      aes67: { supported: true, readable: true, writable: true, reasons: [] },
    },
  };
  const markup = render(h(Aes67Section, { device: { ...device, ...state } }));
  assert.match(markup, />Enable</);
  assert.doesNotMatch(markup, /AES67 multicast address prefix|>Apply</);
  assert.doesNotMatch(markup, /AES67 configuration unavailable/);
});

for (const [capabilities, label] of [
  [{}, null],
  [{ rtp_audio_supported: true }, "Available"],
  [{ rtp_audio_supported: true, rtp_audio_support_suppressed: false }, "Available"],
  [{ rtp_audio_supported: false, rtp_audio_support_suppressed: false }, null],
  [{ rtp_audio_supported: true, rtp_audio_support_suppressed: true }, "Reboot required"],
  [{ rtp_audio_supported: false, rtp_audio_support_suppressed: true }, "Reboot required"],
]) {
  test(`DDM RTP readiness ${JSON.stringify(capabilities)} is ${label}`, () => {
    const markup = section({ ...managed, ddm_capabilities: capabilities });
    if (label) assert.match(markup, new RegExp(`RTP flows</dt><dd>${label}`));
    else assert.doesNotMatch(markup, /RTP flows/);
    assert.doesNotMatch(markup, /<button|<input|rtp_audio_|AES67 is enabled|does not support AES67/);
  });
}
