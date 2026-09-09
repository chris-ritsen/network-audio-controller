import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { aes67Status } = await import(`${WEBAPP}aes67.js`);
const { Aes67Section } = await import(`${WEBAPP}device/aes67.js`);
const { StatusSection } = await import(`${WEBAPP}device/status.js`);
const { DEVICE_FILTERS } = await import(`${WEBAPP}device-filters.js`);
const { h } = await import("preact");
const { render } = await import("preact-render-to-string");

const managed = { ddm_enrolment_state: "ENROLLED", ddm_domain_id: "test-domain", ddm_domain_name: "Test domain" };
const device = { name: "Test device", server_name: "test.local.", online: true };
const section = (state) => render(h(Aes67Section, { device: { ...device, ...state } }));

for (const [state, label] of [
  [{}, "Not reported"],
  [{ aes67_supported: "false", aes67_current: "true" }, "Not reported"],
  [{ aes67_supported: false, aes67_current: false }, "Unsupported"],
  [{ aes67_supported: false, aes67_current: true }, "Unsupported"],
  [{ aes67_supported: true }, "Supported, state unknown"],
  [{ aes67_configured: true }, "Configured enabled"],
  [{ aes67_configured: false }, "Configured disabled"],
  [{ aes67_current: true }, "Enabled"],
  [{ aes67_current: false }, "Disabled"],
  [{ aes67_current: true, aes67_configured: true }, "Enabled"],
  [{ aes67_current: false, aes67_configured: false }, "Disabled"],
  [{ aes67_current: false, aes67_configured: true }, "Enable pending"],
  [{ aes67_current: true, aes67_configured: false }, "Disable pending"],
  [{ ...managed, aes67_supported: true, aes67_current: true }, "Managed by DDM"],
]) {
  test(`AES67 readiness ${JSON.stringify(state)} is ${label} across views`, () => {
    assert.equal(aes67Status(state).label, label);
    assert.deepEqual(DEVICE_FILTERS.find(({ id }) => id === "aes67").values(state), [label]);
    assert.ok(section(state).includes(label));
    assert.ok(render(h(StatusSection, { device: { ...device, ...state } })).includes(label));
  });
}

test("unknown, unsupported and managed AES67 states have no local write controls", () => {
  for (const state of [{}, { aes67_supported: false }, { ...managed, aes67_supported: true }]) {
    assert.doesNotMatch(section(state), /<button|<input|<select/);
    assert.equal(aes67Status(state).canConfigure, false);
  }
});

test("an unenrolled DDM observation does not override direct AES67 state", () => {
  const state = { aes67_supported: true, aes67_current: false, ddm_enrolment_state: "UNENROLLED", ddm_capabilities: { rtp_audio_supported: false } };
  assert.equal(aes67Status(state).label, "Disabled");
  assert.equal(aes67Status(state).canConfigure, true);
  assert.match(section(state), />Enable</);
  assert.doesNotMatch(section(state), /Managed by DDM|RTP flows/);
});

test("pending mode keeps current and configured states distinct without inferring a reboot", () => {
  const markup = section({ aes67_supported: true, aes67_current: false, aes67_configured: true });
  assert.match(markup, /Current mode<\/dt><dd>Disabled/);
  assert.match(markup, /Configured mode<\/dt><dd>Enabled/);
  assert.doesNotMatch(markup, /reboot/i);
});

test("multicast prefix controls appear only for a reported prefix", () => {
  assert.doesNotMatch(section({ aes67_supported: true }), /<input|Multicast address prefix/);
  assert.match(section({ aes67_supported: true, aes67_multicast_prefix: "239.69.0.0" }), /aria-label="AES67 multicast address prefix"/);
});

for (const [capabilities, label] of [
  [{}, "Not reported"],
  [{ rtp_audio_supported: true }, "Support reported"],
  [{ rtp_audio_supported: true, rtp_audio_support_suppressed: false }, "Available"],
  [{ rtp_audio_supported: false, rtp_audio_support_suppressed: false }, "Unavailable"],
  [{ rtp_audio_supported: true, rtp_audio_support_suppressed: true }, "Reboot required"],
  [{ rtp_audio_supported: false, rtp_audio_support_suppressed: true }, "Reboot required"],
]) {
  test(`DDM RTP readiness ${JSON.stringify(capabilities)} is ${label}`, () => {
    const markup = section({ ...managed, ddm_capabilities: capabilities });
    assert.match(markup, new RegExp(`RTP flows</dt><dd>${label}`));
    assert.doesNotMatch(markup, /<button|<input|rtp_audio_|AES67 is enabled|does not support AES67/);
  });
}
