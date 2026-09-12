import assert from "node:assert/strict";
import { test } from "node:test";

import { WEBAPP } from "./setup.mjs";

const { canonicalFlowRequest, flowEvidenceRows } = await import(
  `${WEBAPP}device/flows.js`
);

test("browser creates the canonical legacy flow schema", () => {
  const result = canonicalFlowRequest({
    channels: "2,4",
    encoding: 24,
    flowId: "3",
    protocolId: 0x2729,
    sampleRate: 48000,
  });
  assert.deepEqual(result.channel_slots, [
    { slot: 1, transmitter_channel: 2 },
    { slot: 2, transmitter_channel: 4 },
  ]);
  assert.equal(result.identity.global_flow_id, 3);
  assert.equal(result.protocol.cohort, "legacy_2729");
  assert.deepEqual(result.raw_fields, {});
});

test("browser emits an allocation request for the observed ARC 2.8.9 cohort", () => {
  const result = canonicalFlowRequest({
    channels: [1, 2],
    encoding: 24,
    flowId: "31",
    protocolId: 0x2809,
    sampleRate: 48000,
  });
  assert.equal(result.identity.global_flow_id, null);
  assert.equal(result.protocol.cohort, "modern_2809");
  assert.deepEqual(result.raw_fields, { request_options_word: 0 });
});

test("browser rejects duplicate channels and missing legacy identifiers", () => {
  assert.throws(
    () =>
      canonicalFlowRequest({ channels: "1,1", flowId: 2, protocolId: 0x2729 }),
    /unique/,
  );
  assert.throws(
    () =>
      canonicalFlowRequest({ channels: "1", flowId: "", protocolId: 0x2729 }),
    /flow identifier/,
  );
});

test("browser keeps acknowledgement, device, effective-state, and persistence evidence separate", () => {
  assert.deepEqual(
    flowEvidenceRows({
      request_acknowledgement: {
        received: true,
        accepted: true,
        result_code: 1,
      },
      device_confirmation: null,
      effective_state_confirmation: null,
      persistence_confirmation: null,
    }),
    [
      ["Request acknowledgement", "Accepted (result 1)"],
      ["Device confirmation", "No separate signal from this ARC transport"],
      ["Effective state", "Unverified"],
      ["Persistence", "Not verified"],
    ],
  );
});
