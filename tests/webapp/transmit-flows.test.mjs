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
  assert.equal(result.identity.media_local_flow_id, 31);
  assert.equal(result.protocol.cohort, "modern_2809");
  assert.deepEqual(result.raw_fields, { request_options_word: 0 });
});

test("browser emits scoped RTP fields without treating device format as a wire mutation", () => {
  const result = canonicalFlowRequest({
    channels: "2,1",
    encoding: 24,
    flowId: "7",
    flowName: " Program ",
    framesPerPacket: "48",
    mediaMode: "rtp_aes67",
    primaryAddress: "239.69.1.2",
    primaryPort: "5004",
    protocolId: 0x2809,
    sampleRate: 48000,
    secondaryAddress: "239.69.1.3",
    secondaryPort: "5006",
  });

  assert.equal(result.media_mode, "rtp_aes67");
  assert.equal(result.name, "Program");
  assert.equal(result.frames_per_packet, 48);
  assert.equal(result.identity.global_flow_id, null);
  assert.equal(result.identity.media_local_flow_id, 7);
  assert.deepEqual(result.primary_destination, {
    address: "239.69.1.2",
    port: 5004,
    interface: null,
  });
  assert.deepEqual(result.secondary_destination, {
    address: "239.69.1.3",
    port: 5006,
    interface: null,
  });
  assert.equal(result.sample_rate_hz, 48000);
  assert.equal(result.encoding_bits, 24);
  assert.equal(result.identity.media_type_code, 3);
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
  assert.throws(
    () =>
      canonicalFlowRequest({
        channels: "1",
        flowId: "",
        protocolId: 0x2809,
      }),
    /media-local flow identifier/,
  );
  assert.throws(
    () =>
      canonicalFlowRequest({
        channels: "1",
        flowId: 1,
        mediaMode: "rtp_aes67",
        protocolId: 0x2729,
      }),
    /scoped to ARC 0x2809/,
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
