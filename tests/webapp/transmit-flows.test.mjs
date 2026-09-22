import assert from "node:assert/strict";
import { test } from "node:test";

import { WEBAPP } from "./setup.mjs";

const { canonicalFlowRequest, flowEvidenceRows, ReceiverFlows, TransmitFlows } =
  await import(`${WEBAPP}device/flows.js`);
const { h } = await import("preact");
const { render } = await import("preact-render-to-string");

const legacyAuthoring = {
  protocol_id: 0x2729,
  identity_field: "global_flow_id",
  identifier_max: 32,
  media_modes: ["native_dante"],
  supports_flow_options: false,
};
const modernAuthoring = {
  protocol_id: 0x2809,
  identity_field: "media_local_flow_id",
  identifier_max: 65535,
  media_modes: ["native_dante", "rtp_aes67"],
  supports_flow_options: true,
};

test("browser creates the canonical legacy flow schema", () => {
  const result = canonicalFlowRequest({
    channels: "2,4",
    encoding: 24,
    flowId: "3",
    authoring: legacyAuthoring,
    sampleRate: 48000,
  });
  assert.deepEqual(result.channel_slots, [
    { slot: 1, transmitter_channel: 2 },
    { slot: 2, transmitter_channel: 4 },
  ]);
  assert.equal(result.identity.global_flow_id, 3);
  assert.equal(result.protocol.protocol_id, 0x2729);
  assert.deepEqual(result.raw_fields, {});
});

test("browser emits an allocation request for the observed ARC 2.8.9 cohort", () => {
  const result = canonicalFlowRequest({
    channels: [1, 2],
    encoding: 24,
    flowId: "31",
    authoring: modernAuthoring,
    sampleRate: 48000,
  });
  assert.equal(result.identity.global_flow_id, null);
  assert.equal(result.identity.media_local_flow_id, 31);
  assert.equal(result.protocol.protocol_id, 0x2809);
  assert.deepEqual(result.raw_fields, {});
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
    authoring: modernAuthoring,
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
  assert.equal(result.identity.media_type_code, null);
});

test("browser preserves channel intent for native validation and requires identifiers", () => {
  const duplicate = canonicalFlowRequest({
    channels: "1,1",
    flowId: 2,
    authoring: legacyAuthoring,
  });
  assert.deepEqual(duplicate.channel_slots, [
    { slot: 1, transmitter_channel: 1 },
    { slot: 2, transmitter_channel: 1 },
  ]);
  assert.throws(
    () =>
      canonicalFlowRequest({
        channels: "1",
        flowId: "",
        authoring: legacyAuthoring,
      }),
    /flow identifier/,
  );
  assert.throws(
    () =>
      canonicalFlowRequest({
        channels: "1",
        flowId: "",
        authoring: modernAuthoring,
      }),
    /flow identifier/,
  );
  assert.throws(
    () =>
      canonicalFlowRequest({
        channels: "1",
        flowId: 1,
        mediaMode: "rtp_aes67",
        authoring: legacyAuthoring,
      }),
    /not available/,
  );
});

test("flow editor uses advertised authoring independently of inventory revision", () => {
  const markup = render(
    h(TransmitFlows, {
      device: {
        server_name: "desk.local.",
        flow_protocol_id: 0x2729,
        transmit_flow_authoring: modernAuthoring,
      },
    }),
  );

  assert.match(markup, /Media-local flow identifier/);
  assert.match(markup, /RTP\/AES67/);
  assert.doesNotMatch(markup, /Global flow identifier/);
  assert.throws(
    () => canonicalFlowRequest({ channels: "1", flowId: 1 }),
    /unavailable/,
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

test("receiver flow inventory renders endpoints, identity, correlation, and completeness", () => {
  const markup = render(
    h(ReceiverFlows, {
      device: {
        receiver_flow_completeness: "complete",
        receiver_flows: [
          {
            flow_number: 3,
            flow_type: "multicast",
            transport: 3,
            subscription_status_code: 9,
            receiver_channel_numbers_by_flow_channel: [[7], []],
            interface_endpoints: [
              { ipv4_address: "239.69.1.10", udp_port: 5004 },
              { ipv4_address: "239.69.1.11", udp_port: 5006 },
            ],
            external_identity: {
              source_ipv4: "192.0.2.44",
              session_id: 42,
            },
            sdp_correlation: { matched: false },
          },
        ],
      },
    }),
  );

  assert.match(markup, /Inventory: complete/);
  assert.match(markup, /1:7; 2:none/);
  assert.match(markup, /239\.69\.1\.10:5004/);
  assert.match(markup, /239\.69\.1\.11:5006/);
  assert.match(markup, /192\.0\.2\.44\/42/);
  assert.match(markup, /Not matched/);
});
