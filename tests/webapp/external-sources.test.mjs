import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";
const matrix = await import(`${WEBAPP}matrix.js`);
const {
  externalSubscriptionRequest,
  externalSourcesWithAssignments,
  externalCellState,
} = await import(`${WEBAPP}external-sources.js`);

test("acknowledgement leaves the external cell pending rather than configured", () => {
  const row = { kind: "channel", number: 7, device: { receiver_flows: [] } };
  const column = {
    kind: "channel",
    number: 2,
    sourceKey: "192.0.2.1/123",
    flow: { routable: true },
  };
  assert.equal(
    externalCellState(row, column, {
      external_source_key: column.sourceKey,
      external_slot: 2,
    }).kind,
    "pending",
  );
});

test("expired discovery retains reported assignments without granting routing authority", () => {
  const devices = {
    rx: {
      receiver_flows: [
        {
          effective_subscription_identities: [
            {
              source_ipv4: "192.0.2.1",
              session_id: "9007199254740993",
              receiver_channel: 7,
              flow_slot: 2,
            },
          ],
        },
      ],
    },
  };
  const retained = externalSourcesWithAssignments({}, devices);
  const flow = retained["192.0.2.1/9007199254740993"];
  assert.equal(flow.channel_count, 2);
  assert.equal(flow.routable, false);
  assert.throws(
    () => externalSubscriptionRequest({ flow, number: 2 }, "rx", 7),
    /unavailable/,
  );
});

test("unsupported audio remains visible without inventing channel slots", () => {
  const flow = {
    source_ipv4: "192.0.2.1",
    session_id: "123",
    flow_name: "Unsupported",
    channel_count: null,
    routable: false,
    routability_errors: ["Unsupported encoding"],
    sdp: { media_descriptions: [{ media_type: "audio" }] },
  };
  const built = matrix.buildMatrixModel({
    devices: {},
    externalFlows: { one: flow },
    expandedReceivers: new Set(),
    expandedTransmitters: new Set(),
    receiverFilter: "",
    transmitterFilter: "",
  });
  assert.equal(built.columns.length, 1);
  assert.equal(built.columns[0].channelCount, 0);
  assert.equal(built.columns[0].reason, "Unsupported encoding");
});

test("external sessions remain separate matrix sources and map source slots to receivers", () => {
  const sources = Object.fromEntries(
    ["9007199254740993", "18446744073709551615"].map((session_id) => [
      `192.0.2.1/${session_id}`,
      {
        source_ipv4: "192.0.2.1",
        session_id,
        flow_name: "Same name",
        channel_count: 2,
        routable: true,
        direction: "sendonly",
        content_sha256: session_id,
        expires_at: "2099-01-01T00:00:00Z",
      },
    ]),
  );
  const built = matrix.buildMatrixModel({
    devices: {},
    externalFlows: sources,
    expandedReceivers: new Set(),
    expandedTransmitters: new Set(),
    receiverFilter: "",
    transmitterFilter: "",
  });
  const slots = built.columns.filter((c) => c.kind === "channel");
  assert.equal(slots.length, 4);
  assert.notEqual(slots[0].sourceKey, slots[2].sourceKey);
  const request = externalSubscriptionRequest(slots[1], "rx.local.", 7);
  assert.equal(request.session_id, "9007199254740993");
  assert.deepEqual(request.receiver_channel_ids, [7]);
  assert.deepEqual(request.flow_slot_assignments, [2]);
  assert.equal(request.content_sha256, "9007199254740993");
});

function directionColumn(direction, routable, errors = []) {
  return {
    kind: "channel",
    number: 1,
    sourceKey: "192.0.2.50/3967398212",
    flow: {
      source_ipv4: "192.0.2.50",
      session_id: "3967398212",
      content_sha256: "digest",
      direction,
      routable,
      routability_errors: errors,
      expires_at: "2099-01-01T00:00:00Z",
    },
  };
}

test("recvonly sender is routable", () => {
  const column = directionColumn("recvonly", true);
  const row = { kind: "channel", number: 3, device: { receiver_flows: [] } };
  assert.equal(externalCellState(row, column, null).kind, "empty");
  const request = externalSubscriptionRequest(column, "rx.local.", 3);
  assert.deepEqual(request.flow_slot_assignments, [1]);
  assert.equal(request.session_id, "3967398212");
});

test("inactive uses core verdict", () => {
  const column = directionColumn("inactive", false, [
    "audio media direction is inactive",
  ]);
  const row = { kind: "channel", number: 3, device: { receiver_flows: [] } };
  const state = externalCellState(row, column, null);
  assert.equal(state.kind, "self-unavailable");
  assert.equal(state.reason, "audio media direction is inactive");
  assert.throws(
    () => externalSubscriptionRequest(column, "rx.local.", 3),
    /inactive/,
  );
});
