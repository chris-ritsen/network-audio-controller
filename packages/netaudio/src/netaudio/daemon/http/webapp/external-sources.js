import { t } from "./i18n.js";

export function externalSourceKey(flow) {
  if (typeof flow.session_id !== "string" || !/^\d+$/.test(flow.session_id)) {
    throw new Error(t("External source is unavailable."));
  }
  return `${flow.source_ipv4}/${flow.session_id}`;
}

export function externalSourcesWithAssignments(discovered, devices) {
  const sources = { ...discovered };
  for (const device of Object.values(devices)) {
    for (const record of device.receiver_flows || []) {
      for (const identity of record.effective_subscription_identities || []) {
        if (
          typeof identity.session_id !== "string" ||
          !identity.source_ipv4 ||
          !Number.isInteger(identity.flow_slot) ||
          identity.flow_slot < 1
        )
          continue;
        const key = externalSourceKey(identity);
        if (discovered[key]) continue;
        const previous = sources[key];
        sources[key] = {
          source_ipv4: identity.source_ipv4,
          session_id: identity.session_id,
          channel_count: Math.max(
            previous?.channel_count || 0,
            identity.flow_slot,
          ),
          flow_name: t("External assignment"),
          routable: false,
          routability_errors: ["Source announcement unavailable"],
          retained_assignment: true,
        };
      }
    }
  }
  return sources;
}

export function externalColumns(flows, filter = "") {
  const entries = [];
  for (const flow of Object.values(flows)) {
    const hasSlots =
      Number.isInteger(flow.channel_count) &&
      flow.channel_count > 0 &&
      flow.channel_count <= 65535;
    const audio =
      hasSlots ||
      (flow.sdp?.media_descriptions || []).some(
        (media) => media.media_type.toLowerCase() === "audio",
      );
    if (!audio) continue;
    const count = hasSlots ? flow.channel_count : 0;
    const sourceKey = externalSourceKey(flow);
    const label = `${flow.flow_name || t("External audio")} · ${flow.source_ipv4}`;
    if (filter && !label.toLowerCase().includes(filter.toLowerCase())) continue;
    const channels = Array.from({ length: count }, (_, index) => ({
      number: index + 1,
      name: t("Channel {number}", { number: index + 1 }),
    }));
    const reason = (flow.routability_errors || []).map((error) => t(error)).join("; ");
    const common = {
      sourceKind: "external",
      sourceKey,
      flow,
      label,
      channels,
      device: null,
      reason,
    };
    entries.push({
      ...common,
      kind: "device",
      expanded: true,
      channelCount: count,
    });
    entries.push(
      ...channels.map((channel) => ({
        ...common,
        ...channel,
        channel,
        kind: "channel",
      })),
    );
  }
  return entries;
}

export function externalSubscriptionRequest(
  column,
  receiver,
  channel,
  clear = false,
) {
  const flow = column.flow;
  if (!flow.routable) {
    throw new Error(
      (flow.routability_errors || []).map((error) => t(error)).join("; ") ||
        t("Not routable"),
    );
  }
  if (Date.parse(flow.expires_at) <= Date.now())
    throw new Error(t("This source has expired."));
  externalSourceKey(flow);
  return {
    rx_device: receiver,
    source_ipv4: flow.source_ipv4,
    session_id: flow.session_id,
    content_sha256: flow.content_sha256,
    receiver_channel_ids: [channel],
    flow_slot_assignments: [clear ? 0 : column.number],
  };
}

export function externalCellState(row, column, pending) {
  const flow = column.flow;
  if (row.kind !== "channel" || column.kind !== "channel")
    return { kind: "empty" };
  if (
    pending?.external_source_key === column.sourceKey &&
    pending.external_slot === column.number
  ) {
    return { kind: "pending" };
  }
  const matches = (row.device.receiver_flows || [])
    .flatMap((record) => record.effective_subscription_identities || [])
    .filter(
      (identity) =>
        identity.receiver_channel === row.number &&
        identity.source_ipv4 === flow.source_ipv4 &&
        identity.session_id === flow.session_id &&
        identity.flow_slot === column.number,
    );
  if (matches.length === 1) {
    return {
      kind: "partial",
      subscription: matches[0],
    };
  }
  if (!flow.routable || Date.parse(flow.expires_at) <= Date.now()) {
    return {
      kind: "self-unavailable",
      reason:
        (flow.routability_errors || []).map((error) => t(error)).join("; ") ||
        t("Not routable"),
    };
  }
  return { kind: "empty" };
}
