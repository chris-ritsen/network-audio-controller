import { externalColumns } from "./external-sources.js";
import { groupChannels } from "./channel-groups.js";
import * as format from "./format.js";
import { signal } from "./lib/preact.js";
import { deviceRequestName, pendingKey } from "./store.js";
import { selfConnectionTargetState } from "./self-connection.js";
import {
  externalCellState,
  externalSourcesWithAssignments,
} from "./external-sources.js";

export const CELL = 30;
export const GUTTER_PADDING = 12;
export const HEADER_PADDING = 18;
export const INDENT = 22;
export const MINIMUM_GUTTER = 280;
export const MINIMUM_HEADER = 160;
const STORAGE_KEY = "netaudio.matrix.expanded";

function readExpanded() {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    if (
      parsed &&
      Array.isArray(parsed.receivers) &&
      Array.isArray(parsed.transmitters)
    ) {
      return {
        receivers: new Set(parsed.receivers),
        transmitters: new Set(parsed.transmitters),
        initialized: true,
      };
    }
  } catch (error) {
    return { receivers: new Set(), transmitters: new Set() };
  }
  return { receivers: new Set(), transmitters: new Set() };
}

export const expanded = signal(readExpanded());

export function initializeExpansion(devices) {
  if (expanded.value.initialized || !devices.length) return;
  const forSide = (side) =>
    new Set(
      devices
        .filter((device) => {
          const count = Object.keys(device.channels?.[side] || {}).length;
          return count > 0 && count <= 16;
        })
        .map(format.deviceLabel),
    );
  expanded.value = {
    receivers: forSide("receivers"),
    transmitters: forSide("transmitters"),
    initialized: true,
  };
}

export function toggleExpanded(side, deviceLabel) {
  const current = expanded.value;
  const next = {
    receivers: new Set(current.receivers),
    transmitters: new Set(current.transmitters),
    initialized: true,
  };
  if (next[side].has(deviceLabel)) {
    next[side].delete(deviceLabel);
  } else {
    next[side].add(deviceLabel);
  }
  expanded.value = next;
  try {
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        receivers: [...next.receivers],
        transmitters: [...next.transmitters],
      }),
    );
  } catch (error) {
    return;
  }
}

export function setAllExpanded(side, deviceLabels, value) {
  const current = expanded.value;
  const next = {
    receivers: new Set(current.receivers),
    transmitters: new Set(current.transmitters),
    initialized: true,
  };
  for (const label of deviceLabels) {
    if (value) {
      next[side].add(label);
    } else {
      next[side].delete(label);
    }
  }
  expanded.value = next;
  try {
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        receivers: [...next.receivers],
        transmitters: [...next.transmitters],
      }),
    );
  } catch (error) {
    return;
  }
}

function channelEntries(device, direction) {
  const channels = device.channels ? device.channels[direction] || {} : {};
  return format.sortedChannelNumbers(channels).map((number) => ({
    channel: channels[number],
    name: channels[number].name || `channel ${number}`,
    number,
  }));
}

function matchesFilter(deviceLabel, channels, needle) {
  if (!needle) {
    return { device: true, channels: null };
  }
  if (deviceLabel.toLowerCase().includes(needle)) {
    return { device: true, channels: null };
  }
  const matching = channels.filter((channel) =>
    channel.name.toLowerCase().includes(needle),
  );
  if (!matching.length) {
    return null;
  }
  return {
    device: false,
    channels: new Set(matching.map((channel) => channel.number)),
  };
}

function buildAxis(
  devices,
  direction,
  expandedSet,
  filter,
  subscriptionsFor,
  groups,
) {
  const needle = filter.trim().toLowerCase();
  const entries = [];
  for (const device of devices) {
    const label = format.deviceLabel(device);
    const channels = channelEntries(device, direction);
    if (!channels.length) {
      continue;
    }
    const match = matchesFilter(label, channels, needle);
    if (!match) {
      continue;
    }
    const isExpanded = expandedSet.has(label) || match.channels !== null;
    const visibleChannels = match.channels
      ? channels.filter((channel) => match.channels.has(channel.number))
      : channels;
    entries.push({
      channelCount: channels.length,
      channels,
      device,
      expanded: isExpanded,
      kind: "device",
      label,
      subscription: null,
    });
    if (isExpanded) {
      const appendChannel = (channel, grouped = false) => {
        entries.push({
          channel: channel.channel,
          channels,
          device,
          kind: "channel",
          grouped,
          label,
          name: channel.name,
          number: channel.number,
          subscription: subscriptionsFor
            ? subscriptionsFor(device, channel.number)
            : null,
        });
      };
      if (groups.enabled) {
        for (const group of groupChannels(
          deviceRequestName(device),
          visibleChannels,
        )) {
          const groupExpanded =
            groups[direction].has(group.key) || match.channels !== null;
          entries.push({
            ...group,
            device,
            label,
            kind: "group",
            expanded: groupExpanded,
            channelCount: group.channels.length,
            subscription: null,
          });
          if (groupExpanded)
            for (const channel of group.channels) appendChannel(channel, true);
        }
      } else {
        for (const channel of visibleChannels) appendChannel(channel);
      }
    }
  }
  return entries;
}

export function buildMatrixModel({
  devices,
  externalFlows = {},
  expandedReceivers,
  expandedTransmitters,
  receiverFilter,
  transmitterFilter,
  groups = { enabled: false, receivers: new Set(), transmitters: new Set() },
  pending = {},
}) {
  const sorted = format.sortedDevices(devices);
  const subscriptionIndex = new Map();
  for (const device of sorted) {
    const byChannel = new Map();
    for (const subscription of device.subscriptions || []) {
      if (subscription.tx_device && subscription.tx_channel) {
        byChannel.set(subscription.rx_channel_number, subscription);
      }
    }
    subscriptionIndex.set(format.deviceLabel(device), byChannel);
  }
  const subscriptionsFor = (device, channelNumber) =>
    subscriptionIndex.get(format.deviceLabel(device)).get(channelNumber) ||
    null;
  const columns = buildAxis(
    sorted,
    "transmitters",
    expandedTransmitters,
    transmitterFilter,
    null,
    groups,
  );
  columns.push(
    ...externalColumns(
      externalSourcesWithAssignments(externalFlows, devices),
      transmitterFilter,
    ),
  );
  const rows = buildAxis(
    sorted,
    "receivers",
    expandedReceivers,
    receiverFilter,
    subscriptionsFor,
    groups,
  );
  for (const entry of rows) {
    if (entry.kind !== "channel" && entry.expanded) continue;
    const channels = entry.kind === "channel" ? [entry] : entry.channels;
    const severities = channels.flatMap((channel) => {
      const update =
        pending[pendingKey(deviceRequestName(entry.device), channel.number)] ||
        pending[pendingKey(entry.label, channel.number)];
      if (update) return ["pending"];
      const subscription = subscriptionIndex
        .get(entry.label)
        .get(channel.number);
      return subscription ? [subscriptionSeverity(subscription)] : [];
    });
    if (!severities.length) continue;
    const worst = severityName(Math.max(...severities.map(severityRank)));
    entry.activity = {
      count: severities.length,
      severity:
        worst === "ok" && severities.includes("pending") ? "pending" : worst,
    };
  }
  return {
    columns,
    rows,
    subscriptionIndex,
  };
}

function subscriptionSeverity(subscription) {
  const status = subscription?.status;
  if (
    status?.severity === "progress" ||
    ["in_progress", "resolved"].includes(status?.state)
  ) {
    return "pending";
  }
  return severityName(severityRank(format.subscriptionTone(subscription)));
}

export function severityRank(severity) {
  if (severity === "error" || severity === "critical" || severity === "bad") {
    return 3;
  }
  if (severity === "warning" || severity === "warn") {
    return 2;
  }
  if (severity === "ok" || severity === "success" || severity === "good") {
    return 1;
  }
  return 1;
}

export function severityName(rank) {
  if (rank >= 3) {
    return "error";
  }
  if (rank === 2) {
    return "warning";
  }
  return "ok";
}

export function cellState(
  row,
  column,
  subscriptionIndex,
  pending,
  flipped = false,
) {
  if (flipped) [row, column] = [column, row];
  if (column.sourceKind === "external")
    return externalCellState(
      row,
      column,
      row.kind === "channel"
        ? pending[pendingKey(deviceRequestName(row.device), row.number)]
        : null,
    );
  const receiverSubscriptions = subscriptionIndex.get(row.label) || new Map();
  if (row.kind === "channel" && column.kind === "channel") {
    const pendingEntry =
      pending[pendingKey(deviceRequestName(row.device), row.number)] ||
      pending[pendingKey(row.label, row.number)];
    if (pendingEntry) {
      const targetsThisColumn =
        pendingEntry.action === "add"
          ? pendingEntry.tx_device === column.label &&
            pendingEntry.tx_channel === column.name
          : Boolean(
              row.subscription &&
              row.subscription.tx_device === column.label &&
              row.subscription.tx_channel === column.name,
            );
      if (targetsThisColumn) {
        return { kind: "pending" };
      }
    }
    const subscription = row.subscription;
    if (
      !subscription ||
      subscription.tx_device !== column.label ||
      subscription.tx_channel !== column.name
    ) {
      const target = selfConnectionTargetState(
        row.device,
        row.channel,
        column.device,
      );
      if (!target.allowed)
        return { kind: `self-${target.state}`, reason: target.reason };
      return { kind: "empty" };
    }
    return { kind: subscriptionSeverity(subscription), subscription };
  }
  if (row.kind === "channel") {
    const subscription = row.subscription;
    if (!subscription || subscription.tx_device !== column.label) {
      return { kind: "empty" };
    }
    if (
      column.kind === "group" &&
      !column.channels.some(
        (channel) => channel.name === subscription.tx_channel,
      )
    ) {
      return { kind: "empty" };
    }
    return { kind: "partial", subscription };
  }
  let count = 0;
  let worst = 0;
  for (const subscription of receiverSubscriptions.values()) {
    if (
      row.kind === "group" &&
      !row.channels.some(
        (channel) => channel.number === subscription.rx_channel_number,
      )
    )
      continue;
    if (
      column.kind === "group" &&
      !column.channels.some(
        (channel) => channel.name === subscription.tx_channel,
      )
    )
      continue;
    if (subscription.tx_device !== column.label) {
      continue;
    }
    if (column.kind === "channel" && subscription.tx_channel !== column.name) {
      continue;
    }
    count += 1;
    worst = Math.max(
      worst,
      severityRank(format.subscriptionTone(subscription)),
    );
  }
  if (count === 0) {
    return { kind: "empty" };
  }
  return { count, kind: "aggregate", severity: severityName(worst) };
}
