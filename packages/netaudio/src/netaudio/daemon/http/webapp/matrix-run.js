import { t } from "./i18n.js";
import { cellState } from "./matrix-model.js";
import { deviceRequestName } from "./store.js";

export function patchable(entry) {
  return entry?.kind === "channel" && entry.sourceKind !== "external";
}

export function entryKey(entry) {
  return `${deviceRequestName(entry.device)}\u0000${entry.number}`;
}

export function anchorFor(rows, columns, position, flipped) {
  return {
    columnKey: entryKey(columns[position.columnIndex]),
    flipped,
    rowKey: entryKey(rows[position.rowIndex]),
  };
}

export function anchorPosition(rows, columns, anchor, flipped) {
  if (!anchor || anchor.flipped !== flipped) return null;
  const find = (entries, key) =>
    entries.findIndex((entry) => patchable(entry) && entryKey(entry) === key);
  const rowIndex = find(rows, anchor.rowKey);
  const columnIndex = find(columns, anchor.columnKey);
  return rowIndex < 0 || columnIndex < 0 ? null : { columnIndex, rowIndex };
}

function channelsBetween(entries, from, to) {
  const step = to < from ? -1 : 1;
  const indices = [];
  for (let index = from; index !== to + step; index += step) {
    if (patchable(entries[index])) indices.push(index);
  }
  return indices;
}

export function diagonalRun(rows, columns, start, end) {
  const rowIndices = channelsBetween(rows, start.rowIndex, end.rowIndex);
  const columnIndices = channelsBetween(
    columns,
    start.columnIndex,
    end.columnIndex,
  );
  const length = Math.min(rowIndices.length, columnIndices.length);
  return Array.from({ length }, (_, index) => ({
    columnIndex: columnIndices[index],
    rowIndex: rowIndices[index],
  }));
}

export function channelLabel(entry) {
  return `${entry.name}@${entry.label}`;
}

export function runChanges(
  cells,
  rows,
  columns,
  flipped,
  subscriptionIndex,
  pending,
) {
  return cells.map(({ columnIndex, rowIndex }) => {
    const receiver = flipped ? columns[columnIndex] : rows[rowIndex];
    const transmitter = flipped ? rows[rowIndex] : columns[columnIndex];
    const change = { receiver, transmitter };
    if (!receiver.device.online || !transmitter.device.online) {
      return {
        ...change,
        kind: "blocked",
        note: t("Both devices must be online to change this connection."),
      };
    }
    const state = cellState(receiver, transmitter, subscriptionIndex, pending);
    if (
      state.kind === "self-unsupported" ||
      state.kind === "self-unavailable"
    ) {
      return { ...change, kind: "blocked", note: state.reason };
    }
    if (state.kind === "pending" || state.subscription) {
      return { ...change, kind: "unchanged", note: t("Unchanged") };
    }
    const current = receiver.subscription;
    return {
      ...change,
      kind: "add",
      note:
        current?.tx_device && current?.tx_channel
          ? t("Replaces {source}", {
              source: `${current.tx_channel}@${current.tx_device}`,
            })
          : "",
    };
  });
}

export function routeFor(change) {
  return {
    rx_channel: change.receiver.number,
    rx_device: deviceRequestName(change.receiver.device),
    tx_channel: change.transmitter.name,
    tx_device: change.transmitter.label,
  };
}
