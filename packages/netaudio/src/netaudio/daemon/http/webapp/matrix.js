import { api } from "./api.js";
import { channelGroups, setGroupsExpanded } from "./channel-groups.js";
import { useColorScheme } from "./color-scheme.js";
import { t, tn } from "./i18n.js";
import { Icon } from "./icons.js";
import * as format from "./format.js";
import {
  html,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "./lib/preact.js";
import {
  deviceRequestName,
  markExternalPending,
  requestSubscriptionChange,
} from "./store.js";
import { MatrixTooltip } from "./matrix-tooltip.js";
import { runAction as performAction } from "./actions.js";
import { externalSubscriptionRequest } from "./external-sources.js";
import {
  CELL,
  MINIMUM_GUTTER,
  MINIMUM_HEADER,
  cellState,
  setAllExpanded,
  toggleExpanded,
} from "./matrix-model.js";
import { draw, measureMatrixLayout, readTheme } from "./matrix-drawing.js";

export {
  buildMatrixModel,
  cellState,
  expanded,
  initializeExpansion,
  setAllExpanded,
  toggleExpanded,
} from "./matrix-model.js";
export { measureMatrixLayout } from "./matrix-drawing.js";

export function stickyDevice(entries, offset) {
  const first = Math.min(entries.length - 1, Math.floor(offset / CELL));
  let owner = -1;
  for (let index = first; index >= 0; index -= 1) {
    if (entries[index]?.kind === "device") {
      owner = index;
      break;
    }
  }
  if (owner < 0 || owner * CELL >= offset) return null;
  const next = entries.findIndex(
    (entry, index) => index > owner && entry.kind === "device",
  );
  const position = next < 0 ? 0 : Math.min(0, next * CELL - offset - CELL);
  return { index: owner, position };
}

export function ExpansionButtons({
  label,
  expandLabel,
  collapseLabel,
  onExpand,
  onCollapse,
  vertical = false,
}) {
  const buttonClass = `btn btn-xs btn-square${vertical ? "" : " join-item"}`;
  return html`<div class=${vertical ? "matrix-expansion-stack" : "join"} role="group" aria-label=${label}>
    <button type="button" class=${buttonClass} title=${expandLabel} aria-label=${expandLabel} onClick=${onExpand}><${Icon} name="plus" /></button>
    <button type="button" class=${buttonClass} title=${collapseLabel} aria-label=${collapseLabel} onClick=${onCollapse}><${Icon} name="minus" /></button>
  </div>`;
}

export function RoutingMatrix({
  columns: transmitters,
  onOpenDevice,
  rows: receivers,
  subscriptionIndex,
  flipped = false,
  locked = false,
  onExpandDevices,
  receiverFilter = "",
  transmitterFilter = "",
  onReceiverFilter,
  onTransmitterFilter,
  pending = {},
}) {
  const rows = flipped ? transmitters : receivers;
  const columns = flipped ? receivers : transmitters;
  const stage = useRef(null);
  const viewport = useRef(null);
  const canvas = useRef(null);
  const colorScheme = useColorScheme();
  const theme = useMemo(() => readTheme(), [colorScheme]);
  const [size, setSize] = useState({ height: 0, width: 0 });
  const [scroll, setScroll] = useState({ left: 0, top: 0 });
  const [hover, setHover] = useState(null);
  const [layout, setLayout] = useState({
    gutter: MINIMUM_GUTTER,
    header: MINIMUM_HEADER,
  });
  const busy = useRef(new Set());

  const contentWidth = layout.gutter + columns.length * CELL;
  const contentHeight = layout.header + rows.length * CELL;

  useLayoutEffect(() => {
    const node = viewport.current;
    if (!node) {
      return undefined;
    }
    const observer = new ResizeObserver(() =>
      setSize({ height: node.clientHeight, width: node.clientWidth }),
    );
    const drawing = canvas.current;
    observer.observe(node);
    setSize({ height: node.clientHeight, width: node.clientWidth });
    return () => {
      observer.disconnect();
      drawing.width = 0;
      drawing.height = 0;
    };
  }, []);

  useLayoutEffect(() => {
    const node = canvas.current;
    if (!node) {
      return;
    }
    const measured = measureMatrixLayout(
      node.getContext("2d"),
      rows,
      columns,
      theme,
    );
    if (
      measured.gutter !== layout.gutter ||
      measured.header !== layout.header
    ) {
      setLayout(measured);
    }
  }, [columns, rows, theme]);

  useLayoutEffect(() => {
    const node = canvas.current;
    if (!node || size.width === 0 || size.height === 0) {
      return;
    }
    const frame = requestAnimationFrame(() => {
      const ratio = window.devicePixelRatio || 1;
      const width = Math.floor(size.width * ratio),
        height = Math.floor(size.height * ratio);
      if (node.width !== width) node.width = width;
      if (node.height !== height) node.height = height;
      node.style.width = `${size.width}px`;
      node.style.height = `${size.height}px`;
      const context = node.getContext("2d");
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
      draw(context, {
        columns,
        flipped,
        hover,
        layout,
        pending,
        rows,
        scroll,
        size,
        subscriptionIndex,
        theme,
      });
    });
    return () => cancelAnimationFrame(frame);
  }, [
    columns,
    flipped,
    hover,
    layout,
    pending,
    rows,
    scroll,
    size,
    subscriptionIndex,
    theme,
  ]);

  const locate = (event) => {
    const bounds = canvas.current.getBoundingClientRect();
    const x = event.clientX - bounds.left;
    const y = event.clientY - bounds.top;
    const inGutter = x < layout.gutter;
    const inHeader = y < layout.header;
    let columnIndex = inGutter
      ? -1
      : Math.floor((x - layout.gutter + scroll.left) / CELL);
    let rowIndex = inHeader
      ? -1
      : Math.floor((y - layout.header + scroll.top) / CELL);
    if (inGutter && !inHeader) {
      const pinned = stickyDevice(rows, scroll.top);
      const top = layout.header + (pinned?.position ?? 0);
      if (pinned && y >= top && y < top + CELL) rowIndex = pinned.index;
    }
    if (inHeader && !inGutter) {
      const pinned = stickyDevice(columns, scroll.left);
      const left = layout.gutter + (pinned?.position ?? 0);
      if (pinned && x >= left && x < left + CELL) columnIndex = pinned.index;
    }
    if (
      (!inGutter && (columnIndex < 0 || columnIndex >= columns.length)) ||
      (!inHeader && (rowIndex < 0 || rowIndex >= rows.length))
    ) {
      return null;
    }
    return { columnIndex, inGutter, inHeader, rowIndex, x, y };
  };

  const [error, setError] = useState("");
  const runAction = async (description, action) => {
    setError("");
    const outcome = await performAction(description, action);
    if (!outcome.ok) setError(outcome.error.message);
    return outcome;
  };
  const onClick = async (event) => {
    const position = locate(event);
    if (!position) {
      return;
    }
    if (position.inGutter && position.inHeader) {
      return;
    }
    if (position.inGutter) {
      const row = rows[position.rowIndex];
      if (row.kind === "device") {
        toggleExpanded(flipped ? "transmitters" : "receivers", row.label);
      } else if (row.kind === "group") {
        const side = flipped ? "transmitters" : "receivers";
        setGroupsExpanded(
          side,
          [row.key],
          !channelGroups.value[side].has(row.key),
        );
      } else if (onOpenDevice && row.sourceKind !== "external") {
        onOpenDevice(row.label, flipped ? "transmit" : "receive");
      }
      return;
    }
    if (position.inHeader) {
      const column = columns[position.columnIndex];
      if (column.kind === "device") {
        toggleExpanded(flipped ? "receivers" : "transmitters", column.label);
      } else if (column.kind === "group") {
        const side = flipped ? "receivers" : "transmitters";
        setGroupsExpanded(
          side,
          [column.key],
          !channelGroups.value[side].has(column.key),
        );
      } else if (onOpenDevice && column.sourceKind !== "external") {
        onOpenDevice(column.label, flipped ? "receive" : "transmit");
      }
      return;
    }
    const row = flipped
      ? columns[position.columnIndex]
      : rows[position.rowIndex];
    const column = flipped
      ? rows[position.rowIndex]
      : columns[position.columnIndex];
    if (row.kind !== "channel" || column.kind !== "channel") {
      const entries = [
        [row, "receivers"],
        [column, "transmitters"],
      ].filter(([entry]) => entry.kind !== "channel");
      const expand = entries.some(([entry]) => !entry.expanded);
      for (const [entry, side] of entries) {
        if (entry.kind === "device")
          setAllExpanded(side, [entry.label], expand);
        else setGroupsExpanded(side, [entry.key], expand);
      }
      return;
    }
    if (locked) {
      return;
    }

    if (
      !row.device.online ||
      (column.sourceKind !== "external" && !column.device.online)
    ) {
      setError(t("Both devices must be online to change this connection."));
      return;
    }
    const key = `${position.rowIndex}:${position.columnIndex}`;
    if (busy.current.has(key)) {
      return;
    }
    const state = cellState(row, column, subscriptionIndex, pending);
    if (state.kind === "pending") return;
    if (
      state.kind === "self-unsupported" ||
      state.kind === "self-unavailable"
    ) {
      setError(state.reason);
      return;
    }
    const requestName = deviceRequestName(row.device);
    busy.current.add(key);
    try {
      if (row.kind === "channel" && column.kind === "channel") {
        const subscribed = Boolean(state.subscription);
        if (column.sourceKind === "external") {
          const outcome = await runAction(
            t("Route external audio to {device} {channel}", {
              device: row.label,
              channel: row.name,
            }),
            () =>
              api.subscribeExternal(
                externalSubscriptionRequest(
                  column,
                  requestName,
                  row.number,
                  subscribed,
                ),
              ),
          );
          if (
            outcome.ok &&
            outcome.result.arc_effective_state_confirmed !== true
          ) {
            markExternalPending(
              requestName,
              row.number,
              column.sourceKey,
              column.number,
              subscribed,
            );
          }
          return;
        }
        const change = subscribed
          ? { action: "remove", rx_channel: row.number, rx_device: requestName }
          : {
              action: "add",
              rx_channel: row.number,
              rx_device: requestName,
              tx_channel: column.name,
              tx_device: column.label,
            };
        await requestSubscriptionChange(change, () =>
          runAction(
            subscribed
              ? t("unsubscribe {device} {channel}", {
                  device: row.label,
                  channel: row.name,
                })
              : t(
                  "subscribe {receiverDevice} {receiverChannel} to {transmitterChannel}@{transmitterDevice}",
                  {
                    receiverDevice: row.label,
                    receiverChannel: row.name,
                    transmitterChannel: column.name,
                    transmitterDevice: column.label,
                  },
                ),
            () =>
              subscribed
                ? api.unsubscribe({
                    rx_channel: row.number,
                    rx_device: requestName,
                  })
                : api.subscribe({
                    rx_channel: row.number,
                    rx_device: requestName,
                    tx_channel: column.name,
                    tx_device: column.label,
                  }),
          ),
        );
        return;
      }
    } finally {
      busy.current.delete(key);
    }
  };

  const statusHover =
    hover &&
    ((hover.inHeader &&
      !hover.inGutter &&
      hover.y >= layout.header - CELL &&
      columns[hover.columnIndex]?.activity?.count) ||
      (hover.inGutter &&
        !hover.inHeader &&
        hover.x >= layout.gutter - CELL &&
        rows[hover.rowIndex]?.activity?.count));
  const cellHover =
    hover &&
    !hover.inHeader &&
    !hover.inGutter &&
    rows[hover.rowIndex]?.kind === "channel" &&
    columns[hover.columnIndex]?.kind === "channel";
  const hoverText =
    hover && (cellHover || statusHover)
      ? describeHover(hover, rows, columns, subscriptionIndex, pending, flipped)
      : "";
  let cursor =
    !hover || (hover.inGutter && hover.inHeader)
      ? "default"
      : hover.inGutter || hover.inHeader
        ? "pointer"
        : rows[hover.rowIndex]?.kind !== "channel" ||
            columns[hover.columnIndex]?.kind !== "channel"
          ? "pointer"
          : "crosshair";
  if (cellHover) {
    const state = cellState(
      rows[hover.rowIndex],
      columns[hover.columnIndex],
      subscriptionIndex,
      pending,
      flipped,
    );
    if (
      locked ||
      state.kind === "self-unsupported" ||
      state.kind === "self-unavailable"
    )
      cursor = "not-allowed";
  }

  return html`
    <div class="matrix-shell">
      ${error ? html`<div class="text-error text-sm" role="alert">${error}</div>` : null}
      <div class="matrix-stage" ref=${stage}>
        <canvas class="matrix-canvas" ref=${canvas}></canvas>
        <div
          class="matrix-viewport"
          data-cell-size=${CELL}
          data-header-height=${layout.header}
          data-gutter-width=${layout.gutter}
          aria-describedby=${hoverText ? "routing-matrix-tooltip" : undefined}
          style=${`cursor:${cursor}`}
          ref=${viewport}
          onScroll=${(event) => {
            setHover(null);
            setScroll({
              left: event.target.scrollLeft,
              top: event.target.scrollTop,
            });
          }}
          onMouseMove=${(event) => setHover(locate(event))}
          onMouseLeave=${() => setHover(null)}
          onClick=${onClick}
        >
          <div class="matrix-spacer" style=${`width:${contentWidth}px;height:${contentHeight}px`}></div>
        </div>
        ${hoverText && hover ? html`<${MatrixTooltip} id="routing-matrix-tooltip" text=${hoverText} point=${hover} bounds=${size} onDismiss=${() => setHover(null)} />` : null}
        <div class="matrix-filters" style=${`left:12px;width:${layout.gutter - CELL - 36}px;top:${Math.max(12, layout.header / 2 - 60)}px`}>
          ${["transmitters", "receivers"].map(
            (
              side,
            ) => html`<label class="matrix-filter-label">${side === "receivers" ? t("Filter receivers") : t("Filter transmitters")}
            <input type="search" aria-label=${side === "receivers" ? t("Receivers") : t("Transmitters")}
              value=${side === "receivers" ? receiverFilter : transmitterFilter}
              onInput=${(event) => (side === "receivers" ? onReceiverFilter : onTransmitterFilter)?.(event.target.value)} />
          </label>`,
          )}
        </div>
        ${[false, true].map((columnAxis) => {
          const side = columnAxis !== flipped ? "transmitters" : "receivers";
          const label =
            side === "receivers"
              ? t("receiver devices and groups")
              : t("transmitter devices and groups");
          const expandLabel =
            side === "receivers"
              ? t("Expand all receiver devices and groups")
              : t("Expand all transmitter devices and groups");
          const collapseLabel =
            side === "receivers"
              ? t("Collapse all receiver devices and groups")
              : t("Collapse all transmitter devices and groups");
          const count = (columnAxis ? columns : rows).filter(
            (entry) => entry.kind === "device",
          ).length;
          return html`<div class=${`matrix-axis-controls ${columnAxis ? "column-axis" : "row-axis"}`}
            style=${
              columnAxis
                ? `left:${layout.gutter - CELL - 6}px;top:8px;width:${CELL}px`
                : `left:12px;top:${layout.header - CELL}px;width:${layout.gutter - 24}px;height:${CELL}px`
            }>
            <span class="matrix-axis-title">${side === "receivers" ? t("Receivers ({count})", { count }) : t("Transmitters ({count})", { count })}</span>
            <div class="matrix-expansion-controls"><${ExpansionButtons} vertical=${columnAxis} label=${label} expandLabel=${expandLabel} collapseLabel=${collapseLabel} onExpand=${() => onExpandDevices(side, true)} onCollapse=${() => onExpandDevices(side, false)} /></div>
          </div>`;
        })}
      </div>
    </div>
  `;
}

export function describeHover(
  hover,
  rows,
  columns,
  subscriptionIndex,
  pending,
  flipped,
) {
  if (!hover) {
    return "";
  }
  if (hover.inGutter && hover.inHeader) {
    return "";
  }
  if (hover.inGutter) {
    const row = rows[hover.rowIndex];
    return row.kind === "device"
      ? flipped
        ? tn(
            row.channelCount,
            "{device} — {count} transmit channel",
            "{device} — {count} transmit channels",
            { device: row.label },
          )
        : tn(
            row.channelCount,
            "{device} — {count} receive channel",
            "{device} — {count} receive channels",
            { device: row.label },
          )
      : row.kind === "group"
        ? tn(
            row.channelCount,
            "{device} — {group} ({count} channel)",
            "{device} — {group} ({count} channels)",
            { device: row.label, group: row.name },
          )
        : `${row.label} ${row.name}`;
  }
  if (hover.inHeader) {
    const column = columns[hover.columnIndex];
    return column.kind === "device"
      ? flipped
        ? tn(
            column.channelCount,
            "{device} — {count} receive channel",
            "{device} — {count} receive channels",
            { device: column.label },
          )
        : tn(
            column.channelCount,
            "{device} — {count} transmit channel",
            "{device} — {count} transmit channels",
            { device: column.label },
          )
      : column.kind === "group"
        ? tn(
            column.channelCount,
            "{device} — {group} ({count} channel)",
            "{device} — {group} ({count} channels)",
            { device: column.label, group: column.name },
          )
        : `${column.label} ${column.name}`;
  }
  const row = flipped ? columns[hover.columnIndex] : rows[hover.rowIndex];
  const column = flipped ? rows[hover.rowIndex] : columns[hover.columnIndex];
  const state = cellState(row, column, subscriptionIndex, pending);
  const receiver =
    row.kind === "device" ? row.label : `${row.name}@${row.label}`;
  const transmitter =
    column.kind === "device" ? column.label : `${column.name}@${column.label}`;
  if (state.kind === "aggregate") {
    const summary =
      state.severity === "error"
        ? tn(
            state.count,
            "{count} subscribed, some with errors",
            "{count} subscribed, some with errors",
          )
        : state.severity === "warning"
          ? tn(
              state.count,
              "{count} subscribed, some with warnings",
              "{count} subscribed, some with warnings",
            )
          : tn(state.count, "{count} subscribed", "{count} subscribed");
    return `${receiver} ← ${transmitter}\n${summary}`;
  }
  if (state.kind === "pending") {
    return `${receiver} ← ${transmitter}\n${t("Subscription change pending")}`;
  }
  if (state.kind === "partial") {
    if (column.sourceKind === "external") {
      return `${receiver} ← ${transmitter}`;
    }
    return `${receiver} ← ${state.subscription.tx_channel}@${state.subscription.tx_device}\n${format.subscriptionStatusText(state.subscription)}`;
  }
  if (state.kind === "self-unsupported" || state.kind === "self-unavailable") {
    return `${receiver} ← ${transmitter}\n${state.reason}`;
  }
  if (state.kind !== "empty") {
    return `${receiver} ← ${transmitter}\n${format.subscriptionStatusText(state.subscription)}`;
  }
  return `${receiver} ← ${transmitter}`;
}
