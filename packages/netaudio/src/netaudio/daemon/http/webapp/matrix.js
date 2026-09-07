import { api } from "./api.js";
import { channelGroups, groupChannels, setGroupsExpanded } from "./channel-groups.js";
import { Icon } from "./icons.js";
import * as format from "./format.js";
import { html, signal, useLayoutEffect, useMemo, useRef, useState } from "./lib/preact.js";
import { deviceRequestName, pendingKey, pendingSubscriptions } from "./store.js";
import { MatrixTooltip } from "./matrix-tooltip.js";
import { runAction as performAction } from "./actions.js";

const CELL = 24;
const GUTTER_PADDING = 12;
const HEADER_PADDING = 18;
const INDENT = 22;
const MINIMUM_GUTTER = 240;
const MINIMUM_HEADER = 120;
const MAXIMUM_GUTTER = 280;
const MAXIMUM_HEADER = 180;
const STORAGE_KEY = "netaudio.matrix.expanded";

function readExpanded() {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    if (parsed && Array.isArray(parsed.receivers) && Array.isArray(parsed.transmitters)) {
      return { receivers: new Set(parsed.receivers), transmitters: new Set(parsed.transmitters) };
    }
  } catch (error) {
    return { receivers: new Set(), transmitters: new Set() };
  }
  return { receivers: new Set(), transmitters: new Set() };
}

export const expanded = signal(readExpanded());

export function toggleExpanded(side, deviceLabel) {
  const current = expanded.value;
  const next = { receivers: new Set(current.receivers), transmitters: new Set(current.transmitters) };
  if (next[side].has(deviceLabel)) {
    next[side].delete(deviceLabel);
  } else {
    next[side].add(deviceLabel);
  }
  expanded.value = next;
  try {
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ receivers: [...next.receivers], transmitters: [...next.transmitters] }),
    );
  } catch (error) {
    return;
  }
}

export function setAllExpanded(side, deviceLabels, value) {
  const current = expanded.value;
  const next = { receivers: new Set(current.receivers), transmitters: new Set(current.transmitters) };
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
      JSON.stringify({ receivers: [...next.receivers], transmitters: [...next.transmitters] }),
    );
  } catch (error) {
    return;
  }
}

function channelEntries(device, direction) {
  const channels = device.channels ? device.channels[direction] || {} : {};
  return format.sortedChannelNumbers(channels).map((number) => ({
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
  const matching = channels.filter((channel) => channel.name.toLowerCase().includes(needle));
  if (!matching.length) {
    return null;
  }
  return { device: false, channels: new Set(matching.map((channel) => channel.number)) };
}

function buildAxis(devices, direction, expandedSet, filter, subscriptionsFor, groups) {
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
    const visibleChannels = match.channels ? channels.filter((channel) => match.channels.has(channel.number)) : channels;
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
          channels,
          device,
          kind: "channel",
          grouped,
          label,
          name: channel.name,
          number: channel.number,
          subscription: subscriptionsFor ? subscriptionsFor(device, channel.name) : null,
        });
      };
      if (groups.enabled) {
        for (const group of groupChannels(deviceRequestName(device), visibleChannels)) {
          const groupExpanded = groups[direction].has(group.key) || match.channels !== null;
          entries.push({ ...group, device, label, kind: "group", expanded: groupExpanded,
            channelCount: group.channels.length, subscription: null });
          if (groupExpanded) for (const channel of group.channels) appendChannel(channel, true);
        }
      } else {
        for (const channel of visibleChannels) appendChannel(channel);
      }
    }
  }
  return entries;
}

export function buildMatrixModel({ devices, expandedReceivers, expandedTransmitters, receiverFilter, transmitterFilter,
  groups = { enabled: false, receivers: new Set(), transmitters: new Set() } }) {
  const sorted = format.sortedDevices(devices);
  const subscriptionIndex = new Map();
  for (const device of sorted) {
    const byChannel = new Map();
    for (const subscription of device.subscriptions || []) {
      if (subscription.tx_device && subscription.tx_channel) {
        byChannel.set(subscription.rx_channel, subscription);
      }
    }
    subscriptionIndex.set(format.deviceLabel(device), byChannel);
  }
  const subscriptionsFor = (device, channelName) =>
    subscriptionIndex.get(format.deviceLabel(device)).get(channelName) || null;
  const allSubscriptions = [...subscriptionIndex.entries()].flatMap(([receiver, channels]) =>
    [...channels.values()].map((subscription) => ({ receiver, subscription })));
  const summarize = (entry, receiver) => {
    const subscriptions = allSubscriptions.filter((item) => receiver
      ? item.receiver === entry.label && (entry.kind === "device" || (entry.kind === "group"
        ? entry.channels.some((channel) => channel.name === item.subscription.rx_channel) : item.subscription.rx_channel === entry.name))
      : item.subscription.tx_device === entry.label && (entry.kind === "device" || (entry.kind === "group"
        ? entry.channels.some((channel) => channel.name === item.subscription.tx_channel) : item.subscription.tx_channel === entry.name)));
    return { count: subscriptions.length, severity: severityName(Math.max(0, ...subscriptions.map((item) => severityRank(item.subscription.status?.severity)))) };
  };
  const columns = buildAxis(sorted, "transmitters", expandedTransmitters, transmitterFilter, null, groups);
  const rows = buildAxis(sorted, "receivers", expandedReceivers, receiverFilter, subscriptionsFor, groups);
  for (const entry of rows) entry.activity = summarize(entry, true);
  for (const entry of columns) entry.activity = summarize(entry, false);
  return {
    columns,
    rows,
    subscriptionIndex,
  };
}

function severityRank(severity) {
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

function severityName(rank) {
  if (rank >= 3) {
    return "error";
  }
  if (rank === 2) {
    return "warning";
  }
  return "ok";
}

export function cellState(row, column, subscriptionIndex, pending, flipped = false) {
  if (flipped) [row, column] = [column, row];
  const receiverSubscriptions = subscriptionIndex.get(row.label) || new Map();
  if (row.kind === "channel" && column.kind === "channel") {
    const pendingEntry = pending[pendingKey(deviceRequestName(row.device), row.number)] || pending[pendingKey(row.label, row.number)];
    if (pendingEntry) {
      const targetsThisColumn =
        pendingEntry.action === "add"
          ? pendingEntry.tx_device === column.label && pendingEntry.tx_channel === column.name
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
    if (!subscription || subscription.tx_device !== column.label || subscription.tx_channel !== column.name) {
      return { kind: "empty" };
    }
    const status = subscription.status;
    return { kind: severityName(severityRank(status ? status.severity : "ok")), subscription };
  }
  if (row.kind === "channel") {
    const subscription = row.subscription;
    if (!subscription || subscription.tx_device !== column.label) {
      return { kind: "empty" };
    }
    if (column.kind === "group" && !column.channels.some((channel) => channel.name === subscription.tx_channel)) {
      return { kind: "empty" };
    }
    return { kind: "partial", subscription };
  }
  let count = 0;
  let worst = 0;
  for (const subscription of receiverSubscriptions.values()) {
    if (row.kind === "group" && !row.channels.some((channel) => channel.name === subscription.rx_channel)) continue;
    if (column.kind === "group" && !column.channels.some((channel) => channel.name === subscription.tx_channel)) continue;
    if (subscription.tx_device !== column.label) {
      continue;
    }
    if (column.kind === "channel" && subscription.tx_channel !== column.name) {
      continue;
    }
    count += 1;
    worst = Math.max(worst, severityRank(subscription.status ? subscription.status.severity : "ok"));
  }
  if (count === 0) {
    return { kind: "empty" };
  }
  return { count, kind: "aggregate", severity: severityName(worst) };
}

function readTheme() {
  const style =
    typeof getComputedStyle === "function" && document.documentElement
      ? getComputedStyle(document.documentElement)
      : null;
  const read = (name, fallback) => (style ? style.getPropertyValue(name).trim() || fallback : fallback);
  return {
    accent: read("--accent", "#ff2323"),
    bad: read("--bad", "#ff4d4d"),
    background: read("--black", "#000000"),
    dataFont: read("--font-data", "ui-monospace, Menlo, monospace"),
    good: read("--good", "#2fe36a"),
    hover: "rgba(255, 255, 255, 0.08)",
    line: read("--line", "#242424"),
    lineStrong: read("--line-strong", "#3d3d3d"),
    panel: read("--panel", "#0a0a0a"),
    text: read("--text", "#ffffff"),
    uiFont: read("--font-ui", "system-ui, sans-serif"),
    warn: read("--warn", "#ffc400"),
  };
}

function rowLabelText(row) {
  if (row.kind === "group") return `${row.expanded ? "⊟" : "⊞"} ${row.name}`;
  if (row.kind === "device") {
    return `${row.expanded ? "▾" : "▸"} ${row.label}  (${row.channelCount})`;
  }
  return `${row.number}  ${row.name}`;
}

function columnLabelText(column) {
  if (column.kind === "group") return `${column.name} ${column.expanded ? "⊟" : "⊞"}`;
  if (column.kind === "device") {
    return `${column.label}  (${column.channelCount}) ${column.expanded ? "▾" : "▸"}`;
  }
  return `${column.number}  ${column.name}`;
}

export function measureMatrixLayout(context, rows, columns, theme) {
  context.font = `12px ${theme.dataFont}`;
  let gutter = MINIMUM_GUTTER;
  for (const row of rows) {
    const indent = row.kind === "device" ? 0 : row.grouped ? INDENT * 2 : INDENT;
    const label = context.measureText(rowLabelText(row)).width;
    gutter = Math.max(gutter, CELL + GUTTER_PADDING + indent + label + GUTTER_PADDING);
  }
  context.font = `11px ${theme.dataFont}`;
  let header = Math.max(MINIMUM_HEADER, 160);
  for (const column of columns) {
    header = Math.max(header, CELL + HEADER_PADDING + context.measureText(columnLabelText(column)).width + HEADER_PADDING);
  }
  return { gutter: Math.min(MAXIMUM_GUTTER, Math.ceil(gutter)), header: Math.min(MAXIMUM_HEADER, Math.ceil(header)) };
}

function fitLabel(context, text, width) {
  if (context.measureText(text).width <= width) return text;
  let end = text.length;
  while (end > 0 && context.measureText(text.slice(0, end) + "…").width > width) end -= 1;
  return text.slice(0, end) + "…";
}

export function ExpansionButtons({ label, onExpand, onCollapse }) {
  return html`<div class="join" role="group" aria-label=${label}>
    <button type="button" class="btn btn-xs btn-square join-item" title=${`Expand all ${label}`} aria-label=${`Expand all ${label}`} onClick=${onExpand}><${Icon} name="plus" /></button>
    <button type="button" class="btn btn-xs btn-square join-item" title=${`Collapse all ${label}`} aria-label=${`Collapse all ${label}`} onClick=${onCollapse}><${Icon} name="minus" /></button>
  </div>`;
}

export function RoutingMatrix({ columns: transmitters, onOpenDevice, rows: receivers, subscriptionIndex, flipped = false,
  onExpandDevices, onExpandGroups, grouped = false }) {
  const rows = flipped ? transmitters : receivers;
  const columns = flipped ? receivers : transmitters;
  const stage = useRef(null);
  const viewport = useRef(null);
  const canvas = useRef(null);
  const theme = useMemo(() => readTheme(), []);
  const [size, setSize] = useState({ height: 0, width: 0 });
  const [scroll, setScroll] = useState({ left: 0, top: 0 });
  const [hover, setHover] = useState(null);
  const [layout, setLayout] = useState({ gutter: MINIMUM_GUTTER, header: MINIMUM_HEADER });
  const busy = useRef(new Set());
  const pending = pendingSubscriptions.value;

  const contentWidth = layout.gutter + columns.length * CELL;
  const contentHeight = layout.header + rows.length * CELL;

  useLayoutEffect(() => {
    const node = viewport.current;
    if (!node) {
      return undefined;
    }
    const observer = new ResizeObserver(() => setSize({ height: node.clientHeight, width: node.clientWidth }));
    observer.observe(node);
    setSize({ height: node.clientHeight, width: node.clientWidth });
    return () => observer.disconnect();
  }, []);

  useLayoutEffect(() => {
    const node = canvas.current;
    if (!node) {
      return;
    }
    const measured = measureMatrixLayout(node.getContext("2d"), rows, columns, theme);
    if (measured.gutter !== layout.gutter || measured.header !== layout.header) {
      setLayout(measured);
    }
  }, [columns, rows, theme]);

  useLayoutEffect(() => {
    const node = canvas.current;
    if (!node || size.width === 0 || size.height === 0) {
      return;
    }
    const ratio = window.devicePixelRatio || 1;
    node.width = Math.floor(size.width * ratio);
    node.height = Math.floor(size.height * ratio);
    node.style.width = `${size.width}px`;
    node.style.height = `${size.height}px`;
    const context = node.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    draw(context, { columns, flipped, hover, layout, pending, rows, scroll, size, subscriptionIndex, theme });
  }, [columns, flipped, hover, layout, pending, rows, scroll, size, subscriptionIndex, theme]);

  const locate = (event) => {
    const bounds = canvas.current.getBoundingClientRect();
    const x = event.clientX - bounds.left;
    const y = event.clientY - bounds.top;
    const inGutter = x < layout.gutter;
    const inHeader = y < layout.header;
    const columnIndex = inGutter ? -1 : Math.floor((x - layout.gutter + scroll.left) / CELL);
    const rowIndex = inHeader ? -1 : Math.floor((y - layout.header + scroll.top) / CELL);
    if ((!inGutter && (columnIndex < 0 || columnIndex >= columns.length)) || (!inHeader && (rowIndex < 0 || rowIndex >= rows.length))) {
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
        setGroupsExpanded(side, [row.key], !channelGroups.value[side].has(row.key));
      } else if (onOpenDevice) {
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
        setGroupsExpanded(side, [column.key], !channelGroups.value[side].has(column.key));
      } else if (onOpenDevice) {
        onOpenDevice(column.label, flipped ? "receive" : "transmit");
      }
      return;
    }
    const row = flipped ? columns[position.columnIndex] : rows[position.rowIndex];
    const column = flipped ? rows[position.rowIndex] : columns[position.columnIndex];
    const key = `${position.rowIndex}:${position.columnIndex}`;
    if (busy.current.has(key)) {
      return;
    }
    const state = cellState(row, column, subscriptionIndex, pending);
    const requestName = deviceRequestName(row.device);
    busy.current.add(key);
    try {
      if (row.kind === "channel" && column.kind === "channel") {
        const subscribed = state.kind !== "empty" && state.kind !== "pending";
        await runAction(
          subscribed
            ? `unsubscribe ${row.label} ${row.name}`
            : `subscribe ${row.label} ${row.name} to ${column.name}@${column.label}`,
          () =>
            subscribed
              ? api.unsubscribe({ rx_channel: row.number, rx_device: requestName })
              : api.subscribe({
                  rx_channel: row.number,
                  rx_device: requestName,
                  tx_channel: column.name,
                  tx_device: column.label,
                }),
        );
        return;
      }
      if (row.kind === "device" && column.kind === "device") {
        const receiverSubscriptions = subscriptionIndex.get(row.label) || new Map();
        const existing = row.channels.filter((channel) => {
          const subscription = receiverSubscriptions.get(channel.name);
          return subscription && subscription.tx_device === column.label;
        });
        if (existing.length) {
          await runAction(`unsubscribe ${row.label} from ${column.label} (${existing.length} channels)`, () =>
            api.unsubscribe({ rx_channels: existing.map((channel) => channel.number), rx_device: requestName }),
          );
          return;
        }
        const pairs = row.channels.slice(0, Math.min(row.channels.length, column.channels.length)).map((channel, index) => ({
          rx_channel: channel.number,
          tx_channel: column.channels[index].name,
          tx_device: column.label,
        }));
        await runAction(`subscribe ${row.label} to ${column.label} one-to-one (${pairs.length} channels)`, () =>
          api.subscribe({ rx_device: requestName, subscriptions: pairs }),
        );
      }
    } finally {
      busy.current.delete(key);
    }
  };

  const hoverText = describeHover(hover, rows, columns, subscriptionIndex, pending, flipped);
  const cursor = !hover || (hover.inGutter && hover.inHeader) ? "default"
    : hover.inGutter || hover.inHeader ? "pointer"
    : rows[hover.rowIndex]?.kind === columns[hover.columnIndex]?.kind
      && ["device", "channel"].includes(rows[hover.rowIndex]?.kind) ? "crosshair" : "default";

  return html`
    <div class="matrix-shell">
      ${error ? html`<div class="text-error text-sm" role="alert">${error}</div>` : null}
      <div class="matrix-stage" ref=${stage}>
        <canvas class="matrix-canvas" ref=${canvas}></canvas>
        <div
          class="matrix-viewport"
          aria-describedby=${hoverText ? "routing-matrix-tooltip" : undefined}
          style=${`cursor:${cursor}`}
          ref=${viewport}
          onScroll=${(event) => { setHover(null); setScroll({ left: event.target.scrollLeft, top: event.target.scrollTop }); }}
          onMouseMove=${(event) => setHover(locate(event))}
          onMouseLeave=${() => setHover(null)}
          onClick=${onClick}
        >
          <div class="matrix-spacer" style=${`width:${contentWidth}px;height:${contentHeight}px`}></div>
        </div>
        ${hoverText && hover ? html`<${MatrixTooltip} id="routing-matrix-tooltip" text=${hoverText} point=${hover} bounds=${size} onDismiss=${() => setHover(null)} />` : null}
        ${[false, true].map((columnAxis) => {
          const side = columnAxis !== flipped ? "transmitters" : "receivers";
          const label = side === "receivers" ? "receiver" : "transmitter";
          return html`<div class=${`matrix-axis-controls ${columnAxis ? "column-axis" : "row-axis"}`}
            style=${columnAxis ? `left:${layout.gutter - 152}px;top:8px` : `left:12px;top:${layout.header - (grouped ? 76 : 44)}px`}>
            <div class="flex items-center gap-2"><span>Devices</span><${ExpansionButtons} label=${`${label} devices`} onExpand=${() => onExpandDevices(side, true)} onCollapse=${() => onExpandDevices(side, false)} /></div>
            ${grouped ? html`<div class="flex items-center gap-2"><span>Groups</span><${ExpansionButtons} label=${`${label} groups`} onExpand=${() => onExpandGroups(side, true)} onCollapse=${() => onExpandGroups(side, false)} /></div>` : null}
          </div>`;
        })}
      </div>
      <div class="matrix-status">
        <span>${hoverText}</span>
        <span class="matrix-legend">
          <span class="legend-swatch ok"></span>subscribed
          <span class="legend-swatch warning"></span>warning
          <span class="legend-swatch error"></span>error
          <span class="legend-swatch pending"></span>pending
        </span>
      </div>
    </div>
  `;
}

export function describeHover(hover, rows, columns, subscriptionIndex, pending, flipped) {
  if (!hover) {
    return "";
  }
  if (hover.inGutter && hover.inHeader) {
    return "";
  }
  if (hover.inGutter) {
    const row = rows[hover.rowIndex];
    return row.kind === "device"
      ? `${row.label} — ${row.channelCount} ${flipped ? "transmit" : "receive"} channels`
      : `${row.label} ${row.number} ${row.name}`;
  }
  if (hover.inHeader) {
    const column = columns[hover.columnIndex];
    return column.kind === "device"
      ? `${column.label} — ${column.channelCount} ${flipped ? "receive" : "transmit"} channels`
      : `${column.label} ${column.number} ${column.name}`;
  }
  const row = flipped ? columns[hover.columnIndex] : rows[hover.rowIndex];
  const column = flipped ? rows[hover.rowIndex] : columns[hover.columnIndex];
  const state = cellState(row, column, subscriptionIndex, pending);
  const receiver = row.kind === "device" ? row.label : `${row.name}@${row.label}`;
  const transmitter = column.kind === "device" ? column.label : `${column.name}@${column.label}`;
  if (state.kind === "aggregate") {
    return `${receiver} ← ${transmitter}\n${state.count} subscribed (${state.severity})`;
  }
  if (state.kind === "pending") {
    return `${receiver} ← ${transmitter}\nSubscription change pending`;
  }
  if (state.kind === "partial") {
    return `${receiver} ← ${state.subscription.tx_channel}@${state.subscription.tx_device}\n${format.subscriptionStatusText(state.subscription)}`;
  }
  if (state.kind !== "empty") {
    return `${receiver} ← ${transmitter}\n${format.subscriptionStatusText(state.subscription)}`;
  }
  return `${receiver} ← ${transmitter}`;
}

function draw(context, { columns, flipped, hover, layout, pending, rows, scroll, size, subscriptionIndex, theme }) {
  const { gutter, header } = layout;
  context.clearRect(0, 0, size.width, size.height);
  context.fillStyle = theme.background;
  context.fillRect(0, 0, size.width, size.height);

  const firstColumn = Math.max(0, Math.floor(scroll.left / CELL));
  const lastColumn = Math.min(columns.length, Math.ceil((scroll.left + size.width - gutter) / CELL) + 1);
  const firstRow = Math.max(0, Math.floor(scroll.top / CELL));
  const lastRow = Math.min(rows.length, Math.ceil((scroll.top + size.height - header) / CELL) + 1);
  const columnX = (index) => gutter + index * CELL - scroll.left;
  const rowY = (index) => header + index * CELL - scroll.top;

  context.save();
  context.beginPath();
  context.rect(gutter, header, size.width - gutter, size.height - header);
  context.clip();

  for (let index = firstRow; index < lastRow; index += 1) {
    if (rows[index].kind === "device") {
      context.fillStyle = theme.panel;
      context.fillRect(gutter, rowY(index), size.width - gutter, CELL);
    }
  }
  for (let index = firstColumn; index < lastColumn; index += 1) {
    if (columns[index] && columns[index].kind === "device") {
      context.fillStyle = theme.panel;
      context.fillRect(columnX(index), header, CELL, size.height - header);
    }
  }

  if (hover && !hover.inGutter && !hover.inHeader) {
    context.fillStyle = theme.hover;
    context.fillRect(gutter, rowY(hover.rowIndex), size.width - gutter, CELL);
    context.fillRect(columnX(hover.columnIndex), header, CELL, size.height - header);
  }

  context.lineWidth = 1;
  context.beginPath();
  context.strokeStyle = theme.line;
  for (let index = firstColumn; index <= lastColumn; index += 1) {
    const x = Math.floor(columnX(index)) + 0.5;
    context.moveTo(x, header);
    context.lineTo(x, size.height);
  }
  for (let index = firstRow; index <= lastRow; index += 1) {
    const y = Math.floor(rowY(index)) + 0.5;
    context.moveTo(gutter, y);
    context.lineTo(size.width, y);
  }
  context.stroke();

  context.beginPath();
  context.strokeStyle = theme.lineStrong;
  for (let index = firstColumn; index <= lastColumn; index += 1) {
    if (columns[index] && columns[index].kind === "device") {
      const x = Math.floor(columnX(index)) + 0.5;
      context.moveTo(x, header);
      context.lineTo(x, size.height);
    }
  }
  for (let index = firstRow; index <= lastRow; index += 1) {
    if (rows[index] && rows[index].kind === "device") {
      const y = Math.floor(rowY(index)) + 0.5;
      context.moveTo(gutter, y);
      context.lineTo(size.width, y);
    }
  }
  context.stroke();

  context.textAlign = "center";
  context.textBaseline = "middle";
  context.font = `600 10px ${theme.uiFont}`;
  for (let rowIndex = firstRow; rowIndex < lastRow; rowIndex += 1) {
    const row = rows[rowIndex];
    for (let columnIndex = firstColumn; columnIndex < lastColumn; columnIndex += 1) {
      const column = columns[columnIndex];
      if (!column) {
        continue;
      }
      const state = cellState(row, column, subscriptionIndex, pending, flipped);
      if (state.kind === "empty") {
        continue;
      }
      const x = columnX(columnIndex);
      const y = rowY(rowIndex);
      if (state.kind === "pending") {
        context.strokeStyle = theme.text;
        context.lineWidth = 2;
        context.strokeRect(x + 5, y + 5, CELL - 10, CELL - 10);
        context.lineWidth = 1;
        continue;
      }
      if (state.kind === "partial") {
        context.fillStyle = severityColor(theme, state.subscription.status ? state.subscription.status.severity : "ok");
        context.beginPath();
        context.arc(x + CELL / 2, y + CELL / 2, 3.5, 0, Math.PI * 2);
        context.fill();
        continue;
      }
      if (state.kind === "aggregate") {
        context.fillStyle = severityColor(theme, state.severity);
        context.fillRect(x + 4, y + 4, CELL - 8, CELL - 8);
        context.fillStyle = theme.background;
        context.fillText(String(state.count), x + CELL / 2, y + CELL / 2 + 0.5);
        continue;
      }
      context.fillStyle = severityColor(theme, state.kind);
      context.fillRect(x + 4, y + 4, CELL - 8, CELL - 8);
    }
  }
  context.restore();

  drawGutter(context, { firstRow, gutter, header, hover, rows, scroll, size, theme });
  drawHeader(context, { columns, firstColumn, gutter, header, hover, lastColumn, scroll, size, theme });

  context.fillStyle = theme.panel;
  context.fillRect(0, 0, gutter, header);
  context.strokeStyle = theme.lineStrong;
  context.beginPath();
  context.moveTo(0, header + 0.5);
  context.lineTo(size.width, header + 0.5);
  context.moveTo(gutter + 0.5, 0);
  context.lineTo(gutter + 0.5, size.height);
  context.stroke();
  context.fillStyle = theme.text;
  context.font = `600 10.5px ${theme.uiFont}`;
  context.textAlign = "left";
  context.textBaseline = "alphabetic";
  context.fillText(flipped ? "DANTE TRANSMITTERS" : "DANTE RECEIVERS", GUTTER_PADDING, header - 10);
  context.save();
  context.translate(gutter - 12, header - CELL);
  context.rotate(-Math.PI / 2);
  context.fillText(flipped ? "DANTE RECEIVERS" : "DANTE TRANSMITTERS", 0, 0);
  context.restore();
}

function severityColor(theme, severity) {
  if (severity === "error" || severity === "critical" || severity === "bad") {
    return theme.bad;
  }
  if (severity === "warning" || severity === "warn") {
    return theme.warn;
  }
  return theme.good;
}

function drawGutter(context, { firstRow, gutter, header, hover, rows, scroll, size, theme }) {
  context.save();
  context.beginPath();
  context.rect(0, header, gutter, size.height - header);
  context.clip();
  context.fillStyle = theme.panel;
  context.fillRect(0, header, gutter, size.height - header);
  context.textBaseline = "middle";
  const lastRow = Math.min(rows.length, Math.ceil((scroll.top + size.height - header) / CELL) + 1);
  for (let index = firstRow; index < lastRow; index += 1) {
    const row = rows[index];
    const y = header + index * CELL - scroll.top;
    if (hover && hover.rowIndex === index && !hover.inHeader) {
      context.fillStyle = theme.hover;
      context.fillRect(0, y, gutter, CELL);
    }
    if (row.kind === "device") {
      context.fillStyle = theme.lineStrong;
      context.fillRect(0, Math.floor(y) + 0.5, gutter, 1);
    }
    context.fillStyle = theme.text;
    context.textAlign = "left";
    if (row.activity?.count) {
      context.fillStyle = severityColor(theme, row.activity.severity);
      context.beginPath();
      context.arc(CELL / 2, y + CELL / 2, 4, 0, Math.PI * 2);
      context.fill();
      context.fillStyle = theme.text;
    }
    if (row.kind === "device") {
      context.font = `600 12px ${theme.uiFont}`;
      context.fillText(fitLabel(context, rowLabelText(row), gutter - CELL - GUTTER_PADDING * 2), CELL + GUTTER_PADDING, y + CELL / 2);
    } else {
      context.font = `12px ${theme.dataFont}`;
      const x = CELL + GUTTER_PADDING + (row.grouped ? INDENT * 2 : INDENT);
      context.fillText(fitLabel(context, rowLabelText(row), gutter - x - GUTTER_PADDING), x, y + CELL / 2);
    }
  }
  context.restore();
}

function drawHeader(context, { columns, firstColumn, gutter, header, hover, lastColumn, scroll, size, theme }) {
  context.save();
  context.beginPath();
  context.rect(gutter, 0, size.width - gutter, header);
  context.clip();
  context.fillStyle = theme.panel;
  context.fillRect(gutter, 0, size.width - gutter, header);
  for (let index = firstColumn; index < lastColumn; index += 1) {
    const column = columns[index];
    if (!column) {
      continue;
    }
    const x = gutter + index * CELL - scroll.left;
    if (hover && hover.columnIndex === index && !hover.inGutter) {
      context.fillStyle = theme.hover;
      context.fillRect(x, 0, CELL, header);
    }
    if (column.kind === "device") {
      context.fillStyle = theme.lineStrong;
      context.fillRect(Math.floor(x) + 0.5, 0, 1, header);
    }
    context.save();
    if (column.activity?.count) {
      context.fillStyle = severityColor(theme, column.activity.severity);
      context.beginPath();
      context.arc(x + CELL / 2, header - CELL / 2, 4, 0, Math.PI * 2);
      context.fill();
    }
    context.translate(x + CELL / 2, header - HEADER_PADDING - CELL);
    context.rotate(-Math.PI / 2);
    context.textAlign = "left";
    context.textBaseline = "middle";
    context.fillStyle = theme.text;
    context.font = column.kind === "device" ? `600 11px ${theme.uiFont}` : `11px ${theme.dataFont}`;
    context.fillText(fitLabel(context, columnLabelText(column), header - CELL - HEADER_PADDING * 2), 0, 0);
    context.restore();
  }
  context.restore();
}
