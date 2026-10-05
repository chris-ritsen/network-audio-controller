import { prefersLight } from "./color-scheme.js";
import * as format from "./format.js";
import { t } from "./i18n.js";
import { html, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "./lib/preact.js";
import { observeVisibility, useAnimationFrames } from "./meter-animation.js";
import {
  METER_REGIONS,
  SCALE_TICK_SETS,
  advanceOverIndicator,
  advancePeakMeter,
  advanceReadout,
  chooseScaleTicks,
  clearOverIndicator,
  createOverIndicator,
  createPeakMeter,
  createReadout,
  isOverReading,
  levelFraction,
  overIndicatorLit,
  scaleLabel,
} from "./peak-meter.js";
import { meterValuesFor, onMeterValues } from "./store.js";

const COLUMN_GAP = 14;
const INDICATOR_GAP = 6;
const LAMP_PADDING = 6;
const SCALE_TICK_GAP = 8;
const MONOSPACE = "ui-monospace, SFMono-Regular, Menlo, monospace";
const LABEL_FONT = `11px ${MONOSPACE}`;
const SCALE_FONT = `10px ${MONOSPACE}`;
const LAMP_FONT = `bold 9px ${MONOSPACE}`;
const LAMP_TEXT = t("OVER");
const MINIMUM_BAR_WIDTH = 120;
const ROW_HEIGHT = 20;
const SCALE_HEIGHT = 18;
const BAR_INSET = 5;
const LAMP_INSET = 4;
const PRESENCE_LABELS = {
  below_threshold: t("Quiet"),
  signal_present: t("Signal"),
  clipping: t("Clipping"),
  muted: t("Muted"),
  mute_or_floor: t("Muted or below meter floor"),
  framing_marker: t("Signal unavailable"),
  unknown: t("Unknown"),
};
const WIDEST_LEVEL = "-126.0 dBFS";
const SCALE_TICKS = [...new Set(SCALE_TICK_SETS.flat())];

const DARK_COLORS = {
  critical: "#ff2323",
  grid: "#353a3e",
  high: "#ffc400",
  lamp: "#350909",
  lampLit: "#ff2323",
  lampText: "#ffffff",
  lampTextLit: "#000000",
  nominal: "#2fe36a",
  peak: "#ffffff",
  text: "#ffffff",
  track: "#1b1b1b",
};

const LIGHT_COLORS = {
  critical: "#e02424",
  grid: "#dde1e4",
  high: "#e0a800",
  lamp: "#f6d0d0",
  lampLit: "#e02424",
  lampText: "#1c1f22",
  lampTextLit: "#ffffff",
  nominal: "#1fa850",
  peak: "#1c1f22",
  text: "#1c1f22",
  track: "#e3e6e8",
};

function meterReadout(value, presence, source) {
  const level = format.meteringLabel(value, source);
  const label = presence ? PRESENCE_LABELS[presence] || t("Unknown") : "";
  if (label.toLowerCase() === level.toLowerCase()) return label;
  return label ? `${level}  ${label}` : level;
}

function deviceChannels(device, direction) {
  return device && device.channels ? device.channels[direction === "tx" ? "transmitters" : "receivers"] || {} : {};
}

function channelNames(device, direction) {
  const names = {};
  for (const [number, channel] of Object.entries(deviceChannels(device, direction))) {
    names[Number(number)] = channel.name || "";
  }
  return names;
}

function channelNumbers(device, direction, values) {
  const declared = Object.keys(deviceChannels(device, direction)).map(Number);
  if (declared.length) {
    return declared.sort((first, second) => first - second);
  }
  return Object.keys(values)
    .map(Number)
    .sort((first, second) => first - second);
}

function channelLabel(number, names) {
  return names[number] ? `${number}  ${names[number]}` : String(number);
}

export function MeterBank({ device, direction, serverName }) {
  const canvas = useRef(null);
  const container = useRef(null);
  const latest = useRef(meterValuesFor(serverName));
  const peaks = useRef(new Map());
  const [width, setWidth] = useState(0);
  const [visible, setVisible] = useState(false);

  const names = useMemo(() => channelNames(device, direction), [device, direction]);
  const declaredNumbers = useMemo(() => channelNumbers(device, direction, {}), [device, direction]);
  const initialValues = latest.current ? latest.current[direction] || {} : {};
  const [channelCount, setChannelCount] = useState(channelNumbers(device, direction, initialValues).length);

  useLayoutEffect(() => {
    const node = container.current;
    if (!node) {
      return undefined;
    }
    const observer = new ResizeObserver(() => setWidth(node.clientWidth));
    observer.observe(node);
    setWidth(node.clientWidth);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const node = container.current;
    if (!node) return undefined;
    return observeVisibility([node], (_, shown) => setVisible(shown));
  }, []);

  useEffect(() => {
    latest.current = meterValuesFor(serverName);
    const unsubscribe = onMeterValues((name, values) => {
      if (name === serverName) {
        latest.current = values;
      }
    });
    return unsubscribe;
  }, [serverName]);

  useAnimationFrames(visible && width > 0, (elapsed) => {
    const values = latest.current ? latest.current[direction] || {} : {};
    const presence = latest.current ? latest.current[`${direction}_signal_presence`] || {} : {};
    const numbers = declaredNumbers.length ? declaredNumbers : channelNumbers(device, direction, values);
    if (numbers.length !== channelCount) {
      setChannelCount(numbers.length);
    }
    const node = canvas.current;
    if (node) drawMeters(node, width, numbers, values, presence, names, peaks.current, elapsed, latest.current?.metering_source);
  });

  const indicatorNumber = (event) => {
    const node = canvas.current;
    if (!node) return null;
    const bounds = node.getBoundingClientRect();
    return overIndicatorAt(node, event.clientX - bounds.left, event.clientY - bounds.top);
  };

  const clearOver = useCallback((event) => {
    const number = indicatorNumber(event);
    if (number === null) return;
    const state = peaks.current.get(number);
    if (state) clearOverIndicator(state.over);
  }, []);

  const pointAt = useCallback((event) => {
    const cursor = indicatorNumber(event) === null ? "" : "pointer";
    if (event.currentTarget.style.cursor !== cursor) event.currentTarget.style.cursor = cursor;
  }, []);

  const height = SCALE_HEIGHT + Math.max(ROW_HEIGHT, channelCount * ROW_HEIGHT);
  const title = direction === "tx"
    ? t("Transmit levels. Click an OVER lamp to clear its count.")
    : t("Receive levels. Click an OVER lamp to clear its count.");

  return html`
    <div class="meter-bank" ref=${container}>
      ${channelCount === 0
        ? html`<div class="notice">${direction === "tx" ? t("No transmit levels received yet.") : t("No receive levels received yet.")}</div>`
        : html`<canvas
            ref=${canvas}
            role="img"
            aria-label=${title}
            style=${`height:${height}px`}
            onClick=${clearOver}
            onMouseMove=${pointAt}
          ></canvas>`}
    </div>
  `;
}

export function measureColumns(context, numbers, names) {
  context.font = LABEL_FONT;
  let labelWidth = 0;
  let valueWidth = context.measureText(`${WIDEST_LEVEL}  ${PRESENCE_LABELS.clipping}`).width;
  for (const label of Object.values(PRESENCE_LABELS)) {
    valueWidth = Math.max(valueWidth, context.measureText(label).width);
  }
  for (const number of numbers) {
    labelWidth = Math.max(labelWidth, context.measureText(channelLabel(number, names)).width);
  }
  return { labelWidth: Math.ceil(labelWidth), valueWidth: Math.ceil(valueWidth) };
}

function wrapLabel(context, text, available) {
  const pieces = text.match(/[^\s\-_:./]+[\s\-_:./]*|[\s\-_:./]+/g) || [text];
  const lines = [];
  let line = "";
  const fits = (candidate) => context.measureText(candidate).width <= available;
  for (const piece of pieces) {
    if (line && !fits(line + piece)) {
      lines.push(line);
      line = "";
    }
    if (fits(line + piece)) {
      line += piece;
      continue;
    }
    for (const character of piece) {
      if (line && !fits(line + character)) {
        lines.push(line);
        line = "";
      }
      line += character;
    }
  }
  lines.push(line);
  return lines;
}

const layouts = new WeakMap();
const paintedFrames = new WeakMap();

function meterLayout(context, width, numbers, names) {
  const columns = measureColumns(context, numbers, names);
  context.font = LAMP_FONT;
  const lampWidth = Math.ceil(context.measureText(LAMP_TEXT).width) + LAMP_PADDING * 2;
  context.font = LABEL_FONT;
  const countWidth = Math.ceil(context.measureText("999").width);
  const indicatorWidth = lampWidth + INDICATOR_GAP + countWidth;
  const wideBarLeft = COLUMN_GAP * 2 + columns.labelWidth;
  const wideBarWidth = width - wideBarLeft - indicatorWidth - columns.valueWidth - COLUMN_GAP * 3;
  const compact = wideBarWidth < MINIMUM_BAR_WIDTH;
  const barLeft = compact ? COLUMN_GAP : wideBarLeft;
  const barWidth = Math.max(1, compact ? width - COLUMN_GAP * 3 - indicatorWidth : wideBarWidth);
  const lampLeft = barLeft + barWidth + COLUMN_GAP;
  const countCentre = lampLeft + lampWidth + INDICATOR_GAP + countWidth / 2;
  const readoutRight = width - COLUMN_GAP;
  const readoutWidth = compact ? Math.max(1, width - COLUMN_GAP * 2) : columns.valueWidth;

  context.font = SCALE_FONT;
  const tickWidths = new Map(SCALE_TICKS.map((tick) => [tick, Math.ceil(context.measureText(scaleLabel(tick)).width)]));
  const ticks = chooseScaleTicks(barWidth, (tick) => tickWidths.get(tick), SCALE_TICK_GAP).map((tick) => {
    const x = barLeft + levelFraction(tick) * barWidth;
    const half = tickWidths.get(tick) / 2;
    return { label: scaleLabel(tick), x: Math.round(x) + 0.5, centre: Math.min(width - half, Math.max(half, x)) };
  });

  context.font = LABEL_FONT;
  let height = SCALE_HEIGHT;
  const rows = numbers.map((number) => {
    const label = channelLabel(number, names);
    const lines = compact ? wrapLabel(context, label, width - COLUMN_GAP * 2) : [label];
    const y = height;
    const barY = y + (compact ? lines.length * ROW_HEIGHT : 0);
    height += ROW_HEIGHT * (compact ? lines.length + 2 : 1);
    return { y, barY, lines, readoutY: compact ? barY + ROW_HEIGHT * 1.5 : barY + ROW_HEIGHT / 2 };
  });
  height = Math.max(SCALE_HEIGHT + ROW_HEIGHT, height);
  const regions = METER_REGIONS.map((region) => ({
    name: region.name,
    start: Math.round(barWidth * levelFraction(region.from)),
    end: Math.round(barWidth * levelFraction(region.to)),
  }));
  return { compact, barLeft, barWidth, lampLeft, lampWidth, countCentre, indicatorWidth, readoutRight, readoutWidth, ticks, regions, rows, height };
}

export function overIndicatorAt(node, x, y) {
  const cached = layouts.get(node);
  if (!cached) return null;
  const { lampLeft, indicatorWidth, rows } = cached.layout;
  if (x < lampLeft || x > lampLeft + indicatorWidth) return null;
  const index = rows.findIndex((row) => y >= row.barY && y < row.barY + ROW_HEIGHT);
  return index === -1 ? null : cached.numbers[index];
}

function channelState(peaks, number) {
  let state = peaks.get(number);
  if (!state || typeof state !== "object") {
    state = { meter: createPeakMeter(), over: createOverIndicator(), readout: createReadout() };
    peaks.set(number, state);
  }
  return state;
}

function sameFrame(previous, colors, layout, ratio, frames) {
  return (
    previous &&
    previous.colors === colors &&
    previous.layout === layout &&
    previous.ratio === ratio &&
    previous.frames.length === frames.length &&
    previous.frames.every(
      (frame, index) =>
        frame.filled === frames[index].filled &&
        frame.peak === frames[index].peak &&
        frame.lit === frames[index].lit &&
        frame.count === frames[index].count &&
        frame.readout === frames[index].readout,
    )
  );
}

export function drawMeters(node, width, numbers, values, presence, names, peaks, elapsed, source) {
  const ratio = window.devicePixelRatio || 1;
  const context = node.getContext("2d");
  let cached = layouts.get(node);
  if (!cached || cached.width !== width || cached.names !== names || cached.numbers.length !== numbers.length || cached.numbers.some((number, index) => number !== numbers[index])) {
    cached = { width, names, numbers: [...numbers], layout: meterLayout(context, width, numbers, names) };
    layouts.set(node, cached);
  }
  const layout = cached.layout;
  const { barLeft, barWidth, height, rows } = layout;

  const frames = numbers.map((number) => {
    const raw = values[number];
    const decibels = format.meteringDecibelsFullScale(raw, source);
    const reported = presence[number];
    const clipping = format.meteringSignalPresence(raw, source) === "clipping" || reported === "clipping";
    const reading = decibels !== null ? decibels : clipping ? 0 : null;
    const state = channelState(peaks, number);
    advancePeakMeter(state.meter, reading, elapsed);
    advanceOverIndicator(state.over, isOverReading(reading, clipping), elapsed);
    return {
      filled: Math.round(barWidth * levelFraction(state.meter.level)),
      peak: state.meter.peak > format.METER_FLOOR_DBFS ? Math.round(barWidth * levelFraction(state.meter.peak)) : 0,
      lit: overIndicatorLit(state.over),
      count: state.over.count,
      readout: advanceReadout(state.readout, meterReadout(raw, reported, source), elapsed),
    };
  });

  const canvasWidth = Math.floor(width * ratio);
  const canvasHeight = Math.floor(height * ratio);
  const resized = node.width !== canvasWidth || node.height !== canvasHeight;
  if (resized) {
    node.width = canvasWidth;
    node.height = canvasHeight;
  }
  if (node.style.width !== `${width}px`) node.style.width = `${width}px`;
  if (node.style.height !== `${height}px`) node.style.height = `${height}px`;
  const colors = prefersLight() ? LIGHT_COLORS : DARK_COLORS;
  if (!resized && sameFrame(paintedFrames.get(node), colors, layout, ratio, frames)) return;
  paintedFrames.set(node, { colors, layout, ratio, frames });

  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  context.clearRect(0, 0, width, height);
  context.textBaseline = "middle";

  context.font = SCALE_FONT;
  context.textAlign = "center";
  context.fillStyle = colors.text;
  for (const tick of layout.ticks) context.fillText(tick.label, tick.centre, SCALE_HEIGHT / 2 - 2);
  context.strokeStyle = colors.grid;
  context.lineWidth = 1;
  context.beginPath();
  for (const tick of layout.ticks) {
    context.moveTo(tick.x, SCALE_HEIGHT - 4);
    context.lineTo(tick.x, SCALE_HEIGHT);
  }
  for (const row of rows) {
    const y = Math.floor(row.y) + 0.5;
    context.moveTo(0, y);
    context.lineTo(width, y);
  }
  context.stroke();

  numbers.forEach((number, index) => {
    const row = rows[index];
    const frame = frames[index];
    const centre = row.barY + ROW_HEIGHT / 2;

    context.font = LABEL_FONT;
    context.fillStyle = colors.text;
    context.textAlign = "left";
    row.lines.forEach((line, lineIndex) => context.fillText(line, COLUMN_GAP, layout.compact ? row.y + (lineIndex + 0.5) * ROW_HEIGHT : centre));

    context.fillStyle = colors.track;
    context.fillRect(barLeft, row.barY + BAR_INSET, barWidth, ROW_HEIGHT - BAR_INSET * 2);
    for (const region of layout.regions) {
      const end = Math.min(region.end, frame.filled);
      if (end > region.start) {
        context.fillStyle = colors[region.name];
        context.fillRect(barLeft + region.start, row.barY + BAR_INSET, end - region.start, ROW_HEIGHT - BAR_INSET * 2);
      }
    }
    if (frame.peak > 0) {
      context.fillStyle = colors.peak;
      context.fillRect(barLeft + Math.max(0, Math.min(barWidth - 2, frame.peak - 1)), row.barY + BAR_INSET - 1, 2, ROW_HEIGHT - BAR_INSET * 2 + 2);
    }

    context.fillStyle = frame.lit ? colors.lampLit : colors.lamp;
    context.fillRect(layout.lampLeft, row.barY + LAMP_INSET, layout.lampWidth, ROW_HEIGHT - LAMP_INSET * 2);
    context.font = LAMP_FONT;
    context.textAlign = "center";
    context.fillStyle = frame.lit ? colors.lampTextLit : colors.lampText;
    context.fillText(LAMP_TEXT, layout.lampLeft + layout.lampWidth / 2, centre);
    if (frame.count > 0) {
      context.font = LABEL_FONT;
      context.fillStyle = colors.text;
      context.fillText(String(frame.count), layout.countCentre, centre);
    }

    context.font = LABEL_FONT;
    context.fillStyle = colors.text;
    context.textAlign = "right";
    context.fillText(frame.readout, layout.readoutRight, row.readoutY, layout.readoutWidth);
  });
}
