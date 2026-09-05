import * as format from "./format.js";
import { html, useEffect, useLayoutEffect, useRef, useState } from "./lib/preact.js";
import { meterValuesFor, onMeterValues } from "./store.js";

const COLUMN_GAP = 14;
const LABEL_FONT = "11px ui-monospace, SFMono-Regular, Menlo, monospace";
const MINIMUM_BAR_WIDTH = 120;
const PEAK_DECAY_PER_SECOND = 0.55;
const ROW_HEIGHT = 20;

const COLORS = {
  background: "#000000",
  clip: "#ff2323",
  grid: "#242424",
  hot: "#ffc400",
  nominal: "#2fe36a",
  peak: "#ffffff",
  text: "#ffffff",
  track: "#1b1b1b",
};

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

export function MeterBank({ device, direction, serverName }) {
  const canvas = useRef(null);
  const container = useRef(null);
  const latest = useRef(meterValuesFor(serverName));
  const peaks = useRef(new Map());
  const frame = useRef(0);
  const lastDrawn = useRef(0);
  const [width, setWidth] = useState(0);

  const names = channelNames(device, direction);
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
    const unsubscribe = onMeterValues((name, values) => {
      if (name === serverName) {
        latest.current = values;
      }
    });
    return unsubscribe;
  }, [serverName]);

  useEffect(() => {
    let running = true;
    const step = (timestamp) => {
      if (!running) {
        return;
      }
      frame.current = window.requestAnimationFrame(step);
      const node = canvas.current;
      if (!node || width === 0) {
        return;
      }
      const values = latest.current ? latest.current[direction] || {} : {};
      const presence = latest.current ? latest.current[`${direction}_signal_presence`] || {} : {};
      const numbers = channelNumbers(device, direction, values);
      if (numbers.length !== channelCount) {
        setChannelCount(numbers.length);
      }
      const elapsed = lastDrawn.current ? (timestamp - lastDrawn.current) / 1000 : 0;
      lastDrawn.current = timestamp;
      drawMeters(node, width, numbers, values, presence, names, peaks.current, elapsed);
    };
    frame.current = window.requestAnimationFrame(step);
    return () => {
      running = false;
      window.cancelAnimationFrame(frame.current);
    };
  }, [channelCount, device, direction, names, width]);

  const height = Math.max(ROW_HEIGHT, channelCount * ROW_HEIGHT);

  return html`
    <div class="meter-bank" ref=${container}>
      ${channelCount === 0
        ? html`<div class="notice">No ${direction === "tx" ? "transmit" : "receive"} levels received yet.</div>`
        : html`<canvas ref=${canvas} style=${`height:${height}px`}></canvas>`}
    </div>
  `;
}

export function measureColumns(context, numbers, values, presence, names) {
  context.font = LABEL_FONT;
  let labelWidth = 0;
  let valueWidth = 0;
  for (const number of numbers) {
    const name = names[number] ? `${number}  ${names[number]}` : String(number);
    labelWidth = Math.max(labelWidth, context.measureText(name).width);
    const indication = presence[number];
    const label = indication
      ? `${format.meteringLabel(values[number])}  ${indication}`
      : format.meteringLabel(values[number]);
    valueWidth = Math.max(valueWidth, context.measureText(label).width);
  }
  return { labelWidth: Math.ceil(labelWidth), valueWidth: Math.ceil(valueWidth) };
}

export function drawMeters(node, width, numbers, values, presence, names, peaks, elapsed) {
  const ratio = window.devicePixelRatio || 1;
  const height = Math.max(ROW_HEIGHT, numbers.length * ROW_HEIGHT);
  const context = node.getContext("2d");
  context.setTransform(1, 0, 0, 1, 0, 0);
  const columns = measureColumns(context, numbers, values, presence, names);
  const barLeft = COLUMN_GAP + columns.labelWidth + COLUMN_GAP;
  const canvasWidth = Math.max(width, barLeft + MINIMUM_BAR_WIDTH + COLUMN_GAP + columns.valueWidth + COLUMN_GAP);
  const barWidth = canvasWidth - barLeft - COLUMN_GAP - columns.valueWidth - COLUMN_GAP;

  if (node.width !== Math.floor(canvasWidth * ratio) || node.height !== Math.floor(height * ratio)) {
    node.width = Math.floor(canvasWidth * ratio);
    node.height = Math.floor(height * ratio);
  }
  node.style.width = `${canvasWidth}px`;
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  context.clearRect(0, 0, canvasWidth, height);

  context.textBaseline = "middle";
  numbers.forEach((number, index) => {
    const y = index * ROW_HEIGHT;
    const centre = y + ROW_HEIGHT / 2;
    const raw = values[number];
    const fraction = format.meterFraction(raw);
    const decibels = format.meteringDecibelsFullScale(raw);

    context.font = LABEL_FONT;
    context.fillStyle = COLORS.text;
    context.textAlign = "left";
    const name = names[number] ? `${number}  ${names[number]}` : String(number);
    context.fillText(name, COLUMN_GAP, centre);

    context.fillStyle = COLORS.track;
    context.fillRect(barLeft, y + 5, barWidth, ROW_HEIGHT - 10);

    const filled = Math.round(barWidth * fraction);
    if (filled > 0) {
      context.fillStyle = raw === 0 ? COLORS.clip : decibels !== null && decibels > -6 ? COLORS.hot : COLORS.nominal;
      context.fillRect(barLeft, y + 5, filled, ROW_HEIGHT - 10);
    }

    const previousPeak = peaks.get(number) || 0;
    const decayed = Math.max(0, previousPeak - PEAK_DECAY_PER_SECOND * elapsed);
    const peak = Math.max(decayed, fraction);
    peaks.set(number, peak);
    if (peak > 0.01) {
      context.fillStyle = COLORS.peak;
      context.fillRect(barLeft + Math.min(barWidth - 2, Math.round(barWidth * peak) - 1), y + 4, 2, ROW_HEIGHT - 8);
    }

    context.fillStyle = COLORS.text;
    context.textAlign = "right";
    const indication = presence[number];
    const label = indication ? `${format.meteringLabel(raw)}  ${indication}` : format.meteringLabel(raw);
    context.fillText(label, canvasWidth - COLUMN_GAP, centre);
  });

  context.strokeStyle = COLORS.grid;
  context.beginPath();
  for (let index = 1; index < numbers.length; index += 1) {
    const y = Math.floor(index * ROW_HEIGHT) + 0.5;
    context.moveTo(0, y);
    context.lineTo(canvasWidth, y);
  }
  context.stroke();
}
