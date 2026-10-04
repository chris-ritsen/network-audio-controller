import { METER_FLOOR_DBFS } from "./format.js";

export const METER_CEILING_DBFS = 0;
export const METER_RANGE_DECIBELS = METER_CEILING_DBFS - METER_FLOOR_DBFS;
export const FALL_DECIBELS_PER_SECOND = 20;
export const PEAK_HOLD_SECONDS = 2;
export const PEAK_FALL_DECIBELS_PER_SECOND = 20;
export const OVER_HOLD_SECONDS = 2;
export const OVER_COUNT_LIMIT = 999;
export const READOUT_INTERVAL_SECONDS = 0.25;
export const MAXIMUM_FRAME_SECONDS = 0.1;
const TIMING_TOLERANCE_SECONDS = 0.000001;
export const ALIGNMENT_LEVEL_DBFS = -18;
export const HIGH_LEVEL_DBFS = -6;

export const METER_REGIONS = [
  { name: "nominal", from: METER_FLOOR_DBFS, to: ALIGNMENT_LEVEL_DBFS, color: "#2fe36a" },
  { name: "high", from: ALIGNMENT_LEVEL_DBFS, to: HIGH_LEVEL_DBFS, color: "#ffc400" },
  { name: "critical", from: HIGH_LEVEL_DBFS, to: METER_CEILING_DBFS, color: "#ff2323" },
];

function steps(size) {
  const ticks = [];
  for (let tick = 0; tick >= -60; tick -= size) ticks.push(tick);
  return ticks;
}

export const SCALE_TICK_SETS = [steps(3), steps(6), steps(12), [0, -18, -40, -60], [0, -30, -60], [0, -60]];

function seconds(elapsed) {
  return Number.isFinite(elapsed) && elapsed > 0 ? elapsed : 0;
}

export function meterLevel(reading) {
  if (typeof reading !== "number" || Number.isNaN(reading)) return METER_FLOOR_DBFS;
  return Math.min(METER_CEILING_DBFS, Math.max(METER_FLOOR_DBFS, reading));
}

export function levelFraction(decibels) {
  return (meterLevel(decibels) - METER_FLOOR_DBFS) / METER_RANGE_DECIBELS;
}

export function levelRegion(decibels) {
  const level = meterLevel(decibels);
  return METER_REGIONS.find((region) => level < region.to) || METER_REGIONS[METER_REGIONS.length - 1];
}

export function createPeakMeter() {
  return { level: METER_FLOOR_DBFS, peak: METER_FLOOR_DBFS, peakHeldSeconds: 0 };
}

export function advancePeakMeter(meter, reading, elapsed) {
  const target = meterLevel(reading);
  const duration = seconds(elapsed);
  meter.level = target >= meter.level ? target : Math.max(target, meter.level - FALL_DECIBELS_PER_SECOND * duration);
  if (meter.level >= meter.peak) {
    meter.peak = meter.level;
    meter.peakHeldSeconds = 0;
    return meter;
  }
  const heldBefore = meter.peakHeldSeconds;
  meter.peakHeldSeconds = heldBefore + duration;
  const falling = Math.max(0, meter.peakHeldSeconds - Math.max(heldBefore, PEAK_HOLD_SECONDS));
  meter.peak = Math.max(meter.level, meter.peak - PEAK_FALL_DECIBELS_PER_SECOND * falling);
  return meter;
}

export function isOverReading(reading, clipping) {
  return clipping === true || (typeof reading === "number" && reading >= METER_CEILING_DBFS);
}

export function createOverIndicator() {
  return { count: 0, over: false, sinceOverSeconds: Number.POSITIVE_INFINITY };
}

export function advanceOverIndicator(indicator, over, elapsed) {
  if (over) {
    if (!indicator.over) indicator.count = Math.min(OVER_COUNT_LIMIT, indicator.count + 1);
    indicator.over = true;
    indicator.sinceOverSeconds = 0;
    return indicator;
  }
  indicator.over = false;
  indicator.sinceOverSeconds += seconds(elapsed);
  return indicator;
}

export function overIndicatorLit(indicator) {
  return indicator.over || indicator.sinceOverSeconds < OVER_HOLD_SECONDS;
}

export function clearOverIndicator(indicator) {
  indicator.count = 0;
  indicator.over = false;
  indicator.sinceOverSeconds = Number.POSITIVE_INFINITY;
  return indicator;
}

export function createReadout() {
  return { shown: null, sinceRefreshSeconds: 0 };
}

export function advanceReadout(readout, text, elapsed) {
  readout.sinceRefreshSeconds = Math.min(READOUT_INTERVAL_SECONDS, readout.sinceRefreshSeconds + seconds(elapsed));
  if (readout.shown === null || (readout.shown !== text && readout.sinceRefreshSeconds >= READOUT_INTERVAL_SECONDS - TIMING_TOLERANCE_SECONDS)) {
    readout.shown = text;
    readout.sinceRefreshSeconds = 0;
  }
  return readout.shown;
}

export function formatDecibels(decibels) {
  const rounded = Math.round(decibels * 10) / 10;
  return (rounded === 0 ? 0 : rounded).toFixed(1);
}

export function stripReadout(meters, readings) {
  let loudest = null;
  (readings || []).forEach((reading, index) => {
    if (typeof reading !== "number" || Number.isNaN(reading) || reading === Number.NEGATIVE_INFINITY) return;
    const meter = meters[index];
    const shown = reading < METER_FLOOR_DBFS || !meter ? reading : meter.level;
    if (loudest === null || shown > loudest) loudest = shown;
  });
  return loudest === null ? "" : formatDecibels(loudest);
}

export function scaleLabel(tick) {
  return tick === 0 ? "0" : String(tick);
}

export function chooseScaleTicks(length, labelExtent, minimumGap = 2) {
  const extent = typeof labelExtent === "function" ? labelExtent : () => labelExtent;
  const fallback = SCALE_TICK_SETS[SCALE_TICK_SETS.length - 1];
  if (!Number.isFinite(length) || length <= 0) return fallback;
  for (const ticks of SCALE_TICK_SETS) {
    const placed = ticks
      .map((tick) => ({ position: levelFraction(tick) * length, extent: Number(extent(tick)) || 0 }))
      .sort((first, second) => first.position - second.position);
    const fits = placed.every(
      (label, index) => index === 0 || label.position - placed[index - 1].position >= (label.extent + placed[index - 1].extent) / 2 + minimumGap,
    );
    if (fits) return ticks;
  }
  return fallback;
}
