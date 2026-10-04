import { METER_FLOOR_DBFS } from "./format.js";
import { html, useCallback, useEffect, useMemo, useRef, useState } from "./lib/preact.js";
import { observeVisibility, useAnimationFrames } from "./meter-animation.js";
import {
  ALIGNMENT_LEVEL_DBFS,
  HIGH_LEVEL_DBFS,
  METER_RANGE_DECIBELS,
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
  levelRegion,
  overIndicatorLit,
  scaleLabel,
  stripReadout,
} from "./peak-meter.js";

const SEGMENTS = METER_RANGE_DECIBELS;
const INITIAL_METER_LENGTH = SEGMENTS * 3;
const INITIAL_SCALE_LABEL_HEIGHT = 10;
const NAME_BREAKS = /([-_:./])/;

function percent(fraction) {
  return `${Number((fraction * 100).toFixed(4))}%`;
}

const BANK_STYLE = [
  `--channel-strip-segments:${SEGMENTS}`,
  `--channel-strip-nominal-top:${percent(levelFraction(ALIGNMENT_LEVEL_DBFS))}`,
  `--channel-strip-high-top:${percent(levelFraction(HIGH_LEVEL_DBFS))}`,
].join(";");

function nameWithBreaks(name) {
  return name
    .split(NAME_BREAKS)
    .filter((part) => part !== "")
    .map((part) => (NAME_BREAKS.test(part) ? [part, html`<wbr />`] : part));
}

function trackCount(strip) {
  return Math.max(1, Number.isInteger(strip.tracks) ? strip.tracks : 1);
}

function sameTicks(first, second) {
  return first.length === second.length && first.every((tick, index) => tick === second[index]);
}

function createStripState(tracks) {
  return { meters: Array.from({ length: tracks }, createPeakMeter), over: createOverIndicator(), readout: createReadout() };
}

function findParts(element) {
  return {
    element,
    over: element.querySelector(".channel-strip-over"),
    count: element.querySelector(".channel-strip-over-count"),
    readout: element.querySelector(".channel-strip-readout"),
    tracks: [...element.querySelectorAll(".channel-strip-track")].map((track) => ({
      fill: track.querySelector(".channel-strip-fill"),
      peak: track.querySelector(".channel-strip-peak"),
      lit: null,
      peakSegment: null,
    })),
    lit: null,
    countText: null,
    readoutText: null,
  };
}

function paintOver(parts, indicator) {
  const lit = overIndicatorLit(indicator);
  if (lit !== parts.lit) {
    parts.lit = lit;
    parts.over.toggleAttribute("data-lit", lit);
  }
  const count = indicator.count > 0 ? String(indicator.count) : "";
  if (count !== parts.countText) {
    parts.countText = count;
    parts.count.textContent = count;
  }
}

function segmentDecibels(segment) {
  return METER_FLOOR_DBFS + (segment / SEGMENTS) * METER_RANGE_DECIBELS;
}

function paintStrip(parts, state, readoutText) {
  state.meters.forEach((meter, index) => {
    const track = parts.tracks[index];
    if (!track) return;
    const lit = Math.round(levelFraction(meter.level) * SEGMENTS);
    if (lit !== track.lit) {
      track.lit = lit;
      track.fill.style.clipPath = `inset(${percent((SEGMENTS - lit) / SEGMENTS)} 0 0 0)`;
    }
    const peakSegment = Math.round(levelFraction(meter.peak) * SEGMENTS);
    if (peakSegment !== track.peakSegment) {
      track.peakSegment = peakSegment;
      track.peak.hidden = peakSegment <= 0;
      if (peakSegment > 0) {
        track.peak.style.bottom = percent((peakSegment - 1) / SEGMENTS);
        track.peak.dataset.region = levelRegion(segmentDecibels(peakSegment - 0.5)).name;
      }
    }
  });
  paintOver(parts, state.over);
  if (readoutText !== parts.readoutText) {
    parts.readoutText = readoutText;
    parts.readout.textContent = readoutText;
  }
}

export function ChannelStripToggle({ pressed, pending, label, title, onToggle }) {
  return html`<button
    type="button"
    class="channel-strip-toggle"
    aria-pressed=${pressed ? "true" : "false"}
    aria-busy=${pending ? "true" : "false"}
    disabled=${pending}
    title=${title}
    onClick=${onToggle}
  >
    ${label}
  </button>`;
}

function ChannelStrip({ strip, ticks, controls, onClearOver }) {
  const tracks = trackCount(strip);
  const labels = Array.isArray(strip.trackLabels) ? strip.trackLabels.slice(0, tracks) : [];
  const title = `Channel ${strip.number}${strip.name ? ` ${strip.name}` : ""}`;
  return html`<div class="channel-strip" data-strip-key=${strip.key} role="group" aria-label=${title}>
    <button
      type="button"
      class="channel-strip-over"
      title="Over. Click to clear"
      aria-label=${`Clear the over indicator for channel ${strip.number}`}
      onClick=${() => onClearOver(strip.key)}
    >
      <span class="channel-strip-over-lamp">OVER</span>
      <span class="channel-strip-over-count"></span>
    </button>
    <div class="channel-strip-meter" data-tracks=${tracks} aria-hidden="true">
      <div class="channel-strip-scale">
        ${ticks.map(
          (tick) => html`<span key=${tick} class="channel-strip-scale-label" style=${`bottom:${percent(levelFraction(tick))}`}>${scaleLabel(tick)}</span>`,
        )}
      </div>
      ${Array.from(
        { length: tracks },
        (_, index) => html`<div key=${index} class="channel-strip-track" style=${`grid-column:${index + 2}`}>
          <div class="channel-strip-fill"></div>
          <div class="channel-strip-peak" hidden></div>
        </div>`,
      )}
      ${labels.map(
        (trackLabel, index) => html`<span key=${`label-${index}`} class="channel-strip-track-label" style=${`grid-column:${index + 2}`}>${trackLabel}</span>`,
      )}
    </div>
    <div class="channel-strip-readout"></div>
    ${controls ? html`<div class="channel-strip-controls">${controls(strip)}</div>` : null}
    <div class="channel-strip-number">${strip.number}</div>
    ${strip.name ? html`<div class="channel-strip-name">${nameWithBreaks(strip.name)}</div>` : null}
  </div>`;
}

export function ChannelStrips({ strips, read, label, controls }) {
  const container = useRef(null);
  const reader = useRef(read);
  reader.current = read;
  const states = useRef(new Map());
  const parts = useRef(new Map());
  const visible = useRef(new Set());
  const [running, setRunning] = useState(false);
  const [ticks, setTicks] = useState(() => chooseScaleTicks(INITIAL_METER_LENGTH, INITIAL_SCALE_LABEL_HEIGHT));
  const stripsByKey = useMemo(() => new Map(strips.map((strip) => [strip.key, strip])), [strips]);

  const stateFor = (strip) => {
    let state = states.current.get(strip.key);
    if (!state || state.meters.length !== trackCount(strip)) {
      state = { ...createStripState(trackCount(strip)), ...(state ? { over: state.over } : {}) };
      states.current.set(strip.key, state);
    }
    return state;
  };

  useEffect(() => {
    const node = container.current;
    if (!node) return undefined;
    const found = new Map();
    const keys = new Map();
    for (const element of node.querySelectorAll("[data-strip-key]")) {
      const key = element.getAttribute("data-strip-key");
      found.set(key, findParts(element));
      keys.set(element, key);
    }
    for (const key of states.current.keys()) {
      if (!stripsByKey.has(key)) states.current.delete(key);
    }
    parts.current = found;
    visible.current = new Set();
    return observeVisibility(keys.keys(), (element, shown) => {
      const key = keys.get(element);
      if (shown) visible.current.add(key);
      else visible.current.delete(key);
      setRunning(visible.current.size > 0);
    });
  }, [stripsByKey]);

  useEffect(() => {
    const node = container.current;
    const track = node && node.querySelector(".channel-strip-track");
    if (!track || typeof ResizeObserver !== "function") return undefined;
    const measure = () => {
      const scaleLabelElement = node.querySelector(".channel-strip-scale-label");
      const extent = scaleLabelElement ? scaleLabelElement.getBoundingClientRect().height : INITIAL_SCALE_LABEL_HEIGHT;
      const next = chooseScaleTicks(track.clientHeight, extent);
      setTicks((current) => (sameTicks(current, next) ? current : next));
    };
    const observer = new ResizeObserver(measure);
    observer.observe(track);
    measure();
    return () => observer.disconnect();
  }, [strips.length > 0]);

  useAnimationFrames(running, (elapsed) => {
    for (const key of visible.current) {
      const strip = stripsByKey.get(key);
      const stripParts = parts.current.get(key);
      if (!strip || !stripParts) continue;
      const state = stateFor(strip);
      const sample = reader.current(strip) || {};
      const levels = Array.isArray(sample.levels) ? sample.levels : [];
      const readings = state.meters.map((_, index) => (index < levels.length ? levels[index] : null));
      let over = sample.clipping === true;
      state.meters.forEach((meter, index) => {
        advancePeakMeter(meter, readings[index], elapsed);
        if (isOverReading(readings[index])) over = true;
      });
      advanceOverIndicator(state.over, over, elapsed);
      const text = typeof sample.readout === "string" ? sample.readout : stripReadout(state.meters, readings);
      paintStrip(stripParts, state, advanceReadout(state.readout, text, elapsed));
    }
  });

  const clearOver = useCallback((key) => {
    const state = states.current.get(key);
    const stripParts = parts.current.get(key);
    if (!state) return;
    clearOverIndicator(state.over);
    if (stripParts) paintOver(stripParts, state.over);
  }, []);

  return html`<div class="channel-strips" ref=${container} role="group" aria-label=${label} style=${BANK_STYLE}>
    ${strips.map(
      (strip) => html`<${ChannelStrip} key=${strip.key} strip=${strip} ticks=${ticks} controls=${controls} onClearOver=${clearOver} />`,
    )}
  </div>`;
}
