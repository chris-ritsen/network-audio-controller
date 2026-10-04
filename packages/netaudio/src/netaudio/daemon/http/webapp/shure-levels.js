import { P10T_METER_FULL_SCALE, SHURE_LEVEL_OFFSET } from "./shure-catalog.js";

export const AD4D_PEAK_HOLD_SECONDS = 0.9;
export const AD4D_RELEASE_STEP_DB = 4;
export const P10T_SAMPLE_WINDOW = 2;
const P10T_SIDES = [
  ["AUDIO_IN_LVL_L", 0],
  ["AUDIO_IN_LVL_R", 1],
];

function integer(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return /^-?\d+$/.test(text) ? Number.parseInt(text, 10) : null;
}

export function createShureLevels() {
  return new Map();
}

function channelState(levels, mac, channel) {
  const key = `${mac}|${channel}`;
  let state = levels.get(key);
  if (!state) {
    state = { peak: null, freshAt: null, ad4d: undefined, p10t: [[], []], p10tSeen: [false, false] };
    levels.set(key, state);
  }
  return state;
}

function observeAd4d(state, values, now) {
  const reported = integer(values.AUDIO_LEVEL_PEAK);
  const rms = integer(values.AUDIO_LEVEL_RMS);
  const peak = reported === null ? null : reported - SHURE_LEVEL_OFFSET;
  const previous = state.peak;
  let fresh = false;
  if (peak !== null) {
    if (previous === null || peak > previous) fresh = true;
    else if (peak < previous) fresh = previous - peak < AD4D_RELEASE_STEP_DB;
    else fresh = now - state.freshAt >= AD4D_PEAK_HOLD_SECONDS;
  }
  state.peak = peak;
  if (fresh) state.freshAt = now;
  state.ad4d = fresh ? peak : rms === null ? null : rms - SHURE_LEVEL_OFFSET;
}

function p10tDecibels(window) {
  const loudest = Math.max(0, ...window);
  if (!(loudest > 0)) return null;
  return 20 * Math.log10(Math.min(loudest, P10T_METER_FULL_SCALE) / P10T_METER_FULL_SCALE);
}

export function observeShureMeters(levels, mac, channel, values, now) {
  if (!values || typeof values !== "object") return;
  const state = channelState(levels, mac, channel);
  if ("AUDIO_LEVEL_PEAK" in values) observeAd4d(state, values, now);
  for (const [key, side] of P10T_SIDES) {
    if (!(key in values)) continue;
    const raw = integer(values[key]);
    state.p10t[side] = [...state.p10t[side], raw !== null && raw > 0 ? raw : 0].slice(-P10T_SAMPLE_WINDOW);
    state.p10tSeen[side] = true;
  }
}

export function shureLevels(levels, mac, channel) {
  const state = levels.get(`${mac}|${channel}`);
  if (!state) return [];
  if (state.p10tSeen[0] || state.p10tSeen[1]) return state.p10t.map((window) => p10tDecibels(window));
  return state.ad4d === undefined ? [] : [state.ad4d];
}
