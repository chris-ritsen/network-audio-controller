import * as format from "./format.js";
import { Icon } from "./icons.js";
import { html, useEffect, useState } from "./lib/preact.js";
import { meterRevision, meterValuesFor } from "./store.js";

export const SIGNAL_MAX_AGE_MS = 5000;

const STATES = {
  unknown: { state: "unknown", label: "Signal unavailable", icon: "signal-unknown" },
  muted: { state: "muted", label: "Muted", icon: "signal-muted" },
  mute_or_floor: { state: "muted", label: "Muted or below meter floor", icon: "signal-muted" },
  framing_marker: { state: "unknown", label: "Signal unavailable", icon: "signal-unknown" },
  clipping: { state: "clipping", label: "Clipping", icon: "signal-clipping", level: 1 },
  below_threshold: { state: "quiet", label: "No signal", icon: "signal-quiet" },
  signal_present: { state: "present", label: "Signal present", icon: "signal-high" },
};

export function signalIndicator(values, channelNumber, now = Date.now(), direction = "rx") {
  const timestamp = values?.[`${direction}_updated_at`]?.[channelNumber] ?? values?.wall_time;
  if (!Number.isFinite(timestamp) || now - timestamp * 1000 > SIGNAL_MAX_AGE_MS || timestamp * 1000 > now + 1000) return STATES.unknown;
  const raw = values?.[direction]?.[channelNumber];
  const source = values?.metering_source;
  const presence = raw == null ? values?.[`${direction}_signal_presence`]?.[channelNumber] : format.meteringSignalPresence(raw, source);
  const state = STATES[presence] || STATES.unknown;
  const dbfs = raw == null ? null : format.meteringDecibelsFullScale(raw, source);

  if (dbfs !== null) {
    return { ...state, dbfs, level: Math.max(0, Math.min(1, (dbfs + 126) / 126)), icon: state.state === "quiet" ? state.icon : dbfs < -24 ? "signal-low" : "signal-high" };
  }

  return state;
}

export function SignalPresence({ serverName, channelNumber, online, direction = "rx" }) {
  meterRevision.value;
  const [, expire] = useState(0);
  const values = meterValuesFor(serverName);
  const timestamp = values?.[`${direction}_updated_at`]?.[channelNumber] ?? values?.wall_time;
  useEffect(() => {
    const remaining = timestamp * 1000 + SIGNAL_MAX_AGE_MS - Date.now();
    if (!Number.isFinite(remaining) || remaining < 0) return;
    const timer = setTimeout(() => expire((revision) => revision + 1), remaining + 1);
    return () => clearTimeout(timer);
  }, [timestamp]);
  const indicator = signalIndicator(online ? values : null, channelNumber, Date.now(), direction);
  const detail = indicator.dbfs == null ? indicator.label : `${indicator.dbfs.toFixed(1)} dBFS — ${indicator.label}`;
  return html`<span class=${`signal-presence signal-${indicator.state}`} role="img" aria-label=${indicator.label} title=${detail} data-level-dbfs=${indicator.dbfs ?? ""}>
    <${Icon} name=${indicator.icon} />
    <span class="signal-track" aria-hidden="true"><span class="signal-level" style=${{ width: `${(indicator.level ?? 0) * 100}%` }} /></span>
  </span>`;
}
