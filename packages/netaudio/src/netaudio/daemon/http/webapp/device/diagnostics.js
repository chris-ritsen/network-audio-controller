import { html, useEffect, useState } from "../lib/preact.js";
import { Fields, Panel } from "../components.js";

const number = (value) => value == null ? "Unavailable" : Number(value).toLocaleString(undefined, { maximumFractionDigits: 3 });

function Histogram({ series }) {
  const histogram = series?.histogram;
  if (!histogram) return null;
  const hasUnderflow = histogram.underflow != null;
  const counts = [...(hasUnderflow ? [histogram.underflow] : []), ...histogram.counts, histogram.overflow || 0];
  const maximum = Math.max(1, ...counts);
  return html`<figure>
    <figcaption>${histogram.semantics === "reported_maxima" ? "Reported maximum latency observations" : "Frequency offset observations"} · ${counts.reduce((a, b) => a + b, 0)} observations</figcaption>
    <svg viewBox=${`0 0 ${counts.length * 4} 50`} role="img" aria-label="Observation histogram">
      ${counts.map((count, index) => html`<rect x=${index * 4} y=${50 - count / maximum * 48} width="3" height=${count / maximum * 48} fill="currentColor"><title>${hasUnderflow && index === 0 ? "Underflow" : index === counts.length - 1 ? "Overflow" : `Bin ${index + (hasUnderflow ? 0 : 1)}`}: ${count}</title></rect>`)}
    </svg>
  </figure>`;
}

function History({ series }) {
  const samples = (series?.history || []).filter((sample) => sample.display_epoch === series.display_epoch);
  if (!samples.length) return null;
  const finite = samples.filter((sample) => sample.value != null);
  if (!finite.length) return null;
  const low = Math.min(...finite.map((s) => s.value));
  const span = Math.max(1, Math.max(...finite.map((s) => s.value)) - low);
  const begin = samples[0].observed_monotonic;
  const duration = Math.max(1, samples.at(-1).observed_monotonic - begin);
  const epochs = [...new Set(samples.map((s) => s.epoch))];
  return html`<svg viewBox="0 0 300 60" role="img" aria-label="Observation history with continuity gaps">
    ${epochs.map((epoch) => html`<polyline fill="none" stroke="currentColor" points=${finite.filter((s) => s.epoch === epoch).map((s) => `${(s.observed_monotonic - begin) / duration * 300},${58 - (s.value - low) / span * 56}`).join(" ")} />`)}
  </svg>`;
}

export function DiagnosticsView({ data }) {
  return html`<div>
    ${(data.receiver?.paths || []).map((path) => {
      const latency = path.latency;
      const late = path.late_packets;
      const stats = latency.statistics || {};
      const budget = latency.current?.evidence?.configured_latency_nanoseconds;
      return html`<section>
        <h3>${path.attribution_status === "resolved" ? `Audio flow ${path.audio_receiver_flow_id} · Network ${path.network_interface_index + 1}` : `Telemetry index ${path.telemetry_index}`}</h3>
        ${path.attribution_reason && html`<p>${path.attribution_reason}</p>`}
        <${Fields} entries=${[
          ["Transmitter", path.evidence?.source || "Unavailable"],
          ["Latency configuration source", budget == null ? "Unavailable" : "Receiver flow inventory"],
          ["Configured latency", budget == null ? "Unavailable" : `${number(budget / 1000)} µs`],
          ["Reported maximum", `${number(latency.current?.latency_microseconds)} µs · ${latency.fresh ? "fresh" : "stale"}`],
          ["Mean of retained maxima", `${number(stats.mean == null ? null : stats.mean / 1000)} µs`],
          ["Peak reported maximum", `${number(stats.maximum == null ? null : stats.maximum / 1000)} µs · ${stats.peak_observed_at || "Unavailable"}`],
          ["Remaining latency margin", budget != null && latency.current?.value != null ? `${number((budget - latency.current.value) / 1000)} µs` : "Unavailable"],
          ["Late packet counter", `${number(late.current?.raw)} · ${late.fresh ? "fresh" : "stale"}`],
          ["Counter increase since local baseline", number(late.increase_since_baseline)],
          ["Retained observations", number(stats.count)],
          ["Window", `${stats.window_start || "Unavailable"} – ${stats.window_end || "Unavailable"}`],
          ["Continuity epoch", number(latency.current?.epoch)],
        ]} />
        <${History} series=${latency} /><${Histogram} series=${latency} />
      </section>`;
    })}
    ${["heartbeat", "conmon"].filter((name) => data.clock?.[name]?.current).map((name) => {
      const series = data.clock[name];
      const stats = series.statistics || {};
      return html`<section><h3>${name === "heartbeat" ? "Heartbeat frequency offset" : "Clock-status frequency offset"}</h3>
        <${Fields} entries=${[
          ["Current", `${number(series.current.value)} ppm · ${series.fresh ? "fresh" : "stale"}`],
          ["Minimum / maximum", `${number(stats.minimum)} / ${number(stats.maximum)} ppm`],
          ["Mean", `${number(stats.mean)} ppm`],
          ["Population standard deviation", `${number(stats.population_standard_deviation)} ppm`],
          ["Observations", number(stats.count)],
        ]} />
        <${History} series=${series} /><${Histogram} series=${series} />
      </section>`;
    })}
  </div>`;
}

export function DiagnosticsSection({ device }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const endpoint = `/diagnostics/${encodeURIComponent(device.server_name)}`;
  useEffect(() => {
    let cancelled = false;
    async function refresh() {
      try {
        const response = await fetch(endpoint);
        const body = await response.json();
        if (!response.ok) throw new Error(body.error);
        if (!cancelled) { setData(body); setError(null); }
      } catch (failure) { if (!cancelled) setError(failure.message); }
    }
    refresh();
    const timer = setInterval(refresh, 5000);
    return () => { cancelled = true; clearInterval(timer); };
  }, [endpoint]);
  async function reset() {
    try {
      const response = await fetch("/diagnostics/reset", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({device: device.server_name})});
      const body = await response.json();
      if (!response.ok) throw new Error(body.error);
      setData(body);
    } catch (failure) { setError(failure.message); }
  }
  async function configureWarnings(enabled) {
    try {
      const response = await fetch("/diagnostics/policy", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({device: device.server_name, clock_variation_warnings: enabled})});
      const body = await response.json();
      if (!response.ok) throw new Error(body.error);
      setData(body);
    } catch (failure) { setError(failure.message); }
  }
  return html`<${Panel} title="Receiver and clock diagnostics">
    <a href=${endpoint} download="netaudio-diagnostics.json">Export retained observations</a>
    <button onClick=${reset}>Reset local statistics</button>
    <label><input type="checkbox" checked=${data?.clock?.warning_enabled === true} onChange=${(event) => configureWarnings(event.target.checked)} />Clock frequency variation warnings</label>
    ${error && html`<p>${error}</p>`}
    ${data ? html`<p>Retaining up to ${data.retention_limit} observations per series.</p><${DiagnosticsView} data=${data} />` : html`<p>Loading observations…</p>`}
  <//>`;
}
