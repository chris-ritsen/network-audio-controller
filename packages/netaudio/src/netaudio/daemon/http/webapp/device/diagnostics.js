import { html, useEffect, useState } from "../lib/preact.js";
import { Fields, Notice, Panel } from "../components.js";
import { t } from "../i18n.js";

const MINIMUM_FREQUENCY_OBSERVATIONS = 10;
const REFRESH_MILLISECONDS = 5000;

function finite(value) {
  return value != null && Number.isFinite(Number(value));
}

function milliseconds(nanoseconds) {
  return finite(nanoseconds) ? `${Number((Number(nanoseconds) / 1_000_000).toFixed(3))} ms` : "";
}

function partsPerMillion(value) {
  return finite(value) ? `${Number(Number(value).toFixed(3))} ppm` : "";
}

function Sparkline({ series, label }) {
  const samples = (series?.points || []).filter(([, value]) => finite(value));
  if (samples.length < 2) return null;
  const values = samples.map(([, value]) => Number(value));
  const low = Math.min(...values);
  const span = Math.max(Math.max(...values) - low, Math.abs(low) * 1e-6, 1e-9);
  const begin = samples[0][0];
  const duration = Math.max(samples.at(-1)[0] - begin, 1e-9);
  const points = samples
    .map(([time, value]) => `${(((time - begin) / duration) * 158 + 1).toFixed(1)},${(31 - ((Number(value) - low) / span) * 30).toFixed(1)}`)
    .join(" ");
  return html`<svg class="sparkline" width="160" height="32" viewBox="0 0 160 32" role="img" aria-label=${label}>
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points=${points} />
  </svg>`;
}

function displayedPaths(data) {
  return (data?.receiver?.paths || []).filter((path) => {
    if (path.attribution_status === "resolved") return true;
    const latency = Number(path.latency?.current?.value) || 0;
    const late = Number(path.late_packets?.current?.raw) || 0;
    return latency > 0 || late > 0;
  });
}

function flowLabel(path) {
  if (path.attribution_status !== "resolved") return t("Unidentified flow");
  return path.network_interface_index === 1
    ? t("Flow {number} · secondary", { number: path.audio_receiver_flow_id })
    : t("Flow {number}", { number: path.audio_receiver_flow_id });
}

function LatePackets({ series }) {
  const total = series?.current?.raw;
  if (!finite(total)) return "";
  const increase = Number(series.increase_since_baseline) || 0;
  return html`<span>${Number(total).toLocaleString()}${increase > 0 ? html` <span class="state-warn">${t("(+{count} since reset)", { count: increase.toLocaleString() })}</span>` : ""}</span>`;
}

function ReceiveLatency({ data, endpoint, onReset }) {
  const paths = displayedPaths(data);
  if (!paths.length) return null;
  return html`<${Panel}
    title=${t("Receive latency")}
    headerActions=${html`
      <a class="btn btn-xs" href=${endpoint} download="netaudio-diagnostics.json">${t("Export")}</a>
      <button type="button" class="btn btn-xs" onClick=${onReset}>${t("Reset")}</button>
    `}
  >
    <div class="table-wrapper">
      <table class="data receive-latency-table">
        <thead>
          <tr>
            <th>${t("Flow")}</th>
            <th class="numeric">${t("Latency setting")}</th>
            <th class="numeric">${t("Now")}</th>
            <th class="numeric">${t("Peak")}</th>
            <th class="numeric">${t("Headroom")}</th>
            <th class="numeric">${t("Late packets")}</th>
            <th>${t("Last 5 minutes")}</th>
          </tr>
        </thead>
        <tbody>
          ${paths.map((path) => {
            const latency = path.latency || {};
            const budget = path.evidence?.configured_latency_nanoseconds ?? latency.current?.evidence?.configured_latency_nanoseconds;
            const current = latency.current?.value;
            const peak = latency.statistics?.maximum;
            const headroom = finite(budget) && finite(current) ? Number(budget) - Number(current) : null;
            return html`<tr key=${`${path.telemetry_index}:${path.network_interface_index}`}>
              <td data-label=${t("Flow")}>${flowLabel(path)}</td>
              <td class="numeric" data-label=${t("Latency setting")}>${milliseconds(budget)}</td>
              <td class="numeric" data-label=${t("Now")}>${milliseconds(current)}</td>
              <td class="numeric" data-label=${t("Peak")}>${milliseconds(peak)}</td>
              <td class=${`numeric${headroom != null && headroom <= 0 ? " state-bad" : ""}`} data-label=${t("Headroom")}>${milliseconds(headroom)}</td>
              <td class="numeric" data-label=${t("Late packets")}><${LatePackets} series=${path.late_packets} /></td>
              <td data-label=${t("Last 5 minutes")}><${Sparkline} series=${latency} label=${t("Maximum latency over the last 5 minutes")} /></td>
            </tr>`;
          })}
        </tbody>
      </table>
    </div>
  <//>`;
}

function frequencySeries(data) {
  const candidates = ["conmon", "heartbeat"]
    .map((name) => data?.clock?.[name])
    .filter((series) => series?.fresh && finite(series.current?.value) && (series.statistics?.count || 0) >= MINIMUM_FREQUENCY_OBSERVATIONS);
  return candidates[0] || null;
}

function ClockFrequency({ data, onWarnings }) {
  const series = frequencySeries(data);
  if (!series) return null;
  const stats = series.statistics || {};
  return html`<${Panel}
    title=${t("Clock frequency")}
    headerActions=${html`<label class="status-toggle">
      <input
        type="checkbox"
        checked=${data.clock?.warning_enabled === true}
        onChange=${(event) => onWarnings(event.target.checked)}
      />
      ${t("Warn on drift")}
    </label>`}
  >
    <div class="clock-frequency">
      <${Fields}
        entries=${[
          [t("Offset now"), partsPerMillion(series.current.value)],
          [t("Range"), finite(stats.minimum) && finite(stats.maximum) ? t("{minimum} to {maximum}", { minimum: partsPerMillion(stats.minimum), maximum: partsPerMillion(stats.maximum) }) : undefined],
        ]}
      />
      <${Sparkline} series=${series} label=${t("Clock frequency offset over the last 5 minutes")} />
    </div>
  <//>`;
}

async function post(path, body) {
  const response = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error);
  return payload;
}

export function DiagnosticsSection({ device }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const endpoint = `/diagnostics/${encodeURIComponent(device.server_name)}`;
  useEffect(() => {
    const controller = new AbortController();
    let timer = 0;
    async function refresh() {
      try {
        const response = await fetch(`${endpoint}?history=points`, { signal: controller.signal });
        const body = await response.json();
        if (!response.ok) throw new Error(body.error);
        setData(body);
        setError(null);
      } catch (failure) {
        if (controller.signal.aborted) return;
        setError(failure.message);
      }
      if (!controller.signal.aborted) timer = setTimeout(refresh, REFRESH_MILLISECONDS);
    }
    refresh();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [endpoint]);
  const run = (path, body) => post(path, body).then(setData, (failure) => setError(failure.message));
  return html`
    ${error ? html`<${Notice}>${t(error)}<//>` : null}
    <${ReceiveLatency}
      data=${data}
      endpoint=${endpoint}
      onReset=${() => run("/diagnostics/reset", { device: device.server_name, history: "points" })}
    />
    <${ClockFrequency}
      data=${data}
      onWarnings=${(enabled) => run("/diagnostics/policy", { device: device.server_name, clock_variation_warnings: enabled, history: "points" })}
    />
  `;
}
