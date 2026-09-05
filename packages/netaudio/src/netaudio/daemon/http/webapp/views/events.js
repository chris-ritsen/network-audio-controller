import { Notice, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { html, useState } from "../lib/preact.js";
import { connectionState, events } from "../store.js";

const EVENT_LABELS = {
  device_discovered: "device discovered",
  device_removed: "device removed",
  device_updated: "device updated",
  parse_error: "parse error",
  shure_device_discovered: "Shure device discovered",
  shure_device_removed: "Shure device removed",
  shure_device_updated: "Shure device updated",
  snapshot: "inventory snapshot",
  subscription_pending: "subscription pending",
};

function subjectOf(payload) {
  return payload.server_name || payload.mac || payload.device_name || "";
}

function summaryOf(payload) {
  if (payload.event === "snapshot") {
    return `${Object.keys(payload.devices || {}).length} devices, ${Object.keys(payload.shure_devices || {}).length} Shure devices`;
  }
  if (payload.device) {
    const device = payload.device;
    return [device.model, device.ipv4, device.online === false ? "offline" : "online"].filter(Boolean).join(" · ");
  }
  if (payload.event === "parse_error") {
    return html`<${Value} value=${payload.message} />`;
  }
  return "";
}

function EventRow({ entry, expanded, onToggle }) {
  const payload = entry.payload || {};
  const kind = payload.event || "unknown";
  return html`
    <div>
      <div class="event-row" onClick=${onToggle}>
        <span>${format.timestamp(entry.received)}</span>
        <span class="event-kind">${EVENT_LABELS[kind] || kind}</span>
        <span class="event-summary">${[subjectOf(payload), summaryOf(payload)].filter(Boolean).join("  ·  ")}</span>
      </div>
      ${expanded ? html`<div class="event-detail"><${Value} value=${payload} /></div>` : null}
    </div>
  `;
}

function EventsView() {
  const [filter, setFilter] = useState("");
  const [expanded, setExpanded] = useState(null);
  const needle = filter.trim().toLowerCase();
  const entries = events.value.filter((entry) => {
    if (!needle) {
      return true;
    }
    const payload = entry.payload || {};
    return `${payload.event || ""} ${subjectOf(payload)} ${summaryOf(payload)}`.toLowerCase().includes(needle);
  });

  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">Events</div>
          <div class="content-subtitle">
            Live daemon events held in memory only. Meter samples are excluded so the log stays readable.
          </div>
        </div>
      </div>
      <${Panel}
        title="Daemon event stream"
        actions=${html`
          <input
            type="search"
            size="30"
            placeholder="Filter by event, device, or detail"
            value=${filter}
            onInput=${(event) => setFilter(event.target.value)}
          />
          <span class="nav-count">${entries.length} of ${events.value.length}</span>
          <span class="connection-pill ${connectionState.value}">
            <span class="status-dot${connectionState.value === "open" ? " online" : ""}"></span>
            ${connectionState.value === "open" ? "live" : connectionState.value}
          </span>
        `}
      >
        ${entries.length === 0
          ? html`<${Notice}>No events buffered yet.<//>`
          : html`<div class="event-log">
              ${entries.map(
                (entry, index) => html`<${EventRow}
                  key=${`${entry.received}-${index}`}
                  entry=${entry}
                  expanded=${expanded === `${entry.received}-${index}`}
                  onToggle=${() =>
                    setExpanded(expanded === `${entry.received}-${index}` ? null : `${entry.received}-${index}`)}
                />`,
              )}
            </div>`}
      <//>
    </div>
  `;
}

export const eventsView = {
  component: EventsView,
  id: "events",
  label: "Events",
};
