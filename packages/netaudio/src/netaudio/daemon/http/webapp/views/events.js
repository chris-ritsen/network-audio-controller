import { Notice, Panel } from "../components.js";
import * as format from "../format.js";
import { html, useEffect, useRef, useState } from "../lib/preact.js";
import { events } from "../store.js";

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
    return [format.deviceModelName(device), device.ipv4, device.online === false ? "offline" : "online"].filter(Boolean).join(" · ");
  }
  if (payload.event === "parse_error") {
    return "Could not read a server update";
  }
  return "";
}

function EventRow({ entry }) {
  const payload = entry.payload || {};
  const kind = payload.event || "unknown";
  return html`
    <div>
      <div class="event-row">
        <span>${format.timestamp(entry.received)}</span>
        <span class="event-kind">${EVENT_LABELS[kind] || kind}</span>
        <span class="event-summary">${[subjectOf(payload), summaryOf(payload)].filter(Boolean).join("  ·  ")}</span>
      </div>
    </div>
  `;
}

function EventsView() {
  const [filter, setFilter] = useState("");
  const [paused, setPaused] = useState(true);
  const [snapshot, setSnapshot] = useState(() => events.value);
  const root = useRef(null);
  const current = paused ? snapshot : events.value;
  const hasNew = events.value[0] !== current[0];
  const pause = () => { setSnapshot(events.value); setPaused(true); };
  useEffect(() => {
    if (paused) return;
    const content = root.current?.closest("main");
    const onScroll = () => { if (content.scrollTop > 0) pause(); };
    content?.addEventListener("scroll", onScroll, { passive: true });
    return () => content?.removeEventListener("scroll", onScroll);
  }, [paused]);
  const needle = filter.trim().toLowerCase();
  const entries = current.filter((entry) => {
    if (!needle) {
      return true;
    }
    const payload = entry.payload || {};
    return `${payload.event || ""} ${subjectOf(payload)} ${summaryOf(payload)}`.toLowerCase().includes(needle);
  });

  return html`
    <div ref=${root} class="flex flex-col gap-4">
      <div class="content-header">
        <div>
          <div class="content-title">Events</div>
          <div class="content-subtitle">
            Recent device events. The list stays still until you update it.
          </div>
        </div>
      </div>
      <${Panel}
        title="Recent events"
        actions=${html`
          <input
            type="search"
            class="w-full sm:w-64"
            aria-label="Filter events"
            placeholder="Filter events"
            value=${filter}
            onFocus=${() => { if (!paused) pause(); }}
            onInput=${(event) => setFilter(event.target.value)}
          />
          ${paused && hasNew ? html`<button type="button" class="btn btn-sm" onClick=${() => setSnapshot(events.value)}>Show new events</button>` : null}
          <button type="button" class="btn btn-sm" onClick=${() => paused ? setPaused(false) : pause()}>${paused ? "Resume live" : "Pause"}</button>
        `}
      >
        ${entries.length === 0
          ? html`<${Notice}>${needle ? "No matching events." : "No recent events."}<//>`
          : html`<div class="event-log">
              ${entries.map(
                (entry, index) => html`<${EventRow}
                  key=${`${entry.received}-${index}`}
                  entry=${entry}
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
