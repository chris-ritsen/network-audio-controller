import { Notice, Panel } from "../components.js";
import { api } from "../api.js";
import * as format from "../format.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { setQueryParameters } from "../router.js";
import { events } from "../store.js";

const SEVERITY_LABELS = { error: "Error", info: "Information", warning: "Warning" };

const SEVERITY_RANK = { info: 0, warning: 1, error: 2 };

function isLeader(role) {
  return String(role || "").toLowerCase() === "leader";
}

function danteEvents(entry) {
  const device = entry.device_name || entry.server_name || "";
  const before = entry.previous_value;
  const after = entry.current_value;
  const event = (name, severity, description) => ({
    description,
    device,
    key: `${entry.sequence}:${name}`,
    name,
    severity,
    timestamp: entry.timestamp,
  });
  if (!before || !after) return [];
  if (entry.kind === "clock_status_changed") {
    const found = [];
    if (before.synchronization === "synchronized" && after.synchronization === "lost") {
      found.push(event("Clock Sync Unlocked", "error", `${device} lost clock sync.`));
    }
    if (before.synchronization === "lost" && after.synchronization === "synchronized") {
      found.push(event("Clock Sync Locked", "error", `${device} regained clock sync.`));
    }
    if (Number.isInteger(before.mute_flags) && Number.isInteger(after.mute_flags)) {
      if (!before.mute_flags && after.mute_flags) found.push(event("Audio Mute", "error", `${device} was muted.`));
      if (before.mute_flags && !after.mute_flags) found.push(event("Audio Unmute", "error", `${device} was unmuted.`));
    }
    return found;
  }
  if (entry.kind === "clock_role_changed") {
    if (!isLeader(before) && isLeader(after)) {
      return [event("Elevation to Clock Leader", "error", `${device} is now the clock leader.`)];
    }
    if (isLeader(before) && !isLeader(after)) {
      return [event("Demotion from Clock Leader", "info", `${device} is no longer the clock leader.`)];
    }
    return [];
  }
  if (entry.kind === "setting_changed" && typeof before.is_locked === "boolean" && typeof after.is_locked === "boolean") {
    return after.is_locked
      ? [event("Device Locked", "info", `${device} was locked.`)]
      : [event("Device Unlocked", "info", `${device} was unlocked.`)];
  }
  return [];
}

function SeverityIcon({ severity }) {
  const label = SEVERITY_LABELS[severity] || "Unknown";
  return html`
    <span
      class="event-severity-icon ${severity || "unknown"}"
      title=${label}
      aria-label=${label}
      >${severity === "error" ? "×" : severity === "warning" ? "!" : "i"}</span
    >
  `;
}

function saveLog(entries) {
  const lines = entries.map((event) =>
    [format.timestamp(event.timestamp), event.device, SEVERITY_LABELS[event.severity], event.name, event.description].join("\t"),
  );
  const blob = new Blob([`${lines.join("\n")}\n`], { type: "text/plain" });
  const link = document.createElement("a");
  const timestamp = new Date()
    .toISOString()
    .replaceAll(":", "-")
    .replace(/\.\d{3}Z$/, "Z");
  const url = URL.createObjectURL(blob);
  link.href = url;
  link.download = `netaudio-events-${timestamp}.log`;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function EventDetails({ event }) {
  return html`
    <section class="event-detail-panel selectable-content" aria-label="Event details">
      <div class="event-detail-heading">
        <h3>${event.name}</h3>
        <${SeverityIcon} severity=${event.severity} />
      </div>
      <p>${event.description}</p>
    </section>
  `;
}

function EventLog({ query }) {
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState(null);
  const minimumSeverity = SEVERITY_RANK[query.severity] === undefined ? "info" : query.severity;
  const filter = query.search || "";
  const selectedKey = query.event || null;
  const setSelectedKey = (key) => setQueryParameters({ event: key });
  const [clearing, setClearing] = useState(false);
  const latestJournalSequence = events.value.find(
    (entry) => entry.payload?.event === "monitoring_event",
  )?.payload?.journal_event?.sequence;

  useEffect(() => {
    let disposed = false;
    api.getEventJournal().then(
      (value) => {
        if (!disposed) {
          setPayload({
            ...value,
            events: Array.isArray(value?.events) ? value.events : [],
          });
          setError(null);
        }
      },
      (failure) => {
        if (!disposed) setError(failure.message);
      },
    );
    return () => {
      disposed = true;
    };
  }, [latestJournalSequence]);

  const entries = (payload?.events || []).flatMap(danteEvents);
  const minimumRank = SEVERITY_RANK[minimumSeverity];
  const needle = filter.trim().toLowerCase();
  const visible = entries.filter((event) => {
    if (SEVERITY_RANK[event.severity] < minimumRank) return false;
    if (!needle) return true;
    return `${event.device} ${event.name} ${event.description}`.toLowerCase().includes(needle);
  });
  const selected = visible.find((event) => event.key === selectedKey);
  const toggle = (event) => {
    if (String(globalThis.getSelection?.() || "")) return;
    setSelectedKey(event.key === selectedKey ? null : event.key);
  };

  const clear = async () => {
    if (!window.confirm("Clear all events?")) return;
    setClearing(true);
    try {
      await api.clearEventJournal();
      setPayload({ ...payload, count: 0, events: [] });
      setSelectedKey(null);
      setError(null);
    } catch (failure) {
      setError(failure.message);
    } finally {
      setClearing(false);
    }
  };

  return html`
    <${Panel} title="Event log" wide>
      ${error ? html`<${Notice}>${error}<//>` : null}
      <div class="table-wrapper event-log-table-wrapper">
        <table class="data event-log-table" aria-label="Event log">
          <thead>
            <tr>
              <th><span class="sr-only">Severity</span></th>
              <th>Timestamp</th>
              <th>Device Name</th>
              <th>Event</th>
            </tr>
          </thead>
          <tbody>
            ${visible.map((event) => {
              const active = event.key === selectedKey;
              return html`<tr
                key=${event.key}
                class=${active ? "active" : ""}
                tabindex="0"
                aria-selected=${active ? "true" : "false"}
                onClick=${() => toggle(event)}
                onKeyDown=${(keyEvent) => {
                  if (keyEvent.key !== "Enter" && keyEvent.key !== " ") return;
                  keyEvent.preventDefault();
                  toggle(event);
                }}
              >
                <td class="event-severity-cell" data-label="Severity">
                  <${SeverityIcon} severity=${event.severity} />
                </td>
                <td class="event-timestamp" data-label="Timestamp">${format.timestamp(event.timestamp)}</td>
                <td class="event-device" data-label="Device Name">${event.device}</td>
                <td class="event-message" data-label="Event">${event.name}</td>
              </tr>`;
            })}
          </tbody>
        </table>
      </div>
      <div class="event-log-controls">
        <label class="event-severity-filter">
          <span>Show</span>
          <select
            aria-label="Minimum severity"
            value=${minimumSeverity}
            onChange=${(event) =>
              setQueryParameters({ event: null, severity: event.target.value === "info" ? null : event.target.value })}
          >
            <option value="info">All Events</option>
            <option value="warning">Warnings and Errors</option>
            <option value="error">Errors Only</option>
          </select>
        </label>
        <input
          type="search"
          class="event-log-search"
          aria-label="Search event log"
          placeholder="Search device or event"
          value=${filter}
          onInput=${(event) => setQueryParameters({ event: null, search: event.target.value })}
        />
        <div class="event-log-actions">
          <button type="button" class="btn btn-sm" disabled=${!entries.length} onClick=${() => saveLog(entries)}>
            Save
          </button>
          <button
            type="button"
            class="btn btn-sm"
            disabled=${!payload?.events?.length || clearing}
            aria-busy=${clearing ? "true" : null}
            onClick=${clear}
          >
            ${clearing ? "Clearing…" : "Clear"}
          </button>
        </div>
      </div>
      ${selected ? html`<${EventDetails} event=${selected} />` : null}
    <//>
  `;
}

function EventsView({ location }) {
  return html`<${EventLog} query=${location.query} />`;
}

export const eventsView = {
  filters: false,
  component: EventsView,
  id: "events",
  label: "Events",
};
