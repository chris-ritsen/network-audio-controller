import { Notice, Panel } from "../components.js";
import { api } from "../api.js";
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
  monitoring_event: "monitoring journal event",
};

function subjectOf(payload) {
  return payload.server_name || payload.mac || payload.device_name || "";
}

function summaryOf(payload) {
  if (payload.event === "snapshot") {
    const onlineCount = (records) =>
      Object.values(records || {}).filter(
        (device) =>
          device.online === true && device.availability_state !== "offline",
      ).length;
    return `${onlineCount(payload.devices)} Dante devices online · ${onlineCount(payload.shure_devices)} Shure devices online`;
  }
  if (payload.device) {
    const device = payload.device;
    return [
      format.deviceModelName(device),
      device.ipv4,
      device.online === true
        ? "online"
        : device.online === false
          ? "offline"
          : "availability not reported",
    ]
      .filter(Boolean)
      .join(" · ");
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
        <span class="event-summary"
          >${[subjectOf(payload), summaryOf(payload)].filter(Boolean).join("  ·  ")}</span
        >
      </div>
    </div>
  `;
}

function journalSubject(entry) {
  return (
    entry.flow_identity ||
    entry.channel_identity ||
    entry.interface_identity ||
    ""
  );
}

function journalValue(value) {
  if (value === null || value === undefined) return "unknown";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function JournalRow({ entry }) {
  const device =
    entry.device_name || entry.server_name || entry.device_identity || "";
  const transition = `${journalValue(entry.previous_value)} → ${journalValue(entry.current_value)}`;
  return html`
    <div class="event-row">
      <span>${format.timestamp(entry.timestamp)}</span>
      <span class="event-kind">${entry.kind || "unknown"}</span>
      <span class="event-summary"
        >${[entry.severity, device, journalSubject(entry), transition].filter(Boolean).join(" · ")}</span
      >
    </div>
  `;
}

function IssuePanel() {
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState(null);
  const [state, setState] = useState("open");
  const latestIssueSequence = events.value.find((entry) =>
    ["issue_opened", "issue_updated", "issue_resolved"].includes(
      entry.payload?.journal_event?.kind,
    ),
  )?.payload?.journal_event?.sequence;
  useEffect(() => {
    let disposed = false;
    api.getIssues(state).then(
      (value) => {
        if (!disposed) {
          setPayload(value);
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
  }, [state, latestIssueSequence]);
  const issues = Array.isArray(payload?.issues) ? payload.issues : [];
  return html`<${Panel}
    title="Issues"
    actions=${html`<label class="flex items-center gap-2 text-sm"
      >Show
      <select
        value=${state}
        onChange=${(event) => setState(event.target.value)}
      >
        <option value="open">Current</option>
        <option value="resolved">Resolved history</option>
      </select></label
    >`}
  >
    ${
      error
        ? html`<${Notice} tone="danger">${error}<//>`
        : payload === null
          ? html`<${Notice}>Loading issues…<//>`
          : issues.length === 0
            ? html`<${Notice}
                >${state === "open" ? "No current issues." : "No resolved issues retained."}<//
              >`
            : html`<div class="flex flex-col gap-3">
                ${issues.map((issue) => {
                  const scope = issue.scope || {};
                  const subject =
                    scope.flow_identity ||
                    scope.channel_identity ||
                    scope.interface_identity;
                  return html`<article
                    key=${issue.issue_id}
                    class="border border-base-300 rounded-lg p-3"
                  >
                    <div class="flex flex-wrap gap-2">
                      <strong>${issue.title}</strong
                      ><span>${issue.severity}</span><span>${issue.state}</span>
                    </div>
                    <p class="text-sm">
                      ${[scope.device_name || scope.server_name, subject, issue.summary].filter(Boolean).join(" · ")}
                    </p>
                    <p class="text-sm">
                      Evidence: ${issue.evidence_class} from
                      ${issue.evidence_source} ·
                      ${issue.observation_state || "observed"} ·
                      ${issue.occurrence_count} supporting
                      observation${issue.occurrence_count === 1 ? "" : "s"}
                    </p>
                    <p class="text-sm">
                      Suggested action: ${issue.suggested_remediation}
                    </p>
                  </article>`;
                })}
              </div>`
    }
  <//>`;
}

function JournalPanel() {
  const [entries, setEntries] = useState(null);
  const [error, setError] = useState(null);
  const [filter, setFilter] = useState("");
  const latestJournalSequence = events.value.find(
    (entry) => entry.payload?.event === "monitoring_event",
  )?.payload?.journal_event?.sequence;

  useEffect(() => {
    let disposed = false;
    api.getEventJournal().then(
      (payload) => {
        if (!disposed) {
          setEntries(Array.isArray(payload?.events) ? payload.events : []);
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

  const clear = () =>
    api.clearEventJournal().then(
      () => {
        setEntries([]);
        setError(null);
      },
      (failure) => setError(failure.message),
    );
  const needle = filter.trim().toLowerCase();
  const visible = (entries || []).filter((entry) => {
    if (!needle) return true;
    return `${entry.kind || ""} ${entry.severity || ""} ${entry.device_identity || ""} ${entry.device_name || ""} ${journalSubject(entry)}`
      .toLowerCase()
      .includes(needle);
  });

  return html`
    <${Panel}
      title="Monitoring journal"
      actions=${html`
        <input
          type="search"
          class="w-full sm:w-64"
          aria-label="Filter journal"
          placeholder="Filter journal"
          value=${filter}
          onInput=${(event) => setFilter(event.target.value)}
        />
        <button
          type="button"
          class="btn btn-sm"
          disabled=${!entries?.length}
          onClick=${clear}
        >
          Clear local history
        </button>
      `}
    >
      ${
        error
          ? html`<${Notice} tone="danger">${error}<//>`
          : entries === null
            ? html`<${Notice}>Loading retained events…<//>`
            : visible.length === 0
              ? html`<${Notice}
                  >${needle ? "No matching journal events." : "No retained monitoring events."}<//
                >`
              : html`<div class="event-log">
                  ${visible.map(
                    (entry) =>
                      html`<${JournalRow}
                        key=${entry.sequence}
                        entry=${entry}
                      />`,
                  )}
                </div>`
      }
    <//>
  `;
}

function EventsView() {
  const [filter, setFilter] = useState("");
  const [paused, setPaused] = useState(true);
  const [snapshot, setSnapshot] = useState(() => events.value);
  const root = useRef(null);
  const current = paused ? snapshot : events.value;
  const hasNew = events.value[0] !== current[0];
  const pause = () => {
    setSnapshot(events.value);
    setPaused(true);
  };
  useEffect(() => {
    if (paused) return;
    const content = root.current?.closest("main");
    const onScroll = () => {
      if (content.scrollTop > 0) pause();
    };
    content?.addEventListener("scroll", onScroll, { passive: true });
    return () => content?.removeEventListener("scroll", onScroll);
  }, [paused]);
  const needle = filter.trim().toLowerCase();
  const entries = current.filter((entry) => {
    if (!needle) {
      return true;
    }
    const payload = entry.payload || {};
    return `${payload.event || ""} ${subjectOf(payload)} ${summaryOf(payload)}`
      .toLowerCase()
      .includes(needle);
  });

  return html`
    <div ref=${root} class="flex flex-col gap-4">
      <${IssuePanel} />
      <${JournalPanel} />
      <${Panel}
        title="Recent events"
        actions=${html`
          <input
            type="search"
            class="w-full sm:w-64"
            aria-label="Filter events"
            placeholder="Filter events"
            value=${filter}
            onFocus=${() => {
              if (!paused) pause();
            }}
            onInput=${(event) => setFilter(event.target.value)}
          />
          <button
            type="button"
            class="btn btn-sm"
            disabled=${!paused || !hasNew}
            onClick=${() => setSnapshot(events.value)}
          >
            Show new events
          </button>
          <label class="event-live-control"
            ><input
              type="checkbox"
              checked=${!paused}
              onChange=${(event) => (event.target.checked ? setPaused(false) : pause())}
            />Live updates</label
          >
        `}
      >
        ${
          entries.length === 0
            ? html`<${Notice}
                >${needle ? "No matching events." : "No recent events."}<//
              >`
            : html`<div class="event-log">
                ${entries.map(
                (entry, index) =>
                  html`<${EventRow}
                    key=${`${entry.received}-${index}`}
                    entry=${entry}
                  />`,
              )}
              </div>`
        }
      <//>
    </div>
  `;
}

export const eventsView = {
  component: EventsView,
  id: "events",
  label: "Events",
};
