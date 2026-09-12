import { Notice, Panel, Tabs } from "../components.js";
import { api } from "../api.js";
import * as format from "../format.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { events } from "../store.js";

const EVENT_LABELS = {
  clock_role_changed: "Clock role changed",
  device_disappeared: "Device disappeared",
  device_reappeared: "Device reappeared",
  interface_error_counter_increased: "Interface error count increased",
  interface_error_counter_reset: "Interface error count reset",
  interface_utilization_high: "Interface utilization high",
  interface_utilization_recovered: "Interface utilization recovered",
  issue_opened: "Issue opened",
  issue_resolved: "Issue resolved",
  issue_updated: "Issue updated",
  leader_identity_changed: "Leader identity changed",
  late_packet_count_increased: "Late packet count increased",
  late_packet_counter_reset: "Late packet count reset",
  mute_state_changed: "Mute state changed",
  ptp_port_state_changed: "PTP port state changed",
  receiver_flow_latency_high: "Receiver flow latency high",
  receiver_flow_latency_recovered: "Receiver flow latency recovered",
  subscription_failed: "Subscription failed",
  subscription_recovered: "Subscription recovered",
  configuration_operation: "Configuration operation",
  preset_run: "Preset application",
};

const OPERATION_PHASE_LABELS = {
  requested: "requested",
  request_acknowledged: "request acknowledged",
  request_rejected: "request rejected",
  effective_state_confirmed: "effective state confirmed",
  partial_unobservable: "partial or unobservable",
  inconsistent: "inconsistent",
  transport_or_validation_failure: "transport or validation failure",
  persistence_request_acknowledged: "persistence request acknowledged",
  persistence_confirmed: "persistence confirmed",
};

const OPERATION_LABELS = {
  apply_preset: "Apply preset",
  create_transmit_flow: "Create transmit flow",
  delete_transmit_flow: "Delete transmit flow",
  remove_subscription_associations: "Remove subscription associations",
  set_receive_flow_default_slots: "Set receive-flow default slots",
  set_receive_flow_performance: "Set receive-flow performance",
  set_transmit_flow_performance: "Set transmit-flow performance",
  set_unicast_performance: "Set unicast performance",
  store_current_configuration: "Store current configuration",
  subscribe_external_rtp: "Subscribe external RTP",
};

function operationLabel(name) {
  return OPERATION_LABELS[name] || format.stateLabel(name);
}

const SEVERITY_RANK = { info: 0, warning: 1, error: 2 };

function journalSubject(entry) {
  return (
    entry.flow_identity ||
    entry.channel_identity ||
    entry.interface_identity ||
    ""
  );
}

function eventDevice(entry) {
  return (
    entry.device_name ||
    entry.server_name ||
    entry.device_identity ||
    "Unknown device"
  );
}

function eventLabel(entry) {
  const issue = entry.current_value;
  if (
    entry.kind?.startsWith("issue_") &&
    issue &&
    typeof issue === "object" &&
    typeof issue.title === "string" &&
    issue.title
  ) {
    return issue.title;
  }
  if (EVENT_LABELS[entry.kind]) return EVENT_LABELS[entry.kind];
  const words = String(entry.kind || "Unknown event").replaceAll("_", " ");
  return words.charAt(0).toUpperCase() + words.slice(1);
}

function journalValue(value) {
  if (value === null || value === undefined) return "unknown";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "object") {
    for (const key of ["summary", "label", "title", "state", "kind"]) {
      if (typeof value[key] === "string" && value[key]) return value[key];
    }
    return JSON.stringify(value);
  }
  return String(value);
}

function eventMessage(entry) {
  if (entry.operation_name) {
    const operation = operationLabel(entry.operation_name);
    const phase =
      OPERATION_PHASE_LABELS[entry.lifecycle_phase] ||
      format.stateLabel(entry.lifecycle_phase);
    return [operation, phase, journalSubject(entry)]
      .filter(Boolean)
      .join(" · ");
  }
  const parts = [eventLabel(entry)];
  const subject = journalSubject(entry);
  if (subject) parts.push(subject);
  if (entry.kind?.startsWith("issue_")) {
    const summary = entry.current_value?.summary;
    if (typeof summary === "string" && summary && summary !== parts[0]) {
      parts.push(summary);
    }
  } else if (
    entry.previous_value !== null &&
    entry.previous_value !== undefined &&
    entry.current_value !== null &&
    entry.current_value !== undefined
  ) {
    parts.push(
      `${journalValue(entry.previous_value)} → ${journalValue(entry.current_value)}`,
    );
  }
  return parts.join(" · ");
}

function severityLabel(severity) {
  return (
    { info: "Information", warning: "Warning", error: "Error" }[severity] ||
    "Unknown"
  );
}

function SeverityIcon({ severity }) {
  const label = severityLabel(severity);
  return html`
    <span
      class="event-severity-icon ${severity || "unknown"}"
      title=${label}
      aria-label=${label}
      >${severity === "error" ? "×" : severity === "warning" ? "!" : "i"}</span
    >
  `;
}

function downloadJournal(journal) {
  const entries = journal.events || [];
  const payload = {
    schema_version: journal.schema_version,
    retention_limit: journal.retention_limit,
    count: entries.length,
    events: entries,
  };
  const blob = new Blob([`${JSON.stringify(payload, null, 2)}\n`], {
    type: "application/json",
  });
  const link = document.createElement("a");
  const timestamp = new Date()
    .toISOString()
    .replaceAll(":", "-")
    .replace(/\.\d{3}Z$/, "Z");
  const url = URL.createObjectURL(blob);
  link.href = url;
  link.download = `netaudio-events-${timestamp}.json`;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function EventDetails({ entry }) {
  const change = `${journalValue(entry.previous_value)} → ${journalValue(entry.current_value)}`;
  const evidence = `${format.stateLabel(entry.derivation_status)} from ${format.stateLabel(entry.observation_source)}`;
  return html`
    <section class="event-detail-panel" aria-label="Event details">
      <div class="event-detail-heading">
        <div>
          <span class="event-detail-kicker">Event details</span>
          <h3>${eventLabel(entry)}</h3>
        </div>
        <${SeverityIcon} severity=${entry.severity} />
      </div>
      <dl class="event-detail-grid">
        <dt>Timestamp</dt>
        <dd>${format.timestamp(entry.timestamp)}</dd>
        <dt>Device</dt>
        <dd>${eventDevice(entry)}</dd>
        <dt>Subject</dt>
        <dd>${journalSubject(entry) || "—"}</dd>
        <dt>Change</dt>
        <dd>${change}</dd>
        <dt>Evidence</dt>
        <dd>${evidence}</dd>
        <dt>Sequence</dt>
        <dd>${entry.sequence}</dd>
        ${
          entry.operation_id
            ? html`
                <dt>Operation</dt>
                <dd>${operationLabel(entry.operation_name)}</dd>
                <dt>Lifecycle phase</dt>
                <dd>
                  ${
                    OPERATION_PHASE_LABELS[entry.lifecycle_phase] ||
                    format.stateLabel(entry.lifecycle_phase)
                  }
                </dd>
                <dt>Operation ID</dt>
                <dd>${entry.operation_id}</dd>
                <dt>Correlation ID</dt>
                <dd>${entry.correlation_id || "—"}</dd>
                <dt>Requested values</dt>
                <dd>${journalValue(entry.requested_values)}</dd>
                <dt>Effective values</dt>
                <dd>${journalValue(entry.effective_values)}</dd>
                <dt>Transport</dt>
                <dd>${entry.transport || "—"}</dd>
                <dt>Final state</dt>
                <dd>
                  ${
                    OPERATION_PHASE_LABELS[entry.final_operation_state] ||
                    format.stateLabel(entry.final_operation_state) ||
                    "—"
                  }
                </dd>
                <dt>Persistence confirmation</dt>
                <dd>${journalValue(entry.persistence_confirmation)}</dd>
              `
            : null
        }
      </dl>
      ${
        entry.raw && Object.keys(entry.raw).length
          ? html`<details class="event-raw-evidence">
              <summary>Raw supporting evidence</summary>
              <pre>${JSON.stringify(entry.raw, null, 2)}</pre>
            </details>`
          : null
      }
    </section>
  `;
}

function EventLog() {
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState(null);
  const [minimumSeverity, setMinimumSeverity] = useState("info");
  const [filter, setFilter] = useState("");
  const [selectedSequence, setSelectedSequence] = useState(null);
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

  const entries = payload?.events || [];
  const minimumRank = SEVERITY_RANK[minimumSeverity];
  const needle = filter.trim().toLowerCase();
  const visible = entries.filter((entry) => {
    if ((SEVERITY_RANK[entry.severity] ?? -1) < minimumRank) return false;
    if (!needle) return true;
    return `${eventDevice(entry)} ${eventMessage(entry)} ${entry.severity || ""}`
      .toLowerCase()
      .includes(needle);
  });
  const selected = visible.find((entry) => entry.sequence === selectedSequence);

  const clear = async () => {
    if (
      !window.confirm(
        `Clear ${entries.length} locally retained event${entries.length === 1 ? "" : "s"}? Device state and counters will not be changed.`,
      )
    ) {
      return;
    }
    setClearing(true);
    try {
      await api.clearEventJournal();
      setPayload({ ...payload, count: 0, events: [] });
      setSelectedSequence(null);
      setError(null);
    } catch (failure) {
      setError(failure.message);
    } finally {
      setClearing(false);
    }
  };

  return html`
    <${Panel} title="Event log" wide>
      ${
        error
          ? html`<${Notice}>${error}<//>`
          : payload === null
            ? html`<${Notice}>Loading retained events…<//>`
            : html`
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
                      ${visible.map((entry) => {
                        const message = eventMessage(entry);
                        const active = entry.sequence === selectedSequence;
                        return html`<tr
                          key=${entry.sequence}
                          class=${active ? "active" : ""}
                        >
                          <td class="event-severity-cell" data-label="Severity">
                            <${SeverityIcon} severity=${entry.severity} />
                          </td>
                          <td class="event-timestamp" data-label="Timestamp">
                            ${format.timestamp(entry.timestamp)}
                          </td>
                          <td class="event-device" data-label="Device Name">
                            ${eventDevice(entry)}
                          </td>
                          <td class="event-message" data-label="Event">
                            <button
                              type="button"
                              class="event-message-button"
                              aria-expanded=${active ? "true" : "false"}
                              aria-label=${`${active ? "Hide" : "View"} details for ${message}`}
                              onClick=${() =>
                                setSelectedSequence(
                                  active ? null : entry.sequence,
                                )}
                            >
                              ${message}
                            </button>
                          </td>
                        </tr>`;
                      })}
                    </tbody>
                  </table>
                  ${
                    visible.length === 0
                      ? html`<div class="event-log-empty">
                          ${
                            needle || minimumSeverity !== "info"
                              ? "No events match the current filters."
                              : "No retained monitoring events."
                          }
                        </div>`
                      : null
                  }
                </div>
                <div class="event-log-controls">
                  <label class="event-severity-filter">
                    <span>Show</span>
                    <select
                      aria-label="Minimum severity"
                      value=${minimumSeverity}
                      onChange=${(event) => {
                        setMinimumSeverity(event.target.value);
                        setSelectedSequence(null);
                      }}
                    >
                      <option value="info">Information</option>
                      <option value="warning">Warning</option>
                      <option value="error">Error</option>
                    </select>
                  </label>
                  <input
                    type="search"
                    class="event-log-search"
                    aria-label="Search event log"
                    placeholder="Search device or event"
                    value=${filter}
                    onInput=${(event) => {
                      setFilter(event.target.value);
                      setSelectedSequence(null);
                    }}
                  />
                  <div class="event-log-actions">
                    <button
                      type="button"
                      class="btn btn-sm"
                      disabled=${!entries.length}
                      title="Save all retained events as JSON"
                      onClick=${() => downloadJournal(payload)}
                    >
                      Save
                    </button>
                    <button
                      type="button"
                      class="btn btn-sm"
                      disabled=${!entries.length || clearing}
                      aria-busy=${clearing ? "true" : null}
                      onClick=${clear}
                    >
                      ${clearing ? "Clearing…" : "Clear"}
                    </button>
                  </div>
                  <span class="event-log-count">
                    ${visible.length} of ${entries.length} retained
                  </span>
                </div>
                ${selected ? html`<${EventDetails} entry=${selected} />` : null}
              `
      }
    <//>
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
        ? html`<${Notice}>${error}<//>`
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
                      ${[
                        scope.device_name || scope.server_name,
                        subject,
                        issue.summary,
                      ]
                        .filter(Boolean)
                        .join(" · ")}
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

function EventsView() {
  const [active, setActive] = useState("log");
  return html`
    <div class="flex flex-col gap-4">
      <${Tabs}
        active=${active}
        onSelect=${setActive}
        items=${[
          { id: "log", label: "Event log" },
          { id: "issues", label: "Issues" },
        ]}
      />
      ${active === "issues" ? html`<${IssuePanel} />` : html`<${EventLog} />`}
    </div>
  `;
}

export const eventsView = {
  component: EventsView,
  id: "events",
  label: "Events",
};
