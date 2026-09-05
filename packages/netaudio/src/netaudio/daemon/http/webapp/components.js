import { Fragment, html, useCallback, useRef, useState } from "./lib/preact.js";
import { runAction } from "./toast.js";

export function Panel({ actions, children, title, wide }) {
  return html`
    <section class="panel${wide ? " wide" : ""}">
      ${title === undefined || title === null
        ? null
        : html`<header class="panel-header">
            <h2 class="panel-title">${title}</h2>
            ${actions ? html`<div class="panel-header-actions">${actions}</div>` : null}
          </header>`}
      <div class="panel-body">${children}</div>
    </section>
  `;
}

export function Toolbar({ children }) {
  return html`<div class="toolbar">${children}</div>`;
}

export function FieldRow({ children, label }) {
  return html`
    <div class="field-row">
      <div class="field-row-label">${label}</div>
      <div class="toolbar">${children}</div>
    </div>
  `;
}

export function Fields({ entries }) {
  return html`
    <dl class="fields">
      ${entries
        .filter(([, value]) => value !== undefined)
        .map(
          ([term, value]) => html`
            <${Fragment} key=${term}>
              <dt>${term}</dt>
              <dd>${value}</dd>
            <//>
          `,
        )}
    </dl>
  `;
}

export function Pill({ children, tone }) {
  return html`<span class="pill${tone ? ` ${tone}` : ""}">${children}</span>`;
}

export function StatusDot({ online }) {
  return html`<span class="status-dot${online ? " online" : ""}" aria-hidden="true"></span>`;
}

export function OnlineState({ online }) {
  return html`<span class="state-inline">
    <span class="status-dot${online ? " online" : ""}" aria-hidden="true"></span>
    ${online ? "online" : "offline"}
  </span>`;
}

export function Notice({ children }) {
  return html`<div class="notice">${children}</div>`;
}

export function Metric({ label, value }) {
  return html`
    <div class="metric">
      <div class="metric-label">${label}</div>
      <div class="metric-value">${value}</div>
    </div>
  `;
}

export function DataTable({ headers, rows, height, short }) {
  return html`
    <div class="table-wrapper${short ? " short" : ""}" style=${height ? `max-height:${height}` : null}>
      <table class="data">
        <thead>
          <tr>
            ${headers.map((header, index) => html`<th key=${index}>${header}</th>`)}
          </tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
      </table>
    </div>
  `;
}

export function Tabs({ active, items, onSelect }) {
  return html`
    <nav class="tabs">
      ${items.map(
        (item) => html`
          <button
            key=${item.id}
            type="button"
            class="tab${item.id === active ? " active" : ""}"
            aria-current=${item.id === active ? "page" : null}
            onClick=${() => onSelect(item.id)}
          >
            ${item.label}
          </button>
        `,
      )}
    </nav>
  `;
}

export function AsyncButton({ children, description, disabled, onRun, small, title, variant }) {
  const [pending, setPending] = useState(false);
  const run = useCallback(async () => {
    setPending(true);
    try {
      await runAction(description, onRun);
    } finally {
      setPending(false);
    }
  }, [description, onRun]);
  const classes = ["btn"];
  if (variant) {
    classes.push(`btn-${variant}`);
  }
  if (small) {
    classes.push("btn-small");
  }
  if (pending) {
    classes.push("pending");
  }
  return html`
    <button type="button" class=${classes.join(" ")} disabled=${pending || disabled} title=${title} onClick=${run}>
      ${children}
    </button>
  `;
}

export function Button({ children, onClick, small, title, variant, disabled }) {
  const classes = ["btn"];
  if (variant) {
    classes.push(`btn-${variant}`);
  }
  if (small) {
    classes.push("btn-small");
  }
  return html`
    <button type="button" class=${classes.join(" ")} disabled=${disabled} title=${title} onClick=${onClick}>
      ${children}
    </button>
  `;
}

export function useValueReference(initialValue) {
  const reference = useRef(null);
  const read = useCallback(() => (reference.current ? reference.current.value : initialValue), [initialValue]);
  return [reference, read];
}

export function Disclosure({ children, summary }) {
  return html`<details>
    <summary>${summary}</summary>
    ${children}
  </details>`;
}

export function humanizeKey(key) {
  const spaced = String(key).replace(/[_-]+/g, " ").trim();
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

function isPlainValue(value) {
  return value === null || value === undefined || typeof value !== "object";
}

export function Value({ value }) {
  if (value === null || value === undefined || value === "") {
    return html`<span>—</span>`;
  }
  if (typeof value === "boolean") {
    return html`<span>${value ? "yes" : "no"}</span>`;
  }
  if (typeof value !== "object") {
    return html`<span>${String(value)}</span>`;
  }
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return html`<span>—</span>`;
    }
    if (value.every(isPlainValue)) {
      return html`<span>${value.map((entry) => (entry === null ? "—" : String(entry))).join(", ")}</span>`;
    }
    const columns = [...new Set(value.flatMap((entry) => (entry && typeof entry === "object" ? Object.keys(entry) : [])))];
    return html`
      <div class="table-wrapper short">
        <table class="data">
          <thead>
            <tr>
              ${columns.map((column) => html`<th key=${column}>${humanizeKey(column)}</th>`)}
            </tr>
          </thead>
          <tbody>
            ${value.map(
              (entry, index) => html`
                <tr key=${index}>
                  ${columns.map(
                    (column) => html`<td key=${column}><${Value} value=${entry ? entry[column] : null} /></td>`,
                  )}
                </tr>
              `,
            )}
          </tbody>
        </table>
      </div>
    `;
  }
  const entries = Object.entries(value);
  if (entries.length === 0) {
    return html`<span>—</span>`;
  }
  return html`
    <dl class="fields nested">
      ${entries.map(
        ([key, nested]) => html`
          <${Fragment} key=${key}>
            <dt>${humanizeKey(key)}</dt>
            <dd><${Value} value=${nested} /></dd>
          <//>
        `,
      )}
    </dl>
  `;
}
