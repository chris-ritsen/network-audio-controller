import { Fragment, cloneElement, createContext, html, toChildArray, useCallback, useRef, useSignal, useState } from "./lib/preact.js";
import { runAction } from "./actions.js";

export const PanelHeaderControls = createContext(null);

function HeaderActions({ actions, controls }) {
  return html`<div class="ml-auto flex flex-wrap items-center gap-2">${actions}${controls.value}</div>`;
}

export function Panel({ actions, children, headerActions, title, wide }) {
  const controls = useSignal(null);
  return html`
    <${PanelHeaderControls.Provider} value=${title == null ? null : controls}>
    <section class="card bg-base-100 border border-base-300 min-w-0${wide ? " wide" : ""}">
      ${title === undefined || title === null
        ? null
        : html`<header class="flex flex-wrap items-center gap-3 border-b border-base-300 px-4 py-3">
            <h2 class="card-title text-base">${title}</h2>
            <${HeaderActions} actions=${headerActions} controls=${controls} />
          </header>`}
      <div class="card-body block min-[901px]:overflow-x-auto p-4 space-y-3">
        ${actions ? html`<div class="flex flex-wrap gap-2 mb-4">${actions}</div>` : null}
        ${children}
      </div>
    </section>
    <//>
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

export function DataTable({ headers, rows, numericColumns = [] }) {
  return html`
    <div class="table-wrapper">
      <table class="data">
        <thead>
          <tr>
            ${headers.map((header, index) => html`<th key=${index} class=${numericColumns.includes(index) ? "numeric" : ""}>${header}</th>`)}
          </tr>
        </thead>
        <tbody>
          ${toChildArray(rows).map((row) => row?.type === "tr" ? cloneElement(row, {},
            toChildArray(row.props.children).map((cell, index) => cell?.type === "td" ? cloneElement(cell, { "data-label": headers[index] || "" }) : cell)
          ) : row)}
        </tbody>
      </table>
    </div>
  `;
}

export function Tabs({ active, items, onSelect }) {
  return html`
    <nav class="tabs tabs-border mb-4">
      ${items.map(
        (item) => html`
          <button
            key=${item.id}
            type="button"
            class="tab min-h-11${item.id === active ? " tab-active" : ""}"
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
  const [error, setError] = useState("");
  const run = useCallback(async () => {
    setError("");
    setPending(true);
    try {
      const outcome = await runAction(description, onRun);
      if (!outcome.ok) setError(outcome.error.message);
    } finally {
      setPending(false);
    }
  }, [description, onRun]);
  const classes = ["btn", small ? "btn-xs" : "btn-sm"];
  if (variant) {
    classes.push(variant === "danger" ? "btn-error" : "btn-primary");
  }
  if (pending) {
    classes.push("pending");
  }
  return html`
    <button type="button" class=${classes.join(" ")} disabled=${pending || disabled} aria-busy=${pending ? "true" : null} title=${title} onClick=${run}>
      ${children}
    </button>
    ${error ? html`<span class="text-error text-sm" role="alert">${error}</span>` : null}
  `;
}

export function Button({ children, onClick, small, title, variant, disabled }) {
  const classes = ["btn", small ? "btn-xs" : "btn-sm"];
  if (variant) {
    classes.push(variant === "danger" ? "btn-error" : "btn-primary");
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
    return html`<span>${value.length ? value.map((entry, index) => html`<${Fragment} key=${index}>${index ? ", " : ""}<${Value} value=${entry} /><//>`) : "—"}</span>`;
  }
  const summary = [value.label, value.summary, value.name].find((entry) => typeof entry === "string" && entry.length);
  return html`<span>${summary || "—"}</span>`;
}
