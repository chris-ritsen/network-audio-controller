import { html, useCallback, useEffect, useMemo, useRef, useState } from "./lib/preact.js";

const STORAGE_PREFIX = "netaudio.columns.";

function readLayout(tableId) {
  try {
    const raw = window.localStorage.getItem(`${STORAGE_PREFIX}${tableId}`);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw);
    if (!parsed || !Array.isArray(parsed.order) || !Array.isArray(parsed.hidden)) {
      return null;
    }
    return parsed;
  } catch (error) {
    return null;
  }
}

function writeLayout(tableId, layout) {
  try {
    window.localStorage.setItem(`${STORAGE_PREFIX}${tableId}`, JSON.stringify(layout));
  } catch (error) {
    return;
  }
}

function resolveOrder(columns, storedOrder) {
  const known = new Set(columns.map((column) => column.id));
  const ordered = (storedOrder || []).filter((id) => known.has(id));
  for (const column of columns) {
    if (!ordered.includes(column.id)) {
      ordered.push(column.id);
    }
  }
  return ordered;
}

function defaultLayout(columns) {
  return { hidden: columns.filter((column) => column.defaultHidden).map((column) => column.id), order: [] };
}

export function useColumnLayout(tableId, columns) {
  const [layout, setLayout] = useState(() => readLayout(tableId) || defaultLayout(columns));

  const order = useMemo(() => resolveOrder(columns, layout.order), [columns, layout.order]);
  const hidden = useMemo(() => new Set(layout.hidden), [layout.hidden]);

  const persist = useCallback(
    (next) => {
      setLayout(next);
      writeLayout(tableId, next);
    },
    [tableId],
  );

  const toggle = useCallback(
    (columnId) => {
      const nextHidden = hidden.has(columnId)
        ? [...hidden].filter((id) => id !== columnId)
        : [...hidden, columnId];
      persist({ hidden: nextHidden, order });
    },
    [hidden, order, persist],
  );

  const move = useCallback(
    (sourceId, targetId) => {
      if (sourceId === targetId) {
        return;
      }
      const next = order.filter((id) => id !== sourceId);
      const index = next.indexOf(targetId);
      next.splice(index < 0 ? next.length : index, 0, sourceId);
      persist({ hidden: [...hidden], order: next });
    },
    [hidden, order, persist],
  );

  const reset = useCallback(() => {
    try {
      window.localStorage.removeItem(`${STORAGE_PREFIX}${tableId}`);
    } catch (error) {
      setLayout(defaultLayout(columns));
    }
    setLayout(defaultLayout(columns));
  }, [columns, tableId]);

  const visible = useMemo(() => {
    const byId = new Map(columns.map((column) => [column.id, column]));
    return order.map((id) => byId.get(id)).filter((column) => column && !hidden.has(column.id));
  }, [columns, hidden, order]);

  return { hidden, move, order, reset, toggle, visible };
}

function ColumnMenu({ columns, layout }) {
  const [open, setOpen] = useState(false);
  const container = useRef(null);

  useEffect(() => {
    if (!open) {
      return undefined;
    }
    const dismiss = (event) => {
      if (container.current && !container.current.contains(event.target)) {
        setOpen(false);
      }
    };
    document.addEventListener("pointerdown", dismiss);
    return () => document.removeEventListener("pointerdown", dismiss);
  }, [open]);

  const byId = new Map(columns.map((column) => [column.id, column]));

  return html`
    <div class="menu" ref=${container}>
      <button type="button" class="btn btn-small" onClick=${() => setOpen(!open)}>
        Columns ${layout.visible.length}/${columns.length}
      </button>
      ${open
        ? html`
            <div class="menu-panel column-menu">
              ${layout.order.map((id) => {
                const column = byId.get(id);
                if (!column) {
                  return null;
                }
                return html`
                  <label key=${id} class="column-option">
                    <input
                      type="checkbox"
                      checked=${!layout.hidden.has(id)}
                      onChange=${() => layout.toggle(id)}
                    />
                    <span>${column.label}</span>
                  </label>
                `;
              })}
              <button type="button" class="menu-item" onClick=${layout.reset}>Reset to defaults</button>
            </div>
          `
        : null}
    </div>
  `;
}

export function ConfigurableTable({ columns, height, onRowClick, rowKey, rows, short, tableId, toolbar }) {
  const layout = useColumnLayout(tableId, columns);
  const dragged = useRef(null);
  const [dragTarget, setDragTarget] = useState(null);

  return html`
    <div class="stack">
      <div class="table-toolbar">
        ${toolbar}
        <div class="table-toolbar-spacer"></div>
        <${ColumnMenu} columns=${columns} layout=${layout} />
      </div>
      <div class="table-wrapper${short ? " short" : ""}" style=${height ? `max-height:${height}` : null}>
        <table class="data">
          <thead>
            <tr>
              ${layout.visible.map(
                (column) => html`
                  <th
                    key=${column.id}
                    draggable=${true}
                    class=${`draggable${dragTarget === column.id ? " drop-target" : ""}`}
                    title="Drag to reorder"
                    onDragStart=${() => {
                      dragged.current = column.id;
                    }}
                    onDragOver=${(event) => {
                      event.preventDefault();
                      if (dragTarget !== column.id) {
                        setDragTarget(column.id);
                      }
                    }}
                    onDragLeave=${() => setDragTarget(null)}
                    onDrop=${(event) => {
                      event.preventDefault();
                      if (dragged.current) {
                        layout.move(dragged.current, column.id);
                      }
                      dragged.current = null;
                      setDragTarget(null);
                    }}
                    onDragEnd=${() => {
                      dragged.current = null;
                      setDragTarget(null);
                    }}
                  >
                    ${column.label}
                  </th>
                `,
              )}
            </tr>
          </thead>
          <tbody>
            ${rows.map((row) => {
              const key = rowKey(row);
              return html`
                <tr
                  key=${key}
                  class=${onRowClick ? "selectable" : null}
                  onClick=${onRowClick ? () => onRowClick(row) : null}
                >
                  ${layout.visible.map(
                    (column) => html`<td key=${column.id} class=${column.align === "right" ? "numeric" : null}>
                      ${column.cell(row)}
                    </td>`,
                  )}
                </tr>
              `;
            })}
          </tbody>
        </table>
      </div>
    </div>
  `;
}
