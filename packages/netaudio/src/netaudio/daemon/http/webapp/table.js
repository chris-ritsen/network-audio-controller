import { html, useCallback, useContext, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "./lib/preact.js";
import { PanelHeaderControls } from "./components.js";
import { Icon } from "./icons.js";
import { sortRows } from "./table-sort.js";
import { navigate } from "./router.js";

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
    return order.map((id) => byId.get(id)).filter((column) => column && (column.configurable === false || !hidden.has(column.id)));
  }, [columns, hidden, order]);

  return { hidden, move, order, reset, toggle, visible };
}

function ColumnMenu({ columns, layout }) {
  const menuId = useId();
  const selectable = columns
    .filter((column) => column.configurable !== false && column.label)
    .sort((first, second) => first.label.localeCompare(second.label, undefined, { sensitivity: "base", numeric: true }));
  const [open, setOpen] = useState(false);
  const trigger = useRef(null);
  const panel = useRef(null);
  const position = useCallback(() => {
    if (!trigger.current || !panel.current) return;
    const anchor = trigger.current.getBoundingClientRect();
    const margin = 8;
    const gap = 6;
    const width = Math.min(280, window.innerWidth - margin * 2);
    const below = window.innerHeight - anchor.bottom - margin - gap;
    const above = anchor.top - margin - gap;
    const upwards = below < 200 && above > below;
    const menu = panel.current;
    menu.style.width = `${width}px`;
    menu.style.maxHeight = `${Math.max(40, Math.min(480, upwards ? above : below))}px`;
    menu.style.left = `${Math.max(margin, Math.min(anchor.right - width, window.innerWidth - width - margin))}px`;
    const height = menu.getBoundingClientRect().height;
    const desiredTop = upwards ? anchor.top - gap - height : anchor.bottom + gap;
    menu.style.top = `${Math.max(margin, Math.min(desiredTop, window.innerHeight - height - margin))}px`;
  }, []);

  useEffect(() => {
    if (!open) return;
    position();
    window.addEventListener("resize", position);
    window.addEventListener("scroll", position, true);
    return () => {
      window.removeEventListener("resize", position);
      window.removeEventListener("scroll", position, true);
    };
  }, [open, position]);

  const byId = new Map(selectable.map((column) => [column.id, column]));

  return html`
    <div class="menu">
      <button ref=${trigger} type="button" class="btn btn-sm" aria-expanded=${open}
        popovertarget=${menuId} popovertargetaction="toggle" onClick=${(event) => {
          event.preventDefault();
          panel.current?.togglePopover();
          position();
        }}>
        <span class="column-menu-desktop-label">Columns ${layout.visible.filter((column) => byId.has(column.id)).length}/${selectable.length}</span>
        <span class="column-menu-mobile-label">Fields</span>
      </button>
            <div id=${menuId} ref=${panel} popover="auto" class="menu-panel column-menu" onToggle=${(event) => setOpen(event.newState === "open")}>
              ${selectable.map((column) => {
                const id = column.id;
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
    </div>
  `;
}

export function ConfigurableTable({ columns, mobileSummary, rowHref, rowKey, rows, tableId, toolbar, toolbarActions }) {
  const layout = useColumnLayout(tableId, columns);
  const headerControls = useContext(PanelHeaderControls);
  useLayoutEffect(() => {
    if (!headerControls) return;
    headerControls.value = html`<${ColumnMenu} columns=${columns} layout=${layout} />`;
    return () => { headerControls.value = null; };
  }, [headerControls, columns, layout]);
  const dragged = useRef(null);
  const [dragTarget, setDragTarget] = useState(null);
  const [sort, setSort] = useState(null);
  const [expanded, setExpanded] = useState(() => new Set());
  const sortedRows = sortRows(rows, columns.find((column) => column.id === sort?.id), sort?.direction);

  return html`
    <div class="flex flex-col gap-4">
      <div class=${`table-toolbar${!toolbar && !toolbarActions && headerControls ? " table-toolbar-mobile-only" : ""}`}>
        ${toolbar ? html`<div class="table-filter-controls">${toolbar}</div>` : null}
        <div class="table-display-controls">
        <label class="mobile-table-sort"><span class="sr-only">Sort by</span>
          <select aria-label="Sort by" value=${sort?.id || ""} onChange=${(event) => setSort(event.target.value ? { id: event.target.value, direction: sort?.direction || "ascending" } : null)}>
            <option value="">Sort: default</option>
            ${layout.visible.filter((column) => column.label && column.sortable !== false)
              .sort((first, second) => first.label.localeCompare(second.label, undefined, { numeric: true, sensitivity: "base" }))
              .map((column) => html`<option value=${column.id}>${column.label}</option>`)}
          </select>
        </label>
        ${sort ? html`<button type="button" class="btn btn-sm mobile-table-order" aria-label=${sort.direction === "ascending" ? "Sort descending" : "Sort ascending"}
          onClick=${() => setSort({ ...sort, direction: sort.direction === "ascending" ? "descending" : "ascending" })}><${Icon} name=${sort.direction === "ascending" ? "sort-up" : "sort-down"} /></button>` : null}
        ${toolbarActions}
        ${headerControls ? null : html`<${ColumnMenu} columns=${columns} layout=${layout} />`}
        </div>
      </div>
      <div class="table-wrapper">
        <table class="data">
          <thead>
            <tr>
              ${layout.visible.map(
                (column) => html`
                  <th
                    key=${column.id}
                    scope="col"
                    aria-sort=${sort?.id === column.id ? sort.direction : null}
                    draggable=${true}
                    class=${`draggable${column.align === "right" ? " numeric" : ""}${column.id === "number" ? " channel-number" : ""}${dragTarget === column.id ? " drop-target" : ""}`}
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
                    ${column.label && column.sortable !== false ? html`
                      <button type="button" class="inline-flex items-center gap-2 bg-transparent border-0 p-0 text-inherit font-inherit cursor-pointer"
                        onClick=${() => setSort({ id: column.id, direction: sort?.id === column.id && sort.direction === "ascending" ? "descending" : "ascending" })}>
                        ${column.label}<${Icon} name=${sort?.id === column.id ? (sort.direction === "ascending" ? "sort-up" : "sort-down") : "sort"} />
                      </button>` : column.label}
                  </th>
                `,
              )}
            </tr>
          </thead>
          <tbody>
            ${sortedRows.map((row) => {
              const key = rowKey(row);
              const summary = mobileSummary?.(row);
              const isExpanded = expanded.has(key);
              return html`
                <tr
                  key=${key}
                  class=${`${rowHref ? "cursor-pointer" : ""}${summary ? " expandable-row" : ""}${isExpanded ? " expanded" : ""}`}
                  onClick=${rowHref ? (event) => {
                    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
                    if (event.target.closest("a, button, input, select, textarea, summary, [role=button]")) return;
                    if (window.getSelection()?.toString()) return;
                    navigate(rowHref(row));
                  } : undefined}
                >
                  ${summary ? html`<td class="mobile-card-heading" data-label="" colspan=${layout.visible.length || 1}>
                    <button type="button" class="mobile-card-toggle" aria-expanded=${isExpanded}
                      onClick=${() => setExpanded((current) => {
                        const next = new Set(current);
                        if (next.has(key)) next.delete(key);
                        else next.add(key);
                        return next;
                      })}>
                      <span class="mobile-card-summary">
                        <span class="mobile-card-title">${summary.title}</span>
                        ${summary.detail ? html`<span class="mobile-card-detail">${summary.detail}</span>` : null}
                      </span>
                      <span class="mobile-card-chevron" aria-hidden="true">${isExpanded ? "−" : "+"}</span>
                    </button>
                  </td>` : null}
                  ${layout.visible.map(
                    (column) => html`<td key=${column.id} data-label=${column.label || ""} class=${`${column.align === "right" ? "numeric" : ""}${column.id === "number" ? " channel-number" : ""}${column.id === "status" || column.id.endsWith("-status") ? " status-cell" : ""}`}>
                      ${rowHref && column.id === (layout.visible.find((entry) => entry.id === "name" || entry.id === "device") || layout.visible[0]).id
                        ? html`<a class="device-table-link" href=${rowHref(row)}><${Icon} name="devices" />${column.cell(row)}</a>`
                        : column.cell(row)}
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
