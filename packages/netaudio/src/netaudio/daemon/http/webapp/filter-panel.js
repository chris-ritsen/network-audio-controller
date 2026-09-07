import { html } from "./lib/preact.js";
import { deviceFilterOptions, toggleDeviceFilter } from "./device-filters.js";

export function DeviceFilterPanel({ all, filters, onChange }) {
  return html`<aside class="routing-filter-panel" aria-label="Device filters">
    <div class="routing-filter-heading"><h2>Device filters</h2>
      <button type="button" class="btn btn-xs" onClick=${() => onChange({ ...filters, search: "", receiverSearch: "", transmitterSearch: "", values: {} })}>Clear all</button>
    </div>
    <label class="routing-filter-search">Search devices
      <input type="search" placeholder="Name, address, model…" value=${filters.search || ""}
        onInput=${(event) => onChange({ ...filters, search: event.target.value })} />
    </label>
    <p class="routing-filter-help">Filters device lists across views.</p>
    ${deviceFilterOptions(all, filters).map((group) => html`<details key=${group.id}
      open=${filters.expandedGroups?.includes(group.id) || false}
      onToggle=${(event) => {
        const expanded = filters.expandedGroups || [];
        const open = event.currentTarget.open;
        if (open === expanded.includes(group.id)) return;
        onChange({ ...filters, expandedGroups: open ? [...expanded, group.id] : expanded.filter((id) => id !== group.id) });
      }}>
      <summary title=${group.description}>${group.label}</summary>
      <div class="routing-filter-options">
        ${group.description ? html`<p class="text-xs text-muted">${group.description}</p>` : null}
        ${group.options.map(([value, count]) => html`<label key=${value}>
          <input type="checkbox" checked=${filters.values?.[group.id]?.includes(value) || false}
            onChange=${() => onChange(toggleDeviceFilter(filters, group.id, value))} />
          <span>${value}</span><span class="routing-filter-count">${count}</span>
        </label>`)}
      </div>
    </details>`)}
  </aside>`;
}
