import { html } from "./lib/preact.js";
import { DEVICE_FILTERS, deviceFilterOptions, toggleDeviceFilter } from "./device-filters.js";

export function DeviceFilterPanel({ all, filters, onChange }) {
  return html`<aside class="routing-filter-panel" aria-label="Device filters">
    <div class="routing-filter-heading"><h2>Device filters</h2>
      <button type="button" class="btn btn-xs" onClick=${() => onChange({ search: "", values: {} })}>Clear all</button>
    </div>
    <label class="routing-filter-search">Search devices
      <input type="search" placeholder="Name, address, model…" value=${filters.search || ""}
        onInput=${(event) => onChange({ ...filters, search: event.target.value })} />
    </label>
    <p class="routing-filter-help">Applies to both receivers and transmitters.</p>
    <${ActiveDeviceFilters} filters=${filters} onChange=${onChange} />
    ${deviceFilterOptions(all, filters).map((group) => html`<details key=${group.id} open>
      <summary>${group.label}${filters.values?.[group.id]?.length ? html`<span class="badge badge-sm">${filters.values[group.id].length}</span>` : null}</summary>
      <div class="routing-filter-options">
        ${group.options.map(([value, count]) => html`<label key=${value}>
          <input type="checkbox" checked=${filters.values?.[group.id]?.includes(value) || false}
            onChange=${() => onChange(toggleDeviceFilter(filters, group.id, value))} />
          <span>${value}</span><span class="routing-filter-count">${count}</span>
        </label>`)}
      </div>
    </details>`)}
  </aside>`;
}

export function ActiveDeviceFilters({ filters, onChange }) {
  if (!filters.search && !Object.values(filters.values || {}).some((values) => values.length)) return null;
  return html`<div class="routing-filter-chips" aria-label="Active device filters">
    ${filters.search ? html`<button type="button" class="btn btn-xs" aria-label="Remove device search"
      onClick=${() => onChange({ ...filters, search: "" })}>Search: ${filters.search} ×</button>` : null}
    ${DEVICE_FILTERS.flatMap((group) => (filters.values?.[group.id] || []).map((value) => html`
      <button type="button" class="btn btn-xs" aria-label=${`Remove ${group.label}: ${value}`}
        onClick=${() => onChange(toggleDeviceFilter(filters, group.id, value))}>${group.label}: ${value} ×</button>`))}
  </div>`;
}
