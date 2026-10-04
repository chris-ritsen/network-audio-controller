import { startColorScheme } from "./color-scheme.js";
import { Icon } from "./icons.js";
import "./ui-preferences.js";
import { inventoryFilters, saveRoutingFilters } from "./device-filters.js";
import { DeviceFilterPanel } from "./filter-panel.js";
import { ContextSelector } from "./ddm-connections.js";
import { html, render, useEffect, useRef, useState } from "./lib/preact.js";
import { CommandPalette, openPalette } from "./palette.js";
import { location, navigate, startRouter } from "./router.js";
import { matchesCommandKey } from "./shortcuts.js";
import { connect, contextDevices } from "./store.js";
import { ddmView } from "./views/ddm.js";
import { devicesView, clockStatusView, networkStatusView } from "./views/devices.js";
import { visibleNavigation } from "./navigation.js";
import { eventsView } from "./views/events.js";
import { routingView } from "./views/routing.js";
import { subscriptionsView } from "./views/subscriptions.js";
import { shureView } from "./views/shure.js";
import { settingsView } from "./views/settings.js";
import { presetsView } from "./views/presets.js";

const VIEWS = [
  devicesView,
  clockStatusView,
  networkStatusView,
  routingView,
  subscriptionsView,
  presetsView,
  ddmView,
  shureView,
  eventsView,
  settingsView,
];

function viewById(identifier) {
  return VIEWS.find((view) => view.id === identifier) || routingView;
}

function ViewTabs() {
  return html`<nav class="view-tabs" aria-label="Views">
    ${visibleNavigation.value.map((view) => html`<a key=${view.id} href=${view.path}
      aria-current=${location.value.view === view.id ? "page" : null}>${view.tab || view.label}</a>`)}
  </nav>`;
}

function ViewPicker() {
  const pointerSelection = useRef(false);
  return html`<select class="select view-picker" aria-label="View" value=${visibleNavigation.value.find((view) => view.id === location.value.view)?.path || "/devices"}
    onPointerDown=${() => { pointerSelection.current = true; }}
    onKeyDown=${() => { pointerSelection.current = false; }}
    onChange=${(event) => {
      navigate(event.currentTarget.value);
      if (pointerSelection.current) {
        event.currentTarget.blur();
        pointerSelection.current = false;
      }
    }}>
    ${visibleNavigation.value.map((view) => html`<option value=${view.path}>${view.label}</option>`)}
  </select>`;
}

function TopBar({ compact, filtersAvailable, filtersOpen }) {
  const panel = compact ? "top-panel" : "sidebar";
  return html`
    <header class="topbar">
      <a class="brand" href="/routing" aria-label="netaudio">
        <svg class="brand-mark" viewBox="0 0 64 64" aria-hidden="true">
          <rect width="64" height="64" rx="13" fill="#ff2323" />
          <g fill="#000000">
            <rect x="10" y="36" width="8" height="16" rx="3" />
            <rect x="22" y="22" width="8" height="30" rx="3" />
            <rect x="34" y="30" width="8" height="22" rx="3" />
            <rect x="46" y="16" width="8" height="36" rx="3" />
          </g>
        </svg>
        ${compact ? null : html`<span class="brand-name" aria-hidden="true">netaudio</span>`}
      </a>
      ${compact ? html`<${ViewPicker} />` : html`<${ViewTabs} />`}
      ${compact ? null : html`<${ContextSelector} />`}
      <div class="topbar-controls">
        ${filtersAvailable ? html`<button type="button" class="header-icon-button filter-panel-toggle"
          aria-label=${filtersOpen ? "Hide filters" : "Show filters"}
          title=${filtersOpen ? "Hide filters" : "Show filters"}
          aria-expanded=${filtersOpen} aria-controls="inventory-filters"
          onClick=${() => saveRoutingFilters({ ...inventoryFilters.value, panelOpen: !filtersOpen })}>
          <${Icon} name=${`${panel}-${filtersOpen ? "close" : "open"}`} />
        </button>` : null}
        <button type="button" class="header-icon-button" aria-label="Search" title="Search" onClick=${openPalette}>
          <${Icon} name="search" />
        </button>
      </div>
    </header>
  `;
}

function Content({ compact, filtersOpen }) {
  const current = location.value;
  const view = viewById(current.view);
  return html`<div class=${`app-workspace${filtersOpen ? " with-filters" : ""}`}>
    ${filtersOpen ? html`<div id="inventory-filters" class="routing-filter-container">
      ${compact ? html`<${ContextSelector} />` : null}
      <${DeviceFilterPanel} all=${Object.values(contextDevices.value)} filters=${inventoryFilters.value} onChange=${saveRoutingFilters} />
    </div>` : null}
    <main class=${`content${current.parameters.device ? " selectable-content" : ""}`} id="content">
    <${view.component} location=${current} />
  </main></div>`;
}

function App() {
  const [compact, setCompact] = useState(() => window.matchMedia("(max-width: 900px)").matches);
  useEffect(() => {
    const media = window.matchMedia("(max-width: 900px)");
    const update = () => setCompact(media.matches);
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  const filtersAvailable = viewById(location.value.view).filters !== false;
  const filtersOpen = filtersAvailable && (inventoryFilters.value.panelOpen ?? !compact);
  return html`
    <${TopBar} compact=${compact} filtersAvailable=${filtersAvailable} filtersOpen=${filtersOpen} />
    <${Content} compact=${compact} filtersOpen=${filtersOpen} />
    <${CommandPalette} />
  `;
}

function bindShortcuts() {
  document.addEventListener("keydown", (event) => {
    if (matchesCommandKey(event) && event.key.toLowerCase() === "k") {
      event.preventDefault();
      openPalette();
    }
  });
}

startColorScheme();
startRouter();
bindShortcuts();
render(html`<${App} />`, document.getElementById("root"));
connect();

if (!location.value.found) {
  navigate("/routing", { replace: true });
}
