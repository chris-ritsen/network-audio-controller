import { Icon } from "./icons.js";
import "./ui-preferences.js";
import { inventoryFilters, saveRoutingFilters } from "./device-filters.js";
import { DeviceFilterPanel } from "./filter-panel.js";
import { ContextSelector } from "./ddm-connections.js";
import { html, render, signal, useEffect, useLayoutEffect, useRef, useState } from "./lib/preact.js";
import { CommandPalette, openPalette } from "./palette.js";
import { devicePath, location, navigate, onNavigate, startRouter } from "./router.js";
import { matchesCommandKey } from "./shortcuts.js";
import { connect, contextDevices, deviceByName } from "./store.js";
import { deviceLabel } from "./format.js";
import { ddmView } from "./views/ddm.js";
import { DEVICE_TABS, devicesView, clockStatusView, networkStatusView } from "./views/devices.js";
import { NAVIGATION } from "./navigation.js";
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

const sidebarOpen = signal(false);

function viewById(identifier) {
  return VIEWS.find((view) => view.id === identifier) || routingView;
}

function Breadcrumb() {
  const current = location.value;
  const view = viewById(current.view);
  const deviceName = current.parameters.device;
  if (!deviceName) return null;
  const section = current.parameters.section;
  const sectionLabel = section === "domain" ? "Domain" : DEVICE_TABS.find((tab) => tab.id === section)?.label || "Unknown section";
  return html`
    <nav class="breadcrumb" aria-label="Breadcrumb">
      <a class="link link-hover" href=${`/${view.id}`}>${view.label}</a>
      ${deviceName
        ? html`<span class="breadcrumb-separator">/</span>
            <a class="link link-hover" href=${devicePath(view.id, deviceName)}>${deviceLabel(deviceByName(deviceName) || { name: deviceName })}</a>`
        : null}
      ${current.parameters.section
        ? html`<span class="breadcrumb-separator">/</span>
            <span aria-current="page">${sectionLabel}</span>`
        : null}
    </nav>
  `;
}

function TopBar() {
  return html`
    <header class="topbar">
      <button
        type="button"
        class="app-menu-trigger"
        aria-label=${sidebarOpen.value ? "Hide navigation" : "Show navigation"}
        aria-controls="application-navigation"
        aria-expanded=${sidebarOpen.value}
        onClick=${() => {
          sidebarOpen.value = !sidebarOpen.value;
        }}
      >
        <${Icon} name="menu" />
      </button>
      <div class="topbar-heading">
      <a class="brand" href="/routing">
        <svg class="brand-mark" viewBox="0 0 64 64" aria-hidden="true">
          <rect width="64" height="64" rx="13" fill="#ff2323" />
          <g fill="#000000">
            <rect x="10" y="36" width="8" height="16" rx="3" />
            <rect x="22" y="22" width="8" height="30" rx="3" />
            <rect x="34" y="30" width="8" height="22" rx="3" />
            <rect x="46" y="16" width="8" height="36" rx="3" />
          </g>
        </svg>
        <span class="brand-name">netaudio</span>
      </a>
      <${Breadcrumb} />
      </div>
      <div class="topbar-spacer"></div>
      <${ContextSelector} />
      <div class="topbar-controls">
        <button type="button" class="header-icon-button" aria-label="Search" title="Search" onClick=${openPalette}>
          <${Icon} name="search" />
        </button>
      </div>
    </header>
  `;
}

function NavigationItems() {
  const current = location.value;
  const entry = (view) => html`<a
    key=${view.id}
    class=${`app-menu-link${current.view === view.id ? " active" : ""}`}
    aria-current=${current.view === view.id ? "page" : null}
    aria-label=${view.label}
    href=${view.path} onClick=${() => { sidebarOpen.value = false; }}>
    <${Icon} name=${view.icon} /><span>${view.label}</span>
  </a>`;
  return html`<nav aria-label="Main navigation">
    ${NAVIGATION.filter((view) => view.group === "Tools").map(entry)}
  </nav>`;
}

function Sidebar() {
  const dialog = useRef(null);
  useLayoutEffect(() => {
    if (sidebarOpen.value && !dialog.current.matches(":popover-open")) {
      const button = document.querySelector(".app-menu-trigger").getBoundingClientRect();
      dialog.current.style.left = `${button.left}px`;
      dialog.current.style.top = `${button.bottom + 4}px`;
      dialog.current.showPopover();
      dialog.current.querySelector("a")?.focus();
    } else if (!sidebarOpen.value && dialog.current.matches(":popover-open")) dialog.current.hidePopover();
  }, [sidebarOpen.value]);
  return html`
    <div id="application-navigation" class="app-menu" role="dialog" popover="auto" aria-label="Application navigation" ref=${dialog}
      onToggle=${(event) => { sidebarOpen.value = event.newState === "open"; }}
      onKeyDown=${(event) => { if (event.key === "Escape") {
        event.preventDefault(); sidebarOpen.value = false; document.querySelector(".app-menu-trigger")?.focus();
      } }}>
      <${NavigationItems} />
    </div>
  `;
}

function NetworkNavigation({ filtersOpen }) {
  return html`<nav class="network-navigation" aria-label="Network views">
    <select class="select mobile-view-selector" aria-label="View" value=${NAVIGATION.find((view) => view.id === location.value.view)?.path || "/devices"}
      onChange=${(event) => navigate(event.target.value)}>
      ${NAVIGATION.map((view) => html`<option value=${view.path}>${view.label}</option>`)}
    </select>
    <button type="button" class="header-icon-button filter-panel-toggle"
      aria-label=${filtersOpen ? "Hide filters" : "Show filters"}
      title=${filtersOpen ? "Hide filters" : "Show filters"}
      aria-expanded=${filtersOpen} aria-controls="inventory-filters"
      onClick=${() => saveRoutingFilters({ ...inventoryFilters.value, panelOpen: !filtersOpen })}>
      <${Icon} name=${filtersOpen ? "sidebar-close" : "sidebar-open"} />
    </button>
    ${NAVIGATION.filter((view) => view.group === "Network").map((view) => html`<a href=${view.path}
      aria-current=${location.value.view === view.id ? "page" : null}>${view.label}</a>`)}
  </nav>`;
}

function Content({ filtersOpen }) {
  const current = location.value;
  const view = viewById(current.view);
  return html`<div class=${`app-workspace${filtersOpen ? " with-filters" : ""}`}>
    ${filtersOpen ? html`<div id="inventory-filters" class="routing-filter-container">
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
  const filtersOpen = inventoryFilters.value.panelOpen ?? !compact;
  return html`
    <${TopBar} />
    <${NetworkNavigation} filtersOpen=${filtersOpen} />
    <${Sidebar} />
    <${Content} filtersOpen=${filtersOpen} />
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

startRouter();
bindShortcuts();
onNavigate(() => {
  sidebarOpen.value = false;
});
render(html`<${App} />`, document.getElementById("root"));
connect();

if (!location.value.found) {
  navigate("/routing", { replace: true });
}
