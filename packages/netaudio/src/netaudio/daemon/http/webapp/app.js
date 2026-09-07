import { Icon } from "./icons.js";
import { ContextSelector } from "./ddm-connections.js";
import { Button } from "./components.js";
import { html, render, signal, useLayoutEffect } from "./lib/preact.js";
import { CommandPalette, openPalette } from "./palette.js";
import { devicePath, location, navigate, onNavigate, startRouter } from "./router.js";
import { matchesCommandKey } from "./shortcuts.js";
import { connect } from "./store.js";
import { ddmView } from "./views/ddm.js";
import { DEVICE_TABS, devicesView } from "./views/devices.js";
import { eventsView } from "./views/events.js";
import { routingView } from "./views/routing.js";
import { subscriptionsView } from "./views/subscriptions.js";
import { shureView } from "./views/shure.js";
import { settingsView } from "./views/settings.js";
import { presetsView } from "./views/presets.js";

const VIEWS = [
  devicesView,
  routingView,
  subscriptionsView,
  presetsView,
  ddmView,
  shureView,
  eventsView,
  settingsView,
];

function readSidebarOpen() {
  try {
    const saved = window.localStorage.getItem("netaudio.sidebar.open");
    if (saved === "true" || saved === "false") return saved === "true";
  } catch {}
  return window.innerWidth > 900;
}

const sidebarOpen = signal(readSidebarOpen());

function viewById(identifier) {
  return VIEWS.find((view) => view.id === identifier) || routingView;
}

function Breadcrumb() {
  const current = location.value;
  const view = viewById(current.view);
  const deviceName = current.parameters.device;
  const section = current.parameters.section;
  const sectionLabel = section === "domain" ? "Domain" : DEVICE_TABS.find((tab) => tab.id === section)?.label || "Unknown section";
  return html`
    <nav class="breadcrumb" aria-label="Breadcrumb">
      <a class="link link-hover" href=${`/${view.id}`}>${view.label}</a>
      ${deviceName
        ? html`<span class="breadcrumb-separator">/</span>
            <a class="link link-hover" href=${devicePath(view.id, deviceName)}>${deviceName}</a>`
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
        class="header-icon-button"
        aria-label=${sidebarOpen.value ? "Hide navigation" : "Show navigation"}
        aria-controls="navigation-sidebar"
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
    class=${`btn btn-sm justify-start${current.view === view.id ? " btn-active" : ""}`}
    aria-current=${current.view === view.id ? "page" : null}
    aria-label=${view.label}
    href=${`/${view.id}`}>
    <${Icon} name=${view.id} /><span>${view.label}</span>
  </a>`;
  return html`<nav aria-label="Main navigation" class="flex flex-col gap-1 p-2">
    ${VIEWS.map(entry)}
  </nav>`;
}

function Sidebar() {
  return html`
    <aside id="navigation-sidebar" class=${`sidebar${sidebarOpen.value ? " open" : ""}`} inert=${!sidebarOpen.value}>
      <div class="sidebar-scroll">
        <${NavigationItems} />
      </div>
    </aside>
  `;
}

function SidebarScrim() {
  if (!sidebarOpen.value) {
    return null;
  }
  return html`<div
    class="sidebar-scrim"
    onClick=${() => {
      sidebarOpen.value = false;
    }}
  ></div>`;
}

function Content() {
  const current = location.value;
  const view = viewById(current.view);
  return html`<main class="content" id="content">
    <${view.component} location=${current} />
  </main>`;
}

function App() {
  useLayoutEffect(() => {
    document.getElementById("root").classList.toggle("navigation-collapsed", !sidebarOpen.value);
    try { window.localStorage.setItem("netaudio.sidebar.open", String(sidebarOpen.value)); } catch {}
  }, [sidebarOpen.value]);
  return html`
    <${TopBar} />
    <${Sidebar} />
    <${SidebarScrim} />
    <${Content} />
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
  if (window.innerWidth <= 900) sidebarOpen.value = false;
});
render(html`<${App} />`, document.getElementById("root"));
connect();

if (!location.value.found) {
  navigate("/routing", { replace: true });
}
