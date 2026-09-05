import { api } from "./api.js";
import { Button } from "./components.js";
import * as format from "./format.js";
import { html, render, signal, useEffect, useRef, useState } from "./lib/preact.js";
import { CommandPalette, openPalette } from "./palette.js";
import { devicePath, location, navigate, onNavigate, startRouter } from "./router.js";
import { matchesCommandKey, shortcutLabel } from "./shortcuts.js";
import { connect, connectionState, devices, shureDevices } from "./store.js";
import { clockStatusView } from "./views/clock-status.js";
import { ddmView } from "./views/ddm.js";
import { devicesView } from "./views/devices.js";
import { eventsView } from "./views/events.js";
import { flowsView } from "./views/flows.js";
import { meteringView } from "./views/metering.js";
import { networkStatusView } from "./views/network-status.js";
import { routingView } from "./views/routing.js";
import { shureView } from "./views/shure.js";

const VIEWS = [
  routingView,
  devicesView,
  clockStatusView,
  networkStatusView,
  meteringView,
  flowsView,
  ddmView,
  shureView,
  eventsView,
];
const DEVICE_AWARE_VIEWS = new Set(["devices", "flows", "metering"]);

const sidebarOpen = signal(false);

function viewById(identifier) {
  return VIEWS.find((view) => view.id === identifier) || VIEWS[0];
}

function Breadcrumb() {
  const current = location.value;
  const view = viewById(current.view);
  const deviceName = current.parameters.device;
  return html`
    <div class="breadcrumb">
      <span>${view.label}</span>
      ${deviceName
        ? html`<span class="breadcrumb-separator">/</span>
            <span>${deviceName}</span>`
        : null}
      ${current.parameters.section
        ? html`<span class="breadcrumb-separator">/</span>
            <span>${current.parameters.section}</span>`
        : null}
    </div>
  `;
}

function ConnectionPill() {
  const state = connectionState.value;
  return html`
    <span class="connection-pill ${state}" title="Daemon event stream">
      <span class="status-dot${state === "open" ? " online" : ""}"></span>
      ${state === "open" ? "live" : state}
    </span>
  `;
}

function GlobalMenu() {
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

  return html`
    <div class="menu" ref=${container}>
      <${Button} onClick=${() => setOpen(!open)} title="Daemon actions">Actions<//>
      ${open
        ? html`
            <div class="menu-panel" role="menu">
              <${MenuAction} label="Refresh all devices" description="refresh all devices" onRun=${() => api.refresh(null)} onDone=${() => setOpen(false)} />
              <${MenuAction} label="Forget offline devices" description="forget offline devices" onRun=${() => api.forgetDevices("offline")} onDone=${() => setOpen(false)} />
              <${MenuAction} label="Forget emulated devices" description="forget emulated devices" onRun=${() => api.forgetDevices("emulated")} onDone=${() => setOpen(false)} />
              <${MenuAction} label="Refresh managed inventory" description="refresh managed inventory" onRun=${() => api.managedRefresh(null)} onDone=${() => setOpen(false)} />
              <${MenuAction} label="Shut down daemon" description="shut down daemon" danger onRun=${() => api.shutdown()} onDone=${() => setOpen(false)} />
            </div>
          `
        : null}
    </div>
  `;
}

function MenuAction({ danger, description, label, onDone, onRun }) {
  return html`
    <button
      type="button"
      role="menuitem"
      class="menu-item${danger ? " danger" : ""}"
      onClick=${async () => {
        onDone();
        const { runAction } = await import("./toast.js");
        await runAction(description, onRun);
      }}
    >
      ${label}
    </button>
  `;
}

function TopBar() {
  return html`
    <header class="topbar">
      <button
        type="button"
        class="menu-toggle"
        aria-label="Toggle navigation"
        aria-expanded=${sidebarOpen.value}
        onClick=${() => {
          sidebarOpen.value = !sidebarOpen.value;
        }}
      >
        <span class="menu-toggle-bar"></span>
        <span class="menu-toggle-bar"></span>
        <span class="menu-toggle-bar"></span>
      </button>
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
        netaudio
      </a>
      <${Breadcrumb} />
      <div class="topbar-spacer"></div>
      <div class="topbar-controls">
        <button type="button" class="search-trigger" onClick=${openPalette}>
          <span>Jump to device or view</span>
          <kbd>${shortcutLabel("K")}</kbd>
        </button>
        <${ConnectionPill} />
        <${GlobalMenu} />
      </div>
    </header>
  `;
}

function NavigationItems() {
  const current = location.value;
  const deviceCount = Object.keys(devices.value).length;
  const shureCount = Object.keys(shureDevices.value).length;
  const counts = { devices: deviceCount, shure: shureCount };
  return html`
    <nav class="nav-list">
      ${VIEWS.map(
        (view) => html`
          <a
            key=${view.id}
            class="nav-item${current.view === view.id ? " active" : ""}"
            aria-current=${current.view === view.id ? "page" : null}
            href=${`/${view.id}`}
          >
            <span>${view.label}</span>
            ${counts[view.id] === undefined ? null : html`<span class="nav-count">${counts[view.id]}</span>`}
          </a>
        `,
      )}
    </nav>
  `;
}

function DeviceList({ filter }) {
  const current = location.value;
  const view = DEVICE_AWARE_VIEWS.has(current.view) ? current.view : "devices";
  const section = current.parameters.section;
  const needle = filter.trim().toLowerCase();
  const visible = format
    .sortedDevices(devices.value)
    .filter((device) => !needle || format.deviceHaystack(device).includes(needle));

  if (!visible.length) {
    return html`<div class="sidebar-empty">${needle ? "No device matches this filter." : "No devices discovered."}</div>`;
  }

  return html`
    <div class="device-list">
      ${visible.map((device) => {
        const name = format.deviceLabel(device);
        const active = current.parameters.device === name;
        return html`
          <a
            key=${device.server_name || name}
            class="device-item${active ? " active" : ""}"
            href=${devicePath(view, name, view === "devices" ? section || "receive" : undefined)}
          >
            <span class="status-dot${device.online ? " online" : ""}"></span>
            <span>
              <span class="device-item-name">${name}</span>
              <span class="device-item-detail">${format.deviceSummaryLine(device)}</span>
            </span>
          </a>
        `;
      })}
    </div>
  `;
}

function Sidebar() {
  const [filter, setFilter] = useState("");
  return html`
    <aside class="sidebar${sidebarOpen.value ? " open" : ""}">
      <div class="sidebar-scroll">
        <${NavigationItems} />
        <div class="sidebar-heading">
          <span>Devices</span>
          <span class="nav-count">${Object.keys(devices.value).length}</span>
        </div>
        <div class="sidebar-search">
          <input
            type="search"
            class="input-wide"
            placeholder="Filter devices"
            value=${filter}
            onInput=${(event) => setFilter(event.target.value)}
          />
        </div>
        <${DeviceList} filter=${filter} />
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
  sidebarOpen.value = false;
});
render(html`<${App} />`, document.getElementById("root"));
connect();

if (!location.value.found) {
  navigate("/routing", { replace: true });
}
