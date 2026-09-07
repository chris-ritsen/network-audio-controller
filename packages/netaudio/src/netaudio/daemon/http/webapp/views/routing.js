import { Button, Notice } from "../components.js";
import * as format from "../format.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { RoutingControls } from "../routing-controls.js";
import { Icon } from "../icons.js";
import { buildMatrixModel, expanded, ExpansionButtons, RoutingMatrix, setAllExpanded } from "../matrix.js";
import { channelGroups, enableChannelGroups, groupChannels, setGroupsExpanded } from "../channel-groups.js";
import { devicePath, navigate } from "../router.js";
import { deviceRequestName, scopedDevices as devices } from "../store.js";
import { matchesDeviceFilters } from "../device-filters.js";
import { DeviceFilterPanel } from "../filter-panel.js";

function RoutingView() {
  const all = format.sortedDevices(devices.value);
  const [receiverFilter, setReceiverFilter] = useState("");
  const [transmitterFilter, setTransmitterFilter] = useState("");
  const [compact, setCompact] = useState(() => window.matchMedia("(max-width: 900px)").matches);
  const [listMode, setListMode] = useState(false);
  const [filters, setFilters] = useState({ search: "", values: {} });
  const [filtersOpen, setFiltersOpen] = useState(() => !window.matchMedia("(max-width: 900px)").matches);
  const [flipped, setFlipped] = useState(() => {
    try { return window.localStorage.getItem("netaudio.matrix.flipped") === "true"; }
    catch { return false; }
  });
  useEffect(() => {
    const media = window.matchMedia("(max-width: 900px)");
    const update = () => setCompact(media.matches);
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  const state = expanded.value;
  const groups = channelGroups.value;

  if (!all.length) {
    return html`<${Notice}>No Dante devices have been discovered yet.<//>`;
  }

  const visible = all.filter((device) => matchesDeviceFilters(device, filters));
  const filteredDevices = Object.fromEntries(Object.entries(devices.value).filter(([, device]) => matchesDeviceFilters(device, filters)));
  const showList = compact || listMode;
  const model = buildMatrixModel({
    devices: filteredDevices,
    expandedReceivers: state.receivers,
    expandedTransmitters: state.transmitters,
    receiverFilter,
    transmitterFilter,
    groups,
  });
  const receiverLabels = model.rows.filter((row) => row.kind === "device").map((row) => row.label);
  const transmitterLabels = model.columns.filter((column) => column.kind === "device").map((column) => column.label);
  const expandDevices = (side, value) => {
    setAllExpanded(side, side === "receivers" ? receiverLabels : transmitterLabels, value);
  };
  const expandGroups = (side, value) => {
    if (value) expandDevices(side, true);
    const axis = side === "receivers" ? model.rows : model.columns;
    const keys = axis.filter((entry) => entry.kind === "device").flatMap((entry) =>
      groupChannels(deviceRequestName(entry.device), entry.channels).map((group) => group.key));
    setGroupsExpanded(side, keys, value);
  };

  return html`
    <div class=${`${showList ? "routing-list" : "routing-grid"} flex flex-col gap-4`}>
      <div class="content-header">
        <div>
          <h1 class="content-title">Routing</h1>
          <div class="content-subtitle">
            ${visible.length} of ${all.length} devices · ${receiverLabels.length} receivers · ${transmitterLabels.length} transmitters
          </div>
        </div>
        <div class="toolbar">
          <button type="button" class="btn btn-sm" aria-expanded=${filtersOpen} aria-controls="routing-device-filters"
            onClick=${() => setFiltersOpen(!filtersOpen)}>Filters</button>
          ${!compact ? html`<${Button} onClick=${() => setListMode(!listMode)}>${listMode ? "Show grid" : "Channel list"}<//>` : null}
          ${!showList ? html`
          <button class="btn btn-sm" type="button" aria-pressed=${flipped} onClick=${() => {
            setFlipped(!flipped);
            try { window.localStorage.setItem("netaudio.matrix.flipped", String(!flipped)); } catch {}
          }}><${Icon} name="flip" /> Flip axes</button>
          <label class="inline-field">
            Receivers
            <input type="search" size="16" value=${receiverFilter} onInput=${(event) => setReceiverFilter(event.target.value)} />
          </label>
          <label class="inline-field">
            Transmitters
            <input type="search" size="16" value=${transmitterFilter} onInput=${(event) => setTransmitterFilter(event.target.value)} />
          </label>
          <span class="inline-flex items-center gap-2">Devices
            <${ExpansionButtons} label="devices" onExpand=${() => {
              expandDevices("receivers", true); expandDevices("transmitters", true);
            }} onCollapse=${() => {
              expandDevices("receivers", false); expandDevices("transmitters", false);
            }} />
          </span>
          <label class="inline-field"><input type="checkbox" checked=${groups.enabled} onChange=${(event) => enableChannelGroups(event.target.checked)} />Channel groups</label>
          ${groups.enabled ? html`<${ExpansionButtons} label="channel groups" onExpand=${() => {
            expandGroups("receivers", true); expandGroups("transmitters", true);
          }} onCollapse=${() => {
            expandGroups("receivers", false); expandGroups("transmitters", false);
          }} />` : null}
          ` : null}
        </div>
      </div>
      <div class=${`routing-workspace${filtersOpen ? " with-filters" : ""}`}>
        ${filtersOpen ? html`<div id="routing-device-filters" class="routing-filter-container"><${DeviceFilterPanel} all=${all} filters=${filters} onChange=${setFilters} /></div>` : null}
        <div class="routing-results">
      ${showList ? html`<${RoutingControls} all=${visible.filter((device) => device.online)} />`
        : model.rows.length === 0 || model.columns.length === 0
        ? html`<${Notice}>No devices match the current filters.<//>`
        : html`<${RoutingMatrix}
            key=${flipped ? "transposed" : "normal"}
            flipped=${flipped}
            grouped=${groups.enabled}
            onExpandDevices=${expandDevices}
            onExpandGroups=${expandGroups}
            rows=${model.rows}
            columns=${model.columns}
            subscriptionIndex=${model.subscriptionIndex}
            onOpenDevice=${(label, tab) => navigate(devicePath("devices", label, tab))}
          />`}
        </div>
      </div>
    </div>
  `;
}

export const routingView = {
  component: RoutingView,
  id: "routing",
  label: "Routing",
};
