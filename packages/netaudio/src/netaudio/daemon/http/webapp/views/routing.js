import { Button, Notice } from "../components.js";
import * as format from "../format.js";
import { html, useEffect, useLayoutEffect, useState } from "../lib/preact.js";
import { RoutingControls } from "../routing-controls.js";
import { Icon } from "../icons.js";
import { buildMatrixModel, expanded, ExpansionButtons, initializeExpansion, RoutingMatrix, setAllExpanded } from "../matrix.js";
import { channelGroups, enableChannelGroups, groupChannels, setGroupsExpanded } from "../channel-groups.js";
import { devicePath, navigate } from "../router.js";
import { contextDevices, deviceRequestName, scopedDevices as devices } from "../store.js";
import { inventoryFilters, saveRoutingFilters } from "../device-filters.js";
import { useDropdownDismissal } from "../dropdown.js";

function RoutingView() {
  const all = format.sortedDevices(contextDevices.value);
  useLayoutEffect(() => initializeExpansion(all), [devices.value]);
  const [compact, setCompact] = useState(() => window.matchMedia("(max-width: 900px)").matches);
  const filters = inventoryFilters.value;
  const listMode = filters.listMode === true;
  const receiverFilter = filters.receiverSearch || "";
  const transmitterFilter = filters.transmitterSearch || "";
  const updateFilters = saveRoutingFilters;
  const optionsMenu = useDropdownDismissal();
  const [flipped, setFlipped] = useState(() => {
    try { return window.localStorage.getItem("netaudio.matrix.flipped") !== "false"; }
    catch { return true; }
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

  const visible = format.sortedDevices(devices.value);
  const filteredDevices = devices.value;
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
    const axis = side === "receivers" ? model.rows : model.columns;
    const keys = axis.filter((entry) => entry.kind === "device").flatMap((entry) =>
      groupChannels(deviceRequestName(entry.device), entry.channels).map((group) => group.key));
    setGroupsExpanded(side, keys, value);
  };

  return html`
    <div class=${showList ? "routing-list flex flex-col gap-4" : "routing-grid flex flex-col"}>
      <div class="content-header">
        <div class="toolbar">
          ${!compact ? html`<${Button} onClick=${() => updateFilters({ ...filters, listMode: !listMode })}>${listMode ? "Show grid" : "Channel list"}<//>` : null}
          ${!showList ? html`
          <details class="routing-options" ref=${optionsMenu}><summary class="btn btn-sm">View options</summary><div class="routing-options-panel">
          <button class="btn btn-sm" type="button" aria-pressed=${flipped} onClick=${() => {
            setFlipped(!flipped);
            optionsMenu.current.open = false;
            try { window.localStorage.setItem("netaudio.matrix.flipped", String(!flipped)); } catch {}
          }}><${Icon} name="flip" /> Flip axes</button>
          <span class="inline-flex items-center gap-2">Devices
            <${ExpansionButtons} label="devices and groups" onExpand=${() => {
              expandDevices("receivers", true); expandDevices("transmitters", true);
              optionsMenu.current.open = false;
            }} onCollapse=${() => {
              expandDevices("receivers", false); expandDevices("transmitters", false);
              optionsMenu.current.open = false;
            }} />
          </span>
          <label class="inline-field"><input type="checkbox" checked=${groups.enabled} onChange=${(event) => enableChannelGroups(event.target.checked)} />Channel groups</label>
          </div></details>
          ` : null}
        </div>
      </div>
      <div class="routing-workspace">
        <div class="routing-results">
      ${showList ? html`<${RoutingControls} all=${visible.filter((device) => device.online)} />`
        : model.rows.length === 0 || model.columns.length === 0
        ? html`<${Notice}>No devices match the current filters.<//>`
        : html`<${RoutingMatrix}
            key=${flipped ? "transposed" : "normal"}
            flipped=${flipped}
            receiverFilter=${receiverFilter}
            transmitterFilter=${transmitterFilter}
            onReceiverFilter=${(value) => updateFilters({ ...filters, receiverSearch: value })}
            onTransmitterFilter=${(value) => updateFilters({ ...filters, transmitterSearch: value })}
            onExpandDevices=${expandDevices}
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
