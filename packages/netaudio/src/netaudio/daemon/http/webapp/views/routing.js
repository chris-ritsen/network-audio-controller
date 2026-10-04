import { Button, Notice } from "../components.js";
import * as format from "../format.js";
import { html, useEffect, useLayoutEffect, useState } from "../lib/preact.js";
import { RoutingControls } from "../routing-controls.js";
import { Icon } from "../icons.js";
import { buildMatrixModel, expanded, initializeExpansion, RoutingMatrix, setAllExpanded } from "../matrix.js";
import { channelGroups, enableChannelGroups, groupChannels, setGroupsExpanded } from "../channel-groups.js";
import { devicePath, navigate } from "../router.js";
import { contextDevices, contextExternalFlows, deviceRequestName, pendingSubscriptions, scopedDevices as devices } from "../store.js";
import { inventoryFilters, saveRoutingFilters } from "../device-filters.js";

function RoutingView() {
  const all = format.sortedDevices(contextDevices.value);
  useLayoutEffect(() => initializeExpansion(all), [devices.value]);
  const [compact, setCompact] = useState(() => window.matchMedia("(max-width: 900px)").matches);
  const filters = inventoryFilters.value;
  const listMode = filters.listMode === true;
  const receiverFilter = filters.receiverSearch || "";
  const transmitterFilter = filters.transmitterSearch || "";
  const updateFilters = saveRoutingFilters;
  const [gridLocked, setGridLocked] = useState(() => {
    try { return window.localStorage.getItem("netaudio.matrix.locked") === "true"; }
    catch { return false; }
  });
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
  const pending = pendingSubscriptions.value;

  if (!all.length) {
    return html`<${Notice}>No Dante devices have been discovered yet.<//>`;
  }

  const visible = format.sortedDevices(devices.value);
  const filteredDevices = devices.value;
  const showList = compact || listMode;
  const model = buildMatrixModel({
    devices: filteredDevices,
    externalFlows: contextExternalFlows.value,
    expandedReceivers: state.receivers,
    expandedTransmitters: state.transmitters,
    receiverFilter,
    transmitterFilter,
    groups,
    pending,
  });
  const receiverLabels = model.rows.filter((row) => row.kind === "device").map((row) => row.label);
  const transmitterLabels = model.columns.filter((column) => column.kind === "device").map((column) => column.label);
  const expandGroups = (side, value) => {
    const axis = side === "receivers" ? model.rows : model.columns;
    const keys = axis.filter((entry) => entry.kind === "device" && entry.sourceKind !== "external").flatMap((entry) =>
      groupChannels(deviceRequestName(entry.device), entry.channels).map((group) => group.key));
    setGroupsExpanded(side, keys, value);
  };
  const expandDevices = (side, value) => {
    setAllExpanded(side, side === "receivers" ? receiverLabels : transmitterLabels, value);
    expandGroups(side, value);
  };

  return html`
    <div class=${showList ? "routing-list flex flex-col gap-4" : "routing-grid flex flex-col"}>
      <div class="content-header">
        <div class="toolbar">
          ${!compact ? html`<${Button} onClick=${() => updateFilters({ ...filters, listMode: !listMode })}>${listMode ? "Show grid" : "Channel list"}<//>` : null}
          ${!showList ? html`
          <button class="btn btn-sm" type="button" aria-label="Lock grid" aria-pressed=${gridLocked}
            title=${gridLocked ? "Unlock to change connections" : "Prevent connection changes while browsing the grid"}
            onClick=${() => {
              setGridLocked(!gridLocked);
              try { window.localStorage.setItem("netaudio.matrix.locked", String(!gridLocked)); } catch {}
            }}><${Icon} name=${gridLocked ? "lock" : "unlock"} /> ${gridLocked ? "Grid locked" : "Lock grid"}</button>
          <button class="btn btn-sm" type="button" aria-pressed=${!flipped} onClick=${() => {
            setFlipped(!flipped);
            try { window.localStorage.setItem("netaudio.matrix.flipped", String(!flipped)); } catch {}
          }}><${Icon} name="flip" /> Flip axes</button>
          <button class="btn btn-sm" type="button" aria-pressed=${groups.enabled} onClick=${() => enableChannelGroups(!groups.enabled)}><${Icon} name="groups" /> Channel groups</button>
          ` : null}
        </div>
      </div>
      <div class="routing-workspace">
        <div class="routing-results">
      ${showList ? html`<${RoutingControls} all=${visible.filter((device) => device.online)} />`
        : model.rows.length === 0 || model.columns.length === 0
        ? html`<${Notice}>No devices match the current filters.<//>`
        : html`<${RoutingMatrix}
            locked=${gridLocked}
            pending=${pending}
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
