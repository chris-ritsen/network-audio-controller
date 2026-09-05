import * as format from "./format.js";
import { html, signal, useLayoutEffect, useMemo, useRef, useState } from "./lib/preact.js";
import { navigate } from "./router.js";
import { devices, shureDevices } from "./store.js";

const paletteOpen = signal(false);

const VIEW_ENTRIES = [
  { kind: "view", label: "Routing", path: "/routing" },
  { kind: "view", label: "Device info", path: "/devices" },
  { kind: "view", label: "Clock status", path: "/clock-status" },
  { kind: "view", label: "Network status", path: "/network-status" },
  { kind: "view", label: "Metering", path: "/metering" },
  { kind: "view", label: "Flows", path: "/flows" },
  { kind: "view", label: "Domains", path: "/ddm" },
  { kind: "view", label: "Shure", path: "/shure" },
  { kind: "view", label: "Events", path: "/events" },
];

export function openPalette() {
  paletteOpen.value = true;
}

export function togglePalette() {
  paletteOpen.value = !paletteOpen.value;
}

export function closePalette() {
  paletteOpen.value = false;
}

function buildEntries() {
  const entries = [...VIEW_ENTRIES];
  for (const device of format.sortedDevices(devices.value)) {
    const name = format.deviceLabel(device);
    entries.push({
      detail: format.deviceSummaryLine(device),
      kind: "device",
      label: name,
      online: device.online,
      path: `/devices/${encodeURIComponent(name)}/receive`,
    });
  }
  for (const device of format.sortedShureDevices(shureDevices.value)) {
    const name = device.name || device.mac;
    entries.push({
      detail: format.text(device.model),
      kind: "shure",
      label: name,
      online: device.online,
      path: `/shure/${encodeURIComponent(name)}`,
    });
  }
  return entries;
}

function matches(entry, needle) {
  if (!needle) {
    return true;
  }
  return `${entry.label} ${entry.detail || ""} ${entry.kind}`.toLowerCase().includes(needle);
}

export function CommandPalette() {
  const open = paletteOpen.value;
  const [query, setQuery] = useState("");
  const [highlighted, setHighlighted] = useState(0);
  const dialog = useRef(null);
  const input = useRef(null);

  const entries = useMemo(() => (open ? buildEntries() : []), [open, devices.value, shureDevices.value]);
  const needle = query.trim().toLowerCase();
  const results = entries.filter((entry) => matches(entry, needle)).slice(0, 60);
  const activeIndex = Math.min(highlighted, Math.max(results.length - 1, 0));

  useLayoutEffect(() => {
    const node = dialog.current;
    if (!node) {
      return;
    }
    if (open && !node.open) {
      node.showModal();
      if (input.current) {
        input.current.focus();
      }
    }
    if (!open && node.open) {
      node.close();
    }
  }, [open]);

  if (!open) {
    return null;
  }

  const choose = (entry) => {
    closePalette();
    navigate(entry.path);
  };

  const onKeyDown = (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      closePalette();
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setHighlighted((value) => Math.min(value + 1, results.length - 1));
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      setHighlighted((value) => Math.max(value - 1, 0));
      return;
    }
    if (event.key === "Enter" && results[activeIndex]) {
      event.preventDefault();
      choose(results[activeIndex]);
    }
  };

  return html`
    <dialog class="palette" ref=${dialog} onClose=${closePalette} onCancel=${closePalette}>
      <input
        ref=${input}
        type="text"
        placeholder="Jump to a device, view, or Shure receiver"
        value=${query}
        onInput=${(event) => {
          setQuery(event.target.value);
          setHighlighted(0);
        }}
        onKeyDown=${onKeyDown}
      />
      <div class="palette-results">
        ${results.length === 0
          ? html`<div class="palette-empty">No match for “${query}”.</div>`
          : results.map(
              (entry, index) => html`
                <div
                  key=${`${entry.kind}:${entry.path}`}
                  class="palette-item${index === activeIndex ? " active" : ""}"
                  onPointerEnter=${() => setHighlighted(index)}
                  onClick=${() => choose(entry)}
                >
                  ${entry.kind === "view"
                    ? null
                    : html`<span class="status-dot${entry.online ? " online" : ""}"></span>`}
                  <span class="palette-item-name">${entry.label}</span>
                  ${entry.detail ? html`<span class="palette-item-detail">${entry.detail}</span>` : null}
                  <span class="palette-item-kind">${entry.kind}</span>
                </div>
              `,
            )}
      </div>
    </dialog>
  `;
}
