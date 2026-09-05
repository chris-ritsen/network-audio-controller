import { api } from "./api.js";
import { Button } from "./components.js";
import * as format from "./format.js";
import { html, useLayoutEffect, useMemo, useRef, useState } from "./lib/preact.js";
import { deviceRequestName, devices } from "./store.js";
import { runAction } from "./toast.js";

function transmitterEntries() {
  const entries = [];
  for (const device of format.sortedDevices(devices.value)) {
    const channels = device.channels ? device.channels.transmitters || {} : {};
    for (const number of format.sortedChannelNumbers(channels)) {
      const channel = channels[number];
      if (!channel || !channel.name) {
        continue;
      }
      entries.push({
        channelName: channel.name,
        channelNumber: number,
        deviceLabel: format.deviceLabel(device),
        online: device.online,
      });
    }
  }
  return entries;
}

export function RoutePicker({ onClose, receiver, receiveChannelNumber, receiveChannelName, subscription }) {
  const dialog = useRef(null);
  const input = useRef(null);
  const [query, setQuery] = useState("");
  const [highlighted, setHighlighted] = useState(0);
  const [busy, setBusy] = useState(false);

  const entries = useMemo(() => transmitterEntries(), [devices.value]);
  const needle = query.trim().toLowerCase();
  const results = entries.filter(
    (entry) => !needle || `${entry.channelName} ${entry.deviceLabel}`.toLowerCase().includes(needle),
  );
  const activeIndex = Math.min(highlighted, Math.max(results.length - 1, 0));

  useLayoutEffect(() => {
    if (dialog.current && !dialog.current.open) {
      dialog.current.showModal();
      if (input.current) {
        input.current.focus();
      }
    }
  }, []);

  const requestName = deviceRequestName(receiver);

  const apply = async (entry) => {
    setBusy(true);
    await runAction(
      `route ${entry.channelName}@${entry.deviceLabel} to ${format.deviceLabel(receiver)} channel ${receiveChannelNumber}`,
      () =>
        api.subscribe({
          rx_channel: receiveChannelNumber,
          rx_device: requestName,
          tx_channel: entry.channelName,
          tx_device: entry.deviceLabel,
        }),
    );
    setBusy(false);
    onClose();
  };

  const clear = async () => {
    setBusy(true);
    await runAction(`unsubscribe ${format.deviceLabel(receiver)} channel ${receiveChannelNumber}`, () =>
      api.unsubscribe({ rx_channel: receiveChannelNumber, rx_device: requestName }),
    );
    setBusy(false);
    onClose();
  };

  const onKeyDown = (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      onClose();
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
      apply(results[activeIndex]);
    }
  };

  return html`
    <dialog class="palette route-picker" ref=${dialog} onClose=${onClose} onCancel=${onClose}>
      <header class="route-picker-header">
        <div>
          <div class="route-picker-title">Route to ${format.deviceLabel(receiver)}</div>
          <div class="route-picker-subtitle">
            receive channel ${receiveChannelNumber}${receiveChannelName ? ` · ${receiveChannelName}` : ""}
          </div>
        </div>
        <div class="toolbar">
          ${subscription
            ? html`<${Button} small variant="danger" disabled=${busy} onClick=${clear}>Clear route<//>`
            : null}
          <${Button} small disabled=${busy} onClick=${onClose}>Cancel<//>
        </div>
      </header>
      <input
        ref=${input}
        type="text"
        placeholder="Filter transmit channels"
        value=${query}
        onInput=${(event) => {
          setQuery(event.target.value);
          setHighlighted(0);
        }}
        onKeyDown=${onKeyDown}
      />
      <div class="palette-results">
        ${results.length === 0
          ? html`<div class="palette-empty">No transmit channel matches this filter.</div>`
          : results.map((entry, index) => {
              const active =
                subscription &&
                subscription.tx_channel === entry.channelName &&
                subscription.tx_device === entry.deviceLabel;
              return html`
                <div
                  key=${`${entry.deviceLabel}/${entry.channelName}`}
                  class="palette-item${index === activeIndex ? " active" : ""}"
                  onPointerEnter=${() => setHighlighted(index)}
                  onClick=${() => apply(entry)}
                >
                  <span class="status-dot${entry.online ? " online" : ""}"></span>
                  <span class="palette-item-name">${entry.channelName}</span>
                  <span class="palette-item-detail">${entry.deviceLabel}</span>
                  ${active ? html`<span class="palette-item-kind">current</span>` : null}
                </div>
              `;
            })}
      </div>
    </dialog>
  `;
}
