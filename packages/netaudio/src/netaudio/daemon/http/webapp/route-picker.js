import { api } from "./api.js";
import { Button } from "./components.js";
import { Icon } from "./icons.js";
import * as format from "./format.js";
import { html, useLayoutEffect, useMemo, useRef, useState } from "./lib/preact.js";
import { deviceRequestName, scopedDevices as devices } from "./store.js";
import { runAction } from "./actions.js";

function transmitterEntries(sourceDevices) {
  const entries = [];
  for (const device of sourceDevices) {
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

export function RoutePicker({ onClose, receiver, receiveChannelNumber, receiveChannelName, subscription, sourceDevices }) {
  const dialog = useRef(null);
  const input = useRef(null);
  const [query, setQuery] = useState("");
  const [highlighted, setHighlighted] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const entries = useMemo(() => transmitterEntries(sourceDevices || format.sortedDevices(devices.value)), [sourceDevices, devices.value]);
  const needle = query.trim().toLowerCase();
  const results = entries.filter(
    (entry) => !needle || `${entry.channelName} ${entry.deviceLabel}`.toLowerCase().includes(needle),
  );
  const activeIndex = Math.min(highlighted, Math.max(results.length - 1, 0));

  useLayoutEffect(() => {
    if (dialog.current && !dialog.current.open) {
      dialog.current.showModal();
      if (input.current && window.innerWidth > 900) {
        input.current.focus();
      }
    }
  }, []);

  const requestName = deviceRequestName(receiver);

  const apply = async (entry) => {
    if (busy || !entry.online || !receiver.online) return;
    setError("");
    setBusy(true);
    const result = await runAction(
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
    if (result.ok) onClose();
    else setError(result.error.message);
  };

  const clear = async () => {
    if (busy || !receiver.online) return;
    setError("");
    setBusy(true);
    const result = await runAction(`unsubscribe ${format.deviceLabel(receiver)} channel ${receiveChannelNumber}`, () =>
      api.unsubscribe({ rx_channel: receiveChannelNumber, rx_device: requestName }),
    );
    setBusy(false);
    if (result.ok) onClose();
    else setError(result.error.message);
  };

  const onKeyDown = (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      if (!busy) onClose();
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
    <dialog class="modal modal-middle source-picker" aria-labelledby="source-picker-title" ref=${dialog} onClose=${onClose}
      onClick=${(event) => { if (!busy && event.target === event.currentTarget) onClose(); }}
      onCancel=${(event) => { if (busy) event.preventDefault(); else onClose(); }}>
      <div class="modal-box source-picker-box">
      <header class="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 id="source-picker-title" class="text-lg font-semibold">Choose source</h2>
          <div class="route-picker-subtitle">
            ${format.deviceLabel(receiver)} · ${receiveChannelName || `Channel ${receiveChannelNumber}`}
          </div>
        </div>
        <div class="toolbar">
          ${subscription?.tx_device && subscription?.tx_channel
            ? html`<${Button} variant="danger" disabled=${busy || !receiver.online} onClick=${clear}>Disconnect<//>`
            : null}
          <${Button} disabled=${busy} onClick=${onClose}>Done<//>
        </div>
      </header>
      ${error ? html`<div role="alert" class="alert alert-error"><${Icon} name="warning" /><span>${error}</span></div>` : null}
      <input
        ref=${input}
        type="text"
        class="input input-bordered w-full min-h-11"
        aria-label="Find a source device or channel"
        placeholder="Filter transmit channels"
        value=${query}
        onInput=${(event) => {
          setQuery(event.target.value);
          setHighlighted(0);
        }}
        onKeyDown=${onKeyDown}
      />
      <div class="source-picker-results">
        ${results.length === 0
          ? html`<div class="palette-empty">No transmit channel matches this filter.</div>`
          : results.map((entry, index) => {
              const active =
                subscription &&
                subscription.tx_channel === entry.channelName &&
                subscription.tx_device === entry.deviceLabel;
              return html`
                <button type="button" disabled=${busy || !entry.online || !receiver.online}
                  key=${`${entry.deviceLabel}/${entry.channelName}`}
                  class=${`source-picker-entry${active ? " current" : ""}`}
                  onPointerEnter=${() => setHighlighted(index)}
                  onClick=${() => apply(entry)}
                >
                  <span class="flex-1 min-w-0 break-words"><strong class="block">${entry.channelName}</strong><span class="block font-normal">${entry.deviceLabel}</span></span>
                  ${active ? html`<span class="badge badge-success badge-outline">Current</span>` : !entry.online ? html`<span class="badge">Offline</span>` : null}
                </button>
              `;
            })}
      </div>
      </div>
    </dialog>
  `;
}
