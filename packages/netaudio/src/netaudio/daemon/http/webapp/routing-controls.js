import { html, useState } from "./lib/preact.js";
import * as format from "./format.js";
import { Icon } from "./icons.js";
import { RoutePicker } from "./route-picker.js";
import {
  deviceRequestName,
  pendingKey,
  pendingSubscriptions,
} from "./store.js";

export function receiverChannels(device) {
  const channels = device.channels?.receivers || {};
  return format.sortedChannelNumbers(channels).map((number) => ({
    number,
    name: channels[number].name || `Channel ${number}`,
    subscription: (device.subscriptions || []).find(
      (item) => item.rx_channel === channels[number].name,
    ),
  }));
}

export function RoutingControls({ all }) {
  const receivers = all.filter((device) => receiverChannels(device).length);
  const [selected, setSelected] = useState("");
  const [filter, setFilter] = useState("");
  const [editing, setEditing] = useState(null);
  const receiver =
    receivers.find((device) => deviceRequestName(device) === selected) ||
    receivers[0];
  if (!receiver) return html`<p>No receiving devices found.</p>`;
  const channels = receiverChannels(receiver);
  const visible = channels.filter((channel) =>
    `${channel.number} ${channel.name}`
      .toLowerCase()
      .includes(filter.toLowerCase()),
  );
  return html`<section
    class="routing-channel-list"
    aria-label="Route receiver channels"
  >
    <div class="routing-channel-toolbar">
    <label
      ><span>Receiving device</span
      >
      <select
        value=${deviceRequestName(receiver)}
        onChange=${(event) => {
        setSelected(event.target.value);
        setEditing(null);
        setFilter("");
      }}
      >
        ${receivers.map(
          (device) =>
            html`<option value=${deviceRequestName(device)}>
              ${format.deviceLabel(device)}${device.online ? "" : " · Offline"}
            </option>`,
        )}
      </select>
    </label>
    <label><span>Find a channel</span>
      <input
        type="search"
        aria-label="Find a receiving channel"
        placeholder="Find a channel"
        value=${filter}
        onInput=${(event) => setFilter(event.target.value)}
      />
    </label>
    </div>
    ${!receiver.online ? html`<p role="status">Receiver offline. Routing is unavailable.</p>` : null}
    <div class="routing-channel-head" aria-hidden="true"><span>#</span><span>Receiving channel</span><span>Source channel</span><span>Transmitting device</span><span></span><span></span></div>
    <div class="routing-channel-rows">
      ${visible.map((channel) => {
        const subscription = channel.subscription;
        const routed = Boolean(
          subscription?.tx_device && subscription?.tx_channel,
        );
        const pending =
          pendingSubscriptions.value[
            pendingKey(deviceRequestName(receiver), channel.number)
          ];
        const tone = routed
          ? format.subscriptionTone(subscription)
          : "none";
        const good = subscription?.status?.state === "connected";
        return html`<button
          type="button"
          class="routing-channel-row"
          key=${channel.number}
          disabled=${!receiver.online || Boolean(pending)}
          onClick=${() => setEditing(channel.number)}
          aria-label=${`${channel.name}: ${routed ? `${subscription.tx_channel} from ${subscription.tx_device}` : "Choose source"}`}
        >
          <span class="routing-channel-number"
            >${channel.number}</span
          >
          <strong class="routing-channel-name">${channel.name}</strong>
            <span class="routing-channel-source"
              >${pending ? "Applying…" : routed ? subscription.tx_channel : "Choose source"}</span
            >
            <span class="routing-channel-device">${routed ? subscription.tx_device : "—"}</span>
          <span
            class=${`routing-channel-status state-${tone}`}
            title=${routed ? format.subscriptionStatusText(subscription) : "Not routed"}
          >
            <${Icon} name=${routed ? (good ? "check" : "warning") : "plus"} />
          </span>
          <span class="routing-channel-action"><${Icon} name="chevron" /></span>
        </button>`;
      })}
      ${!visible.length ? html`<p>No channels match this search.</p>` : null}
    </div>
    ${
      editing !== null && channels.some((channel) => channel.number === editing)
        ? html`<${RoutePicker}
            key=${`${deviceRequestName(receiver)}:${editing}`}
            receiver=${receiver}
            sourceDevices=${all}
            receiveChannelNumber=${editing}
            receiveChannelName=${channels.find((channel) => channel.number === editing).name}
            subscription=${channels.find((channel) => channel.number === editing).subscription}
            onClose=${() => setEditing(null)}
          />`
        : null
    }
  </section>`;
}
