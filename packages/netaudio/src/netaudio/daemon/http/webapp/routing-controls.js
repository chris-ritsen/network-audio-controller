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
    class="max-w-2xl flex flex-col gap-4"
    aria-label="Route receiver channels"
  >
    <label class="fieldset"
      ><span class="fieldset-legend text-base"
        ><${Icon} name="devices" />Receiving device</span
      >
      <select
        class="select select-bordered w-full min-h-12 text-base"
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
    <label class="input input-bordered w-full min-h-12"
      ><${Icon} name="search" />
      <input
        type="search"
        aria-label="Find a receiving channel"
        placeholder="Find a channel"
        value=${filter}
        onInput=${(event) => setFilter(event.target.value)}
      />
    </label>
    ${!receiver.online ? html`<p role="status">Receiver offline. Routing is unavailable.</p>` : null}
    <div class="flex flex-col gap-2">
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
          class="btn bg-base-200 border-base-300 justify-start h-auto min-h-20 p-4 gap-3 text-left whitespace-normal"
          key=${channel.number}
          disabled=${!receiver.online || Boolean(pending)}
          onClick=${() => setEditing(channel.number)}
          aria-label=${`${channel.name}: ${routed ? `${subscription.tx_channel} from ${subscription.tx_device}` : "Choose source"}`}
        >
          <span class="badge badge-lg badge-neutral shrink-0"
            >${channel.number}</span
          >
          <span class="flex-1 min-w-0 break-words"
            ><strong class="block">${channel.name}</strong>
            <span class="block font-normal"
              >${pending ? "Applying…" : routed ? subscription.tx_channel : "Choose source"}</span
            >
            ${routed ? html`<small class="block font-normal">${subscription.tx_device}</small>` : null}
          </span>
          <span
            class=${`flex state-${tone}`}
            title=${routed ? format.subscriptionStatusText(subscription) : "Not routed"}
          >
            <${Icon} name=${routed ? (good ? "check" : "warning") : "plus"} />
          </span>
          <${Icon} name="chevron" />
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
