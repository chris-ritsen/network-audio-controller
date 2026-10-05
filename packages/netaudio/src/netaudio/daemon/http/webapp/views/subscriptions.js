import { api } from "../api.js";
import { AsyncButton, Notice, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { t } from "../i18n.js";
import { html } from "../lib/preact.js";
import { Icon } from "../icons.js";
import { SubscriptionTransport } from "../device/receiver-status.js";
import { deviceRequestName, scopedDevices as devices } from "../store.js";
import { ConfigurableTable } from "../table.js";

function subscriptionColumns() {
  return [
    { cell: (row) => format.deviceLabel(row.device), id: "receiver", label: t("Receiver") },
    { cell: (row) => html`<${Value} value=${row.subscription.rx_channel} />`, id: "receive-channel", label: t("Rx channel") },
    { cell: (row) => html`<${Value} value=${row.subscription.tx_device} />`, id: "transmitter", label: t("Transmitter") },
    { cell: (row) => html`<${Value} value=${row.subscription.tx_channel} />`, id: "transmit-channel", label: t("Tx channel") },
    {
      cell: (row) => {
        const status = row.subscription.status || null;
        const tone = format.subscriptionTone(row.subscription);
        const icon = { good: "subscription-ok", warn: "warning", bad: "subscription-blocked" }[tone];
        return html`<span class=${`inline-flex items-center gap-2${tone ? ` state-${tone}` : ""}`}>
          <${SubscriptionTransport} subscription=${row.subscription} />
          ${icon ? html`<${Icon} name=${icon} />` : null}
          <span>${status ? format.subscriptionStatusText(row.subscription) : format.ABSENT}</span>
        </span>`;
      },
      id: "status",
      label: t("Status"),
    },
    { cell: (row) => html`<${Value} value=${t(row.subscription.ddm_summary)} />`, id: "managed", label: t("Domain status"), defaultHidden: true },
    {
      cell: (row) =>
        row.receiveNumber === undefined
          ? format.ABSENT
          : html`<${AsyncButton}
              small
              description=${t("unsubscribe {device} {channel}", { device: format.deviceLabel(row.device), channel: row.subscription.rx_channel })}
              onRun=${() =>
                api.unsubscribe({ rx_channel: row.receiveNumber, rx_device: deviceRequestName(row.device) })}
            >
              ${t("Unsubscribe")}
            <//>`,
      id: "action",
      label: "",
    },
  ];
}

function SubscriptionTable({ all }) {
  const rows = [];
  for (const device of all) {
    const receiveChannels = device.channels ? device.channels.receivers || {} : {};
    for (const subscription of device.subscriptions || []) {
      if (!subscription.tx_device || !subscription.tx_channel) {
        continue;
      }
      rows.push({
        device,
        receiveNumber: receiveChannels[subscription.rx_channel_number] ? subscription.rx_channel_number : undefined,
        subscription,
      });
    }
  }
  return html`
    <${Panel} title=${t("Subscriptions ({count})", { count: rows.length })}>
      ${rows.length === 0
        ? html`<${Notice}>${t("No Dante receiver is currently subscribed.")}<//>`
        : html`<${ConfigurableTable}
            tableId="subscriptions"
            columns=${subscriptionColumns()}
            rows=${rows}
            rowKey=${(row) => `${row.device.server_name}:${row.subscription.rx_channel_number}`}
          />`}
    <//>
  `;
}

function SubscriptionsView() {
  return html`<div class="flex flex-col gap-4">
    <${SubscriptionTable} all=${format.sortedDevices(devices.value)} />
  </div>`;
}

export const subscriptionsView = {
  component: SubscriptionsView,
  id: "subscriptions",
  label: t("Subscriptions"),
};
