import * as format from "../format.js";
import { Icon } from "../icons.js";
import { html } from "../lib/preact.js";

export function subscriptionIndicator(subscription) {
  const status = subscription?.status;
  const tone = format.subscriptionTone(subscription);
  if (tone === "bad") return { icon: "subscription-error", tone, label: "Subscription failed" };
  if (tone === "warn") return { icon: "warning", tone, label: "Subscription warning" };
  if (status?.state === "connected" && tone === "good") return { icon: "subscription-ok", tone, label: "Subscribed" };
  if (status?.state === "pending" || status?.state === "resolving" || status?.state === "unresolved") {
    return { icon: "subscription-pending", tone: "warn", label: "Subscription pending" };
  }
  if (!subscription?.tx_device && (!status || status.state === "none")) {
    return { icon: "subscription-none", tone: "muted", label: "Not subscribed" };
  }
  return { icon: "subscription-unknown", tone: "muted", label: "Subscription status unavailable" };
}

export function SubscriptionTransport({ subscription }) {
  const status = subscription?.status;
  const transport = status?.state === "connected" && format.statusTone(status.severity) === "good"
    ? { DYNAMIC: "unicast", STATIC: "multicast" }[status.status] : null;
  if (!transport) return html`<span class="inline-flex size-5 shrink-0" aria-hidden="true"></span>`;
  const label = transport === "unicast" ? "Unicast" : "Multicast";
  return html`<span class="inline-flex items-center size-5 shrink-0" style="color:#999" role="img" aria-label=${label} title=${label}>
    <${Icon} name=${`subscription-${transport}`} />
  </span>`;
}

export function SubscriptionStatus({ subscription }) {
  const indicator = subscriptionIndicator(subscription);
  const safeText = (value) => typeof value === "string" && !/\b0x[\da-f]+\b|\b[\da-f]{16,}\b/i.test(value) ? value : "";
  const status = subscription?.status;
  const detail = status?.state === "connected" ? "Subscription successful" : safeText(status?.detail) || indicator.label;
  const source = subscription?.tx_device ? safeText(format.subscriptionSource(subscription)) : "";
  return html`<span class=${`receiver-subscription state-${indicator.tone}`} title=${source ? `${source}\n${detail}` : detail} tabindex="0">
    <${SubscriptionTransport} subscription=${subscription} />
    <span role="img" aria-label=${indicator.label}><${Icon} name=${indicator.icon} /></span>
  </span>`;
}
