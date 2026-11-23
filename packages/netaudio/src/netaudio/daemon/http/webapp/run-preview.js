import { api } from "./api.js";
import { Button } from "./components.js";
import { t, tn } from "./i18n.js";
import { Icon } from "./icons.js";
import { html, useLayoutEffect, useRef, useState } from "./lib/preact.js";
import { channelLabel, routeFor } from "./matrix-run.js";
import { requestSubscriptionChanges } from "./store.js";

async function applyRoutes(routes) {
  try {
    return (await api.applyRoutes(routes)).routes;
  } catch (failure) {
    if (Array.isArray(failure.payload?.routes)) return failure.payload.routes;
    throw failure;
  }
}

function concluded(change, outcome) {
  if (outcome?.ok === true) {
    return { ...change, kind: "unchanged", note: t("Subscribed") };
  }
  return {
    ...change,
    kind: outcome?.outcome_unknown ? "blocked" : "add",
    note: outcome?.error ? t(outcome.error) : t("Not applied"),
  };
}

export function RunPreview({ changes: proposed, onClose }) {
  const dialog = useRef(null);
  const confirm = useRef(null);
  const [changes, setChanges] = useState(proposed);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const additions = changes.filter((change) => change.kind === "add");

  useLayoutEffect(() => {
    if (dialog.current && !dialog.current.open) {
      dialog.current.showModal();
      confirm.current?.focus();
    }
  }, []);

  const apply = async () => {
    if (busy || !additions.length) return;
    setError("");
    setBusy(true);
    let outcomes = [];
    try {
      await requestSubscriptionChanges(
        additions.map((change) => ({ action: "add", ...routeFor(change) })),
        async () => {
          outcomes = await applyRoutes(additions.map(routeFor));
          return outcomes.map((outcome) => outcome.ok === true);
        },
      );
    } catch (failure) {
      setError(t(failure.message));
      setBusy(false);
      return;
    }
    setBusy(false);
    const failed = outcomes.filter((outcome) => outcome.ok !== true).length;
    if (!failed) {
      onClose();
      return;
    }
    setChanges(
      changes.map((change) =>
        change.kind === "add"
          ? concluded(change, outcomes[additions.indexOf(change)])
          : change,
      ),
    );
    setError(
      tn(
        failed,
        "{count} connection was not made",
        "{count} connections were not made",
      ),
    );
  };

  return html`
    <dialog class="modal modal-middle run-preview" aria-labelledby="run-preview-title" ref=${dialog} onClose=${onClose}
      onClick=${(event) => {
        if (!busy && event.target === event.currentTarget) onClose();
      }}
      onCancel=${(event) => {
        if (busy) event.preventDefault();
        else onClose();
      }}>
      <div class="modal-box run-preview-box max-w-3xl">
        <header class="flex flex-wrap items-center justify-between gap-3">
          <h2 id="run-preview-title" class="text-lg font-semibold">
            ${tn(additions.length, "Connect {count} channel", "Connect {count} channels")}
          </h2>
          <div class="toolbar">
            <${Button} disabled=${busy} onClick=${onClose}>${t("Cancel")}<//>
            <button type="button" ref=${confirm} class=${`btn btn-sm btn-primary${busy ? " pending" : ""}`}
              disabled=${busy || !additions.length} aria-busy=${busy ? "true" : null} onClick=${apply}>
              ${t("Apply")}
            </button>
          </div>
        </header>
        ${error ? html`<div role="alert" class="alert alert-error"><${Icon} name="warning" /><span>${error}</span></div>` : null}
        <div class="run-preview-routes">
          <table class="data">
            <thead>
              <tr>
                <th>${t("Receiver")}</th>
                <th>${t("Transmitter")}</th>
                <th>${t("Status")}</th>
              </tr>
            </thead>
            <tbody>
              ${changes.map(
                (change) => html`<tr key=${channelLabel(change.receiver)}>
                  <td data-label=${t("Receiver")}>${channelLabel(change.receiver)}</td>
                  <td data-label=${t("Transmitter")}>${channelLabel(change.transmitter)}</td>
                  <td data-label=${t("Status")}>${change.note}</td>
                </tr>`,
              )}
            </tbody>
          </table>
        </div>
      </div>
    </dialog>
  `;
}
