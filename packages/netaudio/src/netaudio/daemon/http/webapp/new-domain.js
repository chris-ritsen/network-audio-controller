import { api } from "./api.js";
import { html, useLayoutEffect, useRef, useState } from "./lib/preact.js";
import { Icon } from "./icons.js";

export function NewDomainDialog({ server, onClose }) {
  const dialog = useRef(null);
  const input = useRef(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useLayoutEffect(() => { dialog.current.showModal(); }, []);
  const create = async (event) => {
    event.preventDefault();
    if (busy) return;
    setBusy(true); setError("");
    try {
      await api.createDdmDomain(server, input.current.value);
      onClose();
    } catch (failure) { setError(failure.message); }
    finally { setBusy(false); }
  };
  return html`<dialog class="modal" aria-labelledby="new-domain-title" ref=${dialog} onClose=${onClose} onCancel=${(event) => { if (busy) event.preventDefault(); }}
    onClick=${(event) => { if (!busy && event.target === event.currentTarget) onClose(); }}>
    <form class="modal-box max-w-sm flex flex-col gap-4" onSubmit=${create}>
      <h2 id="new-domain-title" class="text-lg font-semibold">New domain</h2>
      <label class="flex flex-col gap-2">Server<input class="w-full" value=${server} readonly /></label>
      <label class="flex flex-col gap-2">Name<input class="w-full" ref=${input} autofocus required disabled=${busy} /></label>
      ${error ? html`<div class="alert alert-error" role="alert">${error}</div>` : null}
      <div class="flex gap-2 justify-end">
        <button type="button" class="btn btn-sm" onClick=${onClose} disabled=${busy}>Cancel</button>
        <button type="submit" class="btn btn-sm btn-primary" disabled=${busy}><${Icon} name="plus" />${busy ? "Creating…" : "Create domain"}</button>
      </div>
    </form>
  </dialog>`;
}
