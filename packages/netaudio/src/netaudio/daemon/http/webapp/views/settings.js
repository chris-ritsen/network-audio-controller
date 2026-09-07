import { api } from "../api.js";
import { Panel } from "../components.js";
import { Icon } from "../icons.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { backendSettings } from "../store.js";

function SettingsView() {
  const settings = backendSettings.value;
  const [port, setPort] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    api.getSettings().then((result) => { if (active) backendSettings.value = result; })
      .catch((failure) => { if (active) setError(failure.message); });
    return () => { active = false; };
  }, []);
  useEffect(() => { if (settings) setPort(String(settings.monitoring_port)); }, [settings]);
  const changed = settings && Number(port) !== settings.monitoring_port;
  return html`<div class="flex flex-col gap-6 w-full max-w-lg"><h1 class="content-title">Settings</h1>
    <${Panel} title="Metering">
      <form class="flex flex-col items-start gap-4" onSubmit=${async (event) => {
        event.preventDefault();
        if (busy || !changed) return;
        setBusy(true); setError("");
        try { backendSettings.value = await api.setMonitoringPort(Number(port)); }
        catch (failure) { setError(failure.message); }
        finally { setBusy(false); }
      }}>
        <label class="flex flex-col gap-2">UDP port<input class="w-32" type="number" min="1024" max="65535" required disabled=${busy || !settings} value=${port} onInput=${(event) => setPort(event.target.value)} /></label>
        ${settings && settings.active_monitoring_port !== settings.monitoring_port ? html`<p class="text-sm" role="status">${settings.active_monitoring_port == null ? "Not listening" : `Currently listening on port ${settings.active_monitoring_port}`}</p>` : null}
        <button class="btn btn-sm btn-primary" type="submit" disabled=${busy || !changed}><${Icon} name="check" />${busy ? "Saving…" : "Save"}</button>
      </form>
      ${error ? html`<div class="alert alert-error mt-3" role="alert">${error}</div>` : null}
    <//></div>`;
}

export const settingsView = { component: SettingsView, id: "settings", label: "Settings" };
