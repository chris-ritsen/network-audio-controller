import { api } from "./api.js";
import { html, useEffect, useState } from "./lib/preact.js";
import { inventoryReady } from "./store.js";

export function DetailedMonitoring({ requestName, online }) {
  const ready = inventoryReady.value;
  const [error, setError] = useState(null);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!online || !ready) return;
    let disposed = false;
    const clientId = `netaudio_webapp_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    setError(null);
    const started = api.startMetering(requestName, clientId);
    started.catch((failure) => {
      if (!disposed) setError(failure.message);
    });
    const release = () => api.stopMetering(requestName, clientId).catch(() => {});
    const leave = () => { void started.then(release, release); };
    window.addEventListener("pagehide", leave);
    return () => {
      disposed = true;
      window.removeEventListener("pagehide", leave);
      leave();
    };
  }, [requestName, online, ready, attempt]);
  return error ? html`<div role="alert">Detailed monitoring could not start: ${error}
    <button class="btn btn-sm" onClick=${() => setAttempt(attempt + 1)}>Retry monitoring</button>
  </div>` : null;
}
