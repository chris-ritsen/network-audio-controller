import { api } from "../api.js";
import { AsyncButton, Panel } from "../components.js";
import * as format from "../format.js";
import { html, useLayoutEffect, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";

export function LockSection({ device }) {
  const requestName = deviceRequestName(device);
  const [observation, setObservation] = useState(null);
  const [pin, setPin] = useState("");
  useLayoutEffect(() => { setObservation(null); setPin(""); }, [requestName]);
  const locked = observation ? observation.is_locked : device.is_locked;
  const known = locked === true || locked === false;
  const refresh = async () => {
    const result = await api.getLockStatus(requestName);
    setObservation(result);
    return result;
  };

  return html`
    <div class="w-full max-w-lg"><${Panel}
      title="Device lock"
      headerActions=${html`<${AsyncButton}
        small
        description=${`refresh lock status on ${requestName}`}
        onRun=${refresh}
      >
        Refresh
      <//>`}
    >
      <div class="flex flex-col items-start gap-4">
        <div class="text-sm" role="status">${locked === true ? "Locked" : locked === false ? "Unlocked" : "Lock state unavailable"}</div>
        ${!known ? html`<p class="text-sm">Refresh to check the device before changing its lock.</p>` : null}
        <label class="flex flex-col gap-2">Device PIN
          <input class="w-32" type="password" inputmode="numeric" pattern="[0-9]{4}" maxlength="4" autocomplete="off"
            value=${pin} onInput=${(event) => setPin(event.target.value)} placeholder="4 digits" />
        </label>
        <${AsyncButton}
          small
          disabled=${!known || !/^\d{4}$/.test(pin)}
          description=${`${locked ? "unlock" : "lock"} ${requestName}`}
          onRun=${async () => {
            const result = locked ? await api.unlock(requestName, pin) : await api.lock(requestName, pin);
            setPin("");
            await refresh();
            return result;
          }}
        >
          ${locked ? "Unlock" : "Lock"}
        <//>
        ${observation?.observed_at ? html`<div class="text-xs opacity-70">Last checked ${format.timestamp(observation.observed_at)}</div>` : null}
      </div>
    <//></div>
  `;
}
