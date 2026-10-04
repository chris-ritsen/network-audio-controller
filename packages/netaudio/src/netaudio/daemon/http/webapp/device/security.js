import { api } from "../api.js";
import { AsyncButton, Fields, Panel } from "../components.js";
import { html, useEffect, useLayoutEffect, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";
import { operationAvailability } from "./availability.js";

export function LockSection({ device }) {
  const requestName = deviceRequestName(device);
  const [observation, setObservation] = useState(null);
  const [pin, setPin] = useState("");
  useLayoutEffect(() => { setObservation(null); setPin(""); }, [requestName]);
  const locked = observation ? observation.is_locked : device.is_locked;
  const known = locked === true || locked === false;
  const availabilityDevice = observation?.operation_availability
    ? { operation_availability: { locking: observation.operation_availability } }
    : device;
  const availability = operationAvailability(availabilityDevice, "locking");
  const writable = availability?.writable === true;
  const canRefresh = availability?.supported !== false
    && !(availability?.reasons || []).includes("managed_transport_unavailable");
  const refresh = async () => {
    const result = await api.getLockStatus(requestName);
    setObservation(result);
    return result;
  };
  useEffect(() => {
    if (canRefresh && !known) refresh().catch((error) => console.error(error));
  }, [requestName]);
  if (!known && !writable) return null;

  return html`
    <div class="w-full max-w-lg"><${Panel}
      title="Device lock"
      headerActions=${canRefresh ? html`<${AsyncButton}
        small
        description=${`refresh lock status on ${requestName}`}
        onRun=${refresh}
      >
        Refresh
      <//>` : null}
    >
      <div class="flex flex-col items-start gap-4">
        ${known ? html`<${Fields} entries=${[["State", locked ? "Locked" : "Unlocked"]]} />` : null}
        ${writable && known ? html`
          <label class="flex flex-col gap-2">Device PIN
            <input class="w-32" type="password" inputmode="numeric" pattern="[0-9]{4}" maxlength="4" autocomplete="off"
              value=${pin} onInput=${(event) => setPin(event.target.value)} />
          </label>
          <${AsyncButton}
            small
            description=${`${locked ? "unlock" : "lock"} ${requestName}`}
            onRun=${async () => {
              if (!/^\d{4}$/.test(pin)) throw new Error("Enter the 4-digit device PIN");
              const result = locked ? await api.unlock(requestName, pin) : await api.lock(requestName, pin);
              setPin("");
              await refresh();
              return result;
            }}
          >
            ${locked ? "Unlock" : "Lock"}
          <//>
        ` : null}
      </div>
    <//></div>
  `;
}
