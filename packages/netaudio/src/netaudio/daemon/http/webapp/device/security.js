import { api } from "../api.js";
import {AsyncButton, FieldRow, Fields, Panel, Value} from "../components.js";
import * as format from "../format.js";
import { html, useCallback, useRef, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";
import { runAction } from "../toast.js";

export function LockSection({ device }) {
  const requestName = deviceRequestName(device);
  const [observation, setObservation] = useState(null);
  const pin = useRef(null);

  const probeLockStatus = useCallback(async () => {
    const outcome = await runAction(`probe lock status on ${requestName}`, () => api.getLockStatus(requestName));
    if (outcome.ok) {
      setObservation(outcome.result);
    }
    return outcome.result;
  }, [requestName]);

  return html`
    <${Panel}
      title="Device lock"
      actions=${html`<${AsyncButton}
        small
        description=${`probe lock status on ${requestName}`}
        onRun=${() => api.getLockStatus(requestName).then((result) => {
          setObservation(result);
          return result;
        })}
      >
        Probe lock status
      <//>`}
    >
      <${FieldRow} label="Device PIN">
        <input key=${`lock-pin-${requestName}`} ref=${pin} type="password" size="10" autocomplete="off" />
        <${AsyncButton}
          small
          description=${`lock ${requestName}`}
          onRun=${async () => {
            const result = await api.lock(requestName, pin.current.value);
            await probeLockStatus();
            return result;
          }}
        >
          Lock
        <//>
        <${AsyncButton}
          small
          description=${`unlock ${requestName}`}
          onRun=${async () => {
            const result = await api.unlock(requestName, pin.current.value);
            await probeLockStatus();
            return result;
          }}
        >
          Unlock
        <//>
      <//>
      <${Fields}
        entries=${[
          ["Locked", device.is_locked ? "locked" : "unlocked"],
          ["Lock reset status", html`<${Value} value=${device.lock_reset_status} />`],
          ["License signature length", html`<${Value} value=${device.license_signature_length_bytes} />`],
          ["Clear configuration status", html`<${Value} value=${device.clear_configuration_status} />`],
          ["Diagnostic log export supported", html`<${Value} value=${device.diagnostic_log_export_supported} />`],
        ]}
      />
      ${observation
        ? html`
            <div class="section-label">Last probe</div>
            <${Fields}
              entries=${[
                ["Probed lock state", html`<${Value} value=${observation.is_locked} />`],
                ["Lock state code", html`<${Value} value=${observation.lock_state_code} />`],
                ["Status code", html`<${Value} value=${observation.status_code} />`],
                ["Observed at", format.timestamp(observation.observed_at)],
                ["Observation source", html`<${Value} value=${observation.observation_source} />`],
              ]}
            />
          `
        : null}
    <//>
  `;
}
