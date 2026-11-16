import { api } from "../api.js";
import { APPEARANCES, appearance, setAppearance } from "../color-scheme.js";
import { AsyncButton, Fields, Panel } from "../components.js";
import {
  LANGUAGES,
  languagePreference,
  setLanguagePreference,
  t,
} from "../i18n.js";
import { Icon } from "../icons.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { backendSettings } from "../store.js";

function SettingsView() {
  const settings = backendSettings.value;
  const [editedPort, setPort] = useState(null);
  const port = editedPort ?? (settings ? String(settings.monitoring_port) : "");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    api
      .getSettings()
      .then((result) => {
        if (active) backendSettings.value = result;
      })
      .catch((failure) => {
        if (active) setError(failure.message);
      });
    return () => {
      active = false;
    };
  }, []);
  const changed = settings && Number(port) !== settings.monitoring_port;
  return html`<div class="flex flex-col gap-6 w-full max-w-lg">
    <${Panel} title=${t("Appearance")}>
      <div>
        <div class="join" role="group" aria-label=${t("Appearance")}>
          ${APPEARANCES.map(
            ([id, label]) => html`<button
              key=${id}
              type="button"
              class=${`btn btn-sm join-item${appearance.value === id ? " btn-primary" : ""}`}
              aria-pressed=${appearance.value === id ? "true" : "false"}
              onClick=${() => setAppearance(id)}
            >
              ${label}
            </button>`,
          )}
        </div>
      </div>
    <//>
    <${Panel} title=${t("Language")}>
      <select class="w-64" aria-label=${t("Language")} value=${languagePreference()} onChange=${(event) => setLanguagePreference(event.target.value)}>
        <option value="system">${t("System")}</option>
        ${LANGUAGES.map(([code, name]) => html`<option key=${code} value=${code} lang=${code}>${name}</option>`)}
      </select>
    <//>
    <${Panel} title=${t("Metering")}>
      <form class="flex flex-col items-start gap-4" onSubmit=${async (
        event,
      ) => {
        event.preventDefault();
        if (busy || !changed) return;
        setBusy(true);
        setError("");
        try {
          backendSettings.value = await api.setMonitoringPort(Number(port));
          setPort(null);
        } catch (failure) {
          setError(failure.message);
        } finally {
          setBusy(false);
        }
      }}>
        <label class="flex flex-col gap-2">${t("UDP port")}<input class="w-32" type="number" min="1024" max="65535" required disabled=${busy || !settings} value=${port} onInput=${(event) => setPort(event.target.value)} /></label>
        ${Number(port) === 8751 ? html`<p class="text-sm text-warning" role="alert"><${Icon} name="warning" /> ${t("Dante Controller meters on UDP 8751. Use another port, such as 8752.")}</p>` : null}
        ${settings && settings.active_monitoring_port !== settings.monitoring_port ? html`<p class="text-sm" role="status">${settings.active_monitoring_port == null ? t("Not listening") : t("Currently listening on port {port}", { port: settings.active_monitoring_port })}</p>` : null}
        <button class="btn btn-sm btn-primary" type="submit" disabled=${busy || !changed}><${Icon} name="check" />${busy ? t("Saving…") : t("Save")}</button>
      </form>
      ${error ? html`<div class="alert alert-error mt-3" role="alert">${t(error)}</div>` : null}
    <//>
    <${DanteNetwork} settings=${settings} />
    <${LockKey} />
  </div>`;
}

const QUIET_ZONE = 4;

function LockKeyCode({ modules }) {
  const size = modules.length + QUIET_ZONE * 2;
  const path = modules
    .flatMap((row, y) =>
      [...row].map((module, x) =>
        module === "1" ? `M${x + QUIET_ZONE} ${y + QUIET_ZONE}h1v1h-1z` : "",
      ),
    )
    .join("");
  return html`<svg
    class="lock-key-code"
    role="img"
    aria-label=${t("QR code")}
    viewBox=${`0 0 ${size} ${size}`}
    shape-rendering="crispEdges"
  >
    <rect width=${size} height=${size} fill="#ffffff" />
    <path d=${path} fill="#000000" />
  </svg>`;
}

function chooseFile(accept) {
  return new Promise((resolve) => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = accept;
    input.addEventListener("change", () => resolve(input.files[0] ?? null), {
      once: true,
    });
    input.addEventListener("cancel", () => resolve(null), { once: true });
    input.click();
  });
}

function LockKey() {
  const [found, setFound] = useState(null);
  return html`<${Panel} title=${t("Device lock key")}>
    <div class="flex flex-col items-start gap-4">
      <div class="flex flex-wrap items-start gap-2">
        <${AsyncButton}
          small
          description=${t("Read from Dante Controller")}
          onRun=${async () => {
            setFound(null);
            const result = await api.readInstalledLockKey();
            setFound(result);
            return result;
          }}
        >
          ${t("Read from Dante Controller")}
        <//>
        <${AsyncButton}
          small
          description=${t("Read from installer")}
          onRun=${async () => {
            const file = await chooseFile(".dmg,.exe,.msi,.pkg");
            if (!file) return null;
            setFound(null);
            const result = await api.readInstallerLockKey(file);
            setFound({ ...result, source: file.name });
            return result;
          }}
        >
          ${t("Read from installer")}
        <//>
      </div>
      ${
        found
          ? html`<${Fields}
                entries=${[
                  [t("Key"), html`<span class="font-mono">${found.key}</span>`],
                  [
                    t("Version"),
                    found.version
                      ? `${found.version}, ${found.platform}`
                      : null,
                  ],
                  [t("Source"), found.source],
                ]}
              />
              <${LockKeyCode} modules=${found.modules} />`
          : null
      }
    </div>
  <//>`;
}

function DanteNetwork({ settings }) {
  const [edited, setEdited] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const saved = settings?.dante_interfaces ?? [];
  const present = settings?.network_interfaces ?? [];
  const known = new Set(
    present.flatMap((entry) => [entry.name, ...entry.addresses]),
  );
  const rows = [
    ...present.map((entry) => ({
      addresses: entry.addresses,
      name: entry.name,
      saved:
        saved.includes(entry.name) ||
        entry.addresses.some((address) => saved.includes(address)),
    })),
    ...saved
      .filter((name) => !known.has(name))
      .map((name) => ({ addresses: [], name, saved: true })),
  ];
  const savedNames = rows.filter((row) => row.saved).map((row) => row.name);
  const chosen = edited ?? savedNames;
  const every = chosen.length === 0;
  const changed =
    settings &&
    (chosen.length !== savedNames.length ||
      chosen.some((name) => !savedNames.includes(name)));
  return html`<${Panel} title=${t("Dante network")}>
    <form
      class="flex flex-col items-start gap-4"
      onSubmit=${async (event) => {
        event.preventDefault();
        if (busy || !changed) return;
        setBusy(true);
        setError("");
        try {
          backendSettings.value = await api.setDanteInterfaces(chosen);
          setEdited(null);
        } catch (failure) {
          setError(failure.message);
        } finally {
          setBusy(false);
        }
      }}
    >
      <fieldset class="flex flex-col gap-2">
        <label class="flex items-center gap-2"
          ><input
            type="checkbox"
            checked=${every}
            disabled=${busy || !settings}
            onChange=${(event) =>
              setEdited(
                event.target.checked ? [] : rows.map((row) => row.name),
              )}
          />${t("All interfaces")}</label
        >
        ${rows.map(
          (row) =>
            html`<label key=${row.name} class="flex items-center gap-2"
              ><input
                type="checkbox"
                checked=${every || chosen.includes(row.name)}
                disabled=${busy || every}
                onChange=${(event) =>
                  setEdited(
                    event.target.checked
                      ? [...chosen, row.name]
                      : chosen.filter((name) => name !== row.name),
                  )}
              />${[row.name, ...row.addresses].join(" ")}</label
            >`,
        )}
      </fieldset>
      <button class="btn btn-sm btn-primary" type="submit" disabled=${busy || !changed}><${Icon} name="check" />${busy ? t("Saving…") : t("Save")}</button>
    </form>
    ${error ? html`<div class="alert alert-error mt-3" role="alert">${t(error)}</div>` : null}
  <//>`;
}

export const settingsView = {
  component: SettingsView,
  filters: false,
  id: "settings",
  label: t("Settings"),
};
