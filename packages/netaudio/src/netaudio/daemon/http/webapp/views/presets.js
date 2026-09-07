import { api } from "../api.js";
import { Panel } from "../components.js";
import { Icon } from "../icons.js";
import { html, useLayoutEffect, useRef, useState } from "../lib/preact.js";
import { inventoryReady, scopedDevices, selectedContext } from "../store.js";

function readable(message) {
  const text = String(message || "Preset operation failed. Review the selection and try again.");
  return /\b0x[\da-f]+\b|\b[\da-f]{16,}\b/i.test(text)
    ? "The device returned an unavailable or unsupported response. Refresh its status before trying again." : text;
}

function SavePreset() {
  const devices = Object.entries(scopedDevices.value);
  const [name, setName] = useState("");
  const [selected, setSelected] = useState([]);
  const [sections, setSections] = useState(["routing"]);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const ready = inventoryReady.value;
  const valid = selected.length > 0 && selected.every((id) => scopedDevices.value[id]?.online);
  const toggle = (values, value, enabled) => enabled ? [...values, value] : values.filter((item) => item !== value);
  async function save(event) {
    event.preventDefault();
    if (busy || !ready || !valid || !sections.length) return;
    setBusy(true); setError(""); setMessage("");
    try {
      const result = await api.savePreset({ name, devices: selected, sections });
      const url = URL.createObjectURL(new Blob([result.xml], { type: "application/xml" }));
      const link = document.createElement("a");
      link.href = url; link.download = result.filename;
      document.body.append(link); link.click(); link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      setMessage(`Downloaded ${result.filename}`);
    } catch (failure) { setError(readable(failure.message)); }
    finally { setBusy(false); }
  }
  return html`<${Panel} title="Save preset">
    <form class="flex flex-col gap-4" onSubmit=${save}>
      <label class="flex flex-col gap-2">Preset name<input required maxlength="120" value=${name} disabled=${busy} onInput=${(event) => setName(event.target.value)} placeholder="Show setup" /></label>
      <fieldset disabled=${busy} class="flex flex-col gap-2"><legend class="mb-2">Devices in this view</legend>
        ${devices.length ? devices.map(([id, device]) => html`<label key=${id} class="flex items-center gap-2">
          <input type="checkbox" checked=${selected.includes(id)} disabled=${!device.online || !ready} onChange=${(event) => setSelected(toggle(selected, id, event.target.checked))} />
          <span>${device.name}${device.ipv4 ? ` · ${device.ipv4}` : ""}${device.online ? "" : " · Offline"}</span>
        </label>`) : html`<p class="text-sm">No devices in this view.</p>`}
      </fieldset>
      <fieldset disabled=${busy} class="flex flex-col gap-2"><legend class="mb-2">Include</legend>
        ${[["routing", "Receiver routing and transmitter names"], ["audio", "Sample rate, encoding, latency and preferred leader"], ["network", "Network settings (single interface only)"]].map(([id, label]) => html`<label key=${id} class="flex items-center gap-2"><input type="checkbox" checked=${sections.includes(id)} onChange=${(event) => setSections(toggle(sections, id, event.target.checked))} />${label}</label>`)}
      </fieldset>
      <div><button type="submit" class="btn btn-primary btn-sm" disabled=${busy || !ready || !valid || !name.trim() || !sections.length}><${Icon} name="download" />${busy ? "Reading settings…" : "Download XML"}</button></div>
      ${message ? html`<p role="status" class="text-sm">${message}</p>` : null}
      ${error ? html`<p role="alert" class="alert alert-error">${error}</p>` : null}
    </form>
  <//>`;
}

function LoadPreset() {
  const [xml, setXml] = useState("");
  const [preview, setPreview] = useState(null);
  const [choices, setChoices] = useState([]);
  const [confirmed, setConfirmed] = useState(false);
  const [destructive, setDestructive] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);
  const request = useRef(0);
  useLayoutEffect(() => () => { request.current += 1; }, []);
  const selected = choices.filter((choice) => choice.include);
  const valid = selected.length > 0 && selected.every((choice) => {
    const record = scopedDevices.value[choice.target];
    return record?.online && record.name === choice.name;
  }) && new Set(selected.map((choice) => choice.target)).size === selected.length;
  async function open(file) {
    const token = ++request.current;
    setPreview(null); setChoices([]); setConfirmed(false); setDestructive(false); setResult(null); setError(""); setXml("");
    if (!file) return;
    setBusy(true);
    try {
      if (file.size > 4 * 1024 * 1024) throw new Error("Preset files must be no larger than 4 MiB.");
      const content = await file.text();
      if (token !== request.current) return;
      const data = await api.previewPreset({ xml: content, devices: Object.keys(scopedDevices.value) });
      if (token !== request.current) return;
      setXml(content); setPreview(data);
      setChoices(data.devices.map((device) => {
        const eligible = device.targets.filter((target) => target.online);
        return { name: device.name, include: true, target: eligible.length === 1 ? eligible[0].id : "" };
      }));
    } catch (failure) { if (token === request.current) setError(readable(failure.message)); }
    finally { if (token === request.current) setBusy(false); }
  }
  function change(index, patch) {
    setChoices(choices.map((choice, position) => position === index ? { ...choice, ...patch } : choice));
    setConfirmed(false); setResult(null);
  }
  async function apply(event) {
    event.preventDefault();
    if (busy || !confirmed || !valid || !inventoryReady.value || !preview) return;
    const token = ++request.current;
    setBusy(true); setError(""); setResult(null); setConfirmed(false);
    try {
      const data = await api.loadPreset({ xml, digest: preview.digest, confirmed: true,
        confirm_destructive: destructive,
        targets: Object.fromEntries(selected.map((choice) => [choice.name, choice.target])),
        excluded: choices.filter((choice) => !choice.include).map((choice) => choice.name) });
      if (token === request.current) setResult(data);
    } catch (failure) { if (token === request.current) setError(readable(failure.message)); }
    finally { if (token === request.current) setBusy(false); }
  }
  return html`<${Panel} title="Load preset">
    <div class="flex flex-col gap-4">
      <label class="flex flex-col gap-2">Preset XML file<input type="file" accept=".xml,application/xml,text/xml" disabled=${busy} onChange=${(event) => open(event.target.files[0])} /></label>
      <p class="text-sm">Opening a file only previews it. Devices match by name within the current view; choose explicitly when more than one matches.</p>
      ${preview ? html`<form class="flex flex-col gap-4" onSubmit=${apply}>
        <h3 class="font-semibold">${preview.name}</h3>
        <fieldset disabled=${busy} class="flex flex-col gap-4">
          ${preview.devices.map((device, index) => html`<div key=${device.name} class="flex flex-col gap-2 border border-base-300 rounded-lg p-3">
            <label class="flex items-center gap-2"><input type="checkbox" checked=${choices[index].include} onChange=${(event) => change(index, { include: event.target.checked })} /><span class="font-semibold">${device.name}</span></label>
            ${device.settings.length ? html`<ul class="text-sm">${device.settings.map((setting) => html`<li>${setting.label}: ${readable(setting.value)}</li>`)}</ul>` : html`<p class="text-sm">No supported settings in this entry.</p>`}
            ${device.unsupported.length ? html`<p class="text-sm text-error">Unsupported: ${device.unsupported.join(", ")}. Skip this device to continue.</p>` : null}
            ${choices[index].include ? html`<label class="flex flex-col gap-2 text-sm">Target for ${device.name}
              <select value=${choices[index].target} onChange=${(event) => change(index, { target: event.target.value })}>
                <option value="">${device.targets.some((target) => target.online) ? "Choose a device" : "No online matching device — skip this entry"}</option>
                ${device.targets.map((target) => html`<option value=${target.id} disabled=${!target.online}>${target.name}${target.address ? ` · ${target.address}` : ""}${target.context ? ` · ${target.context}` : ""}${target.online ? "" : " · Offline"}</option>`)}
              </select></label>` : html`<p class="text-sm">Skipped</p>`}
          </div>`)}
          <details><summary class="cursor-pointer">Advanced</summary><label class="flex items-start gap-2 mt-3 text-sm"><input type="checkbox" checked=${destructive} onChange=${(event) => { setDestructive(event.target.checked); setConfirmed(false); }} />Allow sample-rate changes that rebuild routing.</label></details>
          <p class="text-sm">Applying can interrupt audio or change network access. Changes are not rolled back automatically. No devices will be rebooted.</p>
          <label class="flex items-start gap-2"><input type="checkbox" checked=${confirmed} disabled=${!valid || !inventoryReady.value} onChange=${(event) => setConfirmed(event.target.checked)} />I have reviewed the selected devices and settings and want to apply them.</label>
        </fieldset>
        <div><button type="submit" class="btn btn-primary btn-sm" disabled=${busy || !valid || !confirmed || !inventoryReady.value || preview.devices.some((device, index) => choices[index].include && device.unsupported.length)}>${busy ? "Applying preset…" : `Apply to ${selected.length} device${selected.length === 1 ? "" : "s"}`}</button></div>
      </form>` : null}
      ${error ? html`<p role="alert" class="alert alert-error">${error}</p>` : null}
      ${result ? html`<div class="flex flex-col gap-2" role="status"><h3 class="font-semibold">${result.complete ? "Preset applied and verified" : "Preset not fully applied or verified"}</h3>
        <ul class="text-sm">${result.report.results.map(([name, message]) => html`<li>${name}: ${readable(message)}</li>`)}</ul>
        ${result.report.needs_reboot.length ? html`<p class="text-sm">Reboot pending: ${result.report.needs_reboot.join(", ")}. Review each device before rebooting.</p>` : null}
      </div>` : null}
    </div>
  <//>`;
}

function PresetsView() {
  return html`<div class="flex flex-col gap-6 w-full max-w-3xl"><h1 class="content-title">Presets</h1>
    <p class="text-sm">Save and load Dante preset XML files. Supported settings are receiver subscriptions, transmitter names, sample rate, encoding, latency, preferred leader and single-interface network configuration. This is not a full device backup: receiver names, AES67 and other settings are not restored.</p>
    ${!inventoryReady.value ? html`<p class="text-sm" role="status">Waiting for live inventory. You can still preview a file.</p>` : null}
    <${SavePreset} key=${`save:${selectedContext.value}`} />
    <${LoadPreset} key=${`load:${selectedContext.value}`} />
  </div>`;
}

export const presetsView = { component: PresetsView, id: "presets", label: "Presets" };
