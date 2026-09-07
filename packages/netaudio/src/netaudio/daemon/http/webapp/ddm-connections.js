import { api } from "./api.js";
import { Panel } from "./components.js";
import { Icon } from "./icons.js";
import { NewDomainDialog } from "./new-domain.js";
import { html, useEffect, useLayoutEffect, useRef, useState } from "./lib/preact.js";
import { connectionProfiles, managedDomains, selectContext, selectedContext } from "./store.js";

export function ContextSelector() {
  const contexts = connectionProfiles.value?.contexts || [];
  const servers = connectionProfiles.value?.servers || [];
  const domains = managedDomains.value;
  const options = [
    { value: "all", label: "All devices" }, { value: "local", label: "Unmanaged" },
    ...servers.map((server) => ({ value: `server:${server.name}`, label: `${server.name} · All domains` })),
    ...contexts.map((context) => ({ value: context.name, label: `${context.server} · ${context.domain_name || "Domain"}` })),
    ...domains.filter((domain) => !contexts.some((context) => context.server === domain.ddm_server_profile && context.domain_id === domain.id))
      .map((domain) => ({ value: `domain:${JSON.stringify([domain.ddm_server_profile, domain.id])}`, label: `${domain.ddm_server_profile} · ${domain.name || "Domain"}` })),
  ];
  const current = selectedContext.value;
  const selected = options.find((option) => option.value === current);
  let rememberedLabel = current;
  try {
    const saved = JSON.parse(window.localStorage.getItem("netaudio.context-label"));
    if (saved?.value === current && typeof saved.label === "string") rememberedLabel = saved.label;
  } catch {}
  useLayoutEffect(() => {
    if (!selected) return;
    try { window.localStorage.setItem("netaudio.context-label", JSON.stringify(selected)); } catch {}
  }, [current, selected?.label]);
  if (!selected) options.push({ value: current, label: rememberedLabel });
  return html`<select aria-label="Server and domain" class="context-selector" value=${current}
    onChange=${(event) => {
      const option = options.find((item) => item.value === event.target.value);
      try { window.localStorage.setItem("netaudio.context-label", JSON.stringify(option)); } catch {}
      selectContext(event.target.value);
    }}>
    ${options.map((option) => html`<option key=${option.value} value=${option.value}>${option.label}</option>`)}
  </select>`;
}

export function DdmConnections() {
  const config = connectionProfiles.value;
  const [server, setServer] = useState("");
  const [method, setMethod] = useState("saved");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [creatingDomain, setCreatingDomain] = useState(false);
  const name = useRef(null), url = useRef(null), username = useRef(null), secret = useRef(null);
  const profiles = config?.servers || [];
  const profile = profiles.find((entry) => entry.name === server);
  const domains = managedDomains.value.filter((domain) => domain.ddm_server_profile === server);

  useEffect(() => {
    let active = true;
    api.getConnections().then((result) => { if (active) connectionProfiles.value = result; })
      .catch((failure) => { if (active) setError(failure.message); });
    return () => { active = false; };
  }, []);
  useEffect(() => {
    if (!config || server) return;
    const preferred = config.contexts.find((context) => context.name === config.default_context)?.server;
    setServer(preferred || config.servers[0]?.name || "new");
    if (!config.servers.find((entry) => entry.name === (preferred || config.servers[0]?.name))?.configured) setMethod("password");
  }, [config]);

  const login = async (event) => {
    event.preventDefault();
    if (busy) return;
    setBusy(true); setError("");
    try {
      const body = { server: profile?.name || name.current.value, url: profile?.url || url.current.value, method };
      if (method === "password") { body.username = username.current.value; body.password = secret.current.value; }
      if (method === "api_key") body.api_key = secret.current.value;
      const result = await api.loginDdm(body);
      connectionProfiles.value = result;
      setServer(body.server);
      setMethod("saved");
    } catch (failure) { setError(failure.message); }
    finally { if (secret.current) secret.current.value = ""; setBusy(false); }
  };
  const chooseDomain = async (domainId) => {
    if (!domainId || busy) return;
    setBusy(true); setError("");
    try {
      const result = await api.selectDdmContext({ server, domain_id: domainId });
      connectionProfiles.value = result;
      selectContext(result.default_context);
    } catch (failure) { setError(failure.message); }
    finally { setBusy(false); }
  };
  const logout = async () => {
    setBusy(true); setError("");
    try {
      connectionProfiles.value = await api.logoutDdm(server);
      setMethod("password");
      selectContext("all");
    } catch (failure) { setError(failure.message); }
    finally { setBusy(false); }
  };
  const selected = config?.contexts.find((context) => context.name === selectedContext.value);
  return html`<${Panel} title="Connection">
    <form class="grid grid-cols-1 md:grid-cols-2 gap-6 md:gap-8 w-full max-w-2xl" onSubmit=${login}>
      <div class="flex flex-col gap-4 min-w-0">
        <label class="flex flex-col gap-2">Server
        <select class="w-full" aria-label="DDM server" disabled=${busy} value=${server} onChange=${(event) => {
          setServer(event.target.value); setMethod(profiles.find((entry) => entry.name === event.target.value)?.configured ? "saved" : "password"); setError("");
        }}>${profiles.map((entry) => html`<option value=${entry.name}>${entry.name}</option>`)}<option value="new">Add server</option></select>
      </label>
      ${profile ? html`<label class="flex flex-col gap-2">Address<input class="w-full" readonly value=${profile.url} title=${profile.url} /></label>` : html`
        <label class="flex flex-col gap-2">Profile name<input class="w-full" required ref=${name} placeholder="Studio" /></label>
        <label class="flex flex-col gap-2">Server URL<input class="w-full" required type="url" ref=${url} placeholder="https://ddm.example/graphql" /></label>`}
      ${domains.length ? html`<label class="flex flex-col gap-2">Domain
        <select class="w-full" aria-label="DDM domain" disabled=${busy} value=${selected?.server === server ? selected.domain_id : ""} onChange=${(event) => chooseDomain(event.target.value)}>
          <option value="">Choose domain</option>${domains.map((domain) => html`<option value=${domain.id}>${domain.name || "Unnamed domain"}</option>`)}
        </select></label>` : null}
      ${profile?.configured ? html`<button class="btn btn-sm self-start" type="button" disabled=${busy} onClick=${() => setCreatingDomain(true)}><${Icon} name="plus" />New domain</button>` : null}
      </div>
      <div class="flex flex-col gap-4 min-w-0">
      <label class="flex flex-col gap-2">Authentication<select class="w-full" aria-label="Authentication" value=${method} disabled=${busy} onChange=${(event) => setMethod(event.target.value)}>
        ${profile?.configured ? html`<option value="saved">Saved credentials</option>` : null}
        <option value="password">Username and password</option><option value="api_key">API key</option>
      </select></label>
      ${method === "password" ? html`<label class="flex flex-col gap-2">Username<input class="w-full" ref=${username} required autocomplete="username" /></label>` : null}
      ${method !== "saved" ? html`<label class="flex flex-col gap-2">${method === "api_key" ? "API key" : "Password"}<input class="w-full" key=${method} ref=${secret} required type="password" autocomplete=${method === "password" ? "current-password" : "off"} /></label>` : null}
      <div class="flex flex-wrap gap-2 pt-1">
      <button class="btn btn-sm btn-primary" type="submit" disabled=${busy}><${Icon} name="plug" />${busy ? "Connecting…" : "Connect"}</button>
      ${profile?.configured ? html`<button class="btn btn-sm" type="button" disabled=${busy} onClick=${logout}><${Icon} name="unplug" />Log out</button>` : null}
      </div>
      </div>
    </form>
    ${error ? html`<div class="alert alert-error mt-3" role="alert">${error}</div>` : null}
    ${creatingDomain ? html`<${NewDomainDialog} server=${server} onClose=${() => setCreatingDomain(false)} />` : null}
  <//>`;
}
