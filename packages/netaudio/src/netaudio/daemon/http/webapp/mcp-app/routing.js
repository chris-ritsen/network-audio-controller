import { setRequestTransport } from "../api.js";
import { setAppearance } from "../color-scheme.js";
import { Button } from "../components.js";
import { readRoutingFilters, saveRoutingFilters } from "../device-filters.js";
import { loadLanguage, setCatalogLoader, t } from "../i18n.js";
import { html, render, signal } from "../lib/preact.js";
import { setNavigator } from "../router.js";
import { connectionState, receiveEvent } from "../store.js";
import { routingLayout, routingView } from "../views/routing.js";
import { callTool, connect, onNotification, onRequest, request } from "./host.js";

const INLINE_HEIGHT = 640;
const COMPACT_QUERY = "(max-width: 520px)";
const POLL_WAIT_SECONDS = 15;
const RETRY_MILLISECONDS = 2000;

const displayMode = signal("inline");
const availableModes = signal([]);
let containerDimensions = null;
let running = true;

function applyHeight() {
  const fixed = displayMode.value !== "inline" || (containerDimensions && "height" in containerDimensions);
  const limit = containerDimensions?.maxHeight || INLINE_HEIGHT;
  document.documentElement.style.setProperty("--mcp-app-height", fixed ? "100vh" : `${Math.min(INLINE_HEIGHT, limit)}px`);
}

function applyContext(context) {
  if (context.theme === "light" || context.theme === "dark") setAppearance(context.theme);
  if (Array.isArray(context.availableDisplayModes)) availableModes.value = context.availableDisplayModes;
  if (context.containerDimensions) containerDimensions = context.containerDimensions;
  if (context.displayMode) displayMode.value = context.displayMode;
  applyHeight();
}

function applyToolInput(args) {
  saveRoutingFilters({
    ...readRoutingFilters(),
    search: "",
    values: {},
    listMode: false,
    receiverSearch: typeof args.receivers === "string" ? args.receivers : "",
    transmitterSearch: typeof args.transmitters === "string" ? args.transmitters : "",
  });
}

async function pollEvents() {
  let session = null;
  while (running) {
    try {
      const result = await callTool("app_events", session ? { session, wait_seconds: POLL_WAIT_SECONDS } : {});
      session = result.session;
      connectionState.value = "open";
      for (const event of result.events || []) receiveEvent(event);
    } catch (error) {
      connectionState.value = "closed";
      console.warn("netaudio could not read device events", error);
      await new Promise((resolve) => setTimeout(resolve, RETRY_MILLISECONDS));
    }
  }
}

async function toggleFullscreen() {
  const result = await request("ui/request-display-mode", { mode: displayMode.value === "fullscreen" ? "inline" : "fullscreen" });
  if (result?.mode) {
    displayMode.value = result.mode;
    applyHeight();
  }
}

function RoutingApp() {
  const View = routingView.component;
  return html`<div class="mcp-app">
    <main class="content" id="content"><${View} /></main>
    ${availableModes.value.includes("fullscreen")
      ? html`<div class="mcp-app-display"><${Button} onClick=${toggleFullscreen}>${displayMode.value === "fullscreen" ? t("Exit full screen") : t("Full screen")}<//></div>`
      : null}
  </div>`;
}

routingLayout.compactQuery = COMPACT_QUERY;
setNavigator(() => {});
setCatalogLoader(async (language) => (await callTool("app_translations", { language })).catalog);
setRequestTransport((method, path, body) => callTool("app_request", body === undefined ? { method, path } : { method, path, body }));
onNotification("ui/notifications/tool-input", (params) => applyToolInput(params.arguments || {}));
onNotification("ui/notifications/host-context-changed", applyContext);
onRequest("ui/resource-teardown", () => {
  running = false;
  return {};
});

applyToolInput({});
applyHeight();
const initialized = await connect({ name: "netaudio routing", version: "1", displayModes: ["inline", "fullscreen"] });
const context = initialized.hostContext || {};
applyContext(context);
pollEvents();
await loadLanguage(context.locale);
render(html`<${RoutingApp} />`, document.getElementById("view"));
