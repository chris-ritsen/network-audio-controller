import { effect, signal } from "./lib/preact.js";

export const DEFAULT_PATH = "/routing";

const ROUTES = [
  { pattern: "/clock-status", view: "clock-status" },
  { pattern: "/devices", view: "devices" },
  { pattern: "/devices/:device", view: "devices" },
  { pattern: "/devices/:device/:section", view: "devices" },
  { pattern: "/network-status", view: "network-status" },
  { pattern: "/routing", view: "routing" },
  { pattern: "/metering", view: "metering" },
  { pattern: "/metering/:device", view: "metering" },
  { pattern: "/flows", view: "flows" },
  { pattern: "/flows/:device", view: "flows" },
  { pattern: "/ddm", view: "ddm" },
  { pattern: "/shure", view: "shure" },
  { pattern: "/shure/:device", view: "shure" },
  { pattern: "/events", view: "events" },
];

function matchPattern(pattern, segments) {
  const patternSegments = pattern.split("/").filter((part) => part.length);
  if (patternSegments.length !== segments.length) {
    return null;
  }
  const parameters = {};
  for (let index = 0; index < patternSegments.length; index += 1) {
    const patternSegment = patternSegments[index];
    if (patternSegment.startsWith(":")) {
      parameters[patternSegment.slice(1)] = decodeURIComponent(segments[index]);
      continue;
    }
    if (patternSegment !== segments[index]) {
      return null;
    }
  }
  return parameters;
}

export function resolve(pathname, search) {
  const segments = pathname.split("/").filter((part) => part.length);
  const query = Object.fromEntries(new URLSearchParams(search || ""));
  for (const route of ROUTES) {
    const parameters = matchPattern(route.pattern, segments);
    if (parameters) {
      return { found: true, parameters, path: pathname, query, view: route.view };
    }
  }
  return { found: false, parameters: {}, path: pathname, query, view: null };
}

function currentLocation() {
  return resolve(window.location.pathname, window.location.search);
}

export const location = signal(currentLocation());

export function devicePath(view, deviceName, section) {
  const encoded = encodeURIComponent(deviceName);
  return section ? `/${view}/${encoded}/${section}` : `/${view}/${encoded}`;
}

export function navigate(path, { replace = false } = {}) {
  const target = path || DEFAULT_PATH;
  if (target === `${window.location.pathname}${window.location.search}`) {
    return;
  }
  if (replace) {
    window.history.replaceState({}, "", target);
  } else {
    window.history.pushState({}, "", target);
  }
  publish();
}

export function setQueryParameter(name, value) {
  const parameters = new URLSearchParams(window.location.search);
  if (value === null || value === undefined || value === "") {
    parameters.delete(name);
  } else {
    parameters.set(name, String(value));
  }
  const serialized = parameters.toString();
  window.history.replaceState(
    {},
    "",
    serialized ? `${window.location.pathname}?${serialized}` : window.location.pathname,
  );
  publish();
}

function publish() {
  location.value = currentLocation();
}

function shouldInterceptClick(event) {
  return !(
    event.defaultPrevented ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey
  );
}

export function onNavigate(listener) {
  let previous = location.value.path;
  return effect(() => {
    const current = location.value;
    if (current.path !== previous) {
      previous = current.path;
      listener(current);
    }
  });
}

export function startRouter() {
  window.addEventListener("popstate", publish);
  document.addEventListener("click", (event) => {
    if (!shouldInterceptClick(event)) {
      return;
    }
    const anchor = event.target.closest ? event.target.closest("a[href]") : null;
    if (!anchor || anchor.target === "_blank" || anchor.hasAttribute("download")) {
      return;
    }
    const url = new URL(anchor.href, window.location.origin);
    if (url.origin !== window.location.origin) {
      return;
    }
    event.preventDefault();
    navigate(`${url.pathname}${url.search}`);
  });
  if (!location.value.found) {
    window.history.replaceState({}, "", DEFAULT_PATH);
    publish();
  }
}
