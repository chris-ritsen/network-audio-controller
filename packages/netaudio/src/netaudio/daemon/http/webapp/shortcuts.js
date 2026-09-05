const APPLE_PLATFORM = /mac|iphone|ipad|ipod/i;

function platformName() {
  const data = navigator.userAgentData;
  if (data && typeof data.platform === "string" && data.platform) {
    return data.platform;
  }
  return navigator.platform || navigator.userAgent || "";
}

export const usesCommandKey = APPLE_PLATFORM.test(platformName());

export const commandKeyLabel = usesCommandKey ? "⌘" : "Ctrl";

export function shortcutLabel(key) {
  return usesCommandKey ? `${commandKeyLabel}${key}` : `${commandKeyLabel}+${key}`;
}

export function matchesCommandKey(event) {
  return usesCommandKey ? event.metaKey : event.ctrlKey;
}
