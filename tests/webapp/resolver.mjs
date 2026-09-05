const WEBAPP_VENDOR = new URL(
  "../../packages/netaudio/src/netaudio/daemon/http/webapp/vendor/",
  import.meta.url,
);
const TEST_VENDOR = new URL("./vendor/", import.meta.url);

const BARE_SPECIFIERS = {
  "@preact/signals": new URL("signals.module.js", WEBAPP_VENDOR).href,
  "@preact/signals-core": new URL("signals-core.module.js", WEBAPP_VENDOR).href,
  htm: new URL("htm.module.js", WEBAPP_VENDOR).href,
  preact: new URL("preact.module.js", WEBAPP_VENDOR).href,
  "preact-render-to-string": new URL("preact-render-to-string.module.js", TEST_VENDOR).href,
  "preact/hooks": new URL("preact-hooks.module.js", WEBAPP_VENDOR).href,
};

export async function resolve(specifier, context, nextResolve) {
  const mapped = BARE_SPECIFIERS[specifier];
  if (mapped) {
    return { shortCircuit: true, url: mapped };
  }
  return nextResolve(specifier, context);
}
