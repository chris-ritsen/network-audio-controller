import { readFileSync, writeFileSync } from "node:fs";

const names = {
  routing: "network",
  subscriptions: "list-tree",
  devices: "server",
  ddm: "network",
  shure: "mic",
  events: "list",
  settings: "settings",
  presets: "files",
  download: "download",
  check: "check",
  warning: "triangle-alert",
  plus: "plus",
  minus: "minus",
  chevron: "chevron-right",
  back: "chevron-left",
  unplug: "unplug",
  plug: "plug",
  search: "search",
  menu: "menu",
  sort: "arrow-up-down",
  flip: "arrow-down-up",
  "sort-up": "arrow-up",
  "sort-down": "arrow-down",
  "subscription-ok": "circle-check",
  "subscription-error": "circle-x",
  "subscription-blocked": "ban",
  "subscription-none": "circle-minus",
  "subscription-unknown": "circle-help",
  "subscription-pending": "hourglass",
  "signal-muted": "volume-x",
  "signal-quiet": "volume",
  "signal-low": "volume-1",
  "signal-high": "volume-2",
  "signal-clipping": "triangle-alert",
  "signal-unknown": "circle-help",
};
const symbols = Object.fromEntries(Object.entries(names).map(([id, name]) => {
  const source = readFileSync(
    new URL(
      `../../node_modules/lucide-static/icons/${name}.svg`,
      import.meta.url,
    ),
    "utf8",
  );
  const body = source.match(/<svg[^>]*>([\s\S]*?)<\/svg>/)[1];
  return [id, body];
}));
symbols["subscription-unicast"] = '<circle cx="5" cy="12" r="4"/><path d="M9 12h13m-3-3 3 3-3 3"/>';
symbols["subscription-multicast"] = '<circle cx="5" cy="12" r="4"/><path d="M9 12h4m0 0V6q0-2 2-2h7m-9 8v6q0 2 2 2h7m-2-18 2 2-2 2m0 12 2 2-2 2"/>';
const output = new URL(
  "../../packages/netaudio/src/netaudio/daemon/http/webapp/",
  import.meta.url,
);
writeFileSync(
  new URL("icon-paths.js", output),
  `export const iconPaths = ${JSON.stringify(symbols, null, 2)};\n`,
);
writeFileSync(
  new URL("icon-license.txt", output),
  readFileSync(
    new URL("../../node_modules/lucide-static/LICENSE", import.meta.url),
  ),
);
writeFileSync(
  new URL("ui-licenses.txt", output),
  ["tailwindcss", "daisyui"]
    .map(
      (name) =>
        `${name}\n${readFileSync(new URL(`../../node_modules/${name}/LICENSE`, import.meta.url), "utf8")}`,
    )
    .join("\n\n"),
);
