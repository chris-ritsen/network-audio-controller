import { mkdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";
import { chromium } from "@playwright/test";
import { serveWebapp } from "../../tests/webapp/browser/fixture.mjs";

const SHOTS = [
  { name: "clock-status", path: "/clock-status", viewport: { width: 1280, height: 600 } },
  { name: "device-a32-metering", path: "/devices/a32/metering", viewport: { width: 1100, height: 700 } },
  { name: "device-a32-receive", path: "/devices/a32/receive", viewport: { width: 1100, height: 700 } },
  { name: "device-a32-status", path: "/devices/a32/status", viewport: { width: 960, height: 760 } },
  { name: "device-avio-bt-1-bluetooth", path: "/devices/avio-bt-1/device-config", scrollTo: "Bluetooth", viewport: { width: 960, height: 700 } },
  { name: "device-lx-dante-network", path: "/devices/lx-dante/network-config", viewport: { width: 960, height: 680 } },
  {
    name: "routing",
    path: "/routing",
    scale: 4 / 3,
    storage: {
      "netaudio.matrix.expanded": { receivers: ["a32"], transmitters: ["ad4d", "avio-aes3-1", "avio-bt-1", "avio-usb-1", "avio-usb-4", "avio-usb-macbook-1", "avio-usb-tv-1"] },
      "netaudio.routing.filters": { panelOpen: false, receiverSearch: "a32" },
    },
    viewport: { width: 1200, height: 750 },
  },
  { name: "subscriptions", path: "/subscriptions", viewport: { width: 1200, height: 720 } },
];

const { values } = parseArgs({
  options: {
    fixture: { type: "string" },
    output: { type: "string", default: "build/screenshots/web" },
  },
});
if (!values.fixture) {
  console.error("usage: node scripts/webapp/screenshots.mjs --fixture FIXTURE.json [--output DIRECTORY]");
  process.exit(2);
}

const fixture = JSON.parse(await readFile(resolve(values.fixture), "utf8"));
const meteringScale = JSON.parse(await readFile(new URL("../../tests/webapp/fixtures/metering-scale.json", import.meta.url), "utf8"));
const levels = meteringScale.detailed.filter((entry) => entry.dbfs !== null && entry.state !== "muted");
const mutedLevel = meteringScale.detailed.find((entry) => entry.state === "muted")?.raw ?? 255;

function rawLevel(decibels) {
  return levels.reduce((best, entry) => (Math.abs(entry.dbfs - decibels) < Math.abs(best.dbfs - decibels) ? entry : best)).raw;
}

function meteringFor(serverName, device) {
  const meters = fixture.meters?.[serverName] || {};
  const side = (direction, channels) =>
    Object.fromEntries(Object.keys(channels || {}).map((number) => [number, meters[direction]?.[number] === undefined ? mutedLevel : rawLevel(meters[direction][number])]));
  return {
    metering_source: "detailed",
    rx: side("rx", device.channels?.receivers),
    tx: side("tx", device.channels?.transmitters),
    wall_time: Date.now() / 1000,
  };
}

function serverName(reference) {
  if (fixture.devices[reference]) return reference;
  return Object.keys(fixture.devices).find((key) => fixture.devices[key].name === reference) ?? reference;
}

async function answerFromFixture(page) {
  const json = (body) => ({ contentType: "application/json", body: JSON.stringify(body) });
  for (const [prefix, section] of [["/diagnostics/", "diagnostics"], ["/interfaces/", "interfaces"]]) {
    await page.route(`**${prefix}**`, (route) => {
      const name = serverName(decodeURIComponent(new URL(route.request().url()).pathname.slice(prefix.length)));
      const body = fixture[section]?.[name];
      return body ? route.fulfill(json(body)) : route.fulfill({ status: 404, ...json({ error: "device not found" }) });
    });
  }
  await page.route("**/metering/**", (route) => route.fulfill(json({ success: true })));
}

const metering = Object.fromEntries(Object.entries(fixture.devices).map(([serverName, device]) => [serverName, meteringFor(serverName, device)]));
const output = resolve(values.output);
const APPEARANCES = [
  { colorScheme: "dark", directory: output },
  { colorScheme: "light", directory: join(output, "light") },
];
for (const appearance of APPEARANCES) await mkdir(appearance.directory, { recursive: true });
const browser = await chromium.launch();
try {
  for (const shot of SHOTS) {
    for (const appearance of APPEARANCES) {
      const context = await browser.newContext({ colorScheme: appearance.colorScheme, deviceScaleFactor: shot.scale ?? 2, viewport: shot.viewport });
      const page = await context.newPage();
      await page.addInitScript((storage) => {
        for (const [key, value] of Object.entries(storage)) window.localStorage.setItem(key, JSON.stringify(value));
      }, { "netaudio.routing.filters": { panelOpen: false }, ...shot.storage });
      await serveWebapp(page, { devices: fixture.devices, metering, meteringScale });
      await answerFromFixture(page);
      await page.goto(`http://netaudio.test${shot.path}`, { waitUntil: "networkidle" });
      if (shot.scrollTo) await page.getByText(shot.scrollTo, { exact: true }).first().scrollIntoViewIfNeeded();
      await page.waitForTimeout(1000);
      await page.screenshot({ path: join(appearance.directory, `${shot.name}.png`) });
      await context.close();
    }
    console.log(`web: ${shot.name}`);
  }
} finally {
  await browser.close();
}
