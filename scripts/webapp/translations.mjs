import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const WEBAPP = fileURLToPath(new URL("../../packages/netaudio/src/netaudio/daemon/http/webapp/", import.meta.url));
const CATALOGS = join(WEBAPP, "i18n");
const DYNAMIC = fileURLToPath(new URL("./translation-dynamic.json", import.meta.url));
const SKIPPED = new Set(["lib", "vendor", "i18n", "icon-paths.js", "i18n.js"]);
const LITERAL = String.raw`("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\x60(?:[^\x60\\$]|\\.)*\x60)`;
const SINGULAR = new RegExp(String.raw`(?<![\w.$])t\(\s*${LITERAL}`, "g");
const PLURAL = new RegExp(String.raw`(?<![\w.$])tn\(\s*[^,()]+(?:\([^()]*\))?[^,()]*,\s*${LITERAL}\s*,\s*${LITERAL}`, "g");

function sourceFiles(directory) {
  return readdirSync(directory).flatMap((name) => {
    if (SKIPPED.has(name)) return [];
    const path = join(directory, name);
    if (statSync(path).isDirectory()) return sourceFiles(path);
    return name.endsWith(".js") ? [path] : [];
  });
}

function unquote(token) {
  const body = token.slice(1, -1);
  if (token[0] === '"') return JSON.parse(token);
  return JSON.parse(`"${body.replace(/\\'/g, "'").replace(/\\`/g, "`").replace(/"/g, '\\"')}"`);
}

export function collectKeys() {
  const keys = new Map();
  for (const file of sourceFiles(WEBAPP)) {
    const text = readFileSync(file, "utf8");
    const where = relative(WEBAPP, file);
    for (const match of text.matchAll(SINGULAR)) {
      const key = unquote(match[1]);
      if (!keys.has(key)) keys.set(key, { text: key, plural: false, files: new Set() });
      keys.get(key).files.add(where);
    }
    for (const match of text.matchAll(PLURAL)) {
      const one = unquote(match[1]);
      const other = unquote(match[2]);
      if (!keys.has(other) || !keys.get(other).plural) keys.set(other, { text: other, one, plural: true, files: new Set() });
      keys.get(other).files.add(where);
    }
  }
  for (const key of JSON.parse(readFileSync(DYNAMIC, "utf8"))) {
    if (!keys.has(key)) keys.set(key, { text: key, plural: false, files: new Set(["dynamic"]) });
  }
  return keys;
}

function placeholders(text) {
  return [...String(text).matchAll(/\{(\w+)\}/g)].map((match) => match[1]).sort().join(",");
}

async function check() {
  const keys = collectKeys();
  let failed = false;
  const catalogs = readdirSync(CATALOGS).filter((name) => name.endsWith(".js")).sort();
  for (const name of catalogs) {
    const catalog = (await import(pathToFileURL(join(CATALOGS, name)).href)).default;
    const missing = [];
    const mismatched = [];
    for (const [key, entry] of keys) {
      const value = catalog[key];
      if (value === undefined || value === "") {
        missing.push(key);
        continue;
      }
      const forms = entry.plural ? (typeof value === "object" ? Object.values(value) : []) : typeof value === "string" ? [value] : [];
      if (!forms.length || forms.some((form) => placeholders(form) !== placeholders(key))) mismatched.push(key);
    }
    const unused = Object.keys(catalog).filter((key) => !keys.has(key));
    console.log(`${name}: ${keys.size - missing.length}/${keys.size} translated, ${mismatched.length} placeholder or plural problems, ${unused.length} unused`);
    for (const key of missing.slice(0, 20)) console.log(`  missing: ${key}`);
    for (const key of mismatched.slice(0, 20)) console.log(`  mismatched: ${key}`);
    for (const key of unused.slice(0, 20)) console.log(`  unused: ${key}`);
    if (missing.length || mismatched.length) failed = true;
  }
  if (!catalogs.length) console.log("No catalogs in", CATALOGS);
  process.exitCode = failed ? 1 : 0;
}

function keysCommand() {
  const keys = [...collectKeys().values()].map(({ text, one, plural, files }) => (plural ? { text, one, plural, files: [...files] } : { text, files: [...files] }));
  process.stdout.write(`${JSON.stringify(keys, null, 2)}\n`);
}

const command = process.argv[2];
if (command === "keys") keysCommand();
else if (command === "check") await check();
else {
  console.error("Usage: node scripts/webapp/translations.mjs keys|check");
  process.exitCode = 2;
}
