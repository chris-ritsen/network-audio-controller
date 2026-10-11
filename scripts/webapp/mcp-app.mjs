import { readFileSync, writeFileSync } from "node:fs";
import { build } from "esbuild";

const webapp = new URL("../../packages/netaudio/src/netaudio/daemon/http/webapp/", import.meta.url);
const views = ["routing"];
const vendor = {
  "@preact/signals": "signals.module.js",
  "@preact/signals-core": "signals-core.module.js",
  htm: "htm.module.js",
  preact: "preact.module.js",
  "preact/hooks": "preact-hooks.module.js",
};

const vendorModules = {
  name: "vendor-modules",
  setup(builder) {
    builder.onResolve({ filter: /^(preact|preact\/hooks|htm|@preact\/signals|@preact\/signals-core)$/ }, (request) => ({
      path: new URL(`vendor/${vendor[request.path]}`, webapp).pathname,
    }));
  },
};

function inline(text, tag) {
  if (text.toLowerCase().includes(`</${tag}`)) {
    throw new Error(`the bundled ${tag} contains </${tag}, which would end the inline element early`);
  }
  return text;
}

const stylesheet = readFileSync(new URL("app.css", webapp), "utf8") + readFileSync(new URL("mcp-app/shell.css", webapp), "utf8");

for (const view of views) {
  const result = await build({
    entryPoints: [new URL(`mcp-app/${view}.js`, webapp).pathname],
    bundle: true,
    format: "esm",
    minify: true,
    platform: "browser",
    target: "es2022",
    write: false,
    plugins: [vendorModules],
    logLevel: "warning",
  });
  const script = result.outputFiles[0].text;
  const document = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>netaudio ${view}</title>
<style>${inline(stylesheet, "style")}</style>
</head>
<body>
<div id="view"></div>
<script type="module">${inline(script, "script")}</script>
</body>
</html>
`;
  writeFileSync(new URL(`mcp-app/${view}.html`, webapp), document);
  console.log(`mcp-app/${view}.html: ${document.length} bytes`);
}
