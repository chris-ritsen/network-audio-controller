# Architecture map

A pan-and-zoom map of how NetAudio's clients, daemon, Rust core and Dante
devices fit together, with step-by-step journeys (device discovery, routing
from the web UI, CLI and MCP, metering, presets, DDM, daemon startup).

Open it with any static file server:

```bash
python3 -m http.server -d docs/architecture-map 8080
```

Then browse to http://localhost:8080. Opening `index.html` directly from disk
also works.

Content lives in `data.js`; `app.js` draws the canvas.
