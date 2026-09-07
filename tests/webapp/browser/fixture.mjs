import { readFile } from "node:fs/promises";

const root = new URL("../../../packages/netaudio/src/netaudio/daemon/http/webapp/", import.meta.url);
export const deviceFixture = JSON.parse(await readFile(new URL("../fixtures/devices.json", import.meta.url), "utf8"));

export async function serveWebapp(page, { devices = deviceFixture, metering = {} } = {}) {
  await page.route("**/*", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (url.hostname !== "netaudio.test") return route.abort();
    if (url.pathname === "/events" && request.resourceType() !== "document") {
      return route.fulfill({ contentType: "text/event-stream", body: `data: ${JSON.stringify({ event: "snapshot", devices, metering })}\n\n` });
    }
    if (request.method() !== "GET") return route.fulfill({ status: 503, contentType: "application/json", body: '{"error":"No test handler for this action"}' });
    const path = url.pathname.split("/").at(-1).includes(".") ? url.pathname.slice(1) : "index.html";
    try {
      const body = await readFile(new URL(path, root));
      return route.fulfill({ body, contentType: path.endsWith(".js") ? "text/javascript" : path.endsWith(".css") ? "text/css" : "text/html" });
    } catch { return route.fulfill({ status: 404 }); }
  });
}
