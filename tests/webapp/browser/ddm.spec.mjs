import { test, expect } from "@playwright/test";
import { readFile } from "node:fs/promises";

const root = new URL("../../../packages/netaudio/src/netaudio/daemon/http/webapp/", import.meta.url);

test("DDM controls authenticate, log out, filter by scoped domain, and request enrollment", async ({ page }) => {
  const requests = [];
  const profiles = { servers: [{ name: "studio", url: "https://studio.example/graphql", configured: true }, { name: "venue", url: "https://venue.example/graphql", configured: true }], contexts: [], default_context: null };
  const domains = ["studio", "venue"].map((server) => ({ id: "same-id", name: "Test", ddm_server_profile: server, devices: [] }));
  const devices = Object.fromEntries(["studio", "venue", "unmanaged"].map((name) => [name, {
    name, server_name: name, inventory_sources: ["ddm"], online: true,
    ddm_device_id: name, ddm_server_profile: name === "unmanaged" ? "studio" : name,
    ddm_domain_id: name === "unmanaged" ? null : "same-id", ipv4: "192.0.2.1",
    management_state: name === "unmanaged" ? "unenrolled" : "managed",
    ddm_status: { summary: "OK", clocking: "OK" }, ddm_enrolment_state: name === "unmanaged" ? "UNENROLLED" : "ENROLLED",
  }]));
  await page.route("**/*", async (route) => {
    const url = new URL(route.request().url());
    if (url.hostname !== "netaudio.test") return route.abort();
    const json = (value) => route.fulfill({ contentType: "application/json", body: JSON.stringify(value) });
    if (url.pathname === "/events") return route.fulfill({ contentType: "text/event-stream", body: `data: ${JSON.stringify({ event: "snapshot", devices, managed: { status: { enabled: true }, domains, connections: profiles } })}\n\n` });
    if (route.request().method() === "POST") {
      requests.push({ path: url.pathname, body: route.request().postDataJSON() });
      if (url.pathname === "/ddm/enrollment") return json({ accepted: true });
      if (url.pathname === "/ddm/domains") return json({ accepted: true, domain: { id: "new", name: "Control" } });
      profiles.servers[0].configured = url.pathname !== "/ddm/logout";
      return json(profiles);
    }
    if (url.pathname === "/ddm/connections") return json(profiles);
    if (url.pathname === "/ddm/domains") return json(domains);
    if (url.pathname === "/ddm/status") return json({ enabled: true });
    const path = url.pathname === "/ddm" ? "index.html" : url.pathname.slice(1);
    try {
      return route.fulfill({ body: await readFile(new URL(path, root)), contentType: path.endsWith(".js") ? "text/javascript" : path.endsWith(".css") ? "text/css" : "text/html" });
    } catch { return route.fulfill({ status: 404 }); }
  });
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("http://netaudio.test/ddm");
  await expect(page.getByText("Managed devices (3)", { exact: true })).toBeVisible();
  const dialogs = [];
  page.on("dialog", (dialog) => { dialogs.push(dialog.message()); dialog.dismiss(); });
  await page.getByRole("button", { name: "Unenroll", exact: true }).first().click();
  await expect.poll(() => requests.some((request) => request.body.action === "unenroll")).toBe(true);
  expect(dialogs).toEqual([]);
  page.removeAllListeners("dialog");
  await expect(page.locator("body")).not.toContainText("[object Object]");
  await expect(page.getByRole("button", { name: "Read domains" })).toHaveCount(0);
  const serverControl = page.getByRole("combobox", { name: "DDM server", exact: true });
  const authControl = page.getByRole("combobox", { name: "Authentication", exact: true });
  const domainControl = page.getByRole("combobox", { name: "DDM domain", exact: true });
  const serverBox = await serverControl.boundingBox();
  const authBox = await authControl.boundingBox();
  const domainBox = await domainControl.boundingBox();
  expect(authBox.y).toBe(serverBox.y);
  expect(authBox.x).toBeGreaterThan(serverBox.x + serverBox.width);
  expect(domainBox.x).toBe(serverBox.x);
  expect(domainBox.width).toBe(serverBox.width);
  expect((await page.getByRole("button", { name: "Connect", exact: true }).boundingBox()).y).toBeGreaterThan(authBox.y + authBox.height);
  await page.setViewportSize({ width: 390, height: 844 });
  const mobileServer = await serverControl.boundingBox();
  const mobileAuth = await authControl.boundingBox();
  expect(mobileAuth.x).toBe(mobileServer.x);
  expect(mobileAuth.y).toBeGreaterThan((await domainControl.boundingBox()).y);
  expect(mobileAuth.x + mobileAuth.width).toBeLessThanOrEqual(390);
  const overflowing = await page.locator("#content, .topbar, .table-wrapper, .card-body").evaluateAll((nodes) => nodes.filter((node) => node.clientWidth && node.scrollWidth > node.clientWidth + 1).map((node) => node.className));
  expect(overflowing).toEqual([]);
  await page.setViewportSize({ width: 1440, height: 900 });
  const columns = page.getByRole("button", { name: /^Columns/ });
  expect((await columns.boundingBox()).height).toBeLessThan(40);
  const scope = page.getByRole("combobox", { name: "Server and domain", exact: true });
  await scope.selectOption({ label: "studio · Test" });
  await expect(page.getByText("Managed devices (1)", { exact: true })).toBeVisible();
  await expect(page.locator("tbody").last()).toContainText("studio");
  await expect(page.locator("tbody").last()).not.toContainText("venue");
  await scope.selectOption("local");
  await expect(page.getByText("Managed devices (1)", { exact: true })).toBeVisible();
  page.on("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Enroll", exact: true }).click();
  await expect.poll(() => requests.some((request) => request.path === "/ddm/enrollment")).toBe(true);
  await expect.poll(() => requests.some((request) => request.body.action === "enroll")).toBe(true);
  expect(requests.find((request) => request.body.action === "enroll").body).toEqual({ server: "studio", device_id: "unmanaged", action: "enroll", domain_id: "same-id" });
  await page.getByRole("combobox", { name: "Authentication", exact: true }).selectOption("api_key");
  await page.getByLabel("API key", { exact: true }).fill("test-only-key");
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect.poll(() => requests.some((request) => request.body.api_key === "test-only-key")).toBe(true);
  await page.getByRole("button", { name: "Log out", exact: true }).click();
  await expect(page.getByRole("button", { name: "Log out", exact: true })).toHaveCount(0);
  await page.getByLabel("Username", { exact: true }).fill("operator");
  await page.getByLabel("Password", { exact: true }).fill("test-only-password");
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect.poll(() => requests.some((request) => request.body.password === "test-only-password")).toBe(true);
  expect(await page.evaluate(() => JSON.stringify(localStorage))).not.toContain("test-only");
  await page.getByRole("button", { name: "New domain", exact: true }).click();
  const createDialog = page.getByRole("dialog", { name: "New domain", exact: true });
  await expect(createDialog).toBeVisible();
  await expect(createDialog.getByLabel("Server", { exact: true })).toHaveValue("studio");
  await createDialog.getByLabel("Name", { exact: true }).fill("Control");
  await createDialog.getByRole("button", { name: "Create domain", exact: true }).click();
  await expect(createDialog).not.toBeVisible();
  expect(requests.find((request) => request.path === "/ddm/domains").body).toEqual({ server: "studio", name: "Control" });
});
