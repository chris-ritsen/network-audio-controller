import { test, expect } from "@playwright/test";
import { serveWebapp } from "./fixture.mjs";

for (const supported of [true, false]) {
  test(`secondary configuration ${supported ? "saves static and DHCP settings" : "stays unavailable for unsupported devices"}`, async ({ page }) => {
    const record = { server_name: "ddm:manager:domain:wing", name: "Studio Wing", online: true,
      management_state: "managed", inventory_sources: ["ddm"], channels: { receivers: {}, transmitters: {} } };
    const snapshot = {
      interfaces: [
        { interface: "primary", mode: "dynamic", configured: { mode: "static", ip_address: "192.0.2.101", netmask: "255.255.255.0", gateway: "203.0.113.1", dns_server: "203.0.113.54" } },
        { interface: "secondary", mode: "dynamic", configured: { mode: "static", ip_address: "198.51.100.102", netmask: "255.255.255.0", gateway: "203.0.113.2", dns_server: "203.0.113.53" } },
      ],
      interface_configuration_modes: { primary: ["dhcp", "static"], secondary: supported ? ["dhcp", "static"] : [] },
      redundancy: { current: "redundant", configured: "redundant", supported: [] },
    };
    const primary = structuredClone(snapshot.interfaces[0]);
    const writes = [];
    let finishStatic;
    await serveWebapp(page, { devices: { [record.server_name]: record } });
    await page.route("**/interfaces/**", (route) => route.fulfill({ contentType: "application/json", body: JSON.stringify(snapshot) }));
    await page.route("**/interface", async (route) => {
      const request = route.request().postDataJSON();
      writes.push(request);
      snapshot.interfaces[1].configured = request.mode === "dhcp" ? { mode: "dynamic" } : {
        mode: "static", ip_address: request.ip, netmask: request.netmask, dns_server: request.dns, gateway: request.gateway,
      };
      if (writes.length === 1) await new Promise((resolve) => { finishStatic = resolve; });
      return route.fulfill({ contentType: "application/json", body: JSON.stringify({ success: true, ...snapshot }) });
    });
    await page.goto(`http://netaudio.test/devices/${encodeURIComponent(record.server_name)}/network-config`);
    await expect(page.getByLabel("Primary address mode")).toBeVisible();
    if (!supported) {
      await expect(page.getByLabel("Secondary address mode")).toHaveCount(0);
      await expect(page.getByRole("button", { name: "Save secondary settings" })).toHaveCount(0);
      expect(writes).toEqual([]);
      return;
    }
    await page.getByLabel("Secondary DNS server").fill("203.0.113.55");
    await page.getByRole("button", { name: "Save secondary settings" }).click();
    await expect.poll(() => writes.length).toBe(1);
    expect(writes[0]).toEqual({ device: record.server_name, interface: "secondary", mode: "static", ip: "198.51.100.102", netmask: "255.255.255.0", gateway: "203.0.113.2", dns: "203.0.113.55" });
    await expect(page.getByLabel("Secondary address mode")).toBeDisabled();
    await expect(page.getByLabel("Secondary DNS server")).toBeDisabled();
    finishStatic();
    await expect(page.getByLabel("Secondary address mode")).toBeEnabled();
    await expect(page.getByLabel("Secondary DNS server")).toHaveValue("203.0.113.55");
    await page.getByLabel("Secondary address mode").selectOption("dhcp");
    await page.getByRole("button", { name: "Save secondary settings" }).click();
    await expect.poll(() => writes.length).toBe(2);
    expect(writes[1]).toEqual({ device: record.server_name, interface: "secondary", mode: "dhcp" });
    await expect(page.getByLabel("Secondary address mode")).toHaveValue("dhcp");
    await expect(page.getByLabel("Primary DNS server")).toHaveValue("203.0.113.54");
    expect(snapshot.interfaces[0]).toEqual(primary);
  });
}
