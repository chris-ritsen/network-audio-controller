import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { h } = await import("preact");
const { render } = await import("preact-render-to-string");
const { NetworkSection } = await import(`${WEBAPP}device/network.js`);
const { options } = await import("preact");
const { api } = await import(`${WEBAPP}api.js`);

const writableNetwork = {
  static_ipv4: { supported: true, readable: true, writable: true, reasons: [] },
  redundancy: { supported: true, readable: true, writable: true, reasons: [] },
};

const redundancyState = (current = "switched", configured = "switched") => ({
  advertised_support: true,
  current_mode: current,
  configured_mode: configured,
  current_mode_evidence: { status: "known", mode: current },
  configured_mode_evidence: { status: "known", mode: configured },
  available_modes: [
    { code: 0, label: "Switched", mode: "switched" },
    { code: 1, label: "Redundant", mode: "redundant" },
  ],
  interface_inventory: { completeness: "complete", reported_count: 2 },
  licensed_redundancy: { enabled: null, source: null },
  reboot_required: current !== configured,
});

test("redundancy selection submits immediately and blocks overlapping writes", async () => {
  const previousVNode = options.vnode;
  const previousSet = api.setRedundancy;
  let change;
  const calls = [];
  let finish;
  options.vnode = (vnode) => {
    previousVNode?.(vnode);
    if (vnode.type === "select" && vnode.props["aria-label"] === "Dante Redundancy") change = vnode.props.onChange;
  };
  api.setRedundancy = (request) => {
    calls.push(request);
    return new Promise((resolve) => { finish = resolve; });
  };
  try {
    render(
      h(NetworkSection, {
        device: {
          server_name: "test.local.",
          operation_availability: writableNetwork,
          network_redundancy: redundancyState(),
        },
      }),
    );
    assert.equal(typeof change, "function");
    await change({ currentTarget: { value: "switched" } });
    assert.equal(calls.length, 0);
    const request = change({ currentTarget: { value: "redundant" } });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].mode, "redundant");
    await change({ currentTarget: { value: "redundant" } });
    assert.equal(calls.length, 1);
    finish({ redundancy: redundancyState("switched", "redundant") });
    await request;
    api.setRedundancy = async () => { throw new Error("Device did not respond"); };
    await assert.doesNotReject(change({ currentTarget: { value: "redundant" } }));
  } finally {
    options.vnode = previousVNode;
    api.setRedundancy = previousSet;
  }
});

test("network panel separates primary and secondary active/configured values", () => {
  const device = {
    server_name: "device.local.", interface_configuration_modes: { primary: ["dhcp", "static"], secondary: [] }, interfaces: [
      { interface: "primary", mode: "static", ip_address: "192.0.2.34", netmask: "255.255.255.0",
        configured: { mode: "static", ip_address: "192.0.2.34", netmask: "255.255.255.0" } },
      { interface: "secondary", mode: "dynamic", ip_address: "198.51.100.62", netmask: "255.255.0.0",
        configured: { mode: "static", ip_address: "192.0.2.244", netmask: "255.255.255.0" }, reboot_required: true },
    ], network_redundancy: redundancyState("switched", "redundant"),
    operation_availability: writableNetwork,
  };
  const markup = render(h(NetworkSection, { device }));
  for (const value of ["Primary", "Secondary", "192.0.2.34", "192.0.2.244", "198.51.100.62", "Switched", "Redundant", "Configured address"]) {
    assert.ok(markup.includes(value), value);
  }
  assert.match(markup, /Network changes are unavailable for this interface/);
  assert.match(markup, /Pending network change — reboot required/);
  assert.match(markup, /Pending redundancy change — reboot required/);
  assert.match(markup, />Save primary settings/);
  assert.doesNotMatch(markup, /Save redundancy mode/);
  assert.match(markup, /<select aria-label="Dante Redundancy"/);
  assert.doesNotMatch(markup, /type="checkbox"|disabled[^>]*>Save|<button[^>]*>[^<]*[Rr]eboot/);
  assert.doesNotMatch(markup, /0x|pending_config|protocol|unknown\(/);
});

test("supported secondary configuration submits distinct DNS and gateway fields", async () => {
  const previousVNode = options.vnode;
  const previousSet = api.setInterface;
  let save;
  let request;
  options.vnode = (vnode) => {
    previousVNode?.(vnode);
    if (vnode.props?.description === "save secondary network settings") save = vnode.props.onRun;
    if (vnode.type === "input" && vnode.props["aria-label"]?.startsWith("Secondary ")) {
      vnode.ref.current = { value: vnode.props.defaultValue };
    }
  };
  api.setInterface = async (value) => { request = value; return { interfaces: [] }; };
  try {
    const markup = render(
      h(NetworkSection, {
        device: {
          server_name: "wing.local.",
          operation_availability: writableNetwork,
          interface_configuration_modes: { secondary: ["dhcp", "static"] },
          interfaces: [
            {
              interface: "secondary",
              mode: "dynamic",
              configured: {
                mode: "static",
                ip_address: "198.51.100.102",
                netmask: "255.255.255.0",
                gateway: "203.0.113.2",
                dns_server: "203.0.113.53",
              },
            },
          ],
        },
      }),
    );
    assert.match(markup, /Save secondary settings/);
    assert.match(markup, /Secondary address mode/);
    assert.equal(typeof save, "function");
    await save();
    assert.equal(request.interface, "secondary");
    assert.equal(request.gateway, "203.0.113.2");
    assert.equal(request.dns, "203.0.113.53");
    assert.equal(request.ip, "198.51.100.102");
    assert.equal(request.mode, "static");
  } finally {
    options.vnode = previousVNode;
    api.setInterface = previousSet;
  }
});

test("unknown network state has no refresh or guessed write controls", () => {
  const markup = render(h(NetworkSection, { device: { server_name: "unknown.local.", interfaces: [] } }));
  assert.match(markup, /Network settings are unavailable/);
  assert.doesNotMatch(markup, /Refresh/);
  assert.doesNotMatch(markup, /Save primary settings|Save redundancy mode/);
});

test("readable network state keeps backend denial reasons and hides writes", () => {
  const device = {
    server_name: "locked.local.",
    interfaces: [
      {
        interface: "primary",
        mode: "dynamic",
        configured: { mode: "dynamic" },
      },
    ],
    interface_configuration_modes: { primary: ["dhcp", "static"] },
    network_redundancy: redundancyState(),
    operation_availability: {
      static_ipv4: {
        supported: true,
        readable: true,
        writable: false,
        reasons: ["read_only"],
      },
      redundancy: {
        supported: true,
        readable: true,
        writable: false,
        reasons: ["device_locked"],
      },
    },
  };
  const markup = render(h(NetworkSection, { device }));
  assert.match(markup, /Active mode<\/dt><dd>DHCP/);
  assert.match(markup, /setting is read-only/);
  assert.match(markup, /device is locked/);
  assert.doesNotMatch(
    markup,
    /Save primary settings|aria-label="Dante Redundancy"/,
  );
});

for (const [name, support, reasons, expected] of [
  ["unsupported", false, ["unsupported"], /operation is unsupported/],
  ["capability unknown", null, ["capability_unknown"], /Capability support was not reported/],
  ["state unavailable", true, ["state_unavailable"], /state is unavailable/],
  ["read-only", true, ["read_only"], /setting is read-only/],
  ["locked", true, ["device_locked"], /device is locked/],
  ["managed permission denied", true, ["managed_permission_denied"], /Permission.*denied/],
  ["mode not advertised", true, ["requested_mode_not_advertised"], /mode was not advertised/],
]) {
  test(`redundancy UI distinguishes ${name}`, () => {
    const status = {
      ...redundancyState(),
      advertised_support: support,
      ...(reasons.includes("state_unavailable") ? {
        current_mode: null,
        configured_mode: null,
        current_mode_evidence: { status: "unavailable", mode: null },
        configured_mode_evidence: { status: "unavailable", mode: null },
      } : {}),
    };
    const markup = render(h(NetworkSection, { device: {
      server_name: `${name}.local.`,
      network_redundancy: status,
      operation_availability: {
        static_ipv4: { supported: false, readable: false, writable: false, reasons: ["unsupported"] },
        redundancy: { supported: support, readable: true, writable: false, reasons },
      },
      interfaces: [],
    } }));
    assert.match(markup, expected);
    assert.doesNotMatch(markup, /aria-label="Dante Redundancy"/);
  });
}
