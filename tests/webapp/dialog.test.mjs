import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { h, options } = await import("preact");
const { render } = await import("preact-render-to-string");
const { RoutePicker } = await import(`${WEBAPP}route-picker.js`);
const { NewDomainDialog } = await import(`${WEBAPP}new-domain.js`);

for (const [name, component, props] of [
  ["source picker", RoutePicker, { receiver: { name: "receiver", online: true }, receiveChannelNumber: 1 }],
  ["new domain", NewDomainDialog, { server: "test" }],
]) {
  test(`${name} dismisses on backdrop clicks, not content clicks`, () => {
    const previous = options.vnode;
    let click;
    let closed = 0;
    options.vnode = (vnode) => {
      previous?.(vnode);
      if (vnode.type === "dialog") click = vnode.props.onClick;
    };
    try {
      render(h(component, { ...props, onClose: () => { closed++; } }));
      assert.equal(typeof click, "function");
      const dialog = {};
      click({ target: {}, currentTarget: dialog });
      assert.equal(closed, 0);
      click({ target: dialog, currentTarget: dialog });
      assert.equal(closed, 1);
    } finally {
      options.vnode = previous;
    }
  });
}
