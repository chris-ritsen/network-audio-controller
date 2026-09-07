import { html, useEffect, useLayoutEffect, useRef, useState } from "./lib/preact.js";

export function tooltipPosition(point, bounds, tooltip) {
  const margin = 12;
  const gap = 16;
  return {
    left: Math.max(margin, Math.min(point.x + gap, bounds.width - tooltip.width - margin)),
    top: Math.max(margin, Math.min(
      point.y + gap + tooltip.height + margin > bounds.height ? point.y - tooltip.height - gap : point.y + gap,
      bounds.height - tooltip.height - margin,
    )),
  };
}

export function MatrixTooltip({ id, text, point, bounds, onDismiss }) {
  const node = useRef(null);
  const [visible, setVisible] = useState(false);
  const [position, setPosition] = useState({ left: 12, top: 12 });
  useEffect(() => {
    setVisible(false);
    const timer = setTimeout(() => setVisible(true), 250);
    return () => clearTimeout(timer);
  }, [text]);
  useEffect(() => {
    const escape = (event) => { if (event.key === "Escape") onDismiss(); };
    window.addEventListener("keydown", escape);
    return () => window.removeEventListener("keydown", escape);
  }, [onDismiss]);
  useLayoutEffect(() => {
    if (visible && node.current) setPosition(tooltipPosition(point, bounds, node.current.getBoundingClientRect()));
  }, [visible, point.x, point.y, bounds.width, bounds.height, text]);
  if (!visible) return null;
  const [route, ...details] = text.split("\n");
  return html`<div id=${id} ref=${node} role="tooltip" class="matrix-tooltip"
    style=${`left:${position.left}px;top:${position.top}px`}>
    <div class="matrix-tooltip-route">${route}</div>
    ${details.length ? html`<div class="matrix-tooltip-status">${details.join("\n")}</div>` : null}
  </div>`;
}
