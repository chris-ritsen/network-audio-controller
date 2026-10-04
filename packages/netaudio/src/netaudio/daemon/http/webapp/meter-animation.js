import { useEffect, useRef } from "./lib/preact.js";
import { MAXIMUM_FRAME_SECONDS } from "./peak-meter.js";

export function useAnimationFrames(running, onFrame) {
  const latest = useRef(onFrame);
  latest.current = onFrame;
  useEffect(() => {
    if (!running) return undefined;
    let handle = 0;
    let previous = null;
    const step = (timestamp) => {
      handle = window.requestAnimationFrame(step);
      const elapsed = previous === null ? 0 : Math.min(MAXIMUM_FRAME_SECONDS, Math.max(0, (timestamp - previous) / 1000));
      previous = timestamp;
      latest.current(elapsed);
    };
    handle = window.requestAnimationFrame(step);
    return () => window.cancelAnimationFrame(handle);
  }, [running]);
}

export function observeVisibility(elements, onChange) {
  if (typeof IntersectionObserver !== "function") {
    for (const element of elements) onChange(element, true);
    return () => {};
  }
  const observer = new IntersectionObserver((entries) => {
    for (const entry of entries) onChange(entry.target, entry.isIntersecting);
  });
  for (const element of elements) observer.observe(element);
  return () => observer.disconnect();
}
