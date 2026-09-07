import { html } from "./lib/preact.js";
import { iconPaths } from "./icon-paths.js";

export function Icon({ name }) {
  return html`<svg
    class="size-5 shrink-0 inline-block"
    width="20"
    height="20"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    stroke-width="1.8"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
    focusable="false"
    dangerouslySetInnerHTML=${{ __html: iconPaths[name] || "" }}
  />`;
}
