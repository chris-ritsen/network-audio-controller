import { register } from "node:module";

register(new URL("./resolver.mjs", import.meta.url), {
  parentURL: import.meta.url,
});
