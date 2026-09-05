import { register } from "node:module";
import { pathToFileURL } from "node:url";

register(pathToFileURL(new URL("./resolver.mjs", import.meta.url).pathname), {
  parentURL: import.meta.url,
});
