import { useEffect, useState } from "./lib/preact.js";

const LIGHT_QUERY = "(prefers-color-scheme: light)";

let lightMedia = null;

export function prefersLight() {
  lightMedia ??= window.matchMedia(LIGHT_QUERY);
  return Boolean(lightMedia.matches);
}

export function useColorScheme() {
  const [light, setLight] = useState(prefersLight);
  useEffect(() => {
    const media = window.matchMedia(LIGHT_QUERY);
    const update = () => setLight(media.matches);
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  return light ? "light" : "dark";
}
