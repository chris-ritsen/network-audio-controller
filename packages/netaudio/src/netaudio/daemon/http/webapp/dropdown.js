import { useEffect, useRef } from "./lib/preact.js";

export function useDropdownDismissal() {
  const dropdown = useRef(null);
  useEffect(() => {
    const outside = (event) => {
      if (dropdown.current?.open && !dropdown.current.contains(event.target)) {
        dropdown.current.open = false;
      }
    };
    const escape = (event) => {
      if (event.key === "Escape" && dropdown.current?.open) {
        dropdown.current.open = false;
        dropdown.current.querySelector("summary")?.focus();
        event.preventDefault();
      }
    };
    document.addEventListener("pointerdown", outside, true);
    document.addEventListener("focusin", outside);
    document.addEventListener("keydown", escape);
    return () => {
      document.removeEventListener("pointerdown", outside, true);
      document.removeEventListener("focusin", outside);
      document.removeEventListener("keydown", escape);
    };
  }, []);
  return dropdown;
}
