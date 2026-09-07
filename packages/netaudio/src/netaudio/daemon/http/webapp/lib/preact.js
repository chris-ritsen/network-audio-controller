import htm from "htm";
import { Fragment, cloneElement, createContext, h, render, toChildArray } from "preact";

export { Fragment, cloneElement, createContext, h, render, toChildArray };
export { useCallback, useContext, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "preact/hooks";
export { batch, computed, effect, signal, untracked } from "@preact/signals";
export { useComputed, useSignal, useSignalEffect } from "@preact/signals";

export const html = htm.bind(h);
