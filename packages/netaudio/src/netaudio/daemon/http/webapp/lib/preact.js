import htm from "htm";
import { Fragment, h, render } from "preact";

export { Fragment, h, render };
export { useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from "preact/hooks";
export { batch, computed, effect, signal, untracked } from "@preact/signals";
export { useComputed, useSignal, useSignalEffect } from "@preact/signals";

export const html = htm.bind(h);
