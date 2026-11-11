import { computed, effect, signal, untracked } from "./lib/preact.js";

const arrangements = new WeakMap();
const holds = [];

export const captures = signal(0);
export const orderings = signal(0);

export const viewStale = computed(() =>
  holds.some((hold) => hold.stale.value === true),
);

export function dismiss(keys) {
  for (const hold of holds) hold.dismiss(keys);
}

export function refreshView() {
  captures.value += 1;
}

export function reorderView() {
  orderings.value += 1;
}

export function arranged(records) {
  const keys = arrangements.get(records);
  return keys ? keys.map((key) => records[key]) : null;
}

export function narrowed(records, matches) {
  const keys = (arrangements.get(records) || Object.keys(records)).filter(
    (key) => matches(records[key]),
  );
  const next = Object.fromEntries(keys.map((key) => [key, records[key]]));
  if (arrangements.has(records)) arrangements.set(next, keys);
  return next;
}

export function held(source, { compare, departed, identity, present }) {
  const absent = new Map();
  const known = new Map();
  const revision = signal(0);
  const stale = signal(false);
  const view = signal({});
  let capture = null;
  let keys = [];

  effect(() => {
    const records = source.value;
    const generation = captures.value;
    revision.value;
    untracked(() => {
      const current = Object.keys(records)
        .filter((key) => present(records[key]))
        .sort((first, second) => compare(records[first], records[second]));
      if (generation !== capture) {
        capture = generation;
        keys = current;
        absent.clear();
        known.clear();
      } else {
        const holding = new Set(keys);
        for (const key of current) {
          if (holding.has(key)) continue;
          const moved = identity(records[key]);
          const slot = moved
            ? keys.findIndex(
                (candidate) =>
                  !(records[candidate] && present(records[candidate])) &&
                  identity(known.get(candidate)) === moved,
              )
            : -1;
          if (slot < 0) {
            keys.push(key);
          } else {
            absent.delete(keys[slot]);
            known.delete(keys[slot]);
            keys[slot] = key;
          }
          holding.add(key);
        }
      }
      const next = {};
      for (const key of keys) {
        const record = records[key];
        if (record) {
          absent.delete(key);
          known.set(key, record);
          next[key] = record;
        } else {
          if (!absent.has(key)) absent.set(key, departed(known.get(key)));
          next[key] = absent.get(key);
        }
      }
      arrangements.set(next, [...keys]);
      stale.value =
        keys.length !== current.length ||
        keys.some((key, index) => key !== current[index]);
      view.value = next;
    });
  });

  holds.push({
    dismiss(dismissed) {
      const remaining = keys.filter((key) => !dismissed.includes(key));
      if (remaining.length === keys.length) return;
      keys = remaining;
      revision.value += 1;
    },
    stale,
  });
  return view;
}
