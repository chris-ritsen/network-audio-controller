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
  let disturbed = false;
  let holding = new Set();
  let keys = [];
  let order = [];

  const settled = (records) =>
    keys.every(
      (key, index) =>
        records[key] &&
        present(records[key]) &&
        (index === 0 || compare(records[keys[index - 1]], records[key]) <= 0),
    );

  effect(() => {
    const records = source.value;
    const generation = captures.value;
    revision.value;
    untracked(() => {
      let unsettled = disturbed;
      disturbed = false;
      if (generation !== capture) {
        capture = generation;
        keys = Object.keys(records)
          .filter((key) => present(records[key]))
          .sort((first, second) => compare(records[first], records[second]));
        holding = new Set(keys);
        absent.clear();
        known.clear();
        unsettled = true;
      }
      let matched = 0;
      let unheld = 0;
      for (const key in records) {
        const record = records[key];
        const previous = known.get(key);
        if (record === previous) {
          matched += 1;
          continue;
        }
        if (!holding.has(key)) {
          if (!present(record)) {
            unheld += 1;
            continue;
          }
          const moved = identity(record);
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
            holding.delete(keys[slot]);
            known.delete(keys[slot]);
            keys[slot] = key;
          }
          holding.add(key);
          unsettled = true;
        } else if (
          !previous ||
          present(previous) !== present(record) ||
          compare(previous, record) !== 0
        ) {
          unsettled = true;
        }
        known.set(key, record);
        matched += 1;
      }
      let next = records;
      if (
        unsettled ||
        unheld > 0 ||
        absent.size > 0 ||
        matched !== keys.length
      ) {
        next = {};
        for (const key of keys) {
          const record = records[key];
          if (record) {
            if (absent.delete(key)) unsettled = true;
            next[key] = record;
          } else {
            if (!absent.has(key)) {
              absent.set(key, departed(known.get(key)));
              unsettled = true;
            }
            next[key] = absent.get(key);
          }
        }
      }
      if (unsettled) {
        order = [...keys];
        stale.value = !settled(records);
      }
      arrangements.set(next, order);
      view.value = next;
    });
  });

  holds.push({
    dismiss(dismissed) {
      const remaining = keys.filter((key) => !dismissed.includes(key));
      if (remaining.length === keys.length) return;
      for (const key of dismissed) {
        absent.delete(key);
        known.delete(key);
      }
      disturbed = true;
      holding = new Set(remaining);
      keys = remaining;
      revision.value += 1;
    },
    stale,
  });
  return view;
}
