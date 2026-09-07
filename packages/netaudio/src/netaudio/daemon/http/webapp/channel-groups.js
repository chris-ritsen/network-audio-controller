import { signal } from "./lib/preact.js";

const KEY = "netaudio.matrix.groups";

function read() {
  try {
    const value = JSON.parse(window.localStorage.getItem(KEY));
    return {
      enabled: value?.enabled === true,
      receivers: new Set(Array.isArray(value?.receivers) ? value.receivers : []),
      transmitters: new Set(Array.isArray(value?.transmitters) ? value.transmitters : []),
    };
  } catch {
    return { enabled: false, receivers: new Set(), transmitters: new Set() };
  }
}

export const channelGroups = signal(read());

function save(next) {
  channelGroups.value = next;
  try {
    window.localStorage.setItem(KEY, JSON.stringify({
      enabled: next.enabled, receivers: [...next.receivers], transmitters: [...next.transmitters],
    }));
  } catch {}
}

export function enableChannelGroups(enabled) {
  save({ ...channelGroups.value, enabled });
}

export function setGroupsExpanded(side, keys, expanded) {
  const current = channelGroups.value;
  const next = { ...current, [side]: new Set(current[side]) };
  for (const key of keys) {
    if (expanded) next[side].add(key);
    else next[side].delete(key);
  }
  save(next);
}

export function groupChannels(deviceId, channels) {
  const blocks = new Map();
  for (const channel of channels) {
    const start = Math.floor((channel.number - 1) / 16) * 16 + 1;
    if (!blocks.has(start)) blocks.set(start, []);
    blocks.get(start).push(channel);
  }
  return [...blocks.entries()].map(([start, members]) => ({
    key: JSON.stringify([deviceId, start]),
    name: `${start}–${Math.max(...members.map((channel) => channel.number))}`,
    channels: members,
  }));
}
