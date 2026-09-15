function normalizedMac(value) {
  if (typeof value !== "string") return null;
  let normalized = value.replaceAll(":", "").replaceAll("-", "").toLowerCase();
  if (normalized.length === 16) {
    if (normalized.slice(6, 10) === "fffe") {
      normalized = `${normalized.slice(0, 6)}${normalized.slice(10)}`;
    } else if (normalized.endsWith("0000")) {
      normalized = normalized.slice(0, 12);
    }
  }
  return [12, 16].includes(normalized.length) && /^[0-9a-f]+$/.test(normalized)
    ? normalized
    : null;
}

function deviceIdentities(device) {
  const identities = new Set();
  for (const [namespace, field] of [
    ["inventory", "inventory_id"],
    ["managed", "ddm_device_id"],
    ["service", "server_name"],
  ]) {
    const value = device?.[field];
    if (typeof value === "string" && value) {
      identities.add(`${namespace}:${value.toLowerCase()}`);
    }
  }
  const macs = [device?.mac_address];
  for (const networkInterface of Array.isArray(device?.interfaces)
    ? device.interfaces
    : []) {
    macs.push(networkInterface?.mac_address);
  }
  for (const value of macs) {
    const mac = normalizedMac(value);
    if (mac) identities.add(`mac:${mac}`);
  }
  return identities;
}

export function sameCanonicalDevice(first, second) {
  if (first === second) return true;
  const firstIdentities = deviceIdentities(first);
  if (!firstIdentities.size) return false;
  return [...deviceIdentities(second)].some((identity) =>
    firstIdentities.has(identity),
  );
}

export function selfConnectionTargetState(
  receiver,
  receiverChannel,
  transmitter,
) {
  if (!sameCanonicalDevice(receiver, transmitter)) {
    return { allowed: true, selfConnection: false };
  }
  if (receiverChannel?.can_subscribe_self === true) {
    return { allowed: true, selfConnection: true };
  }
  if (receiverChannel?.can_subscribe_self === false) {
    return {
      allowed: false,
      reason: "This receiver channel does not support self-subscriptions.",
      selfConnection: true,
      state: "unsupported",
    };
  }
  return {
    allowed: false,
    reason:
      "Self-subscription capability is unavailable for this receiver channel.",
    selfConnection: true,
    state: "unavailable",
  };
}
