export function visibleInventory(records) {
  return Object.fromEntries(Object.entries(records).filter(([, device]) => {
    const offline = device.online === false || device.availability_state === "offline";
    return !offline;
  }));
}
