import { SLOT_COLUMNS } from "./shure-catalog.js";

export const SLOTS = [1, 2, 3, 4, 5, 6, 7, 8];
export const ON_AIR_SOURCES = {
  SLOT_BATT_BARS: "TX_BATT_BARS",
  SLOT_BATT_CHARGE_PERCENT: "TX_BATT_CHARGE_PERCENT",
  SLOT_BATT_MINS: "TX_BATT_MINS",
  SLOT_BATT_HEALTH_PERCENT: "TX_BATT_HEALTH_PERCENT",
  SLOT_BATT_CYCLE_COUNT: "TX_BATT_CYCLE_COUNT",
  SLOT_BATT_TYPE: "TX_BATT_TYPE",
  SLOT_RF_POWER: "TX_POWER_LEVEL",
  SLOT_INPUT_PAD: "TX_INPUT_PAD",
  SLOT_OFFSET: "TX_OFFSET",
  SLOT_POLARITY: "TX_POLARITY",
};

function text(value) {
  return value === undefined || value === null ? "" : String(value).trim();
}

function onAirSlot(properties, rows) {
  const model = text(properties.TX_MODEL);
  const deviceId = text(properties.TX_DEVICE_ID);
  if (!model || model === "UNKNOWN" || !deviceId) return null;
  const matches = rows.filter(
    ({ values }) => values.SLOT_STATUS === "STANDARD" && text(values.SLOT_TX_MODEL) === model && text(values.SLOT_TX_DEVICE_ID) === deviceId,
  );
  return matches.length === 1 ? matches[0].slot : null;
}

export function slotRows(properties) {
  const rows = SLOTS.map((slot) => ({
    slot,
    onAir: false,
    values: Object.fromEntries(SLOT_COLUMNS.map((column) => [column.key, (properties[column.key] || {})[String(slot)]])),
  }));
  const slot = onAirSlot(properties, rows);
  if (slot === null) return rows;
  return rows.map((row) =>
    row.slot === slot
      ? { ...row, onAir: true, values: { ...row.values, ...Object.fromEntries(Object.entries(ON_AIR_SOURCES).map(([key, source]) => [key, properties[source]])) } }
      : row,
  );
}
