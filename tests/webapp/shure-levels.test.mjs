import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { createShureLevels, observeShureMeters, shureLevels, AD4D_PEAK_HOLD_SECONDS } = await import(`${WEBAPP}shure-levels.js`);
const { P10T_METER_FULL_SCALE } = await import(`${WEBAPP}shure-catalog.js`);
const { slotRows } = await import(`${WEBAPP}shure-slots.js`);
const { advancePeakMeter, createPeakMeter, stripReadout } = await import(`${WEBAPP}peak-meter.js`);

const AD4D = "00:0e:dd:48:96:29";
const P10T = "00:0e:dd:20:9f:35";

function sample(peak, rms) {
  return { AUDIO_LEVEL_PEAK: String(peak + 120).padStart(3, "0"), AUDIO_LEVEL_RMS: String(rms + 120).padStart(3, "0") };
}

function feed(levels, mac, channel, frames) {
  return frames.map(([time, values]) => {
    observeShureMeters(levels, mac, channel, values, time);
    return shureLevels(levels, mac, channel)[0];
  });
}

test("an AD4D peak passes through on the sample where it rises, as dBFS", () => {
  const levels = createShureLevels();
  const [first, second] = feed(levels, AD4D, 2, [[0, sample(-45, -52)], [0.1, sample(-14, -30)]]);
  assert.equal(first, -45);
  assert.equal(second, -14);
});

test("an AD4D peak held by the receiver gives way to its RMS until the hold ends", () => {
  const levels = createShureLevels();
  const seen = feed(levels, AD4D, 2, [
    [0, sample(-8, -22)],
    [0.1, sample(-8, -27)],
    [0.2, sample(-8, -33)],
    [0.5, sample(-8, -41)],
  ]);
  assert.deepEqual(seen, [-8, -27, -33, -41]);
});

test("the receiver's release steps after a hold are not taken as new peaks", () => {
  const levels = createShureLevels();
  const seen = feed(levels, AD4D, 2, [
    [0, sample(-5, -20)],
    [0.95, sample(-10, -45)],
    [1.05, sample(-15, -49)],
    [1.15, sample(-21, -50)],
  ]);
  assert.deepEqual(seen, [-5, -45, -49, -50]);
});

test("a small drop is a new, quieter peak and passes through", () => {
  const levels = createShureLevels();
  const seen = feed(levels, AD4D, 2, [
    [0, sample(-46, -52)],
    [0.1, sample(-48, -53)],
    [0.2, sample(-45, -51)],
  ]);
  assert.deepEqual(seen, [-46, -48, -45]);
});

test("a steady AD4D peak that outlasts the receiver's hold is a sustained level", () => {
  const levels = createShureLevels();
  const frames = [];
  for (let index = 0; index <= 30; index += 1) frames.push([index * 0.1, sample(-20, -23)]);
  const seen = feed(levels, AD4D, 2, frames);
  const sustained = seen.filter((value, index) => frames[index][0] >= AD4D_PEAK_HOLD_SECONDS && value === -20);
  assert.ok(sustained.length >= 2, `sustained level not passed through: ${seen}`);
  const meter = createPeakMeter();
  let lowest = 0;
  frames.forEach(([time], index) => {
    for (let frame = 0; frame < 6; frame += 1) advancePeakMeter(meter, seen[index], 1 / 60);
    if (time > 0.5) lowest = Math.min(lowest, meter.level);
  });
  assert.ok(lowest > -23.5, `steady signal sagged on the meter to ${lowest}`);
});

test("AD4D channels and devices keep separate state", () => {
  const levels = createShureLevels();
  observeShureMeters(levels, AD4D, 1, sample(-30, -40), 0);
  observeShureMeters(levels, AD4D, 2, sample(-10, -20), 0);
  observeShureMeters(levels, "00:0e:dd:00:00:01", 1, sample(-50, -55), 0);
  observeShureMeters(levels, AD4D, 1, sample(-30, -44), 0.1);
  assert.deepEqual(shureLevels(levels, AD4D, 1), [-44]);
  assert.deepEqual(shureLevels(levels, AD4D, 2), [-10]);
  assert.deepEqual(shureLevels(levels, "00:0e:dd:00:00:01", 1), [-50]);
});

test("missing or malformed AD4D values read as no level instead of failing", () => {
  const levels = createShureLevels();
  assert.deepEqual(shureLevels(levels, AD4D, 3), []);
  observeShureMeters(levels, AD4D, 3, { AUDIO_LEVEL_PEAK: "abc", AUDIO_LEVEL_RMS: "" }, 0);
  assert.deepEqual(shureLevels(levels, AD4D, 3), [null]);
  observeShureMeters(levels, AD4D, 3, { AUDIO_LEVEL_PEAK: "080" }, 0.1);
  assert.deepEqual(shureLevels(levels, AD4D, 3), [-40]);
  observeShureMeters(levels, AD4D, 3, { AUDIO_LEVEL_PEAK: "080" }, 0.2);
  assert.deepEqual(shureLevels(levels, AD4D, 3), [null]);
});

test("a P10T side reads the louder of its last two samples against the measured full scale", () => {
  const levels = createShureLevels();
  observeShureMeters(levels, P10T, 1, { AUDIO_IN_LVL_L: String(P10T_METER_FULL_SCALE / 10) }, 0);
  assert.equal(Math.round(shureLevels(levels, P10T, 1)[0]), -20);
  observeShureMeters(levels, P10T, 1, { AUDIO_IN_LVL_L: String(P10T_METER_FULL_SCALE / 100) }, 0.1);
  assert.equal(Math.round(shureLevels(levels, P10T, 1)[0]), -20);
  observeShureMeters(levels, P10T, 1, { AUDIO_IN_LVL_L: String(P10T_METER_FULL_SCALE / 1000) }, 0.2);
  assert.equal(Math.round(shureLevels(levels, P10T, 1)[0]), -40);
});

test("P10T left and right keep separate windows and clamp at full scale", () => {
  const levels = createShureLevels();
  observeShureMeters(levels, P10T, 1, { AUDIO_IN_LVL_L: String(P10T_METER_FULL_SCALE * 4) }, 0);
  observeShureMeters(levels, P10T, 1, { AUDIO_IN_LVL_R: String(P10T_METER_FULL_SCALE / 1000) }, 0);
  const [left, right] = shureLevels(levels, P10T, 1);
  assert.equal(left, 0);
  assert.equal(Math.round(right), -60);
});

test("a zero or malformed P10T sample does not break the reading", () => {
  const levels = createShureLevels();
  observeShureMeters(levels, P10T, 2, { AUDIO_IN_LVL_L: "0" }, 0);
  assert.equal(shureLevels(levels, P10T, 2)[0], null);
  observeShureMeters(levels, P10T, 2, { AUDIO_IN_LVL_L: "junk" }, 0.1);
  assert.equal(shureLevels(levels, P10T, 2)[0], null);
  observeShureMeters(levels, P10T, 2, { AUDIO_IN_LVL_L: String(P10T_METER_FULL_SCALE / 100) }, 0.2);
  assert.equal(Math.round(shureLevels(levels, P10T, 2)[0]), -40);
});

test("the strip readout shows what the meter shows, and the input level below the meter floor", () => {
  const loud = createPeakMeter();
  advancePeakMeter(loud, -12.34, 0);
  const quiet = createPeakMeter();
  advancePeakMeter(quiet, -30, 0);
  assert.equal(stripReadout([quiet, loud], [-30, -12.34]), "-12.3");
  const floor = createPeakMeter();
  advancePeakMeter(floor, -66.2, 0);
  assert.equal(stripReadout([floor, floor], [-66.2, -68]), "-66.2");
  const silent = createPeakMeter();
  advancePeakMeter(silent, null, 0);
  assert.equal(stripReadout([silent], [null]), "");
});

function standardSlots(overrides = {}) {
  const status = { 1: "STANDARD", 2: "EMPTY", 3: "EMPTY", 4: "EMPTY", 5: "EMPTY", 6: "EMPTY", 7: "EMPTY", 8: "EMPTY" };
  const unknown = (value) => Object.fromEntries(Object.keys(status).map((slot) => [slot, value]));
  return {
    SLOT_STATUS: status,
    SLOT_TX_MODEL: { ...unknown("UNKNOWN"), 1: "AD1" },
    SLOT_TX_DEVICE_ID: { ...unknown(""), 1: "AD1" },
    SLOT_BATT_BARS: unknown("255"),
    SLOT_BATT_CHARGE_PERCENT: unknown("255"),
    SLOT_BATT_MINS: unknown("65535"),
    SLOT_BATT_HEALTH_PERCENT: unknown("255"),
    SLOT_BATT_CYCLE_COUNT: unknown("65535"),
    SLOT_BATT_TYPE: unknown("UNKN"),
    SLOT_RF_POWER: unknown("255"),
    SLOT_INPUT_PAD: unknown("255"),
    SLOT_OFFSET: unknown("255"),
    SLOT_POLARITY: unknown("UNKNOWN"),
    SLOT_RF_OUTPUT: unknown("UNKNOWN"),
    SLOT_RF_POWER_MODE: unknown("UNKNOWN"),
    SLOT_SHOWLINK_STATUS: unknown("255"),
    TX_MODEL: "AD1",
    TX_DEVICE_ID: "AD1",
    TX_BATT_BARS: "005",
    TX_BATT_CHARGE_PERCENT: "090",
    TX_BATT_MINS: "00335",
    TX_BATT_HEALTH_PERCENT: "098",
    TX_BATT_CYCLE_COUNT: "00069",
    TX_BATT_TYPE: "LION",
    TX_POWER_LEVEL: "035",
    TX_INPUT_PAD: "012",
    TX_OFFSET: "012",
    TX_POLARITY: "POSITIVE",
    ...overrides,
  };
}

test("the slot holding the transmitter on air shows that transmitter's live values", () => {
  const rows = slotRows(standardSlots());
  const [first, second] = rows;
  assert.equal(rows.length, 8);
  assert.equal(first.onAir, true);
  assert.equal(first.values.SLOT_BATT_BARS, "005");
  assert.equal(first.values.SLOT_BATT_CHARGE_PERCENT, "090");
  assert.equal(first.values.SLOT_BATT_MINS, "00335");
  assert.equal(first.values.SLOT_BATT_HEALTH_PERCENT, "098");
  assert.equal(first.values.SLOT_BATT_CYCLE_COUNT, "00069");
  assert.equal(first.values.SLOT_BATT_TYPE, "LION");
  assert.equal(first.values.SLOT_RF_POWER, "035");
  assert.equal(first.values.SLOT_INPUT_PAD, "012");
  assert.equal(first.values.SLOT_OFFSET, "012");
  assert.equal(first.values.SLOT_POLARITY, "POSITIVE");
  assert.equal(first.values.SLOT_RF_OUTPUT, "UNKNOWN");
  assert.equal(second.onAir, false);
  assert.equal(second.values.SLOT_BATT_BARS, "255");
});

test("no slot is filled while no transmitter is on air", () => {
  const rows = slotRows(standardSlots({ TX_MODEL: "UNKNOWN", TX_DEVICE_ID: "" }));
  assert.ok(rows.every((row) => !row.onAir));
  assert.equal(rows[0].values.SLOT_BATT_BARS, "255");
});

test("two slots that both match the transmitter on air are left as the receiver reports them", () => {
  const properties = standardSlots();
  properties.SLOT_STATUS = { ...properties.SLOT_STATUS, 2: "STANDARD" };
  properties.SLOT_TX_MODEL = { ...properties.SLOT_TX_MODEL, 2: "AD1" };
  properties.SLOT_TX_DEVICE_ID = { ...properties.SLOT_TX_DEVICE_ID, 2: "AD1" };
  const rows = slotRows(properties);
  assert.ok(rows.every((row) => !row.onAir));
  assert.equal(rows[0].values.SLOT_BATT_BARS, "255");
});

test("a ShowLink slot keeps the values the receiver reports for it", () => {
  const properties = standardSlots();
  properties.SLOT_STATUS = { ...properties.SLOT_STATUS, 1: "LINKED.ACTIVE" };
  properties.SLOT_BATT_BARS = { ...properties.SLOT_BATT_BARS, 1: "003" };
  const [first] = slotRows(properties);
  assert.equal(first.onAir, false);
  assert.equal(first.values.SLOT_BATT_BARS, "003");
});

test("empty slots are not matched even when their identity fields look like the transmitter", () => {
  const properties = standardSlots();
  properties.SLOT_STATUS = { ...properties.SLOT_STATUS, 1: "EMPTY" };
  const rows = slotRows(properties);
  assert.ok(rows.every((row) => !row.onAir));
});
