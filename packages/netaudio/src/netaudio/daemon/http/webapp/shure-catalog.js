export const SHURE_LEVEL_OFFSET = 120;
export const P10T_METER_FULL_SCALE = 1190000;

function integer(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return /^-?\d+$/.test(text) ? Number.parseInt(text, 10) : null;
}

function signed(number, unit) {
  return `${number > 0 ? "+" : ""}${number} ${unit}`;
}

function withUnknown(sentinels, render) {
  return (value) => {
    const number = integer(value);
    if (number === null) return value === undefined || value === null || value === "" ? null : String(value);
    if (sentinels[number] !== undefined) return sentinels[number];
    return render(number);
  };
}

const UNKNOWN_255 = { 255: null };

export const decode = {
  text: (value) => (value === undefined || value === null || String(value).trim() === "" ? null : String(value).trim()),
  dbfs: withUnknown({}, (number) => `${number - SHURE_LEVEL_OFFSET} dBFS`),
  dbm: withUnknown({}, (number) => `${number - SHURE_LEVEL_OFFSET} dBm`),
  gain: withUnknown({}, (number) => signed(number - 18, "dB")),
  frequency: withUnknown({}, (number) => `${(number / 1000).toFixed(3)} MHz`),
  quality: withUnknown(UNKNOWN_255, (number) => `${number} of 5`),
  bars: withUnknown(UNKNOWN_255, (number) => `${number} of 5`),
  percent: withUnknown(UNKNOWN_255, (number) => `${number}%`),
  milliwatts: withUnknown(UNKNOWN_255, (number) => `${number} mW`),
  cycles: withUnknown({ 65535: null }, (number) => String(number)),
  minutes: withUnknown(
    { 65535: null, 65534: "Calculating", 65533: "Battery communication warning" },
    (number) => `${Math.floor(number / 60)} h ${number % 60} min`,
  ),
  celsius: withUnknown(UNKNOWN_255, (number) => `${number - 40} °C`),
  fahrenheit: withUnknown(UNKNOWN_255, (number) => `${number - 40} °F`),
  offset: withUnknown(UNKNOWN_255, (number) => signed(number - 12, "dB")),
  pad: withUnknown({ 0: "On (-12 dB)", 12: "Off (0 dB)", 255: null }, (number) => signed(number - 12, "dB")),
  showlink: withUnknown(UNKNOWN_255, (number) => `${number} of 5`),
  milliseconds: withUnknown({ 0: "Off" }, (number) => `${number} ms`),
  groupChannel: (value) => {
    const text = decode.text(value);
    return text === null ? null : text === "--,--" ? "None" : text.replace(",", " / ");
  },
  firmware: (value) => {
    const text = decode.text(value);
    if (text === null) return null;
    return text.endsWith("*") ? `${text.slice(0, -1)} (self test failed)` : text;
  },
  model: (value) => {
    const text = decode.text(value);
    return text === "UNKNOWN" ? null : text;
  },
  batteryType: (value) => (value === "UNKN" ? null : ({ LION: "Lithium-ion", ALKA: "Alkaline", NIMH: "NiMH", LITH: "Lithium" })[value] || decode.text(value)),
  word: (value) => {
    const text = decode.text(value);
    if (text === null) return null;
    return text
      .replace(/[._]/g, " ")
      .toLowerCase()
      .replace(/^\w/, (character) => character.toUpperCase());
  },
  rfMute: (value) => (value === "1" ? "On" : value === "0" ? "Off" : decode.text(value)),
  transmitMode: (value) => ({ 1: "Mono", 2: "Point to point", 3: "Stereo" })[value] || decode.text(value),
  inputLevel: (value) => ({ 1: "Line, +4 dBu", 0: "Aux, -10 dBV" })[value] || decode.text(value),
  fdMode: (value) => ({ OFF: "Off", "FD-C": "Frequency diversity, combining", "FD-S": "Frequency diversity, selection" })[value] || decode.text(value),
};

export const AD4D_GAIN_OPTIONS = Array.from({ length: 61 }, (_, raw) => [String(raw), signed(raw - 18, "dB")]);
export const SLOT_OFFSET_OPTIONS = Array.from({ length: 34 }, (_, raw) => [String(raw - 12), signed(raw - 12, "dB")]);

export const AD4D_DEVICE_FIELDS = [
  { key: "DEVICE_ID", label: "Device ID", edit: { kind: "text", setting: "DEVICE_ID", maxLength: 8 } },
  { key: "MODEL", label: "Model" },
  { key: "FW_VER", label: "Firmware", decode: decode.firmware },
  { key: "RF_BAND", label: "RF band" },
  { key: "TRANSMISSION_MODE", label: "Transmission mode", decode: decode.word },
  { key: "QUADVERSITY_MODE", label: "Quadversity", decode: decode.word },
  { key: "ENCRYPTION_MODE", label: "Encryption", decode: decode.word },
  { key: "NA_DEVICE_NAME", label: "Dante device name" },
];

export const AD4D_AUDIO_FIELDS = [
  { key: "CHAN_NAME", label: "Name", edit: { kind: "text", setting: "CHAN_NAME", maxLength: 8 } },
  { key: "AUDIO_GAIN", label: "Gain", decode: decode.gain, edit: { kind: "select", setting: "AUDIO_GAIN", options: AD4D_GAIN_OPTIONS, raw: (value) => String(Number.parseInt(value, 10)) } },
  { key: "AUDIO_MUTE", label: "Mute", decode: decode.word, edit: { kind: "toggle", setting: "AUDIO_MUTE", isOn: (value) => value === "ON" } },
  { key: "AUDIO_LEVEL_PEAK", label: "Peak level", decode: decode.dbfs, live: true },
  { key: "AUDIO_LEVEL_RMS", label: "RMS level", decode: decode.dbfs, live: true },
  { key: "AUDIO_LED_BITMAP", label: "Audio LEDs", kind: "audio-leds", live: true },
  { key: "NA_CHAN_NAME", label: "Dante channel name" },
];

export const AD4D_RF_FIELDS = [
  { key: "FREQUENCY", label: "Frequency", decode: decode.frequency, edit: { kind: "frequency", setting: "FREQUENCY" } },
  { key: "GROUP_CHANNEL", label: "Group / channel", decode: decode.groupChannel, edit: { kind: "group", setting: "GROUP_CHANNEL" } },
  { key: "FD_MODE", label: "Frequency diversity", decode: decode.fdMode },
  { key: "FREQUENCY2", label: "Second frequency", decode: decode.frequency, edit: { kind: "frequency", setting: "FREQUENCY2" }, onlyWhen: (properties) => properties.FD_MODE === "FD-C" },
  { key: "GROUP_CHANNEL2", label: "Second group / channel", decode: decode.groupChannel, edit: { kind: "group", setting: "GROUP_CHANNEL2" }, onlyWhen: (properties) => properties.FD_MODE === "FD-C" },
  { key: "ANTENNA_STATUS", label: "Antennas", kind: "antennas", live: true },
  { key: "RSSI", label: "Signal strength", kind: "rssi", live: true },
  { key: "RSSI_LED_BITMAP", label: "RF LEDs", kind: "rf-leds", live: true },
  { key: "ANTENNA_STATUS2", label: "Second antennas", kind: "antennas", live: true, onlyWhen: (properties) => properties.FD_MODE === "FD-C" },
  { key: "RSSI2", label: "Second signal strength", kind: "rssi", live: true, onlyWhen: (properties) => properties.FD_MODE === "FD-C" },
  { key: "CHAN_QUALITY", label: "Channel quality", decode: decode.quality, live: true },
  { key: "INTERFERENCE_STATUS", label: "Interference", decode: decode.word },
  { key: "INTERFERENCE_STATUS2", label: "Second interference", decode: decode.word, onlyWhen: (properties) => properties.FD_MODE === "FD-C" },
  { key: "ENCRYPTION_STATUS", label: "Encryption", decode: decode.word },
  { key: "UNREGISTERED_TX_STATUS", label: "Unregistered transmitter", decode: decode.word },
];

export const AD4D_TRANSMITTER_FIELDS = [
  { key: "TX_MODEL", label: "Model", decode: decode.model },
  { key: "TX_DEVICE_ID", label: "Device ID" },
  { key: "TX_BATT_BARS", label: "Battery bars", decode: decode.bars },
  { key: "TX_BATT_CHARGE_PERCENT", label: "Battery charge", decode: decode.percent },
  { key: "TX_BATT_MINS", label: "Battery runtime", decode: decode.minutes },
  { key: "TX_BATT_HEALTH_PERCENT", label: "Battery health", decode: decode.percent },
  { key: "TX_BATT_CYCLE_COUNT", label: "Battery cycles", decode: decode.cycles },
  { key: "TX_BATT_TEMP_C", label: "Battery temperature", decode: decode.celsius },
  { key: "TX_BATT_TYPE", label: "Battery type", decode: decode.batteryType },
  { key: "TX_POWER_LEVEL", label: "RF power", decode: decode.milliwatts },
  { key: "TX_MUTE_MODE_STATUS", label: "Mute switch", decode: decode.word },
  { key: "TX_LOCK", label: "Lock", decode: decode.word },
  { key: "TX_INPUT_PAD", label: "Input pad", decode: decode.pad },
  { key: "TX_OFFSET", label: "Offset", decode: decode.offset },
  { key: "TX_POLARITY", label: "Polarity", decode: decode.word },
  { key: "TX_TALK_SWITCH", label: "Talk switch", decode: decode.word },
];

export const SLOT_COLUMNS = [
  { key: "SLOT_STATUS", label: "Status", decode: decode.word },
  { key: "SLOT_TX_MODEL", label: "Model", decode: decode.model },
  { key: "SLOT_TX_DEVICE_ID", label: "Device ID", edit: { kind: "text", setting: "SLOT_TX_DEVICE_ID", maxLength: 8 } },
  { key: "SLOT_BATT_BARS", label: "Battery bars", decode: decode.bars },
  { key: "SLOT_BATT_CHARGE_PERCENT", label: "Charge", decode: decode.percent },
  { key: "SLOT_BATT_MINS", label: "Runtime", decode: decode.minutes },
  { key: "SLOT_BATT_HEALTH_PERCENT", label: "Health", decode: decode.percent },
  { key: "SLOT_BATT_CYCLE_COUNT", label: "Cycles", decode: decode.cycles },
  { key: "SLOT_BATT_TYPE", label: "Battery", decode: decode.batteryType },
  { key: "SLOT_RF_POWER", label: "RF power", decode: decode.milliwatts },
  {
    key: "SLOT_RF_POWER_MODE",
    label: "Power mode",
    decode: decode.word,
    edit: { kind: "select", setting: "SLOT_RF_POWER_MODE", options: [["LOW", "Low"], ["NORMAL", "Normal"], ["HIGH", "High"]] },
  },
  { key: "SLOT_RF_OUTPUT", label: "RF output", decode: decode.word, edit: { kind: "select", setting: "SLOT_RF_OUTPUT", options: [["RF_ON", "On"], ["RF_MUTE", "Muted"]] } },
  { key: "SLOT_INPUT_PAD", label: "Input pad", decode: decode.pad, edit: { kind: "select", setting: "SLOT_INPUT_PAD", options: [["on", "On (-12 dB)"], ["off", "Off (0 dB)"]], raw: (value) => (integer(value) === 0 ? "on" : integer(value) === 12 ? "off" : "") } },
  { key: "SLOT_OFFSET", label: "Offset", decode: decode.offset, edit: { kind: "select", setting: "SLOT_OFFSET", options: SLOT_OFFSET_OPTIONS, raw: (value) => (integer(value) === null || integer(value) === 255 ? "" : String(integer(value) - 12)) } },
  { key: "SLOT_POLARITY", label: "Polarity", decode: decode.word, edit: { kind: "select", setting: "SLOT_POLARITY", options: [["POSITIVE", "Positive"], ["NEGATIVE", "Negative"]] } },
  { key: "SLOT_SHOWLINK_STATUS", label: "ShowLink", decode: decode.showlink },
];

export const P10T_DEVICE_FIELDS = [{ key: "DEVICE_NAME", label: "Device name", edit: { kind: "text", setting: "DEVICE_NAME" } }];

export const P10T_CHANNEL_FIELDS = [
  { key: "CHAN_NAME", label: "Name", edit: { kind: "text", setting: "CHAN_NAME" } },
  { key: "FREQUENCY", label: "Frequency", decode: decode.frequency, edit: { kind: "frequency", setting: "FREQUENCY" } },
  { key: "GROUP_CHAN", label: "Group / channel", decode: decode.groupChannel, edit: { kind: "group", setting: "GROUP_CHAN" } },
  { key: "RF_TX_LVL", label: "RF power", decode: decode.milliwatts, edit: { kind: "select", setting: "RF_TX_LVL", options: [["10", "10 mW"], ["50", "50 mW"], ["100", "100 mW"]], raw: (value) => String(integer(value)) } },
  { key: "RF_MUTE", label: "RF mute", decode: decode.rfMute, edit: { kind: "toggle", setting: "RF_MUTE", isOn: (value) => value === "1" } },
  { key: "AUDIO_TX_MODE", label: "Audio mode", decode: decode.transmitMode, edit: { kind: "select", setting: "AUDIO_TX_MODE", options: [["1", "Mono"], ["2", "Point to point"], ["3", "Stereo"]] } },
  { key: "AUDIO_IN_LINE_LVL", label: "Input", decode: decode.inputLevel, edit: { kind: "select", setting: "AUDIO_IN_LINE_LVL", options: [["1", "Line, +4 dBu"], ["0", "Aux, -10 dBV"]] } },
  { key: "AUDIO_IN_LVL", label: "Audio level", edit: { kind: "number", setting: "AUDIO_IN_LVL" } },
  { key: "AUDIO_IN_LVL_L", label: "Input meter left", live: true, decode: p10tMeterLabel },
  { key: "AUDIO_IN_LVL_R", label: "Input meter right", live: true, decode: p10tMeterLabel },
  { key: "METER_RATE", label: "Meter rate", decode: decode.milliseconds },
];

export function p10tMeterDecibels(value) {
  const number = integer(value);
  if (number === null || number <= 0) return null;
  return 20 * Math.log10(Math.min(number, P10T_METER_FULL_SCALE) / P10T_METER_FULL_SCALE);
}

export function p10tMeterLabel(value) {
  const decibels = p10tMeterDecibels(value);
  return decibels === null ? null : `${decibels.toFixed(1)} dB`;
}

export function bits(value, count) {
  const number = integer(value);
  return Array.from({ length: count }, (_, index) => (number === null ? false : Boolean(number & (1 << index))));
}
