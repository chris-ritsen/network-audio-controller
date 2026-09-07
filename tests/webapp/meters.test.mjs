import assert from "node:assert/strict";
import { test } from "node:test";
import { WEBAPP } from "./setup.mjs";

const { drawMeters } = await import(`${WEBAPP}meters.js`);

test("meter frames reuse text layout until names or width change", () => {
  let measured = 0;
  const context = {
    setTransform() {}, clearRect() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() {},
    fillRect() {}, fillText() {},
    measureText(text) { measured += 1; return { width: text.length * 7 }; },
  };
  const node = { getContext: () => context, style: {} };
  const names = { 1: "Receiver" };
  const peaks = new Map();
  drawMeters(node, 240, [1], { 1: 20 }, {}, names, peaks, 0);
  const initial = measured;
  for (let frame = 0; frame < 120; frame += 1) {
    drawMeters(node, 240, [1], { 1: frame }, {}, names, peaks, 1 / 60);
  }
  assert.equal(measured, initial);
  drawMeters(node, 300, [1], { 1: 20 }, {}, names, peaks, 0);
  assert.ok(measured > initial);
  const resized = measured;
  drawMeters(node, 300, [1], { 1: 20 }, {}, { 1: "Renamed" }, peaks, 0);
  assert.ok(measured > resized);
});

test("narrow meters wrap long channel names and keep tracks within the canvas", () => {
  const tracks = [];
  const labels = [];
  const context = {
    setTransform() {}, clearRect() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() {},
    measureText: (text) => ({ width: text.length * 7 }),
    fillRect(...rectangle) { if (this.fillStyle === "#1b1b1b") tracks.push(rectangle); },
    fillText(text, x, y) { if (this.textAlign === "left") labels.push({ text, x, y }); },
  };
  const node = { getContext: () => context, style: {} };
  const name = "A long receiver channel name that cannot fit beside a meter";
  drawMeters(node, 240, [1], { 1: 20 }, {}, { 1: name }, new Map(), 0);
  assert.equal(node.style.width, "240px");
  assert.ok(labels.length > 1);
  assert.equal(labels.map((label) => label.text).join(""), `1  ${name}`);
  for (const label of labels) assert.ok(label.x + context.measureText(label.text).width <= 240);
  for (const [x, , width] of tracks) assert.ok(x >= 0 && x + width <= 240);
});

test("meter tracks keep their geometry as readable signal labels and levels change", () => {
  const tracks = [];
  const labels = [];
  const context = {
    setTransform() {}, clearRect() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() {},
    measureText: (text) => ({ width: text.length * 7 }),
    fillRect(...rectangle) { if (this.fillStyle === "#1b1b1b") tracks.push(rectangle); },
    fillText(text) { if (this.textAlign === "right") labels.push(text); },
  };
  const node = { getContext: () => context, style: {} };
  const samples = [
    [253, "below_threshold", "-126.0 dBFS  Quiet"],
    [30, "signal_present", "-14.5 dBFS  Signal"],
    [0, "clipping", "Clipping"],
    [254, "muted", "Muted"],
    [255, "unknown", "invalid  Unknown"],
    [undefined, undefined, "—"],
  ];
  for (const [level, presence] of samples) {
    drawMeters(node, 600, [1], { 1: level }, { 1: presence }, { 1: "Left" }, new Map(), 0);
  }
  assert.deepEqual(labels, samples.map((sample) => sample[2]));
  assert.equal(tracks.length, samples.length);
  for (const track of tracks) assert.deepEqual(track, tracks[0]);
});
