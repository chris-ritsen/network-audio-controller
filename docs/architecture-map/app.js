// ---------- render ----------
const world = document.getElementById("world");
const viewport = document.getElementById("viewport");
const edgeg = document.getElementById("edgeg");
let labelg;
const svgNS = "http://www.w3.org/2000/svg";

for (const z of ZONES) {
  const el = document.createElement("div");
  el.className = "zone";
  el.style.cssText = `left:${z.x}px;top:${z.y}px;width:${z.w}px;height:${z.h}px;--zc:var(${z.c})`;
  el.innerHTML = `<h2>${z.name}</h2><span class="zsub">${z.sub}</span>`;
  world.appendChild(el);
}
const zoneColor = Object.fromEntries(ZONES.map(z => [z.id, z.c]));
const zoneName = Object.fromEntries(ZONES.map(z => [z.id, z.name]));

for (const n of Object.values(N)) {
  const el = document.createElement("div");
  el.className = "node";
  el.tabIndex = 0;
  el.dataset.id = n.id;
  el.style.cssText = `left:${n.x}px;top:${n.y}px;--zc:var(${zoneColor[n.zone]})`;
  el.innerHTML = `<div class="steps"></div>${n.note ? '<div class="pip" title="Surprise">!</div>' : ""}<div class="t">${n.t}</div>` +
    (n.s ? `<div class="s">${n.s}</div>` : "") +
    (n.chips.length ? `<div class="chips">${n.chips.map(c => `<span class="chip">${c}</span>`).join("")}</div>` : "");
  world.appendChild(el);
  n.el = el;
}

{ const ls = document.createElementNS(svgNS, "svg"); ls.setAttribute("class", "labels"); ls.setAttribute("width", "1"); ls.setAttribute("height", "1");
  labelg = document.createElementNS(svgNS, "g"); ls.appendChild(labelg); world.appendChild(ls); }
const rect = id => { const n = N[id]; return { x: n.x, y: n.y, w: n.el.offsetWidth, h: n.el.offsetHeight }; };

function edgePath(a, b) {
  const A = rect(a), B = rect(b);
  let p1, p2, c1, c2;
  if (B.x >= A.x + A.w + 10) {
    p1 = [A.x + A.w, A.y + A.h / 2]; p2 = [B.x, B.y + B.h / 2];
    const dx = Math.max(40, (p2[0] - p1[0]) / 2); c1 = [p1[0] + dx, p1[1]]; c2 = [p2[0] - dx, p2[1]];
  } else if (B.x + B.w <= A.x - 10) {
    p1 = [A.x, A.y + A.h / 2]; p2 = [B.x + B.w, B.y + B.h / 2];
    const dx = Math.max(40, (p1[0] - p2[0]) / 2); c1 = [p1[0] - dx, p1[1]]; c2 = [p2[0] + dx, p2[1]];
  } else {
    const down = B.y > A.y;
    const gap = down ? B.y - (A.y + A.h) : A.y - (B.y + B.h);
    if (gap < 120) {
      p1 = [A.x + A.w / 2, down ? A.y + A.h : A.y]; p2 = [B.x + B.w / 2, down ? B.y : B.y + B.h];
      c1 = [p1[0], (p1[1] + p2[1]) / 2]; c2 = [p2[0], (p1[1] + p2[1]) / 2];
    } else {
      p1 = [A.x + A.w, A.y + A.h / 2]; p2 = [B.x + B.w, B.y + B.h / 2];
      const bulge = 50 + Math.min(60, gap / 10);
      c1 = [p1[0] + bulge, p1[1]]; c2 = [p2[0] + bulge, p2[1]];
    }
  }
  const off = a < b ? -5 : 5;
  p1[1] += off; p2[1] += off; c1[1] += off; c2[1] += off;
  const mid = [0.125 * p1[0] + 0.375 * c1[0] + 0.375 * c2[0] + 0.125 * p2[0], 0.125 * p1[1] + 0.375 * c1[1] + 0.375 * c2[1] + 0.125 * p2[1]];
  return { d: `M${p1} C${c1} ${c2} ${p2}`, mid };
}

const EDGES = {};
function ensureEdge(key) {
  if (EDGES[key]) return EDGES[key];
  const [a, b] = key.split(">");
  if (!N[a] || !N[b]) { console.warn("bad edge", key); return null; }
  const { d, mid } = edgePath(a, b);
  const p = document.createElementNS(svgNS, "path");
  p.setAttribute("d", d); p.setAttribute("class", "e"); p.setAttribute("marker-end", "url(#arr)");
  edgeg.appendChild(p);
  return (EDGES[key] = { key, a, b, el: p, mid, base: false });
}

function layoutEdges() {
  BASE.forEach(k => { const e = ensureEdge(k); if (e) e.base = true; });
  FLOWS.forEach(f => f.steps.forEach(s => s.e && ensureEdge(s.e)));
  for (const e of Object.values(EDGES)) if (!e.base) e.el.style.display = "none";
}

// ---------- camera ----------
const cam = { x: 0, y: 0, k: 0.5 };
const zread = document.getElementById("zread");
function apply() {
  world.style.transform = `translate(${cam.x}px,${cam.y}px) scale(${cam.k})`;
  world.classList.toggle("lod-far", cam.k < 0.42);
  zread.textContent = Math.round(cam.k * 100) + "%";
}
const clampK = k => Math.min(2.5, Math.max(0.12, k));
function zoomAt(px, py, factor) {
  const k = clampK(cam.k * factor); const f = k / cam.k;
  cam.x = px - (px - cam.x) * f; cam.y = py - (py - cam.y) * f; cam.k = k; apply();
}
let anim = null;
const reduce = matchMedia("(prefers-reduced-motion: reduce)").matches;
function animateTo(t) {
  cancelAnimationFrame(anim);
  if (reduce) { Object.assign(cam, t); apply(); return; }
  const s = { ...cam }; const t0 = performance.now(); const dur = 520;
  const step = now => {
    const u = Math.min(1, (now - t0) / dur); const e = 1 - Math.pow(1 - u, 3);
    cam.x = s.x + (t.x - s.x) * e; cam.y = s.y + (t.y - s.y) * e; cam.k = s.k + (t.k - s.k) * e; apply();
    if (u < 1) anim = requestAnimationFrame(step);
  };
  anim = requestAnimationFrame(step);
}
function boundsOf(ids) {
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  for (const id of ids) { const r = rect(id); x0 = Math.min(x0, r.x); y0 = Math.min(y0, r.y); x1 = Math.max(x1, r.x + r.w); y1 = Math.max(y1, r.y + r.h); }
  return { x0, y0, x1, y1 };
}
function fitBox(b, pad = 60, maxK = 1.1, animate = true) {
  const W = viewport.clientWidth, H = viewport.clientHeight;
  const k = clampK(Math.min(maxK, (W - pad * 2) / (b.x1 - b.x0), (H - pad * 2) / (b.y1 - b.y0)));
  const t = { k, x: W / 2 - ((b.x0 + b.x1) / 2) * k, y: H / 2 - ((b.y0 + b.y1) / 2) * k };
  animate ? animateTo(t) : (Object.assign(cam, t), apply());
}
function fitAll(animate = true) {
  fitBox({ x0: Math.min(...ZONES.map(z => z.x)), y0: Math.min(...ZONES.map(z => z.y)) - 60,
    x1: Math.max(...ZONES.map(z => z.x + z.w)), y1: Math.max(...ZONES.map(z => z.y + z.h)) }, 24, 1, animate);
}
function focusNode(id) {
  const r = rect(id); const W = viewport.clientWidth, H = viewport.clientHeight;
  const k = Math.max(cam.k, 0.75);
  animateTo({ k, x: W / 2 - (r.x + r.w / 2) * k, y: H / 2 - (r.y + r.h / 2) * k });
}

// pointer: pan, pinch, click
const pts = new Map(); let dragMoved = 0, pinchD = 0, downNode = null;
viewport.addEventListener("pointerdown", e => {
  if (e.target.closest(".controls")) return;
  viewport.setPointerCapture(e.pointerId);
  pts.set(e.pointerId, { x: e.clientX, y: e.clientY });
  dragMoved = 0; downNode = e.target.closest(".node");
  if (pts.size === 2) { const [p, q] = [...pts.values()]; pinchD = Math.hypot(p.x - q.x, p.y - q.y); }
  cancelAnimationFrame(anim);
});
viewport.addEventListener("pointermove", e => {
  if (!pts.has(e.pointerId)) return;
  const prev = pts.get(e.pointerId); const cur = { x: e.clientX, y: e.clientY };
  pts.set(e.pointerId, cur);
  if (pts.size === 1) {
    cam.x += cur.x - prev.x; cam.y += cur.y - prev.y; dragMoved += Math.abs(cur.x - prev.x) + Math.abs(cur.y - prev.y);
    if (dragMoved > 4) viewport.classList.add("dragging");
    apply();
  } else if (pts.size === 2) {
    const [p, q] = [...pts.values()]; const d = Math.hypot(p.x - q.x, p.y - q.y);
    const r = viewport.getBoundingClientRect();
    if (pinchD) zoomAt((p.x + q.x) / 2 - r.left, (p.y + q.y) / 2 - r.top, d / pinchD);
    pinchD = d; dragMoved = 99;
  }
});
function endPtr(e) {
  if (!pts.has(e.pointerId)) return;
  pts.delete(e.pointerId); viewport.classList.remove("dragging");
  if (pts.size < 2) pinchD = 0;
  if (pts.size === 0 && dragMoved < 5 && downNode) showNode(downNode.dataset.id);
}
viewport.addEventListener("pointerup", endPtr);
viewport.addEventListener("pointercancel", endPtr);
viewport.addEventListener("wheel", e => {
  e.preventDefault();
  const r = viewport.getBoundingClientRect();
  // Trackpad pinch arrives as ctrl+wheel; a mouse wheel sends line steps or large whole-number deltas; anything else is a two-finger pan.
  const mouseWheel = e.deltaMode !== 0 || (e.deltaX === 0 && Number.isInteger(e.deltaY) && Math.abs(e.deltaY) >= 50);
  if (e.ctrlKey || mouseWheel) {
    const dy = e.deltaMode === 1 ? e.deltaY * 33 : e.deltaY;
    zoomAt(e.clientX - r.left, e.clientY - r.top, Math.exp(-dy * (e.ctrlKey ? 0.01 : 0.0015)));
  } else { cam.x -= e.deltaX; cam.y -= e.deltaY; apply(); }
}, { passive: false });
const center = () => [viewport.clientWidth / 2, viewport.clientHeight / 2];
document.getElementById("zin").onclick = () => zoomAt(...center(), 1.25);
document.getElementById("zout").onclick = () => zoomAt(...center(), 0.8);
document.getElementById("zfit").onclick = () => activeFlow ? fitFlow() : fitAll();
world.addEventListener("keydown", e => { const n = e.target.closest(".node"); if (n && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); showNode(n.dataset.id); } });

// ---------- flows & panel ----------
const pane = document.getElementById("pane");
const tabs = { j: document.getElementById("tab-j"), s: document.getElementById("tab-s"), d: document.getElementById("tab-d") };
let tab = "j", activeFlow = null, stepIdx = -1, selected = null;
function setTab(t) { tab = t; for (const k in tabs) tabs[k].setAttribute("aria-selected", k === t); render(); try { localStorage.setItem("nasm.tab", t); } catch {} }
tabs.j.onclick = () => setTab("j"); tabs.s.onclick = () => setTab("s"); tabs.d.onclick = () => setTab("d");

function flowNodes(f) { return [...new Set(f.steps.flatMap(s => [s.n, ...(s.e ? s.e.split(">") : [])]))]; }
function fitFlow() { fitBox(boundsOf(flowNodes(activeFlow)), 70, 0.9); }

function paintFlow() {
  world.classList.toggle("flowing", !!activeFlow);
  for (const n of Object.values(N)) { n.el.classList.remove("inflow", "now"); n.el.querySelector(".steps").innerHTML = ""; }
  for (const e of Object.values(EDGES)) { e.el.classList.remove("inflow", "now"); e.el.setAttribute("marker-end", "url(#arr)"); e.el.style.display = e.base ? "" : "none"; }
  labelg.innerHTML = "";
  if (!activeFlow) return;
  const nums = {};
  activeFlow.steps.forEach((s, i) => {
    (nums[s.n] ||= []).push(i + 1);
    N[s.n].el.classList.add("inflow");
    if (s.e) {
      const e = EDGES[s.e]; if (!e) return;
      e.el.style.display = ""; e.el.classList.add("inflow"); e.el.setAttribute("marker-end", "url(#arrhot)");
      e.el.parentNode.appendChild(e.el);
      s.e.split(">").forEach(id => N[id].el.classList.add("inflow"));
      const t = document.createElementNS(svgNS, "text");
      t.setAttribute("x", e.mid[0]); t.setAttribute("y", e.mid[1] - 6); t.setAttribute("text-anchor", "middle");
      t.setAttribute("class", "elabel" + (i === stepIdx ? " now" : "")); t.textContent = `${i + 1} · ${s.l || ""}`;
      labelg.appendChild(t);
      if (i === stepIdx) e.el.classList.add("now");
    }
  });
  for (const [id, ns] of Object.entries(nums)) {
    const shown = ns.length > 3 ? [...ns.slice(0, 3), "…"] : ns;
    N[id].el.querySelector(".steps").innerHTML = shown.map(x => `<b>${x}</b>`).join("");
  }
  if (stepIdx >= 0) N[activeFlow.steps[stepIdx].n].el.classList.add("now");
}

function selectFlow(id) {
  activeFlow = activeFlow && activeFlow.id === id ? null : FLOWS.find(f => f.id === id);
  stepIdx = -1; paintFlow(); render();
  activeFlow ? fitFlow() : fitAll();
  try { localStorage.setItem("nasm.flow", activeFlow ? activeFlow.id : ""); } catch {}
}
function goStep(i) {
  if (!activeFlow) return;
  stepIdx = Math.max(0, Math.min(activeFlow.steps.length - 1, i));
  paintFlow(); render(); focusNode(activeFlow.steps[stepIdx].n);
  pane.querySelector("li.now")?.scrollIntoView({ block: "nearest" });
}
function showNode(id) {
  if (selected) N[selected]?.el.classList.remove("selected");
  selected = id; N[id].el.classList.add("selected");
  setTab("d");
}

function render() {
  if (tab === "j") {
    if (!activeFlow) {
      pane.innerHTML = `<p class="intro">Each journey lights up its path across the map and numbers the hops. Click a step to fly to it, or use ← → once a journey is open.</p>
        <div class="flowlist">${FLOWS.map(f => `<button class="flowbtn" data-f="${f.id}" aria-pressed="false"><strong>${f.name}</strong><span>${f.steps.length} steps</span></button>`).join("")}</div>`;
    } else {
      const f = activeFlow;
      pane.innerHTML = `<div class="flowhead"><h3>${f.name}</h3><button class="small" id="closeflow">All journeys</button></div>
        <p class="flowsum">${f.sum}</p>
        <ol class="steplist">${f.steps.map((s, i) => `<li data-i="${i}" class="${i === stepIdx ? "now" : ""}"><span class="n">${i + 1}</span><div><div class="st">${s.t}</div><div class="sd">${s.d}</div></div></li>`).join("")}</ol>
        <div class="stepnav"><button class="small" id="prev">← Previous</button><button class="small" id="next">${stepIdx < 0 ? "Start →" : "Next →"}</button></div>`;
      pane.querySelector("#closeflow").onclick = () => selectFlow(f.id);
      pane.querySelector("#prev").onclick = () => goStep(stepIdx - 1);
      pane.querySelector("#next").onclick = () => goStep(stepIdx + 1);
      pane.querySelectorAll("li[data-i]").forEach(li => li.onclick = () => goStep(+li.dataset.i));
    }
    pane.querySelectorAll(".flowbtn").forEach(b => b.onclick = () => selectFlow(b.dataset.f));
  } else if (tab === "s") {
    pane.innerHTML = `<p class="intro">Things in the code that are easy to get wrong when you hold the whole system in your head. Nodes with a yellow <b>!</b> carry one.</p>` +
      SURPRISES.map(s => `<div class="notecard" data-n="${s.node}" tabindex="0"><strong>${s.t}</strong><span>${s.d.replace(/`([^`]+)`/g, "<code>$1</code>")}</span></div>`).join("");
    pane.querySelectorAll(".notecard").forEach(c => c.onclick = () => { focusNode(c.dataset.n); if (selected) N[selected].el.classList.remove("selected"); selected = c.dataset.n; N[selected].el.classList.add("selected"); });
  } else {
    if (!selected) { pane.innerHTML = `<p class="intro">Click any box on the map to see its files, ports and what it does.</p>`; return; }
    const n = N[selected];
    const inFlows = FLOWS.filter(f => flowNodes(f).includes(n.id));
    const surprise = SURPRISES.filter(s => s.node === n.id);
    pane.innerHTML = `<div class="detail"><span class="zone-tag" style="color:var(${zoneColor[n.zone]})">${zoneName[n.zone]}</span>
      <h3>${n.t}</h3>${n.s ? `<div style="color:var(--muted)">${n.s}</div>` : ""}
      <p>${n.body || ""}</p>
      ${n.chips.length ? `<div class="chips" style="display:flex;flex-wrap:wrap;gap:4px">${n.chips.map(c => `<span class="chip">${c}</span>`).join("")}</div>` : ""}
      ${surprise.map(s => `<div class="notecard" style="margin-top:12px;cursor:default"><strong>${s.t}</strong><span>${s.d.replace(/`([^`]+)`/g, "<code>$1</code>")}</span></div>`).join("")}
      ${n.files.length ? `<h4>Where it lives</h4><ul class="files">${n.files.map(f => `<li>${f}</li>`).join("")}</ul>` : ""}
      ${inFlows.length ? `<h4>Appears in</h4><div class="inflows">${inFlows.map(f => `<button class="small" data-f="${f.id}">${f.name}</button>`).join("")}</div>` : ""}
      </div>`;
    pane.querySelectorAll("[data-f]").forEach(b => b.onclick = () => { setTab("j"); if (!activeFlow || activeFlow.id !== b.dataset.f) selectFlow(b.dataset.f); });
  }
}

document.addEventListener("keydown", e => {
  if (e.target.closest("input,textarea")) return;
  if (e.key === "ArrowRight" && activeFlow) { e.preventDefault(); goStep(stepIdx + 1); }
  else if (e.key === "ArrowLeft" && activeFlow) { e.preventDefault(); goStep(stepIdx - 1); }
  else if (e.key === "+" || e.key === "=") zoomAt(...center(), 1.2);
  else if (e.key === "-") zoomAt(...center(), 1 / 1.2);
  else if (e.key === "0") activeFlow ? fitFlow() : fitAll();
  else if (e.key === "Escape" && activeFlow) selectFlow(activeFlow.id);
});

function boot() {
  layoutEdges();
  let saved = "", savedTab = "j";
  try { saved = localStorage.getItem("nasm.flow") || ""; savedTab = localStorage.getItem("nasm.tab") || "j"; } catch {}
  if (savedTab === "d") savedTab = "j";
  tab = savedTab; for (const k in tabs) tabs[k].setAttribute("aria-selected", k === tab);
  fitAll(false);
  if (saved && FLOWS.some(f => f.id === saved)) { activeFlow = FLOWS.find(f => f.id === saved); paintFlow(); fitFlow(); }
  render();
}
(document.fonts && document.fonts.ready ? document.fonts.ready : Promise.resolve()).then(boot);
addEventListener("resize", () => { if (!activeFlow) fitAll(false); });
