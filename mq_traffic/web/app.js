"use strict";

const byId = id => document.getElementById(id);
const form = byId("workload");
const metricKeys = ["puts", "gets", "empty", "errors", "done"];
const format = new Intl.NumberFormat();
const windowSeconds = 120;
const pageSize = 12;
let state, previous, samples = [], page = 0, pending = false;

function timeLabel(seconds) {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor(seconds / 60) % 60;
  return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds % 60).padStart(2, "0")}`;
}

function byteLabel(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let index = 0;
  bytes /= 1024;
  while (bytes >= 1024 && index < units.length - 1) {
    bytes /= 1024;
    index++;
  }
  return `${bytes.toFixed(1)} ${units[index]}`;
}

async function request(path, body) {
  const response = await fetch(path, body === undefined ? {} : {
    method: "POST",
    headers: {"X-MQ-Traffic": "1"},
    body
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error);
  return data;
}

function feedback(message) {
  byId("feedback").hidden = !message;
  byId("feedback").textContent = message;
}

function element(tag, text, className) {
  const node = document.createElement(tag);
  node.textContent = text;
  if (className) node.className = className;
  return node;
}

function renderQueues() {
  const query = byId("search").value.trim().toUpperCase();
  const manager = byId("filter").value;
  const matches = (state?.managers ?? []).flatMap(qm =>
    manager === "both" || manager === qm.name
      ? (qm.queues ?? []).filter(q => q.name.includes(query)).map(q => [qm.name, q]) : []);
  const pages = Math.max(1, Math.ceil(matches.length / pageSize));
  page = Math.min(page, pages - 1);
  byId("queue-count").textContent = format.format(matches.length) + " test queues";
  byId("page").textContent = `${page + 1} / ${pages}`;
  byId("prev").disabled = page === 0;
  byId("next").disabled = page + 1 >= pages;
  const rows = matches.slice(page * pageSize, (page + 1) * pageSize).map(([name, q]) => {
    const row = document.createElement("tr");
    row.append(element("td", q.name), element("td", name));
    for (const key of metricKeys.slice(0, 4)) {
      const cell = element("td", format.format(q[key]), key === "errors" && q.errors ? "error" : "");
      if (key === "errors" && q.error) cell.title = q.error;
      row.append(cell);
    }
    return row;
  });
  if (!rows.length) {
    const row = document.createElement("tr");
    const cell = element("td", "No matching queues");
    cell.colSpan = 6;
    row.append(cell);
    rows.push(row);
  }
  byId("queue-rows").replaceChildren(...rows);
}

function drawChart() {
  const canvas = byId("chart");
  const {width, height} = canvas.getBoundingClientRect();
  if (width < 100 || height < 60) return;
  const scale = window.devicePixelRatio || 1;
  canvas.width = Math.round(width * scale);
  canvas.height = Math.round(height * scale);
  const ctx = canvas.getContext("2d");
  ctx.scale(scale, scale);
  const left = 44, right = width - 14, top = 14, bottom = height - 27;
  const now = performance.now() / 1000;
  const visible = samples.filter(sample => sample.time >= now - windowSeconds);
  const peak = Math.max(4, ...visible.flatMap(sample => [sample.puts, sample.gets]));
  const ceiling = 4 * Math.ceil(peak / 4);
  const x = time => right - (now - time) / windowSeconds * (right - left);
  const y = value => bottom - value / ceiling * (bottom - top);
  ctx.font = "12px system-ui";
  ctx.fillStyle = "#9dacbf";
  ctx.strokeStyle = "#29384f";
  ctx.textAlign = "right";
  for (let line = 0; line <= 4; line++) {
    const value = line * ceiling / 4, at = y(value);
    ctx.beginPath();
    ctx.moveTo(left, at);
    ctx.lineTo(right, at);
    ctx.stroke();
    ctx.fillText(String(value), left - 8, at + 4);
  }
  for (let seconds = 0; seconds <= windowSeconds; seconds += 30) {
    const at = x(now - seconds);
    ctx.textAlign = seconds === 0 ? "right" : seconds === windowSeconds ? "left" : "center";
    ctx.fillText(seconds ? `−${seconds}s` : "Now", at, bottom + 19);
  }
  if (!visible.length) {
    ctx.textAlign = "center";
    ctx.fillText("Start a run to see throughput", (left + right) / 2, (top + bottom) / 2);
    return;
  }
  for (const [key, color, dashed] of [["puts", "#54e4c1", false], ["gets", "#88aaff", true]]) {
    ctx.beginPath();
    ctx.lineWidth = dashed ? 2 : 3;
    ctx.strokeStyle = color;
    ctx.setLineDash(dashed ? [5, 5] : []);
    visible.forEach((sample, index) => {
      const coordinate = [x(sample.time), y(sample[key])];
      if (index === 0) ctx.moveTo(...coordinate);
      else ctx.lineTo(...coordinate);
    });
    ctx.stroke();
    ctx.fillStyle = color;
    const tail = visible.at(-1);
    ctx.beginPath();
    ctx.arc(x(tail.time), y(tail[key]), dashed ? 2 : 3, 0, 2 * Math.PI);
    ctx.fill();
  }
}

function renderStatus(next) {
  state = next;
  const totals = Object.fromEntries(metricKeys.map(key =>
    [key, next.managers.reduce((sum, qm) => sum + qm[key], 0)]));
  const time = performance.now() / 1000;
  if (previous && previous.run !== next.run) samples = [];
  if (next.run && previous && previous.run === next.run &&
      (next.active || previous.active || totals.done !== previous.done)) {
    const elapsed = time - previous.time;
    samples.push({time, puts: (totals.puts - previous.puts) / elapsed,
      gets: (totals.gets - previous.gets) / elapsed});
    samples = samples.filter(sample => sample.time >= time - windowSeconds);
  }
  previous = {...totals, time, run: next.run, active: next.active};
  for (const key of metricKeys.slice(0, 4)) byId("total-" + key).textContent = format.format(totals[key]);
  byId("progress-kind").textContent = next.profile
    ? `Elapsed time · ${next.profile[0].toUpperCase() + next.profile.slice(1)}${next.persistent ? " · Mixed" : ""}`
    : "Elapsed time";
  byId("progress").max = next.duration || 1;
  byId("progress").value = next.elapsed;
  byId("progress-label").textContent =
    `${timeLabel(next.elapsed)} / ${timeLabel(next.duration)} · ${format.format(totals.done)} visits`;
  const tuning = byId("tuning-status");
  tuning.hidden = !next.adaptiveWorkers;
  const cpu = next.hostCpuPercent >= 0 ? ` · Host CPU ${next.hostCpuPercent.toFixed(1)}%` : "";
  tuning.textContent = next.adaptiveWorkers
    ? `${next.adaptiveWorkers} active slots per QM · ${byteLabel(next.adaptivePayloadBytes)} fast payload${cpu} · ${next.tuning}`
    : "";
  next.managers.forEach((qm, index) => {
    const header = element("div", "", "section-head");
    header.append(element("h2", qm.name), element("span", next.stopping && qm.state === "Running" ? "Stopping" : qm.state));
    const stats = element("div", "", "manager-data");
    for (const [title, key] of [["Put", "puts"], ["Get", "gets"], ["Empty", "empty"], ["Errors", "errors"]]) {
      const stat = element("span", title + " ");
      stat.append(element("b", format.format(qm[key])));
      stats.append(stat);
    }
    const card = byId("manager-" + index);
    const volume = element("p", `${byteLabel(qm.bytesPut)} put · ${byteLabel(qm.bytesGot)} retrieved`);
    const workload = qm.configuredConnections
      ? element("p", `${qm.connections}/${qm.configuredConnections} connections · Slowest visit ${format.format(qm.slowestVisitMs)} ms`)
      : null;
    card.replaceChildren(header, stats, volume);
    if (workload) card.append(workload);
    if (next.persistent) {
      card.append(element("p", `Persistent: ${format.format(qm.persistentPuts)} put · ${format.format(qm.persistentGets)} retrieved`));
    }
    if (qm.error) card.append(element("p", qm.error, "error"));
  });
  byId("start").disabled = pending || !!next.active;
  byId("stop").disabled = pending || !next.active || next.stopping;
  byId("run-id").hidden = !next.tag;
  byId("tag").textContent = next.tag;
  renderQueues();
  drawChart();
}

async function refresh() {
  try {
    if (!pending) {
      const next = await request("/api/queues");
      if (!pending) renderStatus(next);
    }
    byId("link-state").textContent = "Console online";
    byId("link-state").dataset.online = "true";
  } catch {
    byId("link-state").textContent = "Console disconnected";
    byId("link-state").dataset.online = "false";
  } finally {
    setTimeout(refresh, 1000);
  }
}

async function command(path, body) {
  if (pending) return;
  pending = true;
  byId("start").disabled = true;
  byId("stop").disabled = true;
  feedback("");
  try {
    await request(path, body);
    renderStatus(await request("/api/queues"));
    return true;
  } catch (error) {
    feedback(error.message);
    return false;
  } finally {
    pending = false;
    byId("start").disabled = !!state?.active;
    byId("stop").disabled = !state?.active || state.stopping;
  }
}

form.addEventListener("submit", event => {
  event.preventDefault();
  command("/api/start", new URLSearchParams(new FormData(form)));
});
byId("stop").addEventListener("click", () => command("/api/stop", new URLSearchParams()));
function updateWorkloadFields() {
  const get = form.elements.mode.value === "get";
  byId("tag-field").hidden = !get;
  form.elements.tag.required = get;
  const diskLoad = form.elements.mode.value === "roundtrip" && form.elements.profile.value === "extreme";
  byId("disk-load-field").hidden = !diskLoad;
  form.elements.diskLoad.disabled = !diskLoad;
}
form.elements.mode.addEventListener("change", updateWorkloadFields);
form.elements.profile.addEventListener("change", updateWorkloadFields);
updateWorkloadFields();
form.elements.target.addEventListener("change", () => {
  for (let i = 0; i < 2; i++) {
    byId("connection-" + i).hidden = !["both", String(i)].includes(form.elements.target.value);
  }
});
byId("copy").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(state.tag);
    byId("copy").textContent = "Copied";
    setTimeout(() => { byId("copy").textContent = "Copy"; }, 1000);
  } catch {
    feedback("Select and copy the run ID above.");
  }
});
for (const id of ["search", "filter"]) byId(id).addEventListener("input", () => { page = 0; renderQueues(); });
byId("prev").addEventListener("click", () => { page--; renderQueues(); });
byId("next").addEventListener("click", () => { page++; renderQueues(); });
new ResizeObserver(drawChart).observe(byId("chart"));
refresh();
