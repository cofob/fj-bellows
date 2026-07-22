"use strict";

const $ = (id) => document.getElementById(id);
const refreshEvery = 5000;
let timer;
let prompting = false;
let socket;
let socketConnected = false;
let socketRetry;
let eventRefresh;
let apiAuthorized = false;

function escapeHTML(value) {
  return String(value ?? "").replace(/[&<>'"]/g, (char) => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"})[char]);
}

function token() { return sessionStorage.getItem("fjb-dashboard-token") || ""; }
function authHeaders() { const value = token(); return value ? {Authorization: `Bearer ${value}`} : {}; }

function base64URL(value) {
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function askForToken() {
  if (prompting) return false;
  prompting = true;
  const value = window.prompt("This control plane requires a bearer token. The token stays in this browser tab only.", token());
  prompting = false;
  if (value === null) return false;
  if (value.trim()) sessionStorage.setItem("fjb-dashboard-token", value.trim());
  else sessionStorage.removeItem("fjb-dashboard-token");
  return true;
}

function duration(ns) {
  if (!ns || ns < 0) return "—";
  let seconds = Math.round(ns / 1e9);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60); seconds %= 60;
  if (minutes < 60) return `${minutes}m${seconds ? ` ${seconds}s` : ""}`;
  const hours = Math.floor(minutes / 60); const rest = minutes % 60;
  return `${hours}h${rest ? ` ${rest}m` : ""}`;
}

function relativeTime(value) {
  if (!value) return "—";
  const at = new Date(value); if (Number.isNaN(at.getTime())) return "—";
  const seconds = Math.round((at.getTime() - Date.now()) / 1000);
  const abs = Math.abs(seconds);
  let text;
  if (abs < 60) text = `${abs}s`;
  else if (abs < 3600) text = `${Math.round(abs / 60)}m`;
  else if (abs < 86400) text = `${Math.round(abs / 3600)}h`;
  else text = `${Math.round(abs / 86400)}d`;
  return seconds >= 0 ? `in ${text}` : `${text} ago`;
}

function money(nanos, currency) {
  const value = Number(nanos || 0) / 1e9;
  try { return new Intl.NumberFormat(undefined, {style:"currency", currency: currency || "USD", maximumFractionDigits: 4}).format(value); }
  catch (_) { return `${value.toFixed(4)} ${currency || ""}`.trim(); }
}

function statusClass(value) { return String(value || "neutral").toLowerCase().replace(/[^a-z_]/g, ""); }
function taskLabel(item) { return item.job_name || item.workflow || item.handle || "unknown task"; }

function renderHealth(data) {
  const pill = $("health-pill");
  const health = data.health;
  let label = health.healthy ? "healthy" : "degraded";
  let cls = health.healthy ? "good" : "bad";
  if (health.paused) { label = "paused"; cls = "warn"; }
  pill.className = `pill ${cls}`; pill.textContent = label;
  $("freshness").textContent = `${socketConnected ? "live · " : ""}updated ${relativeTime(data.generated_at)}`;
}

function renderSummary(data) {
  const s = data.summary;
  $("workers-total").textContent = s.workers;
  $("workers-idle").textContent = s.idle_workers;
  $("workers-busy").textContent = s.busy_workers;
  $("queue-total").textContent = s.queued_jobs;
  $("queue-optimized").textContent = s.optimization_queued_jobs;
  $("jobs-progress").textContent = s.in_progress_jobs;
  $("jobs-completed").textContent = s.completed_jobs;
  $("jobs-succeeded").textContent = s.succeeded_jobs;
  $("jobs-failed").textContent = s.failed_jobs;
  $("queue-p95").textContent = duration(data.statistics.queue_p95_ns);
  $("run-p95").textContent = duration(data.statistics.run_p95_ns);
}

function renderWorkers(workers) {
  $("worker-count").textContent = workers.length;
  $("workers-body").innerHTML = workers.length ? workers.map((w) => `
    <tr>
      <td><span class="primary mono">${escapeHTML(w.instance_id)}</span><span class="secondary">${escapeHTML(w.ip || w.vpc_ip || "no address")}</span></td>
      <td><span class="primary">${escapeHTML(w.tier || "—")}</span><span class="secondary">${escapeHTML(w.provider || w.driver || "—")}</span></td>
      <td><span class="state ${statusClass(w.state)}">${escapeHTML(w.state)}</span></td>
      <td><span class="primary mono">${escapeHTML(w.current_job || "—")}</span></td>
      <td><span class="primary">${relativeTime(w.created_at)}</span><span class="secondary">${escapeHTML(w.billing_model || "—")}</span></td>
      <td><span class="primary">${relativeTime(w.paid_hour_end_at || w.reap_eligible_at)}</span><span class="secondary">reap ${relativeTime(w.reap_eligible_at)}</span></td>
    </tr>`).join("") : '<tr><td colspan="6" class="empty">No provisioned workers</td></tr>';
}

function renderQueue(queue) {
  $("queue-count").textContent = queue.length;
  $("queue-body").innerHTML = queue.length ? queue.map((job) => {
    const strategy = job.optimization_queued
      ? `<span class="state optimized">paid-window wait #${job.optimization_queue_position}</span><span class="secondary mono">${escapeHTML(job.selected_worker_id)}</span>`
      : `<span class="state observed">${job.routing_state === "pending" ? "routing" : "normal queue"}</span><span class="secondary">${escapeHTML(job.selection_reason || "ready for capacity")}</span>`;
    return `<tr>
      <td><span class="primary">${escapeHTML(taskLabel(job))}</span><span class="secondary">${escapeHTML(job.repository || job.handle)}</span></td>
      <td><span class="primary">${escapeHTML(job.tier || "unassigned")}</span><span class="secondary">${escapeHTML(job.route ? `route ${job.route}` : job.provider || "explicit")}</span></td>
      <td>${strategy}</td>
      <td><span class="primary">wait ${job.optimization_queued ? duration(job.optimization_wait_ns) : relativeTime(job.first_seen_at)}</span><span class="secondary">P95 ${duration(job.predicted_p95_ns)} · start ${relativeTime(job.scheduled_start_at)}</span></td>
    </tr>`;
  }).join("") : '<tr><td colspan="4" class="empty">Queue is empty</td></tr>';
}

function renderJobs(jobs) {
  $("job-count").textContent = jobs.length;
  $("jobs-body").innerHTML = jobs.length ? jobs.map((job) => `
    <tr>
      <td><span class="primary">${escapeHTML(taskLabel(job))}</span><span class="secondary">${escapeHTML(job.repository || job.handle)}</span></td>
      <td><span class="state ${statusClass(job.status)}">${escapeHTML(job.status)}</span>${job.infrastructure_failure ? `<span class="secondary">${escapeHTML(job.infrastructure_failure)}</span>` : ""}</td>
      <td><span class="primary">${escapeHTML(job.tier || "—")}</span><span class="secondary">${escapeHTML(job.provider || "—")}</span></td>
      <td><span class="primary">${duration(job.queue_duration_ns)} / ${duration(job.run_duration_ns)}</span><span class="secondary">queue / run</span></td>
    </tr>`).join("") : '<tr><td colspan="4" class="empty">No task history in this window</td></tr>';
}

function renderCosts(stats) {
  const grouped = new Map();
  for (const cost of [...stats.direct_costs, ...stats.fleet_costs]) {
    const key = cost.currency || "unknown";
    const current = grouped.get(key) || {nanos:0, unknown:0, entries:0};
    current.nanos += Number(cost.nanos || 0); current.unknown += Number(cost.unknown_entries || 0); current.entries += Number(cost.entries || 0);
    grouped.set(key, current);
  }
  $("costs").innerHTML = grouped.size ? [...grouped].map(([currency, total]) => `
    <div class="cost-row"><div><span class="primary">${escapeHTML(currency)} usage</span><span class="secondary">${total.entries} ledger entries · ${total.unknown} unknown prices</span></div><span class="money">${money(total.nanos, currency)}</span></div>`).join("")
    : '<div class="empty">No priced usage in this window</div>';
}

function renderRouting(routes) {
  $("routing").innerHTML = routes.length ? routes.map((route) => {
    const measured = route.p95_hits + route.p95_misses;
    const hit = measured ? `${Math.round(100 * route.p95_hits / measured)}%` : "—";
    return `<div class="route-card">
      <div><strong>${escapeHTML(route.route)}</strong><span class="secondary">${route.decisions} decisions · ${route.idle_decisions} alive-worker selections</span></div>
      <div class="route-stat"><b>${hit}</b><small>P95 hit</small></div>
      <div class="route-stat"><b>${route.fallback_decisions}</b><small>fallback</small></div>
      <div class="route-stat"><b>${money(route.estimated_savings_nanos, route.currency)}</b><small>est. savings</small></div>
    </div>`;
  }).join("") : '<div class="empty">No automatic route decisions</div>';
}

function renderWarnings(warnings) {
  const node = $("warnings");
  if (!warnings?.length) { node.classList.add("hidden"); node.textContent = ""; return; }
  node.classList.remove("hidden"); node.textContent = warnings.join(" · ");
}

function render(data) {
  renderHealth(data); renderSummary(data); renderWorkers(data.workers); renderQueue(data.queue);
  renderJobs(data.jobs); renderCosts(data.statistics); renderRouting(data.statistics.routing); renderWarnings(data.warnings);
}

async function refresh(retryAuth = true) {
  try {
    const response = await fetch(`api?window=${encodeURIComponent($("window").value)}`, {headers: authHeaders(), cache:"no-store"});
    if (response.status === 401) apiAuthorized = false;
    if (response.status === 401 && retryAuth && askForToken()) return refresh(false);
    if (!response.ok) throw new Error(`${response.status} ${await response.text()}`);
    apiAuthorized = true;
    render(await response.json());
    connectEvents();
  } catch (error) {
    $("freshness").textContent = "update failed";
    $("health-pill").className = "pill bad"; $("health-pill").textContent = "offline";
    renderWarnings([String(error)]);
  }
}

function scheduleEventRefresh() {
  clearTimeout(eventRefresh);
  eventRefresh = setTimeout(() => refresh(), 120);
}

function connectEvents() {
  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;
  clearTimeout(socketRetry);
  const scheme = location.protocol === "https:" ? "wss:" : "ws:";
  const protocols = ["fjb-events-v1"];
  if (token()) protocols.push(`fjb-bearer.${base64URL(token())}`);
  socket = new WebSocket(`${scheme}//${location.host}/dashboard/ws`, protocols);
  socket.addEventListener("open", () => { socketConnected = true; });
  socket.addEventListener("message", (message) => {
    try {
      const event = JSON.parse(message.data);
      if (event.kind === "event") scheduleEventRefresh();
    } catch (_) { scheduleEventRefresh(); }
  });
  socket.addEventListener("close", () => {
    socketConnected = false; socket = undefined;
    socketRetry = setTimeout(() => { if (apiAuthorized) connectEvents(); }, 2000);
  });
  socket.addEventListener("error", () => { socketConnected = false; });
}

function arm() { clearInterval(timer); timer = setInterval(() => { if (!document.hidden) refresh(); }, refreshEvery); }
$("window").addEventListener("change", () => { refresh(); arm(); });
$("token").addEventListener("click", () => {
  if (!askForToken()) return;
  if (socket) socket.close();
  socket = undefined;
  refresh(false);
});
document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
refresh(); arm();
