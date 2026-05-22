const DATA_URL = "/data/demo-insights.json";
const state = {
  report: null,
  signals: [],
  filtered: [],
};

const elements = {
  body: document.querySelector("[data-signals-body]"),
  sourceHealthBody: document.querySelector("[data-source-health-body]"),
  meta: document.querySelector("[data-report-meta]"),
  exportCsv: document.querySelector("[data-export-csv]"),
  market: document.querySelector("[data-filter-market]"),
  status: document.querySelector("[data-filter-status]"),
  delta: document.querySelector("[data-filter-delta]"),
  minMw: document.querySelector("[data-filter-min-mw]"),
  query: document.querySelector("[data-filter-query]"),
  currentSnapshot: document.querySelector("[data-current-snapshot]"),
  previousSnapshot: document.querySelector("[data-previous-snapshot]"),
  sourceHealthOk: document.querySelector("[data-source-health-ok]"),
  sourceHealthTotal: document.querySelector("[data-source-health-total]"),
  limitations: document.querySelector("[data-limitations]"),
  pilotTitle: document.querySelector("[data-pilot-title]"),
  pilotIncluded: document.querySelector("[data-pilot-included]"),
};

const formatNumber = new Intl.NumberFormat("en-US");
const formatDate = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  timeZoneName: "short",
});

async function loadReport() {
  try {
    const response = await fetch(DATA_URL);
    if (!response.ok) throw new Error(`Report data failed to load: ${response.status}`);
    const report = await response.json();
    state.report = report;
    state.signals = report.signals || [];
    state.filtered = state.signals;
    renderSummary(report.summary || {});
    renderFreshness(report);
    renderSourceHealth(report.source_health || []);
    renderLimitations(report.limitations || []);
    renderPilotPackage(report.pilot_package || {});
    populateFilters(state.signals);
    applyFilters();
    elements.meta.textContent = `${report.report_label || "QueueWatch report"} generated ${report.generated_at}. ${report.source_note || ""}`;
  } catch (error) {
    elements.body.innerHTML = `<tr><td colspan="10" class="empty">${escapeHtml(error.message)}</td></tr>`;
    elements.meta.textContent = "Report data unavailable.";
  }
}

function renderSummary(summary) {
  for (const [key, value] of Object.entries(summary)) {
    const target = document.querySelector(`[data-summary="${key}"]`);
    if (target) target.textContent = formatNumber.format(Number(value || 0));
  }
}

function populateFilters(signals) {
  addOptions(elements.market, unique(signals.map((signal) => signal.market)));
  addOptions(elements.status, unique(signals.map((signal) => signal.status)));
  addOptions(elements.delta, unique(signals.map((signal) => signal.delta_type)));
}

function renderFreshness(report) {
  elements.currentSnapshot.textContent = formatObserved(report.current_snapshot_at || report.generated_at);
  elements.previousSnapshot.textContent = formatObserved(report.previous_snapshot_at);
}

function renderSourceHealth(sources) {
  const healthyCount = sources.filter((source) => source.parser_status === "HEALTHY").length;
  elements.sourceHealthOk.textContent = formatNumber.format(healthyCount);
  elements.sourceHealthTotal.textContent = formatNumber.format(sources.length);

  if (!sources.length) {
    elements.sourceHealthBody.innerHTML = '<tr><td colspan="8" class="empty">No source health rows are available.</td></tr>';
    return;
  }

  elements.sourceHealthBody.innerHTML = sources.map(renderSourceHealthRow).join("");
}

function renderSourceHealthRow(source) {
  const healthClass = source.parser_status === "HEALTHY" ? "review-ok" : "review-needs";
  return `
    <tr>
      <td><span class="market">${escapeHtml(source.market || "")}</span></td>
      <td><strong>${escapeHtml(source.source_name || source.source_queue_id || "")}</strong><br>${escapeHtml(source.source_queue_id || "")}</td>
      <td><span class="pill">${escapeHtml(source.format || "SOURCE")}</span></td>
      <td>${escapeHtml(formatObserved(source.last_checked_at))}</td>
      <td class="${healthClass}">${escapeHtml(source.parser_status || "")}</td>
      <td>${formatNumber.format(Number(source.rows_snapshotted || 0))}</td>
      <td>${formatNumber.format(Number(source.change_count || 0))}</td>
      <td>${escapeHtml(source.limitation || "")}</td>
    </tr>
  `;
}

function renderLimitations(limitations) {
  elements.limitations.innerHTML = limitations.length
    ? limitations.map((item) => `<li>${escapeHtml(item)}</li>`).join("")
    : "<li>No report limitations were provided.</li>";
}

function renderPilotPackage(pilotPackage) {
  elements.pilotTitle.textContent = pilotPackage.title || "30-day territory pilot";
  const included = pilotPackage.included || [];
  elements.pilotIncluded.innerHTML = included.length
    ? included.map((item) => `<li>${escapeHtml(item)}</li>`).join("")
    : "<li>Scope source coverage, report cadence, evidence access, and review workflow with the buyer.</li>";
}

function addOptions(select, values) {
  for (const value of values) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.append(option);
  }
}

function applyFilters() {
  const market = elements.market.value;
  const status = elements.status.value;
  const delta = elements.delta.value;
  const minMw = Number(elements.minMw.value || 0);
  const query = elements.query.value.trim().toLowerCase();

  state.filtered = state.signals.filter((signal) => {
    const searchable = [
      signal.interconnection_queue_id,
      signal.project_name,
      signal.substation_or_node,
      signal.developer_name,
      signal.market,
      signal.status,
      signal.delta_type,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();

    return (
      (!market || signal.market === market) &&
      (!status || signal.status === status) &&
      (!delta || signal.delta_type === delta) &&
      Number(signal.capacity_mw || 0) >= minMw &&
      (!query || searchable.includes(query))
    );
  });

  renderSignals(state.filtered);
}

function renderSignals(signals) {
  if (!signals.length) {
    elements.body.innerHTML = '<tr><td colspan="10" class="empty">No signals match the current filters.</td></tr>';
    return;
  }

  elements.body.innerHTML = signals.map(renderSignalRow).join("");
}

function renderSignalRow(signal) {
  const reviewClass = signal.review_status === "AUTO_REVIEWED" ? "review-ok" : "review-needs";
  const sourceLink = signal.source_url
    ? `<a class="source-link" href="${escapeAttribute(signal.source_url)}" target="_blank" rel="noreferrer">Open public source</a>`
    : "";
  return `
    <tr>
      <td>${escapeHtml(formatObserved(signal.observed_at))}</td>
      <td><span class="market">${escapeHtml(signal.market || "")}</span></td>
      <td><span class="pill">${escapeHtml(signal.delta_type || "SOURCE")}</span></td>
      <td><strong>${escapeHtml(signal.interconnection_queue_id || signal.queue_id || "")}</strong><br>${escapeHtml(signal.project_name || "")}</td>
      <td><span class="capacity">${formatNumber.format(Number(signal.capacity_mw || 0))} MW</span></td>
      <td>${escapeHtml(signal.substation_or_node || "")}</td>
      <td>${escapeHtml(signal.status || "")}</td>
      <td>${escapeHtml(signal.developer_name || "")}</td>
      <td class="${reviewClass}">${escapeHtml(signal.review_status || "")}<br><span>${escapeHtml(signal.parser_status || "")}</span></td>
      <td><span class="evidence" title="${escapeAttribute(signal.evidence_key || "")}">${escapeHtml(signal.evidence_key || "")}</span>${sourceLink}</td>
    </tr>
  `;
}

function exportCsv() {
  const fields = [
    "observed_at",
    "market",
    "delta_type",
    "interconnection_queue_id",
    "project_name",
    "capacity_mw",
    "substation_or_node",
    "status",
    "developer_name",
    "review_status",
    "parser_status",
    "evidence_key",
    "source_url",
  ];
  const rows = [fields.join(",")].concat(
    state.filtered.map((signal) =>
      fields.map((field) => csvValue(signal[field])).join(","),
    ),
  );
  const blob = new Blob([rows.join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "queuewatch-filtered-signals.csv";
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function formatObserved(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return formatDate.format(date);
}

function unique(values) {
  return [...new Set(values.filter(Boolean))].sort();
}

function csvValue(value) {
  const text = Array.isArray(value) ? value.join("; ") : String(value ?? "");
  return `"${text.replaceAll('"', '""')}"`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttribute(value) {
  return escapeHtml(value);
}

for (const control of [elements.market, elements.status, elements.delta, elements.minMw, elements.query]) {
  control.addEventListener("input", applyFilters);
}
elements.exportCsv.addEventListener("click", exportCsv);

loadReport();
