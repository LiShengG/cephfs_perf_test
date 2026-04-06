const chartPalette = ["#c95f36", "#1f7a8c", "#6c8c3c", "#8646b5", "#ba3f3a", "#cc8b1f"];

const state = {
  tree: [],
  defaultConfig: null,
  metricsSchema: [],
  activeConfigName: null,
  configText: "",
  activeRun: null,
  viewedRun: null,
  stdoutText: "",
  stderrText: "",
  summaryText: "",
  summaryJson: null,
  snapshot: null,
  selectedMetric: "",
  chartCards: [],
  chartSeries: {},
  analysisNotes: "",
  savedAnalysisNotes: "",
  analysisNotesSavedAt: null,
  analysisNotesSaving: false,
  importing: false,
  importInputMounted: false,
};

let autosaveTimer = null;
let activeRunPoller = null;
let chartPoller = null;

function renderApp() {
  const root = document.getElementById("root");
  root.innerHTML = `
    <div class="app-shell">
      <aside class="sidebar-shell">
        <div class="sidebar-top">
          <div>
            <p class="eyebrow">Experiment Registry</p>
            <h1>CephFS Lab Console</h1>
          </div>
          <div class="button-row sidebar-actions">
            <button id="import-experiment-btn" class="ghost-button" type="button">${state.importing ? "Importing..." : "Import Experiment"}</button>
            <button id="refresh-btn" class="ghost-button" type="button">Refresh</button>
          </div>
        </div>
        <div id="experiment-tree" class="sidebar-groups"></div>
      </aside>
      <main class="main-shell">
        <section class="workspace-grid">
          <div class="workspace-main">
            <details class="panel collapsible-panel panel-density-dense panel-control" open>
              <summary class="panel-header collapsible-summary">
                <div>
                  <p class="eyebrow">Execution</p>
                  <h2>Run Control</h2>
                </div>
                <div class="collapsible-actions">
                  <div class="button-row">
                    <button id="start-run-btn" type="button">Start</button>
                    <button id="stop-run-btn" type="button" class="danger-button">Stop</button>
                  </div>
                  <span class="collapse-indicator" aria-hidden="true">▾</span>
                </div>
              </summary>
              <div class="collapsible-content">
                <div class="control-grid">
                  <div class="stat-box">
                    <h3>Active Run</h3>
                    <div class="stat-box-body"><pre id="active-run-box">No active experiment</pre></div>
                  </div>
                  <div class="stat-box">
                    <h3>Config Snapshot</h3>
                    <div class="stat-box-body"><pre id="snapshot-box">Load a run to inspect the captured config snapshot</pre></div>
                  </div>
                  <div class="stat-box">
                    <h3>Parsed Summary</h3>
                    <div class="stat-box-body"><pre id="summary-json-box">summary.json has not been generated yet</pre></div>
                  </div>
                </div>
                <p id="viewing-run-line" class="viewing-line" ${state.viewedRun ? "" : 'style="display:none"'}></p>
              </div>
            </details>

            <details class="panel collapsible-panel panel-density-dense panel-detail">
              <summary class="panel-header collapsible-summary">
                <div>
                  <p class="eyebrow">Analysis</p>
                  <h2>Run Detail</h2>
                </div>
                <div class="collapsible-actions"><span class="collapse-indicator" aria-hidden="true">▾</span></div>
              </summary>
              <div class="collapsible-content">
                <div class="detail-grid">
                  <div class="detail-pane">
                    <h3>Raw Summary</h3>
                    <div class="detail-body"><pre id="summary-text-box">Load a run to inspect final_summary.txt</pre></div>
                  </div>
                  <div class="detail-pane">
                    <h3>Stdout</h3>
                    <div class="detail-body"><pre id="stdout-box">Load a run to inspect stdout</pre></div>
                  </div>
                  <div class="detail-pane">
                    <h3>Stderr</h3>
                    <div class="detail-body"><pre id="stderr-box">Load a run to inspect stderr</pre></div>
                  </div>
                </div>
              </div>
            </details>

            <details class="panel collapsible-panel panel-density-dense panel-detail" open>
              <summary class="panel-header collapsible-summary">
                <div>
                  <p class="eyebrow">Metrics</p>
                  <h2>Reported Data Charts</h2>
                </div>
                <div class="collapsible-actions">
                  <div class="button-row chart-toolbar">
                    <select id="metric-select" class="metric-select"></select>
                    <span id="chart-counter" class="toolbar-counter">Charts: 0</span>
                    <button id="add-chart-btn" type="button">Add Chart</button>
                  </div>
                  <span class="collapse-indicator" aria-hidden="true">▾</span>
                </div>
              </summary>
              <div class="collapsible-content">
                <div id="chart-grid" class="chart-grid">
                  <div class="chart-empty">Load a run and add metrics to compare the reported data.</div>
                </div>
              </div>
            </details>

            <details class="panel collapsible-panel panel-density-dense panel-detail">
              <summary class="panel-header collapsible-summary">
                <div>
                  <p class="eyebrow">Notes</p>
                  <h2>Experiment Conclusion Analysis</h2>
                </div>
                <div class="collapsible-actions"><span class="collapse-indicator" aria-hidden="true">▾</span></div>
              </summary>
              <div class="collapsible-content">
                <div class="analysis-editor-shell">
                  <div class="analysis-toolbar">
                    <p class="analysis-helper">Record conclusions, anomalies, bottlenecks, and next-step actions for the current experiment run.</p>
                    <div class="analysis-actions">
                      <span id="analysis-state" class="analysis-state analysis-state-clean">Not saved yet for this run</span>
                      <button id="save-analysis-btn" type="button">Save</button>
                    </div>
                  </div>
                  <textarea id="analysis-editor" class="analysis-editor" spellcheck="false" placeholder="Write experiment conclusions and analysis notes here..."></textarea>
                </div>
              </div>
            </details>
          </div>

          <aside class="workspace-side">
            <details class="panel collapsible-panel panel-density-dense panel-editor">
              <summary class="panel-header collapsible-summary">
                <div>
                  <p class="eyebrow">Parameter Profile</p>
                  <h2 id="config-panel-title">Select or create a config</h2>
                </div>
                <div class="collapsible-actions">
                  <div class="button-row">
                    <button id="new-config-btn" type="button">New</button>
                    <button id="save-config-btn" type="button">Save</button>
                    <button id="rename-config-btn" type="button">Rename</button>
                    <button id="delete-config-btn" type="button" class="danger-button">Delete</button>
                  </div>
                  <span class="collapse-indicator" aria-hidden="true">▾</span>
                </div>
              </summary>
              <div class="collapsible-content">
                <div class="status-strip">
                  <span id="current-config-label">Current config: none</span>
                  <span id="message-box" class="status-ok"></span>
                </div>
                <textarea id="config-editor" class="editor-area" spellcheck="false"></textarea>
              </div>
            </details>
          </aside>
        </section>
      </main>
    </div>
  `;

  mountImportInput();
  bindStaticEvents();
  syncView();
}

function mountImportInput() {
  if (state.importInputMounted) {
    return;
  }
  const input = document.createElement("input");
  input.type = "file";
  input.multiple = true;
  input.style.display = "none";
  input.setAttribute("webkitdirectory", "");
  input.setAttribute("directory", "");
  input.id = "import-directory-input";
  input.addEventListener("change", handleImportSelection);
  document.body.appendChild(input);
  state.importInputMounted = true;
}

function bindStaticEvents() {
  document.getElementById("refresh-btn").addEventListener("click", () => {
    refreshConfigs().catch(handleError);
  });
  document.getElementById("import-experiment-btn").addEventListener("click", () => {
    if (state.importing) {
      return;
    }
    document.getElementById("import-directory-input").click();
  });
  document.getElementById("start-run-btn").addEventListener("click", handleStartRun);
  document.getElementById("stop-run-btn").addEventListener("click", handleStopRun);
  document.getElementById("add-chart-btn").addEventListener("click", handleAddChart);
  document.getElementById("metric-select").addEventListener("change", (event) => {
    state.selectedMetric = event.target.value;
  });
  document.getElementById("new-config-btn").addEventListener("click", handleCreateConfig);
  document.getElementById("save-config-btn").addEventListener("click", handleSaveConfig);
  document.getElementById("rename-config-btn").addEventListener("click", handleRenameConfig);
  document.getElementById("delete-config-btn").addEventListener("click", handleDeleteConfig);
  document.getElementById("config-editor").addEventListener("input", (event) => {
    state.configText = event.target.value;
  });
  document.getElementById("save-analysis-btn").addEventListener("click", handleSaveAnalysisNotes);
  document.getElementById("analysis-editor").addEventListener("input", (event) => {
    state.analysisNotes = event.target.value;
    syncAnalysisState();
    queueAnalysisAutosave();
  });
}

function syncView() {
  syncMessage();
  syncConfigPanel();
  syncRunControl();
  syncMetrics();
  syncRunDetails();
  syncAnalysisState();
  renderTree();
}

function setMessage(text, isError = false) {
  state.message = text || "";
  state.messageError = isError;
  syncMessage();
}

function syncMessage() {
  const node = document.getElementById("message-box");
  if (!node) {
    return;
  }
  node.textContent = state.message || "";
  node.className = state.messageError ? "status-error" : "status-ok";
}

function syncConfigPanel() {
  document.getElementById("config-panel-title").textContent = state.activeConfigName || "Select or create a config";
  document.getElementById("current-config-label").textContent = state.activeConfigName
    ? `Current config: ${state.activeConfigName}`
    : "Current config: none";
  document.getElementById("config-editor").value = state.configText || "";
  document.getElementById("save-config-btn").disabled = !state.activeConfigName;
  document.getElementById("rename-config-btn").disabled = !state.activeConfigName;
  document.getElementById("delete-config-btn").disabled = !state.activeConfigName;
}

function syncRunControl() {
  document.getElementById("active-run-box").textContent = state.activeRun
    ? JSON.stringify(state.activeRun, null, 2)
    : "No active experiment";
  document.getElementById("snapshot-box").textContent = state.snapshot
    ? JSON.stringify(state.snapshot, null, 2)
    : "Load a run to inspect the captured config snapshot";
  document.getElementById("summary-json-box").textContent = state.summaryJson
    ? JSON.stringify(state.summaryJson, null, 2)
    : "summary.json has not been generated yet";
  document.getElementById("start-run-btn").disabled = !state.activeConfigName || !!state.activeRun;
  document.getElementById("stop-run-btn").disabled = !state.activeRun;
  const viewingLine = document.getElementById("viewing-run-line");
  if (state.viewedRun) {
    viewingLine.style.display = "";
    viewingLine.textContent = `Viewing run: ${state.viewedRun.runId}`;
  } else {
    viewingLine.style.display = "none";
    viewingLine.textContent = "";
  }
}

function syncRunDetails() {
  document.getElementById("summary-text-box").textContent = state.summaryText || "Load a run to inspect final_summary.txt";
  document.getElementById("stdout-box").textContent = state.stdoutText || "Load a run to inspect stdout";
  document.getElementById("stderr-box").textContent = state.stderrText || "Load a run to inspect stderr";
}

function syncMetrics() {
  const select = document.getElementById("metric-select");
  const chartCounter = document.getElementById("chart-counter");
  const addChartBtn = document.getElementById("add-chart-btn");
  const currentValue = state.selectedMetric;

  select.innerHTML = "";
  for (const metric of state.metricsSchema || []) {
    const option = document.createElement("option");
    option.value = metric.key;
    option.textContent = `${metric.label} (${metric.key})`;
    option.selected = metric.key === currentValue;
    select.appendChild(option);
  }
  if (!state.selectedMetric && state.metricsSchema.length) {
    state.selectedMetric = state.metricsSchema[0].key;
    select.value = state.selectedMetric;
  }
  chartCounter.textContent = `Charts: ${state.chartCards.length}`;
  addChartBtn.disabled = !state.viewedRun;
  renderCharts();
}

function syncAnalysisState() {
  const textarea = document.getElementById("analysis-editor");
  const stateNode = document.getElementById("analysis-state");
  const saveButton = document.getElementById("save-analysis-btn");
  const dirty = state.analysisNotes !== state.savedAnalysisNotes;
  textarea.value = state.analysisNotes || "";
  saveButton.disabled = state.analysisNotesSaving || !dirty;
  stateNode.className = `analysis-state ${dirty ? "analysis-state-dirty" : "analysis-state-clean"}`;
  if (state.analysisNotesSaving) {
    stateNode.textContent = "Saving...";
  } else if (dirty) {
    stateNode.textContent = "Unsaved changes";
  } else if (state.analysisNotesSavedAt) {
    stateNode.textContent = `Saved at ${state.analysisNotesSavedAt}`;
  } else {
    stateNode.textContent = "Not saved yet for this run";
  }
}

function renderTree() {
  const root = document.getElementById("experiment-tree");
  root.innerHTML = "";
  if (!state.tree.length) {
    root.innerHTML = '<div class="empty-state">No parameter configs yet.</div>';
    return;
  }
  for (const item of state.tree) {
    const details = document.createElement("details");
    details.className = "nav-group";
    details.open = true;

    const summary = document.createElement("summary");
    summary.textContent = item.config.name;
    details.appendChild(summary);

    const body = document.createElement("div");
    body.className = "nav-group-body";

    const configButton = document.createElement("button");
    configButton.type = "button";
    configButton.className = `nav-config-button ${state.activeConfigName === item.config.name ? "active" : ""}`;
    configButton.textContent = "Edit this config";
    configButton.addEventListener("click", () => handleSelectConfig(item.config.name));
    body.appendChild(configButton);

    if (!item.runs.length) {
      const empty = document.createElement("div");
      empty.className = "nav-run-card muted";
      empty.textContent = "No experiment instances";
      body.appendChild(empty);
    } else {
      for (const run of item.runs) {
        const card = document.createElement("div");
        card.className = "nav-run-card";

        const main = document.createElement("button");
        main.type = "button";
        main.className = "nav-run-main";
        main.addEventListener("click", () => handleSelectRun(item.config.name, run.run_id));
        main.innerHTML = `
          <div class="nav-run-topline">
            <strong>${escapeHtml(run.run_name || run.run_id)}</strong>
            <span class="run-status-chip run-status-${escapeHtml((run.status || "unknown").toLowerCase())}">
              ${escapeHtml((run.status || "unknown").toUpperCase())}
            </span>
          </div>
          <span class="nav-run-id">${escapeHtml(run.run_id)}</span>
          <span>${escapeHtml(run.started_at || "-")}</span>
        `;

        const deleteBtn = document.createElement("button");
        deleteBtn.type = "button";
        deleteBtn.className = "ghost-button nav-delete-btn";
        deleteBtn.textContent = "Delete";
        deleteBtn.addEventListener("click", () => handleDeleteRun(item.config.name, run.run_id));

        card.appendChild(main);
        card.appendChild(deleteBtn);
        body.appendChild(card);
      }
    }

    details.appendChild(body);
    root.appendChild(details);
  }
}

function renderCharts() {
  const root = document.getElementById("chart-grid");
  root.innerHTML = "";
  if (!state.chartCards.length) {
    root.innerHTML = '<div class="chart-empty">Load a run and add metrics to compare the reported data.</div>';
    return;
  }

  for (const metricKey of state.chartCards) {
    const metric = state.metricsSchema.find((item) => item.key === metricKey) || { label: metricKey, key: metricKey };
    const card = document.createElement("div");
    card.className = "chart-card";
    card.appendChild(createChartCard(metric, state.chartSeries[metricKey]));
    root.appendChild(card);
  }
}

function createChartCard(metric, data) {
  const wrapper = document.createElement("div");
  const header = document.createElement("div");
  header.className = "chart-card-header";
  header.innerHTML = `
    <div>
      <h3>${escapeHtml(metric.label)}</h3>
      <p>${escapeHtml(metric.key)}</p>
    </div>
  `;
  const removeButton = document.createElement("button");
  removeButton.type = "button";
  removeButton.className = "ghost-button";
  removeButton.textContent = "Remove";
  removeButton.addEventListener("click", () => {
    state.chartCards = state.chartCards.filter((item) => item !== metric.key);
    delete state.chartSeries[metric.key];
    syncMetrics();
  });
  header.appendChild(removeButton);
  wrapper.appendChild(header);
  wrapper.appendChild(createChart(metric, data));
  return wrapper;
}

function createChart(metric, data) {
  const wrap = document.createElement("div");
  wrap.className = "chart-wrap";
  const entries = Object.entries((data && data.series) || {});
  if (!entries.length) {
    wrap.innerHTML = '<div class="chart-empty">No data for this metric.</div>';
    return wrap;
  }

  const allPoints = [];
  for (const [seriesName, seriesPoints] of entries) {
    for (const point of seriesPoints) {
      const ts = Date.parse(point.ts || "");
      const value = Number(point.value);
      if (Number.isFinite(ts) && Number.isFinite(value)) {
        allPoints.push({ seriesName, ts, value });
      }
    }
  }
  if (!allPoints.length) {
    wrap.innerHTML = '<div class="chart-empty">Metric exists but no numeric samples are available.</div>';
    return wrap;
  }

  const width = 760;
  const height = 260;
  const left = 60;
  const right = 18;
  const top = 24;
  const bottom = 44;
  const innerWidth = width - left - right;
  const innerHeight = height - top - bottom;

  const minValue = Math.min(...allPoints.map((point) => point.value));
  const maxValue = Math.max(...allPoints.map((point) => point.value));
  const minTs = Math.min(...allPoints.map((point) => point.ts));
  const maxTs = Math.max(...allPoints.map((point) => point.ts));
  const valueRange = maxValue - minValue || 1;
  const tsRange = maxTs - minTs || 1;

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("class", "chart-svg");
  svg.setAttribute("preserveAspectRatio", "none");

  svg.appendChild(svgRect(0, 0, width, height, "#fffdfa"));
  svg.appendChild(svgLine(left, height - bottom, width - right, height - bottom, "#dbcdb6"));
  svg.appendChild(svgLine(left, top, left, height - bottom, "#dbcdb6"));

  entries.forEach(([seriesName, seriesPoints], index) => {
    const color = chartPalette[index % chartPalette.length];
    const points = seriesPoints
      .map((point) => {
        const ts = Date.parse(point.ts || "");
        const value = Number(point.value);
        if (!Number.isFinite(ts) || !Number.isFinite(value)) {
          return null;
        }
        const x = left + ((ts - minTs) / tsRange) * innerWidth;
        const y = top + innerHeight - ((value - minValue) / valueRange) * innerHeight;
        return { x, y, ts, value, seriesName };
      })
      .filter(Boolean);
    if (!points.length) {
      return;
    }
    const polyline = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
    polyline.setAttribute("fill", "none");
    polyline.setAttribute("stroke", color);
    polyline.setAttribute("stroke-width", "2.8");
    polyline.setAttribute("points", points.map((point) => `${point.x.toFixed(2)},${point.y.toFixed(2)}`).join(" "));
    svg.appendChild(polyline);
  });

  svg.appendChild(svgText(left, 16, `min=${minValue.toFixed(2)} max=${maxValue.toFixed(2)}`, "11", "start"));
  svg.appendChild(svgText(width / 2, height - 10, "Time", "12", "middle"));
  const yAxis = svgText(16, height / 2, metric.label || metric.key, "12", "middle");
  yAxis.setAttribute("transform", `rotate(-90 16 ${height / 2})`);
  svg.appendChild(yAxis);
  svg.appendChild(svgText(left, height - bottom + 18, formatDate(minTs), "11", "start"));
  svg.appendChild(svgText(width - right, height - bottom + 18, formatDate(maxTs), "11", "end"));
  svg.appendChild(svgText(left - 8, top + 4, maxValue.toFixed(2), "11", "end"));
  svg.appendChild(svgText(left - 8, height - bottom + 4, minValue.toFixed(2), "11", "end"));

  const coordinates = document.createElement("div");
  coordinates.className = "chart-coordinates";
  coordinates.innerHTML = `
    <span>Move cursor over the chart</span>
    <span>X: Time</span>
    <span>Y: ${escapeHtml(metric.label || metric.key)}</span>
  `;

  const legend = document.createElement("div");
  legend.className = "legend-row";
  entries.forEach(([seriesName], index) => {
    const chip = document.createElement("span");
    chip.className = "legend-chip";
    chip.innerHTML = `
      <span class="legend-dot" style="background-color:${chartPalette[index % chartPalette.length]}"></span>
      ${escapeHtml(seriesName)}
    `;
    legend.appendChild(chip);
  });

  wrap.appendChild(svg);
  wrap.appendChild(coordinates);
  wrap.appendChild(legend);
  return wrap;
}

function svgRect(x, y, width, height, fill) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  node.setAttribute("x", String(x));
  node.setAttribute("y", String(y));
  node.setAttribute("width", String(width));
  node.setAttribute("height", String(height));
  node.setAttribute("fill", fill);
  return node;
}

function svgLine(x1, y1, x2, y2, stroke) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", "line");
  node.setAttribute("x1", String(x1));
  node.setAttribute("y1", String(y1));
  node.setAttribute("x2", String(x2));
  node.setAttribute("y2", String(y2));
  node.setAttribute("stroke", stroke);
  return node;
}

function svgText(x, y, value, fontSize, anchor) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", "text");
  node.setAttribute("x", String(x));
  node.setAttribute("y", String(y));
  node.setAttribute("fill", "#7a6c5b");
  node.setAttribute("font-size", fontSize);
  node.setAttribute("text-anchor", anchor);
  node.textContent = value;
  return node;
}

function formatDate(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "-" : date.toLocaleString();
}

async function api(path, options = {}) {
  const headers = options.headers || {};
  const shouldSetJsonHeader = !(options.body instanceof FormData);
  const response = await fetch(path, {
    ...options,
    headers: shouldSetJsonHeader ? { "Content-Type": "application/json", ...headers } : headers,
  });
  const contentType = response.headers.get("content-type") || "";
  const rawText = await response.text();
  let payload = null;

  if (contentType.includes("application/json")) {
    payload = rawText ? JSON.parse(rawText) : {};
  } else {
    const preview = rawText.trim().slice(0, 120);
    if (preview.startsWith("<!DOCTYPE") || preview.startsWith("<html")) {
      throw new Error(`API ${path} returned HTML instead of JSON. Make sure the Flask backend is running on 127.0.0.1:18080.`);
    }
    throw new Error(`API ${path} returned unexpected content type: ${contentType || "unknown"}`);
  }

  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || `request failed: ${response.status}`);
  }
  return payload;
}

async function refreshConfigs(keepCurrent = true) {
  const payload = await api("/api/configs");
  state.tree = payload.tree;
  state.defaultConfig = payload.default_config;
  state.metricsSchema = payload.metrics_schema.metrics || [];
  if (!state.selectedMetric && state.metricsSchema.length) {
    state.selectedMetric = state.metricsSchema[0].key;
  }
  if (!keepCurrent && payload.configs.length) {
    await handleSelectConfig(payload.configs[0].name);
  }
  syncView();
  return payload;
}

async function refreshCurrentRun() {
  const payload = await api("/api/runs/current");
  state.activeRun = payload.run || null;
  syncRunControl();
}

async function handleSelectConfig(name) {
  try {
    const payload = await api(`/api/configs/${encodeURIComponent(name)}`);
    state.activeConfigName = name;
    state.configText = JSON.stringify(payload.config, null, 2);
    syncConfigPanel();
    renderTree();
    setMessage(`Loaded config ${name}`);
  } catch (error) {
    handleError(error);
  }
}

async function handleSelectRun(configName, runId) {
  try {
    const initialMetric = state.selectedMetric || state.metricsSchema[0]?.key || "";
    state.chartCards = [];
    state.chartSeries = {};
    state.analysisNotes = "";
    state.savedAnalysisNotes = "";
    state.analysisNotesSavedAt = null;
    state.analysisNotesSaving = false;

    const base = `/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}`;
    const [run, stdout, stderr, summary, snapshot, notes] = await Promise.all([
      api(base),
      api(`${base}/logs/stdout`),
      api(`${base}/logs/stderr`),
      api(`${base}/summary`),
      api(`${base}/snapshot`),
      api(`${base}/analysis-notes`),
    ]);

    state.viewedRun = { configName, runId, run: run.run };
    state.stdoutText = stdout.content || "";
    state.stderrText = stderr.content || "";
    state.summaryText = summary.summary.summary_text || "";
    state.summaryJson = summary.summary.summary_json || null;
    state.snapshot = snapshot.snapshot || null;
    state.analysisNotes = notes.analysis_notes?.text || "";
    state.savedAnalysisNotes = notes.analysis_notes?.text || "";
    state.analysisNotesSavedAt = notes.analysis_notes?.updated_at || null;

    if (initialMetric) {
      state.selectedMetric = initialMetric;
      state.chartCards = [initialMetric];
      const metricPayload = await api(`${base}/metrics/series?metric=${encodeURIComponent(initialMetric)}`);
      state.chartSeries[initialMetric] = metricPayload.data;
    }
    restartChartPoller();
    syncView();
    setMessage(`Loaded run ${runId}`);
  } catch (error) {
    handleError(error);
  }
}

async function handleSaveConfig() {
  if (!state.activeConfigName) {
    setMessage("Select or create a config first", true);
    return;
  }
  try {
    await api(`/api/configs/${encodeURIComponent(state.activeConfigName)}`, {
      method: "PUT",
      body: JSON.stringify({ config: JSON.parse(state.configText) }),
    });
    await refreshConfigs();
    setMessage(`Saved config ${state.activeConfigName}`);
  } catch (error) {
    handleError(error);
  }
}

async function handleCreateConfig() {
  const name = window.prompt("Input a new config name");
  if (!name) {
    return;
  }
  try {
    const baseConfig = state.activeConfigName ? JSON.parse(state.configText) : { ...state.defaultConfig };
    delete baseConfig.config_name;
    await api("/api/configs", {
      method: "POST",
      body: JSON.stringify({ name, config: baseConfig }),
    });
    await refreshConfigs();
    await handleSelectConfig(name);
    setMessage(`Created config ${name}`);
  } catch (error) {
    handleError(error);
  }
}

async function handleRenameConfig() {
  if (!state.activeConfigName) {
    setMessage("Select a config first", true);
    return;
  }
  const newName = window.prompt("Input the new config name", state.activeConfigName);
  if (!newName || newName === state.activeConfigName) {
    return;
  }
  try {
    await api(`/api/configs/${encodeURIComponent(state.activeConfigName)}/rename`, {
      method: "POST",
      body: JSON.stringify({ new_name: newName }),
    });
    await refreshConfigs();
    await handleSelectConfig(newName);
    setMessage(`Renamed config to ${newName}`);
  } catch (error) {
    handleError(error);
  }
}

async function handleDeleteConfig() {
  if (!state.activeConfigName) {
    setMessage("Select a config first", true);
    return;
  }
  if (!window.confirm(`Delete config ${state.activeConfigName} and all of its experiment runs, logs, summaries, and metrics data?`)) {
    return;
  }
  try {
    await api(`/api/configs/${encodeURIComponent(state.activeConfigName)}?force=1`, { method: "DELETE" });
    state.activeConfigName = null;
    state.configText = "";
    state.viewedRun = null;
    state.stdoutText = "";
    state.stderrText = "";
    state.summaryText = "";
    state.summaryJson = null;
    state.snapshot = null;
    state.chartCards = [];
    state.chartSeries = {};
    state.analysisNotes = "";
    state.savedAnalysisNotes = "";
    state.analysisNotesSavedAt = null;
    stopChartPoller();
    const payload = await refreshConfigs();
    if (payload.configs.length) {
      await handleSelectConfig(payload.configs[0].name);
    }
    setMessage("Config deleted");
  } catch (error) {
    handleError(error);
  }
}

async function handleDeleteRun(configName, runId) {
  if (!window.confirm(`Delete experiment run ${runId}? This will remove logs, summaries, metrics, and snapshots.`)) {
    return;
  }
  try {
    await api(`/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}`, { method: "DELETE" });
    if (state.viewedRun?.configName === configName && state.viewedRun?.runId === runId) {
      state.viewedRun = null;
      state.stdoutText = "";
      state.stderrText = "";
      state.summaryText = "";
      state.summaryJson = null;
      state.snapshot = null;
      state.chartCards = [];
      state.chartSeries = {};
      state.analysisNotes = "";
      state.savedAnalysisNotes = "";
      state.analysisNotesSavedAt = null;
      stopChartPoller();
      syncView();
    }
    await refreshConfigs();
    await refreshCurrentRun();
    setMessage(`Deleted run ${runId}`);
  } catch (error) {
    handleError(error);
  }
}

async function handleStartRun() {
  if (!state.activeConfigName) {
    setMessage("Select a config first", true);
    return;
  }
  try {
    const payload = await api("/api/runs", {
      method: "POST",
      body: JSON.stringify({ config_name: state.activeConfigName }),
    });
    state.activeRun = payload.run;
    syncRunControl();
    await refreshConfigs();
    setMessage(`Started run ${payload.run.run_name}`);
  } catch (error) {
    handleError(error);
  }
}

async function handleStopRun() {
  try {
    const payload = await api("/api/runs/stop", { method: "POST" });
    state.activeRun = payload.run;
    syncRunControl();
    await refreshConfigs();
    setMessage(`Stop requested for ${payload.run.run_name}`);
  } catch (error) {
    handleError(error);
  }
}

async function handleAddChart() {
  if (!state.viewedRun) {
    setMessage("Load a run first", true);
    return;
  }
  if (!state.selectedMetric || state.chartCards.includes(state.selectedMetric)) {
    return;
  }
  try {
    const payload = await api(
      `/api/configs/${encodeURIComponent(state.viewedRun.configName)}/runs/${encodeURIComponent(state.viewedRun.runId)}/metrics/series?metric=${encodeURIComponent(state.selectedMetric)}`
    );
    state.chartCards.push(state.selectedMetric);
    state.chartSeries[state.selectedMetric] = payload.data;
    syncMetrics();
  } catch (error) {
    handleError(error);
  }
}

async function handleSaveAnalysisNotes() {
  if (!state.viewedRun?.configName || !state.viewedRun?.runId) {
    setMessage("Load a run first", true);
    return;
  }
  try {
    await saveAnalysisNotes(state.analysisNotes);
    setMessage(`Saved analysis notes for ${state.viewedRun.runId}`);
  } catch (error) {
    handleError(error);
  }
}

async function saveAnalysisNotes(text) {
  state.analysisNotesSaving = true;
  syncAnalysisState();
  const payload = await api(
    `/api/configs/${encodeURIComponent(state.viewedRun.configName)}/runs/${encodeURIComponent(state.viewedRun.runId)}/analysis-notes`,
    {
      method: "PUT",
      body: JSON.stringify({ text }),
    }
  );
  state.savedAnalysisNotes = payload.analysis_notes.text || "";
  state.analysisNotes = payload.analysis_notes.text || "";
  state.analysisNotesSavedAt = payload.analysis_notes.updated_at || null;
  state.analysisNotesSaving = false;
  syncAnalysisState();
}

function queueAnalysisAutosave() {
  if (!state.viewedRun?.configName || !state.viewedRun?.runId || state.analysisNotes === state.savedAnalysisNotes) {
    return;
  }
  window.clearTimeout(autosaveTimer);
  autosaveTimer = window.setTimeout(() => {
    saveAnalysisNotes(state.analysisNotes).catch(handleError);
  }, 700);
}

async function handleImportSelection(event) {
  const files = Array.from(event.target.files || []);
  if (!files.length) {
    return;
  }
  state.importing = true;
  renderApp();
  try {
    const manifest = files.map((file) => ({
      path: file.webkitRelativePath || file.name,
      size: file.size,
    }));
    const formData = new FormData();
    formData.append("manifest", JSON.stringify(manifest));
    for (const file of files) {
      formData.append("files", file, file.name);
    }
    const payload = await api("/api/imports/experiment", {
      method: "POST",
      body: formData,
    });
    state.importing = false;
    renderApp();
    await refreshConfigs();
    await handleSelectRun(payload.config_name, payload.run_id);
    setMessage(`Imported experiment ${payload.source_dir_name}`);
  } catch (error) {
    state.importing = false;
    renderApp();
    handleError(error);
  } finally {
    event.target.value = "";
  }
}

async function refreshActiveCharts() {
  if (!state.viewedRun || !state.chartCards.length) {
    return;
  }
  const nextSeries = {};
  for (const metricKey of state.chartCards) {
    const payload = await api(
      `/api/configs/${encodeURIComponent(state.viewedRun.configName)}/runs/${encodeURIComponent(state.viewedRun.runId)}/metrics/series?metric=${encodeURIComponent(metricKey)}`
    );
    nextSeries[metricKey] = payload.data;
  }
  state.chartSeries = nextSeries;
  renderCharts();
}

function restartChartPoller() {
  stopChartPoller();
  if (!state.viewedRun) {
    return;
  }
  chartPoller = window.setInterval(() => {
    refreshActiveCharts().catch(handleError);
  }, 3000);
}

function stopChartPoller() {
  if (chartPoller) {
    window.clearInterval(chartPoller);
    chartPoller = null;
  }
}

function startActiveRunPoller() {
  if (activeRunPoller) {
    window.clearInterval(activeRunPoller);
  }
  activeRunPoller = window.setInterval(() => {
    refreshCurrentRun().catch(handleError);
  }, 5000);
}

function handleError(error) {
  setMessage(error.message || String(error), true);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function bootstrap() {
  renderApp();
  startActiveRunPoller();
  try {
    const payload = await refreshConfigs(false);
    if (payload.configs.length) {
      await handleSelectConfig(payload.configs[0].name);
    }
    await refreshCurrentRun();
  } catch (error) {
    handleError(error);
  }
}

bootstrap();
