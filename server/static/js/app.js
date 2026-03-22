const state = {
  activeConfigName: null,
  activeRun: null,
  viewedRun: null,
  defaultConfig: null,
  metricsSchema: [],
  chartMetrics: [],
};

const chartPalette = ["#a14d28", "#246a73", "#758e4f", "#7d4e9f", "#b23a2e", "#006d77"];

function setMessage(message, isError = false) {
  const node = document.getElementById("message");
  node.textContent = message || "";
  node.style.color = isError ? "#b23a2e" : "#7e3111";
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const payload = await response.json();
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || `request failed: ${response.status}`);
  }
  return payload;
}

function updateActiveConfigLabel() {
  const label = document.getElementById("active-config-label");
  label.textContent = state.activeConfigName ? `Current Config: ${state.activeConfigName}` : "Current Config: None";
  document.getElementById("start-run-btn").disabled = !state.activeConfigName || !!state.activeRun;
  document.getElementById("stop-run-btn").disabled = !state.activeRun;
  document.getElementById("add-chart-btn").disabled = !state.viewedRun;
}

function formatRunLabel(run) {
  return `${run.run_name || run.run_id} | ${run.status || "unknown"} | ${run.started_at || "-"}`;
}

function renderTree(tree) {
  const root = document.getElementById("experiment-tree");
  root.innerHTML = "";
  if (!tree.length) {
    root.innerHTML = "<p>No configs yet.</p>";
    return;
  }

  for (const item of tree) {
    const group = document.createElement("details");
    group.className = "tree-group";
    group.open = true;

    const summary = document.createElement("summary");
    summary.textContent = item.config.name;
    group.appendChild(summary);

    const runList = document.createElement("div");
    runList.className = "tree-run-list";

    const configButton = document.createElement("button");
    configButton.className = "tree-config-button";
    configButton.textContent = "Edit Config";
    if (item.config.name === state.activeConfigName) {
      configButton.classList.add("active");
    }
    configButton.addEventListener("click", () => loadConfig(item.config.name));
    runList.appendChild(configButton);

    if (!item.runs.length) {
      const empty = document.createElement("div");
      empty.className = "tree-run-item";
      empty.textContent = "No experiment runs";
      runList.appendChild(empty);
    } else {
      for (const run of item.runs) {
        const runItem = document.createElement("button");
        runItem.className = "tree-run-item";
        runItem.innerHTML = `<strong>${run.run_name || run.run_id}</strong><span class="tree-run-meta">${formatRunLabel(run)}</span>`;
        runItem.addEventListener("click", () => loadRun(item.config.name, run.run_id));
        runList.appendChild(runItem);
      }
    }

    group.appendChild(runList);
    root.appendChild(group);
  }
}

function renderCurrentRun(run) {
  const currentBox = document.getElementById("current-run-box");
  state.activeRun = run || null;
  currentBox.textContent = run ? JSON.stringify(run, null, 2) : "No active experiment";
  updateActiveConfigLabel();
}

function populateMetricSelect(metrics) {
  const select = document.getElementById("metric-select");
  select.innerHTML = "";
  for (const metric of metrics) {
    const option = document.createElement("option");
    option.value = metric.key;
    option.textContent = `${metric.label} (${metric.key})`;
    select.appendChild(option);
  }
}

async function refreshConfigs() {
  try {
    const payload = await api("/api/configs");
    state.defaultConfig = payload.default_config;
    state.metricsSchema = payload.metrics_schema.metrics || [];
    populateMetricSelect(state.metricsSchema);
    renderTree(payload.tree);
    if (!state.activeConfigName && payload.configs.length) {
      await loadConfig(payload.configs[0].name);
    } else {
      updateActiveConfigLabel();
    }
  } catch (error) {
    setMessage(error.message, true);
  }
}

async function refreshTreeOnly() {
  const payload = await api("/api/configs");
  renderTree(payload.tree);
}

async function refreshCurrentRun() {
  try {
    const payload = await api("/api/runs/current");
    renderCurrentRun(payload.run);
  } catch (error) {
    setMessage(error.message, true);
  }
}

async function loadConfig(name) {
  try {
    const payload = await api(`/api/configs/${encodeURIComponent(name)}`);
    state.activeConfigName = name;
    document.getElementById("config-editor").value = JSON.stringify(payload.config, null, 2);
    updateActiveConfigLabel();
    await refreshTreeOnly();
    setMessage(`Loaded config ${name}`);
  } catch (error) {
    setMessage(error.message, true);
  }
}

async function loadRun(configName, runId) {
  try {
    const [runPayload, stdoutPayload, stderrPayload, summaryPayload, snapshotPayload] = await Promise.all([
      api(`/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}`),
      api(`/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}/logs/stdout`),
      api(`/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}/logs/stderr`),
      api(`/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}/summary`),
      api(`/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}/snapshot`),
    ]);
    state.viewedRun = { configName, runId };
    document.getElementById("stdout-box").textContent = stdoutPayload.content || "stdout is empty";
    document.getElementById("stderr-box").textContent = stderrPayload.content || "stderr is empty";
    document.getElementById("current-run-box").textContent = JSON.stringify(runPayload.run, null, 2);
    document.getElementById("snapshot-box").textContent = JSON.stringify(snapshotPayload.snapshot, null, 2);
    document.getElementById("summary-text-box").textContent = summaryPayload.summary.summary_text || "summary text is empty";
    document.getElementById("summary-json-box").textContent = summaryPayload.summary.summary_json
      ? JSON.stringify(summaryPayload.summary.summary_json, null, 2)
      : "summary.json not generated";
    state.chartMetrics = [];
    renderCharts();
    updateActiveConfigLabel();
    setMessage(`Loaded run ${runId}`);
  } catch (error) {
    setMessage(error.message, true);
  }
}

async function saveCurrentConfig() {
  if (!state.activeConfigName) {
    setMessage("Select or create a config first", true);
    return;
  }
  try {
    const config = JSON.parse(document.getElementById("config-editor").value);
    await api(`/api/configs/${encodeURIComponent(state.activeConfigName)}`, {
      method: "PUT",
      body: JSON.stringify({ config }),
    });
    await refreshTreeOnly();
    setMessage(`Saved config ${state.activeConfigName}`);
  } catch (error) {
    setMessage(error.message, true);
  }
}

async function createConfig() {
  const name = window.prompt("Input a new config name");
  if (!name) {
    return;
  }
  try {
    const baseConfig = state.activeConfigName
      ? JSON.parse(document.getElementById("config-editor").value)
      : { ...state.defaultConfig };
    delete baseConfig.config_name;
    await api("/api/configs", {
      method: "POST",
      body: JSON.stringify({ name, config: baseConfig }),
    });
    await refreshConfigs();
    await loadConfig(name);
    setMessage(`Created config ${name}`);
  } catch (error) {
    setMessage(error.message, true);
  }
}

async function renameConfig() {
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
    state.activeConfigName = newName;
    await refreshConfigs();
    await loadConfig(newName);
    setMessage(`Renamed config to ${newName}`);
  } catch (error) {
    setMessage(error.message, true);
  }
}

async function deleteConfig() {
  if (!state.activeConfigName) {
    setMessage("Select a config first", true);
    return;
  }
  if (!window.confirm(`Delete config ${state.activeConfigName}?`)) {
    return;
  }
  try {
    await api(`/api/configs/${encodeURIComponent(state.activeConfigName)}`, { method: "DELETE" });
    state.activeConfigName = null;
    document.getElementById("config-editor").value = "";
    await refreshConfigs();
    setMessage("Config deleted");
  } catch (error) {
    setMessage(error.message, true);
  }
}

async function startRun() {
  if (!state.activeConfigName) {
    setMessage("Select a config first", true);
    return;
  }
  try {
    const payload = await api("/api/runs", {
      method: "POST",
      body: JSON.stringify({ config_name: state.activeConfigName }),
    });
    renderCurrentRun(payload.run);
    await refreshTreeOnly();
    setMessage(`Started run ${payload.run.run_name}`);
  } catch (error) {
    setMessage(error.message, true);
  }
}

async function stopRun() {
  try {
    const payload = await api("/api/runs/stop", { method: "POST" });
    renderCurrentRun(payload.run);
    await refreshTreeOnly();
    setMessage(`Stop requested for ${payload.run.run_name}`);
  } catch (error) {
    setMessage(error.message, true);
  }
}

function addChartMetric() {
  if (!state.viewedRun) {
    setMessage("Load a run first", true);
    return;
  }
  const metric = document.getElementById("metric-select").value;
  if (!metric || state.chartMetrics.includes(metric)) {
    return;
  }
  state.chartMetrics.push(metric);
  renderCharts();
}

function removeChartMetric(metric) {
  state.chartMetrics = state.chartMetrics.filter((item) => item !== metric);
  renderCharts();
}

async function renderCharts() {
  const chartGrid = document.getElementById("chart-grid");
  chartGrid.innerHTML = "";
  if (!state.viewedRun || !state.chartMetrics.length) {
    return;
  }

  for (const metric of state.chartMetrics) {
    const metricInfo = state.metricsSchema.find((item) => item.key === metric) || { key: metric, label: metric };
    const card = document.createElement("div");
    card.className = "chart-card";
    card.innerHTML = `
      <div class="chart-card-header">
        <h3>${metricInfo.label}</h3>
        <button type="button" class="ghost-btn remove-chart-btn">Remove</button>
      </div>
      <div class="chart-area">Loading...</div>
      <div class="chart-legend"></div>
    `;
    card.querySelector(".remove-chart-btn").addEventListener("click", () => removeChartMetric(metric));
    chartGrid.appendChild(card);

    try {
      const payload = await api(
        `/api/configs/${encodeURIComponent(state.viewedRun.configName)}/runs/${encodeURIComponent(state.viewedRun.runId)}/metrics/series?metric=${encodeURIComponent(metric)}`
      );
      drawChart(card.querySelector(".chart-area"), card.querySelector(".chart-legend"), payload.data);
    } catch (error) {
      card.querySelector(".chart-area").textContent = error.message;
    }
  }
}

function drawChart(container, legendNode, payload) {
  const entries = Object.entries(payload.series || {});
  if (!entries.length) {
    container.textContent = "No data for this metric";
    legendNode.innerHTML = "";
    return;
  }

  const width = 700;
  const height = 240;
  const padding = 28;
  const allPoints = entries.flatMap(([, series]) => series);
  const values = allPoints.map((point) => Number(point.value)).filter((value) => Number.isFinite(value));
  if (!values.length) {
    container.textContent = "No numeric data";
    legendNode.innerHTML = "";
    return;
  }

  const timestamps = allPoints
    .map((point) => Date.parse(point.ts || ""))
    .filter((value) => Number.isFinite(value));
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const minTs = Math.min(...timestamps);
  const maxTs = Math.max(...timestamps);
  const valueSpan = maxValue - minValue || 1;
  const timeSpan = maxTs - minTs || 1;

  let svg = `<svg viewBox="0 0 ${width} ${height}" width="100%" height="${height}" xmlns="http://www.w3.org/2000/svg">`;
  svg += `<rect x="0" y="0" width="${width}" height="${height}" fill="#ffffff" />`;
  svg += `<line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" stroke="#d4c7ae" />`;
  svg += `<line x1="${padding}" y1="${padding}" x2="${padding}" y2="${height - padding}" stroke="#d4c7ae" />`;

  legendNode.innerHTML = "";

  entries.forEach(([name, series], index) => {
    const color = chartPalette[index % chartPalette.length];
    const points = series
      .map((item) => {
        const ts = Date.parse(item.ts || "");
        const value = Number(item.value);
        if (!Number.isFinite(ts) || !Number.isFinite(value)) {
          return null;
        }
        const x = padding + ((ts - minTs) / timeSpan) * (width - padding * 2);
        const y = height - padding - ((value - minValue) / valueSpan) * (height - padding * 2);
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      })
      .filter(Boolean)
      .join(" ");

    if (points) {
      svg += `<polyline fill="none" stroke="${color}" stroke-width="2.5" points="${points}" />`;
    }

    const legend = document.createElement("span");
    legend.className = "legend-item";
    legend.innerHTML = `<span class="legend-swatch" style="background:${color}"></span>${name}`;
    legendNode.appendChild(legend);
  });

  svg += `<text x="${padding}" y="18" fill="#6b6154" font-size="11">min=${minValue.toFixed(2)} max=${maxValue.toFixed(2)}</text>`;
  svg += "</svg>";
  container.innerHTML = svg;
}

function bindEvents() {
  document.getElementById("refresh-tree-btn").addEventListener("click", refreshConfigs);
  document.getElementById("save-config-btn").addEventListener("click", saveCurrentConfig);
  document.getElementById("new-config-btn").addEventListener("click", createConfig);
  document.getElementById("rename-config-btn").addEventListener("click", renameConfig);
  document.getElementById("delete-config-btn").addEventListener("click", deleteConfig);
  document.getElementById("start-run-btn").addEventListener("click", startRun);
  document.getElementById("stop-run-btn").addEventListener("click", stopRun);
  document.getElementById("add-chart-btn").addEventListener("click", addChartMetric);
}

document.addEventListener("DOMContentLoaded", async () => {
  bindEvents();
  await refreshConfigs();
  await refreshCurrentRun();
  window.setInterval(refreshCurrentRun, 5000);
});
