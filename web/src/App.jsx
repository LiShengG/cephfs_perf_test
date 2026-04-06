import { useEffect, useMemo, useState, startTransition } from "react";
import Sidebar from "./components/Sidebar";
import ConfigEditor from "./components/ConfigEditor";
import RunControl from "./components/RunControl";
import RunDetail from "./components/RunDetail";
import {
  createConfig,
  deleteConfig,
  deleteRun,
  fetchConfig,
  fetchConfigs,
  fetchCurrentRun,
  fetchMetricSeries,
  fetchRun,
  renameConfig,
  saveAnalysisNotes,
  saveConfig,
  startRun,
  stopRun,
} from "./lib/api";

function useMessage() {
  const [message, setMessageState] = useState("");
  const [error, setError] = useState(false);

  function setMessage(text, isError = false) {
    setMessageState(text);
    setError(isError);
  }

  return { message, error, setMessage };
}

export default function App() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [tree, setTree] = useState([]);
  const [defaultConfig, setDefaultConfig] = useState(null);
  const [metricsSchema, setMetricsSchema] = useState([]);
  const [activeConfigName, setActiveConfigName] = useState(null);
  const [configText, setConfigText] = useState("");
  const [activeRun, setActiveRun] = useState(null);
  const [viewedRun, setViewedRun] = useState(null);
  const [stdoutText, setStdoutText] = useState("");
  const [stderrText, setStderrText] = useState("");
  const [summaryText, setSummaryText] = useState("");
  const [summaryJson, setSummaryJson] = useState(null);
  const [snapshot, setSnapshot] = useState(null);
  const [selectedMetric, setSelectedMetric] = useState("");
  const [chartCards, setChartCards] = useState([]);
  const [chartSeries, setChartSeries] = useState({});
  const [analysisNotes, setAnalysisNotes] = useState("");
  const [savedAnalysisNotes, setSavedAnalysisNotes] = useState("");
  const [analysisNotesSavedAt, setAnalysisNotesSavedAt] = useState(null);
  const [analysisNotesSaving, setAnalysisNotesSaving] = useState(false);
  const { message, error, setMessage } = useMessage();

  const metricOptions = useMemo(() => metricsSchema || [], [metricsSchema]);
  const totalRunCount = useMemo(
    () => tree.reduce((count, item) => count + (item.runs?.length || 0), 0),
    [tree]
  );
  const runningRunCount = useMemo(
    () =>
      tree.reduce(
        (count, item) =>
          count +
          (item.runs?.filter((run) => (run.status || "").toLowerCase() === "running").length || 0),
        0
      ),
    [tree]
  );
  const viewedRunLabel = viewedRun?.run?.run_name || viewedRun?.runId || "No run selected";

  async function refreshRunDetail(configName, runId) {
    const payload = await fetchRun(configName, runId);
    setViewedRun({ configName, runId, run: payload.run });
    setStdoutText(payload.stdout || "");
    setStderrText(payload.stderr || "");
    setSummaryText(payload.summary.summary_text || "");
    setSummaryJson(payload.summary.summary_json || null);
    setSnapshot(payload.snapshot || null);
    setAnalysisNotes(payload.analysisNotes?.text || "");
    setSavedAnalysisNotes(payload.analysisNotes?.text || "");
    setAnalysisNotesSavedAt(payload.analysisNotes?.updated_at || null);
    return payload;
  }

  async function persistAnalysisNotes(configName, runId, text) {
    setAnalysisNotesSaving(true);
    try {
      const payload = await saveAnalysisNotes(configName, runId, text);
      setSavedAnalysisNotes(payload.analysis_notes.text || "");
      setAnalysisNotesSavedAt(payload.analysis_notes.updated_at || null);
    } finally {
      setAnalysisNotesSaving(false);
    }
  }

  async function refreshChartSeries(configName, runId, metricsToRefresh = chartCards) {
    if (!metricsToRefresh.length) {
      return;
    }
    const metricPayloads = await Promise.all(
      metricsToRefresh.map(async (metricKey) => {
        const metricPayload = await fetchMetricSeries(configName, runId, metricKey);
        return [metricKey, metricPayload.data];
      })
    );
    setChartSeries(Object.fromEntries(metricPayloads));
  }

  async function refreshConfigs(keepCurrent = true) {
    const payload = await fetchConfigs();
    setTree(payload.tree);
    setDefaultConfig(payload.default_config);
    setMetricsSchema(payload.metrics_schema.metrics || []);
    startTransition(() => {
      if (!selectedMetric && payload.metrics_schema.metrics?.length) {
        setSelectedMetric(payload.metrics_schema.metrics[0].key);
      }
    });
    if (!keepCurrent && payload.configs.length) {
      await handleSelectConfig(payload.configs[0].name);
    }
    return payload;
  }

  async function refreshCurrentRun() {
    const payload = await fetchCurrentRun();
    setActiveRun(payload.run || null);
  }

  async function handleSelectConfig(name) {
    try {
      const payload = await fetchConfig(name);
      setActiveConfigName(name);
      setConfigText(JSON.stringify(payload.config, null, 2));
      setMessage(`Loaded config ${name}`);
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  async function handleSelectRun(configName, runId) {
    try {
      const initialMetric = selectedMetric || metricOptions[0]?.key || "";
      setChartCards([]);
      setChartSeries({});
      setAnalysisNotes("");
      setSavedAnalysisNotes("");
      setAnalysisNotesSavedAt(null);
      setAnalysisNotesSaving(false);
      await refreshRunDetail(configName, runId);

      if (initialMetric) {
        setSelectedMetric(initialMetric);
        setChartCards([initialMetric]);
        await refreshChartSeries(configName, runId, [initialMetric]);
      }

      setMessage(`Loaded run ${runId}`);
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  async function handleSave() {
    if (!activeConfigName) {
      setMessage("Select or create a config first", true);
      return;
    }
    try {
      const parsed = JSON.parse(configText);
      await saveConfig(activeConfigName, parsed);
      await refreshConfigs();
      setMessage(`Saved config ${activeConfigName}`);
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  async function handleCreate() {
    const name = window.prompt("Input a new config name");
    if (!name) {
      return;
    }
    try {
      const baseConfig = activeConfigName ? JSON.parse(configText) : { ...defaultConfig };
      delete baseConfig.config_name;
      await createConfig(name, baseConfig);
      await refreshConfigs();
      await handleSelectConfig(name);
      setMessage(`Created config ${name}`);
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  async function handleRename() {
    if (!activeConfigName) {
      setMessage("Select a config first", true);
      return;
    }
    const newName = window.prompt("Input the new config name", activeConfigName);
    if (!newName || newName === activeConfigName) {
      return;
    }
    try {
      await renameConfig(activeConfigName, newName);
      await refreshConfigs();
      await handleSelectConfig(newName);
      setMessage(`Renamed config to ${newName}`);
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  async function handleDelete() {
    if (!activeConfigName) {
      setMessage("Select a config first", true);
      return;
    }
    if (
      !window.confirm(
        `Delete config ${activeConfigName} and all of its experiment runs, logs, summaries, and metrics data?`
      )
    ) {
      return;
    }
    try {
      await deleteConfig(activeConfigName, { force: true });
      setActiveConfigName(null);
      setConfigText("");
      if (viewedRun?.configName === activeConfigName) {
        setViewedRun(null);
        setStdoutText("");
        setStderrText("");
        setSummaryText("");
        setSummaryJson(null);
        setSnapshot(null);
        setChartCards([]);
        setChartSeries({});
        setAnalysisNotes("");
        setSavedAnalysisNotes("");
        setAnalysisNotesSavedAt(null);
        setAnalysisNotesSaving(false);
      }
      const payload = await refreshConfigs();
      if (payload.configs.length) {
        await handleSelectConfig(payload.configs[0].name);
      }
      setMessage("Config deleted");
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  async function handleDeleteRun(configName, runId) {
    if (!window.confirm(`Delete experiment run ${runId}? This will remove logs, summaries, metrics, and snapshots.`)) {
      return;
    }
    try {
      await deleteRun(configName, runId);
      if (viewedRun?.configName === configName && viewedRun?.runId === runId) {
        setViewedRun(null);
        setStdoutText("");
        setStderrText("");
        setSummaryText("");
        setSummaryJson(null);
        setSnapshot(null);
        setChartCards([]);
        setChartSeries({});
        setAnalysisNotes("");
        setSavedAnalysisNotes("");
        setAnalysisNotesSavedAt(null);
        setAnalysisNotesSaving(false);
      }
      await refreshConfigs();
      await refreshCurrentRun();
      setMessage(`Deleted run ${runId}`);
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  async function handleStartRun() {
    if (!activeConfigName) {
      setMessage("Select a config first", true);
      return;
    }
    try {
      const payload = await startRun(activeConfigName);
      setActiveRun(payload.run);
      await refreshConfigs();
      setMessage(`Started run ${payload.run.run_name}`);
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  async function handleStopRun() {
    try {
      const payload = await stopRun();
      setActiveRun(payload.run);
      await refreshConfigs();
      setMessage(`Stop requested for ${payload.run.run_name}`);
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  async function handleAddChart() {
    if (!viewedRun) {
      setMessage("Load a run first", true);
      return;
    }
    if (!selectedMetric || chartCards.includes(selectedMetric)) {
      return;
    }
    try {
      const payload = await fetchMetricSeries(viewedRun.configName, viewedRun.runId, selectedMetric);
      setChartCards((items) => [...items, selectedMetric]);
      setChartSeries((items) => ({ ...items, [selectedMetric]: payload.data }));
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  function handleRemoveChart(metricKey) {
    setChartCards((items) => items.filter((item) => item !== metricKey));
    setChartSeries((items) => {
      const next = { ...items };
      delete next[metricKey];
      return next;
    });
  }

  async function handleSaveAnalysisNotes() {
    if (!viewedRun?.configName || !viewedRun?.runId) {
      setMessage("Load a run first", true);
      return;
    }
    try {
      await persistAnalysisNotes(viewedRun.configName, viewedRun.runId, analysisNotes);
      setMessage(`Saved analysis notes for ${viewedRun.runId}`);
    } catch (err) {
      setMessage(err.message, true);
    }
  }

  useEffect(() => {
    let alive = true;

    async function bootstrap() {
      try {
        const configs = await refreshConfigs(false);
        if (alive && configs.configs.length) {
          await handleSelectConfig(configs.configs[0].name);
        }
        await refreshCurrentRun();
      } catch (err) {
        if (alive) {
          setMessage(err.message, true);
        }
      }
    }

    bootstrap();
    const timer = window.setInterval(() => {
      refreshCurrentRun().catch((err) => setMessage(err.message, true));
    }, 5000);

    return () => {
      alive = false;
      window.clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    if (!viewedRun?.configName || !viewedRun?.runId) {
      return undefined;
    }

    let alive = true;

    async function tick() {
      try {
        await refreshChartSeries(viewedRun.configName, viewedRun.runId);
      } catch (err) {
        if (alive) {
          setMessage(err.message, true);
        }
      }
    }

    tick();
    const timer = window.setInterval(tick, 3000);

    return () => {
      alive = false;
      window.clearInterval(timer);
    };
  }, [viewedRun?.configName, viewedRun?.runId, chartCards]);

  useEffect(() => {
    if (!viewedRun?.configName || !viewedRun?.runId) {
      return undefined;
    }
    if (analysisNotes === savedAnalysisNotes) {
      return undefined;
    }

    const timer = window.setTimeout(() => {
      persistAnalysisNotes(viewedRun.configName, viewedRun.runId, analysisNotes)
        .catch((err) => setMessage(err.message, true));
    }, 700);

    return () => {
      window.clearTimeout(timer);
    };
  }, [viewedRun?.configName, viewedRun?.runId, analysisNotes, savedAnalysisNotes]);

  return (
    <div className={`app-shell ${sidebarCollapsed ? "app-shell-sidebar-collapsed" : ""}`}>
      <Sidebar
        tree={tree}
        activeConfigName={activeConfigName}
        collapsed={sidebarCollapsed}
        onToggleCollapsed={() => setSidebarCollapsed((value) => !value)}
        onRefresh={() => refreshConfigs().catch((err) => setMessage(err.message, true))}
        onSelectConfig={handleSelectConfig}
        onSelectRun={handleSelectRun}
        onDeleteRun={handleDeleteRun}
      />
      <main className="main-shell">
        <section className="topbar">
          <div className="topbar-copy">
            <p className="eyebrow">Observability Workspace</p>
            <h1>CephFS Performance Dashboard</h1>
            <p className="topbar-description">
              Inspect benchmark configurations, monitor active experiments, and compare metric timelines in one control
              room.
            </p>
          </div>
          <div className="topbar-stats">
            <div className="topbar-stat">
              <span className="topbar-stat-label">Configs</span>
              <strong>{tree.length}</strong>
            </div>
            <div className="topbar-stat">
              <span className="topbar-stat-label">Runs</span>
              <strong>{totalRunCount}</strong>
            </div>
            <div className="topbar-stat">
              <span className="topbar-stat-label">Running</span>
              <strong>{runningRunCount}</strong>
            </div>
            <div className="topbar-stat topbar-stat-wide">
              <span className="topbar-stat-label">Focus</span>
              <strong>{viewedRunLabel}</strong>
            </div>
          </div>
        </section>
        <section className="workspace-grid">
          <div className="workspace-top-grid">
            <RunControl
              activeConfigName={activeConfigName}
              activeRun={activeRun}
              viewedRun={viewedRun}
              snapshot={snapshot}
              summaryJson={summaryJson}
              onStart={handleStartRun}
              onStop={handleStopRun}
            />
            <ConfigEditor
              activeConfigName={activeConfigName}
              configText={configText}
              onChange={setConfigText}
              onCreate={handleCreate}
              onSave={handleSave}
              onRename={handleRename}
              onDelete={handleDelete}
              message={message}
              messageError={error}
            />
          </div>
          <div className="workspace-main">
            <RunDetail
              summaryText={summaryText}
              stdout={stdoutText}
              stderr={stderrText}
              metrics={metricOptions}
              selectedMetric={selectedMetric}
              onMetricChange={setSelectedMetric}
              onAddChart={handleAddChart}
              chartCards={chartCards}
              chartSeries={chartSeries}
              onRemoveChart={handleRemoveChart}
              analysisNotes={analysisNotes}
              onAnalysisNotesChange={setAnalysisNotes}
              analysisNotesSavedAt={analysisNotesSavedAt}
              analysisNotesSaving={analysisNotesSaving}
              analysisNotesDirty={analysisNotes !== savedAnalysisNotes}
              onSaveAnalysisNotes={handleSaveAnalysisNotes}
            />
          </div>
        </section>
      </main>
    </div>
  );
}
