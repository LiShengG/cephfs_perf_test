import LineChart from "./LineChart";
import CollapsiblePanel from "./CollapsiblePanel";

function DetailPane({ title, children }) {
  return (
    <div className="detail-pane">
      <h3>{title}</h3>
      <div className="detail-body">{children}</div>
    </div>
  );
}

export default function RunDetail({
  summaryText,
  stdout,
  stderr,
  metrics,
  selectedMetric,
  onMetricChange,
  onAddChart,
  chartCards,
  chartSeries,
  onRemoveChart,
  analysisNotes,
  onAnalysisNotesChange,
  analysisNotesSavedAt,
  analysisNotesSaving,
  analysisNotesDirty,
  onSaveAnalysisNotes,
}) {
  return (
    <>
      <CollapsiblePanel eyebrow="Analysis" title="Run Detail" defaultOpen={false} density="dense" className="panel-detail">
        <div className="detail-grid">
          <DetailPane title="Raw Summary">
            <pre>{summaryText || "Load a run to inspect final_summary.txt"}</pre>
          </DetailPane>
          <DetailPane title="Stdout">
            <pre>{stdout || "Load a run to inspect stdout"}</pre>
          </DetailPane>
          <DetailPane title="Stderr">
            <pre>{stderr || "Load a run to inspect stderr"}</pre>
          </DetailPane>
        </div>
      </CollapsiblePanel>
      <CollapsiblePanel
        eyebrow="Metrics"
        title="Reported Data Charts"
        defaultOpen={true}
        density="dense"
        className="panel-detail"
        actions={
          <div className="button-row chart-toolbar">
            <select className="metric-select" value={selectedMetric} onChange={(event) => onMetricChange(event.target.value)}>
              {metrics.map((metric) => (
                <option key={metric.key} value={metric.key}>
                  {metric.label} ({metric.key})
                </option>
              ))}
            </select>
            <span className="toolbar-counter">Charts: {chartCards.length}</span>
            <button type="button" onClick={onAddChart}>
              Add Chart
            </button>
          </div>
        }
      >
        <div className="chart-grid">
          {chartCards.length ? chartCards.map((metricKey) => {
            const metric = metrics.find((item) => item.key === metricKey) || { label: metricKey };
            return (
              <div className="chart-card" key={metricKey}>
                <div className="chart-card-header">
                  <div>
                    <h3>{metric.label}</h3>
                    <p>{metricKey}</p>
                  </div>
                  <button type="button" className="ghost-button" onClick={() => onRemoveChart(metricKey)}>
                    Remove
                  </button>
                </div>
                <LineChart data={chartSeries[metricKey]} xAxisLabel="Time" yAxisLabel={metric.label || metricKey} />
              </div>
            );
          }) : <div className="chart-empty">Load a run and add metrics to compare the reported data.</div>}
        </div>
      </CollapsiblePanel>
      <CollapsiblePanel eyebrow="Notes" title="Experiment Conclusion Analysis" defaultOpen={false} density="dense" className="panel-detail">
        <div className="analysis-editor-shell">
          <div className="analysis-toolbar">
            <p className="analysis-helper">
              Record conclusions, anomalies, bottlenecks, and next-step actions for the current experiment run.
            </p>
            <div className="analysis-actions">
              <span className={`analysis-state ${analysisNotesDirty ? "analysis-state-dirty" : "analysis-state-clean"}`}>
                {analysisNotesSaving
                  ? "Saving..."
                  : analysisNotesDirty
                    ? "Unsaved changes"
                    : analysisNotesSavedAt
                      ? `Saved at ${analysisNotesSavedAt}`
                      : "Not saved yet for this run"}
              </span>
              <button type="button" onClick={onSaveAnalysisNotes} disabled={analysisNotesSaving || !analysisNotesDirty}>
                Save
              </button>
            </div>
          </div>
          <textarea
            className="analysis-editor"
            spellCheck="false"
            value={analysisNotes}
            onChange={(event) => onAnalysisNotesChange(event.target.value)}
            placeholder="Write experiment conclusions and analysis notes here..."
          />
        </div>
      </CollapsiblePanel>
    </>
  );
}
