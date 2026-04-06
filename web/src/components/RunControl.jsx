import CollapsiblePanel from "./CollapsiblePanel";

function Box({ title, children }) {
  return (
    <div className="stat-box">
      <h3>{title}</h3>
      <div className="stat-box-body">{children}</div>
    </div>
  );
}

export default function RunControl({ activeConfigName, activeRun, viewedRun, snapshot, summaryJson, onStart, onStop }) {
  return (
    <CollapsiblePanel
      eyebrow="Execution"
      title="Run Control"
      defaultOpen={true}
      density="dense"
      className="panel-control"
      actions={
        <div className="button-row">
          <button type="button" onClick={onStart} disabled={!activeConfigName || !!activeRun}>
            Start
          </button>
          <button type="button" className="danger-button" onClick={onStop} disabled={!activeRun}>
            Stop
          </button>
        </div>
      }
    >
      <div className="control-grid">
        <Box title="Active Run">
          <pre>{activeRun ? JSON.stringify(activeRun, null, 2) : "No active experiment"}</pre>
        </Box>
        <Box title="Config Snapshot">
          <pre>{snapshot ? JSON.stringify(snapshot, null, 2) : "Load a run to inspect the captured config snapshot"}</pre>
        </Box>
        <Box title="Parsed Summary">
          <pre>{summaryJson ? JSON.stringify(summaryJson, null, 2) : "summary.json has not been generated yet"}</pre>
        </Box>
      </div>
      {viewedRun ? <p className="viewing-line">Viewing run: {viewedRun.runId}</p> : null}
    </CollapsiblePanel>
  );
}
