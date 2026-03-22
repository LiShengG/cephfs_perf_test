export default function Sidebar({ tree, activeConfigName, onRefresh, onSelectConfig, onSelectRun, onDeleteRun }) {
  function formatStatus(status) {
    return (status || "unknown").toUpperCase();
  }

  return (
    <aside className="sidebar-shell">
      <div className="sidebar-top">
        <div>
          <p className="eyebrow">Experiment Registry</p>
          <h1>CephFS Lab Console</h1>
        </div>
        <button className="ghost-button" type="button" onClick={onRefresh}>
          Refresh
        </button>
      </div>
      <div className="sidebar-groups">
        {tree.length === 0 ? (
          <div className="empty-state">No parameter configs yet.</div>
        ) : (
          tree.map((item) => (
            <details className="nav-group" key={item.config.name} open>
              <summary>{item.config.name}</summary>
              <div className="nav-group-body">
                <button
                  type="button"
                  className={`nav-config-button ${activeConfigName === item.config.name ? "active" : ""}`}
                  onClick={() => onSelectConfig(item.config.name)}
                >
                  Edit this config
                </button>
                {item.runs.length === 0 ? (
                  <div className="nav-run-card muted">No experiment instances</div>
                ) : (
                  item.runs.map((run) => (
                    <div className="nav-run-card" key={run.run_id}>
                      <button type="button" className="nav-run-main" onClick={() => onSelectRun(item.config.name, run.run_id)}>
                        <div className="nav-run-topline">
                          <strong>{run.run_name || run.run_id}</strong>
                          <span className={`run-status-chip run-status-${(run.status || "unknown").toLowerCase()}`}>
                            {formatStatus(run.status)}
                          </span>
                        </div>
                        <span className="nav-run-id">{run.run_id}</span>
                        <span>{run.started_at || "-"}</span>
                      </button>
                      <button
                        type="button"
                        className="ghost-button nav-delete-btn"
                        onClick={() => onDeleteRun(item.config.name, run.run_id)}
                      >
                        Delete
                      </button>
                    </div>
                  ))
                )}
              </div>
            </details>
          ))
        )}
      </div>
    </aside>
  );
}
