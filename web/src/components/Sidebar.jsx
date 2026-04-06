export default function Sidebar({
  tree,
  activeConfigName,
  collapsed = false,
  onToggleCollapsed,
  onRefresh,
  onSelectConfig,
  onSelectRun,
  onDeleteRun,
}) {
  function formatStatus(status) {
    return (status || "unknown").toUpperCase();
  }

  const totalRuns = tree.reduce((count, item) => count + (item.runs?.length || 0), 0);

  return (
    <aside className={`sidebar-shell ${collapsed ? "sidebar-collapsed" : ""}`}>
      <div className="sidebar-top">
        <div>
          {collapsed ? (
            <div className="sidebar-collapsed-brand" aria-hidden="true">
              C
            </div>
          ) : (
            <>
              <p className="eyebrow">Experiment Registry</p>
              <h1>CephFS Lab Console</h1>
              <p className="sidebar-subtitle">Grafana-inspired control plane for configs, runs, and metrics.</p>
            </>
          )}
        </div>
        <div className="sidebar-top-actions">
          {!collapsed ? (
            <button className="ghost-button" type="button" onClick={onRefresh}>
              Refresh
            </button>
          ) : null}
          <button
            className="ghost-button sidebar-toggle-btn"
            type="button"
            onClick={onToggleCollapsed}
            aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {collapsed ? ">" : "<"}
          </button>
        </div>
      </div>
      {collapsed ? (
        <div className="sidebar-collapsed-list">
          <button className="ghost-button sidebar-icon-btn" type="button" onClick={onRefresh} title="Refresh">
            R
          </button>
          <div className="sidebar-mini-stat" title={`Configs: ${tree.length}`}>
            <span className="sidebar-mini-label">C</span>
            <strong>{tree.length}</strong>
          </div>
          <div className="sidebar-mini-stat" title={`Runs: ${totalRuns}`}>
            <span className="sidebar-mini-label">R</span>
            <strong>{totalRuns}</strong>
          </div>
          <div className="sidebar-mini-divider" />
          {tree.map((item) => (
            <button
              key={item.config.name}
              type="button"
              className={`sidebar-config-pill ${activeConfigName === item.config.name ? "active" : ""}`}
              onClick={() => onSelectConfig(item.config.name)}
              title={item.config.name}
            >
              {(item.config.name || "?").slice(0, 1).toUpperCase()}
            </button>
          ))}
        </div>
      ) : (
        <>
          <div className="sidebar-summary">
            <div className="sidebar-summary-card">
              <span className="sidebar-summary-label">Config Profiles</span>
              <strong>{tree.length}</strong>
            </div>
            <div className="sidebar-summary-card">
              <span className="sidebar-summary-label">Experiment Runs</span>
              <strong>{totalRuns}</strong>
            </div>
          </div>
          <div className="sidebar-groups">
            {tree.length === 0 ? (
              <div className="empty-state">No parameter configs yet.</div>
            ) : (
              tree.map((item) => (
                <details className="nav-group" key={item.config.name} open>
                  <summary>
                    <span>{item.config.name}</span>
                    <span className="nav-group-count">{item.runs.length}</span>
                  </summary>
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
        </>
      )}
    </aside>
  );
}
