export default function CollapsiblePanel({ eyebrow, title, actions, defaultOpen = false, children, className = "" }) {
  const panelClassName = ["panel", "collapsible-panel", className].filter(Boolean).join(" ");
  const handleActionsClick = (event) => {
    event.stopPropagation();
  };

  return (
    <details className={panelClassName} open={defaultOpen}>
      <summary className="panel-header collapsible-summary">
        <div>
          {eyebrow ? <p className="eyebrow">{eyebrow}</p> : null}
          <h2>{title}</h2>
        </div>
        <div className="collapsible-actions" onClick={handleActionsClick} onMouseDown={handleActionsClick}>
          {actions}
          <span className="collapse-indicator" aria-hidden="true">
            ▾
          </span>
        </div>
      </summary>
      <div className="collapsible-content">{children}</div>
    </details>
  );
}
