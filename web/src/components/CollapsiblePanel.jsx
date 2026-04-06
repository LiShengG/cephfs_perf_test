export default function CollapsiblePanel({
  eyebrow,
  title,
  actions,
  defaultOpen = false,
  density = "normal",
  children,
  className = "",
}) {
  const panelClassName = ["panel", "collapsible-panel", `panel-density-${density}`, className].filter(Boolean).join(" ");
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
            v
          </span>
        </div>
      </summary>
      <div className="collapsible-content">{children}</div>
    </details>
  );
}
