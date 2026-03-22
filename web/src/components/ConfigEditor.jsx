import CollapsiblePanel from "./CollapsiblePanel";

export default function ConfigEditor({
  activeConfigName,
  configText,
  onChange,
  onCreate,
  onSave,
  onRename,
  onDelete,
  message,
  messageError,
}) {
  return (
    <CollapsiblePanel
      eyebrow="Parameter Profile"
      title={activeConfigName || "Select or create a config"}
      defaultOpen={false}
      className="panel-editor"
      actions={
        <div className="button-row">
          <button type="button" onClick={onCreate}>
            New
          </button>
          <button type="button" onClick={onSave} disabled={!activeConfigName}>
            Save
          </button>
          <button type="button" onClick={onRename} disabled={!activeConfigName}>
            Rename
          </button>
          <button type="button" className="danger-button" onClick={onDelete} disabled={!activeConfigName}>
            Delete
          </button>
        </div>
      }
    >
      <div className="status-strip">
        <span>{activeConfigName ? `Current config: ${activeConfigName}` : "Current config: none"}</span>
        <span className={messageError ? "status-error" : "status-ok"}>{message}</span>
      </div>
      <textarea className="editor-area" spellCheck="false" value={configText} onChange={(event) => onChange(event.target.value)} />
    </CollapsiblePanel>
  );
}
