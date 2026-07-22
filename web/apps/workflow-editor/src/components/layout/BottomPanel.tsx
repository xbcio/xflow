import { useMemo } from "react";
import { useEditor } from "../../context/EditorContext";

export function BottomPanel() {
  const { diagnostics, panels, toggleBottomPanel } = useEditor();

  const counts = useMemo(() => {
    const result = { error: 0, warning: 0, info: 0 };
    for (const d of diagnostics) {
      if (d.severity in result) {
        result[d.severity] += 1;
      }
    }
    return result;
  }, [diagnostics]);

  return (
    <section
      className="bottom-panel"
      aria-label="Diagnostics and logs panel"
      data-testid="bottom-panel"
    >
      <header className="bottom-panel__header">
        <h3 className="bottom-panel__title">Diagnostics / Logs</h3>
        <div className="bottom-panel__summary" aria-live="polite">
          <span
            className="bottom-panel__count bottom-panel__count--error"
            data-testid="diag-count-error"
          >
            {counts.error} errors
          </span>
          <span
            className="bottom-panel__count bottom-panel__count--warning"
            data-testid="diag-count-warning"
          >
            {counts.warning} warnings
          </span>
          <span
            className="bottom-panel__count bottom-panel__count--info"
            data-testid="diag-count-info"
          >
            {counts.info} info
          </span>
        </div>
        <button
          type="button"
          className="bottom-panel__collapse"
          onClick={toggleBottomPanel}
          aria-expanded={panels.bottomPanelOpen}
          aria-label="Collapse panel"
          data-testid="bottom-panel-collapse"
        >
          Collapse
        </button>
      </header>

      <div className="bottom-panel__body">
        <div className="bottom-panel__diagnostics">
          {diagnostics.length === 0 ? (
            <p className="bottom-panel__empty" data-testid="diagnostics-empty">
              No diagnostics.
            </p>
          ) : (
            <ul className="bottom-panel__list" data-testid="diagnostics-list">
              {diagnostics.map((d, index) => (
                <li
                  key={`${d.code}-${d.nodeId ?? "global"}-${index}`}
                  className={[
                    "bottom-panel__item",
                    `bottom-panel__item--${d.severity}`,
                  ].join(" ")}
                  data-testid="diagnostics-item"
                >
                  <span className="bottom-panel__code">{d.code}</span>
                  <span className="bottom-panel__message">{d.message}</span>
                  {d.nodeId && (
                    <span className="bottom-panel__node">{d.nodeId}</span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="bottom-panel__logs" data-testid="execution-log">
          <p className="bottom-panel__placeholder">Execution log placeholder</p>
        </div>
      </div>
    </section>
  );
}
