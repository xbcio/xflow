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
      className="flex flex-col h-bottom-panel bg-editor-panel border-t border-editor-border shrink-0 transition-all"
      aria-label="诊断和日志面板"
      data-testid="bottom-panel"
      style={{ height: panels.bottomPanelOpen ? undefined : "36px" }}
    >
      <header className="flex items-center gap-4 px-3 py-2 border-b border-editor-border bg-editor-bg">
        <div className="flex items-center gap-2">
          <svg className="w-4 h-4 text-editor-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l3 3-3 3m5 0h4M4 6h16a2 2 0 012 2v8a2 2 0 01-2 2H4a2 2 0 01-2-2V8a2 2 0 012-2z" />
          </svg>
          <h3 className="font-medium text-sm text-editor-text">诊断 / 日志</h3>
        </div>
        <div className="flex items-center gap-3 text-sm" aria-live="polite">
          <span
            className="flex items-center gap-1.5 text-red-600 dark:text-red-400"
            data-testid="diag-count-error"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            {counts.error} 错误
          </span>
          <span
            className="flex items-center gap-1.5 text-yellow-600 dark:text-yellow-400"
            data-testid="diag-count-warning"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
            {counts.warning} 警告
          </span>
          <span
            className="flex items-center gap-1.5 text-blue-600 dark:text-blue-400"
            data-testid="diag-count-info"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            {counts.info} 信息
          </span>
        </div>
        <button
          type="button"
          className="inline-flex items-center justify-center w-8 h-8 rounded text-editor-text-secondary hover:text-editor-text hover:bg-editor-hover transition-colors ml-auto"
          onClick={toggleBottomPanel}
          aria-expanded={panels.bottomPanelOpen}
          aria-label="折叠面板"
          data-testid="bottom-panel-collapse"
        >
          <svg
            className="w-4 h-4 transition-transform"
            style={{ transform: panels.bottomPanelOpen ? "" : "rotate(180deg)" }}
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </button>
      </header>

      {panels.bottomPanelOpen && (
        <div className="flex-1 min-h-0 overflow-auto p-2">
          {diagnostics.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full text-sm text-editor-text-secondary" data-testid="diagnostics-empty">
              <svg className="w-8 h-8 mb-2 opacity-40" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <span>暂无诊断信息</span>
              <span className="text-xs opacity-60 mt-0.5">保存或运行后将在此显示校验结果</span>
            </div>
          ) : (
            <ul className="list-none p-0 m-0 space-y-1" data-testid="diagnostics-list">
              {diagnostics.map((d, index) => (
                <li
                  key={`${d.code}-${d.nodeId ?? "global"}-${index}`}
                  className="flex items-center gap-2 text-sm text-editor-text"
                  data-testid="diagnostics-item"
                >
                  <span className="font-mono text-xs px-1.5 py-0.5 rounded bg-editor-hover text-editor-text">{d.code}</span>
                  <span>{d.message}</span>
                  {d.nodeId && (
                    <span className="text-editor-text-secondary text-xs">{d.nodeId}</span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </section>
  );
}
