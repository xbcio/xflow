import { useEditor } from "../../context/EditorContext";
import {
  publishWorkflow,
  redo,
  runWorkflow,
  saveWorkflow,
  undo,
  validateWorkflow,
} from "../../actions/editorActions";

export interface TopToolbarProps {
  workflowId?: string;
}

const baseButton =
  "inline-flex items-center justify-center gap-1.5 px-3 py-1.5 text-sm rounded border border-editor-border bg-editor-panel text-editor-text hover:bg-editor-hover transition-colors";
const primaryButton = `${baseButton} bg-editor-accent text-white border-editor-accent hover:bg-blue-700`;
const iconButton =
  "inline-flex items-center justify-center w-8 h-8 rounded text-editor-text-secondary hover:text-editor-text hover:bg-editor-hover transition-colors";
const activeIconButton = `${iconButton} bg-blue-50 border border-editor-accent text-editor-accent dark:bg-blue-900/30 dark:text-blue-300`;

export function TopToolbar({ workflowId }: TopToolbarProps) {
  const {
    definition,
    viewport,
    panels,
    theme,
    zoomIn,
    zoomOut,
    fitCanvas,
    toggleLeftSidebar,
    toggleRightSidebar,
    toggleBottomPanel,
    toggleTheme,
  } = useEditor();

  const namespace = definition?.namespace;
  const name = definition?.name;

  const breadcrumbLabel =
    namespace && name
      ? `工作流 / ${namespace} / ${name}`
      : `编辑器: ${workflowId ?? "未命名"}`;

  const zoomPercent = `${Math.round(viewport.zoom * 100)}%`;

  const handleSave = () => {
    if (definition) {
      void saveWorkflow(definition);
    }
  };

  const handleValidate = () => {
    if (definition) {
      void validateWorkflow(definition);
    }
  };

  const handlePublish = () => {
    if (definition) {
      void publishWorkflow(definition);
    }
  };

  const handleRun = () => {
    if (definition) {
      void runWorkflow(definition);
    }
  };

  return (
    <header
      className="flex items-center justify-between h-toolbar px-4 bg-editor-panel border-b border-editor-border shrink-0"
      role="toolbar"
      aria-label="工作流编辑器工具栏"
    >
      <nav aria-label="面包屑">
        <span className="font-medium text-sm text-editor-text">{breadcrumbLabel}</span>
      </nav>

      <div className="flex items-center gap-2">
        <div className="flex items-center gap-1">
          <button
            type="button"
            className={primaryButton}
            onClick={handleSave}
            aria-label="保存工作流"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4" />
            </svg>
            <span>保存</span>
          </button>
          <button
            type="button"
            className={baseButton}
            onClick={handleValidate}
            aria-label="校验工作流"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <span>校验</span>
          </button>
          <button
            type="button"
            className={baseButton}
            onClick={handlePublish}
            aria-label="发布工作流"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
            </svg>
            <span>发布</span>
          </button>
          <button
            type="button"
            className={baseButton}
            onClick={handleRun}
            aria-label="运行工作流"
          >
            <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
              <path d="M8 5v14l11-7z" />
            </svg>
            <span>运行</span>
          </button>
        </div>

        <div className="flex items-center gap-1">
          <button
            type="button"
            className={iconButton}
            onClick={() => void undo()}
            aria-label="撤销"
            title="撤销"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 10h10a8 8 0 018 8v2M3 10l6 6m-6-6l6-6" />
            </svg>
          </button>
          <button
            type="button"
            className={iconButton}
            onClick={() => void redo()}
            aria-label="重做"
            title="重做"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 10h-10a8 8 0 00-8 8v2M21 10l-6 6m6-6l-6-6" />
            </svg>
          </button>
        </div>

        <div className="flex items-center gap-1">
          <button
            type="button"
            className={iconButton}
            onClick={zoomOut}
            aria-label="缩小"
            title="缩小"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 12H4" />
            </svg>
          </button>
          <span
            className="inline-block min-w-[3ch] text-center text-sm tabular-nums text-editor-text"
            aria-live="polite"
          >
            {zoomPercent}
          </span>
          <button
            type="button"
            className={iconButton}
            onClick={zoomIn}
            aria-label="放大"
            title="放大"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
          </button>
          <button
            type="button"
            className={iconButton}
            onClick={fitCanvas}
            aria-label="适配画布"
            title="适配画布"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" />
            </svg>
          </button>
        </div>

        <div className="flex items-center gap-1">
          <button
            type="button"
            className={panels.leftSidebarOpen ? activeIconButton : iconButton}
            onClick={toggleLeftSidebar}
            aria-pressed={panels.leftSidebarOpen}
            aria-label="切换左侧面板"
            title="左侧面板"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h7" />
            </svg>
          </button>
          <button
            type="button"
            className={panels.rightSidebarOpen ? activeIconButton : iconButton}
            onClick={toggleRightSidebar}
            aria-pressed={panels.rightSidebarOpen}
            aria-label="切换右侧面板"
            title="右侧面板"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h7M20 18l-3-3m0 6l3-3" />
            </svg>
          </button>
          <button
            type="button"
            className={panels.bottomPanelOpen ? activeIconButton : iconButton}
            onClick={toggleBottomPanel}
            aria-pressed={panels.bottomPanelOpen}
            aria-label="切换底部面板"
            title="底部面板"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 10h16M4 14h16M4 18h16" />
            </svg>
          </button>
        </div>

        <div className="flex items-center gap-1">
          <button
            type="button"
            className={iconButton}
            onClick={toggleTheme}
            aria-label="切换主题"
            title={theme === "dark" ? "切换到浅色" : "切换到深色"}
          >
            {theme === "dark" ? (
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
              </svg>
            ) : (
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
              </svg>
            )}
          </button>
        </div>
      </div>
    </header>
  );
}
