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

export function TopToolbar({ workflowId }: TopToolbarProps) {
  const {
    definition,
    viewport,
    panels,
    zoomIn,
    zoomOut,
    fitCanvas,
    toggleLeftSidebar,
    toggleRightSidebar,
    toggleBottomPanel,
  } = useEditor();

  const namespace = definition?.namespace;
  const name = definition?.name;

  const breadcrumbLabel =
    namespace && name
      ? `Workflows / ${namespace} / ${name}`
      : `Editor: ${workflowId ?? "untitled"}`;

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
    <header className="top-toolbar" role="toolbar" aria-label="Workflow editor toolbar">
      <nav className="top-toolbar__breadcrumb" aria-label="Breadcrumb">
        <span className="top-toolbar__breadcrumb-text">{breadcrumbLabel}</span>
      </nav>

      <div className="top-toolbar__actions">
        <div className="top-toolbar__group top-toolbar__group--primary">
          <button
            type="button"
            className="top-toolbar__button top-toolbar__button--primary"
            onClick={handleSave}
            aria-label="Save workflow"
          >
            Save
          </button>
          <button
            type="button"
            className="top-toolbar__button"
            onClick={handleValidate}
            aria-label="Validate workflow"
          >
            Validate
          </button>
          <button
            type="button"
            className="top-toolbar__button"
            onClick={handlePublish}
            aria-label="Publish workflow"
          >
            Publish
          </button>
          <button
            type="button"
            className="top-toolbar__button"
            onClick={handleRun}
            aria-label="Run workflow"
          >
            Run
          </button>
        </div>

        <div className="top-toolbar__group top-toolbar__group--history">
          <button
            type="button"
            className="top-toolbar__button"
            onClick={() => void undo()}
            aria-label="Undo"
          >
            Undo
          </button>
          <button
            type="button"
            className="top-toolbar__button"
            onClick={() => void redo()}
            aria-label="Redo"
          >
            Redo
          </button>
        </div>

        <div className="top-toolbar__group top-toolbar__group--zoom">
          <button
            type="button"
            className="top-toolbar__button top-toolbar__button--zoom"
            onClick={zoomOut}
            aria-label="Zoom out"
          >
            -
          </button>
          <span className="top-toolbar__zoom-value" aria-live="polite">
            {zoomPercent}
          </span>
          <button
            type="button"
            className="top-toolbar__button top-toolbar__button--zoom"
            onClick={zoomIn}
            aria-label="Zoom in"
          >
            +
          </button>
          <button
            type="button"
            className="top-toolbar__button"
            onClick={fitCanvas}
            aria-label="Fit canvas"
          >
            Fit
          </button>
        </div>

        <div className="top-toolbar__group top-toolbar__group--panels">
          <button
            type="button"
            className={[
              "top-toolbar__button",
              panels.leftSidebarOpen ? "top-toolbar__button--active" : "",
            ]
              .filter(Boolean)
              .join(" ")}
            onClick={toggleLeftSidebar}
            aria-pressed={panels.leftSidebarOpen}
            aria-label="Toggle left sidebar"
          >
            Left
          </button>
          <button
            type="button"
            className={[
              "top-toolbar__button",
              panels.rightSidebarOpen ? "top-toolbar__button--active" : "",
            ]
              .filter(Boolean)
              .join(" ")}
            onClick={toggleRightSidebar}
            aria-pressed={panels.rightSidebarOpen}
            aria-label="Toggle right sidebar"
          >
            Right
          </button>
          <button
            type="button"
            className={[
              "top-toolbar__button",
              panels.bottomPanelOpen ? "top-toolbar__button--active" : "",
            ]
              .filter(Boolean)
              .join(" ")}
            onClick={toggleBottomPanel}
            aria-pressed={panels.bottomPanelOpen}
            aria-label="Toggle bottom panel"
          >
            Bottom
          </button>
        </div>
      </div>
    </header>
  );
}
