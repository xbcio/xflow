// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as editorActions from "../../actions/editorActions";
import * as editorContext from "../../context/EditorContext";
import type { EditorContextValue } from "../../context/EditorContext";
import { TopToolbar } from "./TopToolbar";

const mockSetDefinition = vi.fn();
const mockSetDiagnostics = vi.fn();
const mockSetExecutionSnapshot = vi.fn();
const mockSelectNode = vi.fn();
const mockToggleNodeSelected = vi.fn();
const mockSetCatalogKeyword = vi.fn();
const mockZoomIn = vi.fn();
const mockZoomOut = vi.fn();
const mockFitCanvas = vi.fn();
const mockToggleLeftSidebar = vi.fn();
const mockToggleRightSidebar = vi.fn();
const mockToggleBottomPanel = vi.fn();

const defaultDefinition = {
  id: "wf-1",
  namespace: "default",
  name: "health-check",
  version: "1.0.0",
};

const defaultPanels = {
  leftSidebarOpen: true,
  rightSidebarOpen: false,
  bottomPanelOpen: true,
};

const defaultViewport = { x: 0, y: 0, zoom: 1 };

const defaultEditorMetadata = {
  positions: {},
  viewport: { ...defaultViewport },
  ui: {},
  notes: {},
};

function createMockEditorValue(
  overrides: Partial<EditorContextValue> = {}
): EditorContextValue {
  return {
    definition: defaultDefinition,
    editorMetadata: defaultEditorMetadata,
    diagnostics: [],
    executionSnapshot: null,
    selectedNodeIds: [],
    catalogKeyword: "",
    viewport: defaultViewport,
    panels: defaultPanels,
    setDefinition: mockSetDefinition,
    setDiagnostics: mockSetDiagnostics,
    setExecutionSnapshot: mockSetExecutionSnapshot,
    selectNode: mockSelectNode,
    toggleNodeSelected: mockToggleNodeSelected,
    setCatalogKeyword: mockSetCatalogKeyword,
    zoomIn: mockZoomIn,
    zoomOut: mockZoomOut,
    fitCanvas: mockFitCanvas,
    toggleLeftSidebar: mockToggleLeftSidebar,
    toggleRightSidebar: mockToggleRightSidebar,
    toggleBottomPanel: mockToggleBottomPanel,
    ...overrides,
  };
}

vi.mock("../../context/EditorContext", () => ({
  useEditor: vi.fn(),
}));

vi.mock("../../actions/editorActions", () => ({
  saveWorkflow: vi.fn(),
  validateWorkflow: vi.fn(),
  publishWorkflow: vi.fn(),
  runWorkflow: vi.fn(),
  undo: vi.fn(),
  redo: vi.fn(),
}));

describe("TopToolbar", () => {
  const mockedUseEditor = vi.mocked(editorContext.useEditor);
  const mockedActions = vi.mocked(editorActions);

  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    vi.clearAllMocks();
    mockedUseEditor.mockReturnValue(createMockEditorValue());
  });

  it("renders the breadcrumb and all toolbar buttons", () => {
    render(<TopToolbar workflowId="wf-1" />);

    expect(screen.getByText("Workflows / default / health-check")).toBeDefined();
    expect(screen.getByLabelText("Save workflow")).toBeDefined();
    expect(screen.getByLabelText("Validate workflow")).toBeDefined();
    expect(screen.getByLabelText("Publish workflow")).toBeDefined();
    expect(screen.getByLabelText("Run workflow")).toBeDefined();
    expect(screen.getByLabelText("Undo")).toBeDefined();
    expect(screen.getByLabelText("Redo")).toBeDefined();
    expect(screen.getByLabelText("Zoom out")).toBeDefined();
    expect(screen.getByText("100%")).toBeDefined();
    expect(screen.getByLabelText("Zoom in")).toBeDefined();
    expect(screen.getByLabelText("Fit canvas")).toBeDefined();
    expect(screen.getByLabelText("Toggle left sidebar")).toBeDefined();
    expect(screen.getByLabelText("Toggle right sidebar")).toBeDefined();
    expect(screen.getByLabelText("Toggle bottom panel")).toBeDefined();
  });

  it("falls back to Editor: workflowId when namespace or name is missing", () => {
    mockedUseEditor.mockReturnValue(
      createMockEditorValue({ definition: { id: "wf-2" } })
    );
    render(<TopToolbar workflowId="wf-2" />);

    expect(screen.getByText("Editor: wf-2")).toBeDefined();
  });

  it("calls save, validate, publish and run actions on click", () => {
    render(<TopToolbar workflowId="wf-1" />);

    fireEvent.click(screen.getByLabelText("Save workflow"));
    expect(mockedActions.saveWorkflow).toHaveBeenCalledTimes(1);
    expect(mockedActions.saveWorkflow).toHaveBeenCalledWith(defaultDefinition);

    fireEvent.click(screen.getByLabelText("Validate workflow"));
    expect(mockedActions.validateWorkflow).toHaveBeenCalledTimes(1);
    expect(mockedActions.validateWorkflow).toHaveBeenCalledWith(
      defaultDefinition
    );

    fireEvent.click(screen.getByLabelText("Publish workflow"));
    expect(mockedActions.publishWorkflow).toHaveBeenCalledTimes(1);
    expect(mockedActions.publishWorkflow).toHaveBeenCalledWith(
      defaultDefinition
    );

    fireEvent.click(screen.getByLabelText("Run workflow"));
    expect(mockedActions.runWorkflow).toHaveBeenCalledTimes(1);
    expect(mockedActions.runWorkflow).toHaveBeenCalledWith(defaultDefinition);
  });

  it("calls undo and redo actions on click", () => {
    render(<TopToolbar workflowId="wf-1" />);

    fireEvent.click(screen.getByLabelText("Undo"));
    expect(mockedActions.undo).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("Redo"));
    expect(mockedActions.redo).toHaveBeenCalledTimes(1);
  });

  it("calls zoom and fit actions on click", () => {
    render(<TopToolbar workflowId="wf-1" />);

    fireEvent.click(screen.getByLabelText("Zoom out"));
    expect(mockZoomOut).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("Zoom in"));
    expect(mockZoomIn).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("Fit canvas"));
    expect(mockFitCanvas).toHaveBeenCalledTimes(1);
  });

  it("calls panel toggle actions on click", () => {
    render(<TopToolbar workflowId="wf-1" />);

    fireEvent.click(screen.getByLabelText("Toggle left sidebar"));
    expect(mockToggleLeftSidebar).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("Toggle right sidebar"));
    expect(mockToggleRightSidebar).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("Toggle bottom panel"));
    expect(mockToggleBottomPanel).toHaveBeenCalledTimes(1);
  });

  it("does not call primary action handlers when no definition is loaded", () => {
    mockedUseEditor.mockReturnValue(
      createMockEditorValue({ definition: null })
    );
    render(<TopToolbar workflowId="wf-empty" />);

    fireEvent.click(screen.getByLabelText("Save workflow"));
    expect(mockedActions.saveWorkflow).not.toHaveBeenCalled();

    fireEvent.click(screen.getByLabelText("Validate workflow"));
    expect(mockedActions.validateWorkflow).not.toHaveBeenCalled();

    fireEvent.click(screen.getByLabelText("Publish workflow"));
    expect(mockedActions.publishWorkflow).not.toHaveBeenCalled();

    fireEvent.click(screen.getByLabelText("Run workflow"));
    expect(mockedActions.runWorkflow).not.toHaveBeenCalled();
  });
});
