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
const mockToggleTheme = vi.fn();

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
    theme: "light",
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
    toggleTheme: mockToggleTheme,
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

    expect(screen.getByText("工作流 / default / health-check")).toBeDefined();
    expect(screen.getByLabelText("保存工作流")).toBeDefined();
    expect(screen.getByLabelText("校验工作流")).toBeDefined();
    expect(screen.getByLabelText("发布工作流")).toBeDefined();
    expect(screen.getByLabelText("运行工作流")).toBeDefined();
    expect(screen.getByLabelText("撤销")).toBeDefined();
    expect(screen.getByLabelText("重做")).toBeDefined();
    expect(screen.getByLabelText("缩小")).toBeDefined();
    expect(screen.getByText("100%")).toBeDefined();
    expect(screen.getByLabelText("放大")).toBeDefined();
    expect(screen.getByLabelText("适配画布")).toBeDefined();
    expect(screen.getByLabelText("切换左侧面板")).toBeDefined();
    expect(screen.getByLabelText("切换右侧面板")).toBeDefined();
    expect(screen.getByLabelText("切换底部面板")).toBeDefined();
    expect(screen.getByLabelText("切换主题")).toBeDefined();
  });

  it("falls back to Editor: workflowId when namespace or name is missing", () => {
    mockedUseEditor.mockReturnValue(
      createMockEditorValue({ definition: { id: "wf-2" } })
    );
    render(<TopToolbar workflowId="wf-2" />);

    expect(screen.getByText("编辑器: wf-2")).toBeDefined();
  });

  it("calls save, validate, publish and run actions on click", () => {
    render(<TopToolbar workflowId="wf-1" />);

    fireEvent.click(screen.getByLabelText("保存工作流"));
    expect(mockedActions.saveWorkflow).toHaveBeenCalledTimes(1);
    expect(mockedActions.saveWorkflow).toHaveBeenCalledWith(defaultDefinition);

    fireEvent.click(screen.getByLabelText("校验工作流"));
    expect(mockedActions.validateWorkflow).toHaveBeenCalledTimes(1);
    expect(mockedActions.validateWorkflow).toHaveBeenCalledWith(
      defaultDefinition
    );

    fireEvent.click(screen.getByLabelText("发布工作流"));
    expect(mockedActions.publishWorkflow).toHaveBeenCalledTimes(1);
    expect(mockedActions.publishWorkflow).toHaveBeenCalledWith(
      defaultDefinition
    );

    fireEvent.click(screen.getByLabelText("运行工作流"));
    expect(mockedActions.runWorkflow).toHaveBeenCalledTimes(1);
    expect(mockedActions.runWorkflow).toHaveBeenCalledWith(defaultDefinition);
  });

  it("calls undo and redo actions on click", () => {
    render(<TopToolbar workflowId="wf-1" />);

    fireEvent.click(screen.getByLabelText("撤销"));
    expect(mockedActions.undo).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("重做"));
    expect(mockedActions.redo).toHaveBeenCalledTimes(1);
  });

  it("calls zoom and fit actions on click", () => {
    render(<TopToolbar workflowId="wf-1" />);

    fireEvent.click(screen.getByLabelText("缩小"));
    expect(mockZoomOut).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("放大"));
    expect(mockZoomIn).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("适配画布"));
    expect(mockFitCanvas).toHaveBeenCalledTimes(1);
  });

  it("calls panel toggle actions on click", () => {
    render(<TopToolbar workflowId="wf-1" />);

    fireEvent.click(screen.getByLabelText("切换左侧面板"));
    expect(mockToggleLeftSidebar).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("切换右侧面板"));
    expect(mockToggleRightSidebar).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByLabelText("切换底部面板"));
    expect(mockToggleBottomPanel).toHaveBeenCalledTimes(1);
  });

  it("calls theme toggle on click", () => {
    render(<TopToolbar workflowId="wf-1" />);

    fireEvent.click(screen.getByLabelText("切换主题"));
    expect(mockToggleTheme).toHaveBeenCalledTimes(1);
  });

  it("does not call primary action handlers when no definition is loaded", () => {
    mockedUseEditor.mockReturnValue(
      createMockEditorValue({ definition: null })
    );
    render(<TopToolbar workflowId="wf-empty" />);

    fireEvent.click(screen.getByLabelText("保存工作流"));
    expect(mockedActions.saveWorkflow).not.toHaveBeenCalled();

    fireEvent.click(screen.getByLabelText("校验工作流"));
    expect(mockedActions.validateWorkflow).not.toHaveBeenCalled();

    fireEvent.click(screen.getByLabelText("发布工作流"));
    expect(mockedActions.publishWorkflow).not.toHaveBeenCalled();

    fireEvent.click(screen.getByLabelText("运行工作流"));
    expect(mockedActions.runWorkflow).not.toHaveBeenCalled();
  });
});
