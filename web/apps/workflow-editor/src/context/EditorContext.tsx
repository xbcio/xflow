import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useReducer,
} from "react";
import type { ReactNode } from "react";
import type {
  Diagnostic,
  ExecutionSnapshot,
  WorkflowDef,
  WorkflowEditorMetadata,
} from "@xflow/workflow-core";
import type {
  EditorAction,
  EditorState,
  PanelVisibility,
  ViewportState,
} from "../types/editor";

const ZOOM_STEP = 0.1;
const MIN_ZOOM = 0.1;
const MAX_ZOOM = 2.0;

export interface EditorContextValue extends EditorState {
  setDefinition: (definition: WorkflowDef) => void;
  setDiagnostics: (diagnostics: Diagnostic[]) => void;
  setExecutionSnapshot: (snapshot: ExecutionSnapshot | null) => void;
  selectNode: (nodeId: string) => void;
  toggleNodeSelected: (nodeId: string) => void;
  setCatalogKeyword: (keyword: string) => void;
  zoomIn: () => void;
  zoomOut: () => void;
  fitCanvas: () => void;
  toggleLeftSidebar: () => void;
  toggleRightSidebar: () => void;
  toggleBottomPanel: () => void;
}

const defaultPanelVisibility: PanelVisibility = {
  leftSidebarOpen: true,
  rightSidebarOpen: true,
  bottomPanelOpen: false,
};

const defaultViewport: ViewportState = {
  x: 0,
  y: 0,
  zoom: 1,
};

const defaultEditorMetadata: WorkflowEditorMetadata = {
  positions: {},
  viewport: { ...defaultViewport },
  ui: {},
  notes: {},
};

const initialState: EditorState = {
  definition: null,
  editorMetadata: defaultEditorMetadata,
  diagnostics: [],
  executionSnapshot: null,
  selectedNodeIds: [],
  catalogKeyword: "",
  viewport: { ...defaultViewport },
  panels: { ...defaultPanelVisibility },
};

function clampZoom(zoom: number): number {
  return Math.min(Math.max(zoom, MIN_ZOOM), MAX_ZOOM);
}

function editorReducer(state: EditorState, action: EditorAction): EditorState {
  switch (action.type) {
    case "SET_DEFINITION":
      return { ...state, definition: action.payload };

    case "SET_DIAGNOSTICS":
      return { ...state, diagnostics: action.payload };

    case "SET_EXECUTION_SNAPSHOT":
      return { ...state, executionSnapshot: action.payload };

    case "SELECT_NODE":
      return { ...state, selectedNodeIds: [action.payload] };

    case "TOGGLE_NODE_SELECTED": {
      const nodeId = action.payload;
      const selected = new Set(state.selectedNodeIds);
      if (selected.has(nodeId)) {
        selected.delete(nodeId);
      } else {
        selected.add(nodeId);
      }
      return { ...state, selectedNodeIds: Array.from(selected) };
    }

    case "SET_CATALOG_KEYWORD":
      return { ...state, catalogKeyword: action.payload };

    case "ZOOM_IN":
      return {
        ...state,
        viewport: {
          ...state.viewport,
          zoom: clampZoom(state.viewport.zoom + ZOOM_STEP),
        },
      };

    case "ZOOM_OUT":
      return {
        ...state,
        viewport: {
          ...state.viewport,
          zoom: clampZoom(state.viewport.zoom - ZOOM_STEP),
        },
      };

    case "FIT_CANVAS":
      return {
        ...state,
        viewport: { ...defaultViewport },
      };

    case "TOGGLE_LEFT_SIDEBAR":
      return {
        ...state,
        panels: {
          ...state.panels,
          leftSidebarOpen: !state.panels.leftSidebarOpen,
        },
      };

    case "TOGGLE_RIGHT_SIDEBAR":
      return {
        ...state,
        panels: {
          ...state.panels,
          rightSidebarOpen: !state.panels.rightSidebarOpen,
        },
      };

    case "TOGGLE_BOTTOM_PANEL":
      return {
        ...state,
        panels: {
          ...state.panels,
          bottomPanelOpen: !state.panels.bottomPanelOpen,
        },
      };

    case "SET_VIEWPORT":
      return {
        ...state,
        viewport: {
          ...action.payload,
          zoom: clampZoom(action.payload.zoom),
        },
      };

    default:
      return state;
  }
}

const EditorContext = createContext<EditorContextValue | null>(null);

export interface EditorProviderProps {
  children: ReactNode;
  initialDefinition?: WorkflowDef | null;
}

export function EditorProvider({
  children,
  initialDefinition = null,
}: EditorProviderProps) {
  const [state, dispatch] = useReducer(editorReducer, {
    ...initialState,
    definition: initialDefinition,
  });

  const setDefinition = useCallback((definition: WorkflowDef) => {
    dispatch({ type: "SET_DEFINITION", payload: definition });
  }, []);

  const setDiagnostics = useCallback((diagnostics: Diagnostic[]) => {
    dispatch({ type: "SET_DIAGNOSTICS", payload: diagnostics });
  }, []);

  const setExecutionSnapshot = useCallback(
    (snapshot: ExecutionSnapshot | null) => {
      dispatch({ type: "SET_EXECUTION_SNAPSHOT", payload: snapshot });
    },
    []
  );

  const selectNode = useCallback((nodeId: string) => {
    dispatch({ type: "SELECT_NODE", payload: nodeId });
  }, []);

  const toggleNodeSelected = useCallback((nodeId: string) => {
    dispatch({ type: "TOGGLE_NODE_SELECTED", payload: nodeId });
  }, []);

  const setCatalogKeyword = useCallback((keyword: string) => {
    dispatch({ type: "SET_CATALOG_KEYWORD", payload: keyword });
  }, []);

  const zoomIn = useCallback(() => {
    dispatch({ type: "ZOOM_IN" });
  }, []);

  const zoomOut = useCallback(() => {
    dispatch({ type: "ZOOM_OUT" });
  }, []);

  const fitCanvas = useCallback(() => {
    dispatch({ type: "FIT_CANVAS" });
  }, []);

  const toggleLeftSidebar = useCallback(() => {
    dispatch({ type: "TOGGLE_LEFT_SIDEBAR" });
  }, []);

  const toggleRightSidebar = useCallback(() => {
    dispatch({ type: "TOGGLE_RIGHT_SIDEBAR" });
  }, []);

  const toggleBottomPanel = useCallback(() => {
    dispatch({ type: "TOGGLE_BOTTOM_PANEL" });
  }, []);

  const value = useMemo<EditorContextValue>(
    () => ({
      ...state,
      setDefinition,
      setDiagnostics,
      setExecutionSnapshot,
      selectNode,
      toggleNodeSelected,
      setCatalogKeyword,
      zoomIn,
      zoomOut,
      fitCanvas,
      toggleLeftSidebar,
      toggleRightSidebar,
      toggleBottomPanel,
    }),
    [
      state,
      setDefinition,
      setDiagnostics,
      setExecutionSnapshot,
      selectNode,
      toggleNodeSelected,
      setCatalogKeyword,
      zoomIn,
      zoomOut,
      fitCanvas,
      toggleLeftSidebar,
      toggleRightSidebar,
      toggleBottomPanel,
    ]
  );

  return (
    <EditorContext.Provider value={value}>{children}</EditorContext.Provider>
  );
}

export function useEditor(): EditorContextValue {
  const context = useContext(EditorContext);
  if (!context) {
    throw new Error("useEditor must be used within an EditorProvider");
  }
  return context;
}
