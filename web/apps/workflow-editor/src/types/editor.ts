import type {
  Diagnostic,
  ExecutionSnapshot,
  WorkflowDef,
  WorkflowEditorMetadata,
} from "@xflow/workflow-core";

import type { ReactNode } from "react";

export interface CatalogItem {
  type: string;
  label: string;
  category: string;
  icon?: ReactNode;
}

export type EditorTheme = "light" | "dark";

export interface PanelVisibility {
  leftSidebarOpen: boolean;
  rightSidebarOpen: boolean;
  bottomPanelOpen: boolean;
}

export type EditorActionHandlers = {
  save: () => void | Promise<void>;
  validate: () => void | Promise<void>;
  publish: () => void | Promise<void>;
  run: () => void | Promise<void>;
  undo: () => void | Promise<void>;
  redo: () => void | Promise<void>;
};

export interface ViewportState {
  x: number;
  y: number;
  zoom: number;
}

export interface EditorState {
  definition: WorkflowDef | null;
  editorMetadata: WorkflowEditorMetadata;
  diagnostics: Diagnostic[];
  executionSnapshot: ExecutionSnapshot | null;
  selectedNodeIds: string[];
  catalogKeyword: string;
  viewport: ViewportState;
  panels: PanelVisibility;
  theme: EditorTheme;
}

export type EditorAction =
  | { type: "SET_DEFINITION"; payload: WorkflowDef }
  | { type: "SET_DIAGNOSTICS"; payload: Diagnostic[] }
  | { type: "SET_EXECUTION_SNAPSHOT"; payload: ExecutionSnapshot | null }
  | { type: "SELECT_NODE"; payload: string }
  | { type: "TOGGLE_NODE_SELECTED"; payload: string }
  | { type: "SET_CATALOG_KEYWORD"; payload: string }
  | { type: "ZOOM_IN" }
  | { type: "ZOOM_OUT" }
  | { type: "FIT_CANVAS" }
  | { type: "TOGGLE_LEFT_SIDEBAR" }
  | { type: "TOGGLE_RIGHT_SIDEBAR" }
  | { type: "TOGGLE_BOTTOM_PANEL" }
  | { type: "SET_VIEWPORT"; payload: ViewportState }
  | { type: "TOGGLE_THEME" };
