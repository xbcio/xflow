export type XFlowEditorAppearance = "light" | "dark" | "system";
export type XFlowEditorThemeVariant = "graphite" | "blueprint";
export type ResolvedXFlowEditorAppearance = Exclude<XFlowEditorAppearance, "system">;

/**
 * The editor palette for each theme variant and appearance. It feeds the AntD
 * ConfigProvider tokens and mirrors the --xflow-* custom properties declared in
 * styles.css; theme.test.ts fails when the two disagree. Not part of the
 * package's public API.
 */
export interface EditorThemePalette {
  app: string;
  border: string;
  borderStrong: string;
  field: string;
  hover: string;
  panel: string;
  panelRaised: string;
  primary: string;
  text: string;
  textSecondary: string;
  textTertiary: string;
  toolbar: string;
  success: string;
  warning: string;
  error: string;
}

export const editorThemePalettes: Record<
  XFlowEditorThemeVariant,
  Record<ResolvedXFlowEditorAppearance, EditorThemePalette>
> = {
  graphite: {
    light: {
      app: "#f5f6f8",
      border: "#d9dee7",
      borderStrong: "#bcc5d0",
      field: "#ffffff",
      hover: "#edf3ff",
      panel: "#ffffff",
      panelRaised: "#f7f8fa",
      primary: "#2f7cff",
      text: "#1f2329",
      textSecondary: "#4e5969",
      textTertiary: "#86909c",
      toolbar: "#ffffff",
      success: "#1a9b67",
      warning: "#c98600",
      error: "#d9363e"
    },
    dark: {
      app: "#151617",
      border: "#3a3d43",
      borderStrong: "#4b5058",
      field: "#1e2023",
      hover: "#30343a",
      panel: "#24262a",
      panelRaised: "#2b2e33",
      primary: "#4c8dff",
      text: "#f2f4f7",
      textSecondary: "#c8cdd5",
      textTertiary: "#a4abb6",
      toolbar: "#1e1f21",
      success: "#35c98b",
      warning: "#f7b955",
      error: "#ff5f57"
    }
  },
  blueprint: {
    light: {
      app: "#f6f8fc",
      border: "#d5dfeb",
      borderStrong: "#b7c7d9",
      field: "#ffffff",
      hover: "#ecf3ff",
      panel: "#ffffff",
      panelRaised: "#f1f5fb",
      primary: "#316dff",
      text: "#1b2b3f",
      textSecondary: "#50647d",
      textTertiary: "#8191a5",
      toolbar: "#ffffff",
      success: "#168b63",
      warning: "#b87700",
      error: "#cf3c45"
    },
    dark: {
      app: "#111827",
      border: "#314158",
      borderStrong: "#465b76",
      field: "#121b28",
      hover: "#24344a",
      panel: "#17202d",
      panelRaised: "#1e2a3a",
      primary: "#5b8cff",
      text: "#e6edf7",
      textSecondary: "#c0cddc",
      textTertiary: "#8fa0b5",
      toolbar: "#141e2d",
      success: "#48c78e",
      warning: "#eab85b",
      error: "#ff6b6b"
    }
  }
};
