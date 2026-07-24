/** @type {import('tailwindcss').Config} */
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        "editor-bg": "var(--editor-bg, #f4f5f7)",
        "editor-surface": "var(--editor-surface, #ffffff)",
        "editor-panel": "var(--editor-panel-bg, #ffffff)",
        "editor-border": "var(--editor-border-color, #e5e7eb)",
        "editor-text": "var(--editor-text-primary, #1a1a1a)",
        "editor-text-secondary": "var(--editor-text-secondary, #6b7280)",
        "editor-muted": "var(--editor-muted, #a0a0a0)",
        "editor-accent": "var(--editor-accent, #2563eb)",
        "editor-hover": "var(--editor-hover, #f3f4f6)",
        "editor-input": "var(--editor-input-bg, #f9fafb)",
        "editor-canvas": "var(--editor-canvas-bg, #ffffff)",
        "editor-grid": "var(--editor-grid-line, #f0f0f0)",
        "editor-ruler-bg": "var(--editor-ruler-bg, #e5e7eb)",
        "editor-ruler-tick": "var(--editor-ruler-tick, #c4c4c4)",
        "editor-ruler-tick-major": "var(--editor-ruler-tick-major, #9ca3af)",
        "editor-ruler-text": "var(--editor-ruler-text, #6b7280)",
        "editor-selected-bg": "var(--editor-selected-bg, #dbeafe)",
        "editor-selected-text": "var(--editor-selected-text, #2563eb)",
      },
      height: {
        toolbar: "var(--editor-toolbar-height, 48px)",
        "bottom-panel": "var(--editor-bottom-panel-height, 200px)",
      },
      width: {
        "left-sidebar": "var(--editor-left-sidebar-width, 240px)",
        "right-sidebar": "var(--editor-right-sidebar-width, 280px)",
      },
    },
  },
  plugins: [],
};
