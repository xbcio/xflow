/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        "editor-canvas": "var(--editor-canvas-bg, #f4f5f7)",
        "editor-sidebar": "var(--editor-sidebar-bg, #ffffff)",
        "editor-panel": "var(--editor-panel-bg, #ffffff)",
        "editor-border": "var(--editor-border-color, #d1d5db)",
        "editor-text": "var(--editor-text-primary, #1a1a1a)",
        "editor-text-secondary": "var(--editor-text-secondary, #6b7280)",
        "editor-accent": "var(--editor-accent, #2563eb)",
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
